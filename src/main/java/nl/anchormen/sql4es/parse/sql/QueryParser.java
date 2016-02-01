package nl.anchormen.sql4es.parse.sql;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SortItem;

import nl.anchormen.sql4es.QueryState;
import nl.anchormen.sql4es.model.BasicQueryState;
import nl.anchormen.sql4es.model.Column;
import nl.anchormen.sql4es.model.Heading;
import nl.anchormen.sql4es.model.OrderBy;
import nl.anchormen.sql4es.model.Utils;
import nl.anchormen.sql4es.model.expression.IComparison;

/**
 * Interprets the parsed query and build the appropriate ES query (a {@link SearchRequestBuilder} instance). 
 * The other parses within this package are used to parse their speicific clause (WHERE, HAVING etc)
 *  
 * @author cversloot
 *
 */
public class QueryParser extends AstVisitor<Object[], SearchRequestBuilder>{
	
	private final static SelectParser selectParser = new SelectParser();
	private final static WhereParser whereParser = new WhereParser();
	private final static HavingParser havingParser = new HavingParser();
	private final static RelationParser relationParser = new RelationParser();
	private final static GroupParser groupParser = new GroupParser();
	
	private String sql;
	private int maxRows = -1;
	private Heading heading = new Heading();
	private Properties props;
	private Map<String, Map<String, Integer>> tableColumnInfo;
	
	/**
	 * Builds the provided {@link SearchRequestBuilder} by parsing the {@link Query} using the properties provided.
	 * @param sql the original sql statement
	 * @param queryBody the Query parsed from the sql
	 * @param searchReq the request to build from
	 * @param props a set of properties to use in certain cases
	 * @param tableColumnInfo mapping from available tables to columns and their typesd
	 * @return an array containing [ {@link Heading}, {@link IComparison} having, List&lt;{@link OrderBy}&gt; orderings, Integer limit]
	 * @throws SQLException
	 */
	public Object[] parse(String sql, QueryBody queryBody, int maxRows, SearchRequestBuilder searchReq, 
			Properties props, Map<String, Map<String, Integer>> tableColumnInfo) throws SQLException{
		this.sql = sql.replace("\r", " ").replace("\n", " ");;
		this.props = props;
		this.maxRows = maxRows;
		this.tableColumnInfo = tableColumnInfo;
		
		if(queryBody instanceof QuerySpecification){
			Object[] result = queryBody.accept(this, searchReq);
			if(result.length > 0 && result[0] instanceof QueryState ) throw ((QueryState)result[0]).getException();
			else if (result.length < 4) throw new SQLException("Failed to parse query due to unknown reason");
			return result;
		}
		throw new SQLException("The provided query does not contain a QueryBody");
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	protected Object[] visitQuerySpecification(QuerySpecification node, SearchRequestBuilder searchReq){
		this.heading = new Heading();
		QueryState state = new BasicQueryState(sql, heading, props);
		int limit = -1;
		List<String> relations = new ArrayList<String>();
		AggregationBuilder aggregation = null;
		QueryBuilder query = null;
		IComparison having = null;
		List<OrderBy> orderings = new ArrayList<OrderBy>();
		boolean useCache = false;
		
		// check for distinct in combination with group by
		if(node.getSelect().isDistinct() && !node.getGroupBy().isEmpty()){
			state.addException("Unable to combine DISTINCT and GROUP BY within a single query");
			return new Object[]{state};
		};
		
		// get limit (possibly used by other parsers)
		if(node.getLimit().isPresent()){
			limit = Integer.parseInt(node.getLimit().get());
		}
		if(state.hasException()) return new Object[]{state};
		
		//req.getHeading().fixAliases(req.originalSql());
		if(node.getFrom().isPresent()){
			relations = node.getFrom().get().accept(relationParser, state);
			if(state.hasException()) return new Object[]{state};
			if(relations.size() < 1) {
				state.addException("Specify atleast one valid table to execute the query on!");
				return new Object[]{state};
			}
			for(int i=0; i<relations.size(); i++){
				if(relations.get(i).toLowerCase().equals(props.getProperty(Utils.PROP_QUERY_CACHE_TABLE, "query_cache"))){
					useCache = true;
					relations.remove(i);
					i--;
				}
			}
			heading.setTypes(this.typesForColumns(relations));
		}
		
		// get columns to fetch (builds the header)
		for(SelectItem si : node.getSelect().getSelectItems()){
			si.accept(selectParser, state);
		}
		if(state.hasException()) return new Object[]{state};
		boolean requestScore = heading.hasLabel("_score");
		
		
		// Translate column references and their aliases back to their case sensitive forms
		heading.reorderAndFixColumns(this.sql, "select.+", ".+from");
		heading.setTypes(this.typesForColumns(relations));
		
		// create aggregation in case of DISTINCT
		if(node.getSelect().isDistinct()){
			aggregation = groupParser.addDistinctAggregation(state);
		}

		// add a Query
		query = QueryBuilders.matchAllQuery();
		if(node.getWhere().isPresent()){
			query = node.getWhere().get().accept(whereParser, state);
		}
		if(state.hasException()) return new Object[]{state};
		
		// parse group by and create aggregations accordingly
		if(node.getGroupBy() != null && node.getGroupBy().size() > 0){
			aggregation = groupParser.parse(node.getGroupBy(), state);
		}else if(heading.aggregateOnly()){
			aggregation = groupParser.buildFilterAggregation(query, heading);
		}
		if(state.hasException()) return new Object[]{state};
		
		// parse Having (is executed client side after results have been fetched)
		if(node.getHaving().isPresent()){
			having = node.getHaving().get().accept(havingParser, state);
		}

		// parse ORDER BY
		if(!node.getOrderBy().isEmpty()){
			for(SortItem si : node.getOrderBy()){
				String orderKey;
				if(si.getSortKey() instanceof DereferenceExpression){
					orderKey = SelectParser.visitDereferenceExpression((DereferenceExpression)si.getSortKey());
				}else if (si.getSortKey() instanceof FunctionCall){
					orderKey = si.getSortKey().toString()
							.replaceAll("\"","").replaceAll("\\*", "\\\\*")
							.replaceAll("\\(", "\\s*\\\\(\\s*").replaceAll("\\)", "\\s*\\\\)\\s*");
				}else {
					orderKey = ((QualifiedNameReference)si.getSortKey()).getName().toString();
				}
				orderKey = Heading.findOriginal(state.originalSql()+";", orderKey, "order by.+", "\\W");
				Column column = heading.getColumnByLabel(orderKey);
				if(column != null){
					if(si.getOrdering().toString().startsWith("ASC")){
						orderings.add(new OrderBy(column.getColumn(), SortOrder.ASC, column.getIndex()));
					}else{
						orderings.add(new OrderBy(column.getColumn(), SortOrder.DESC, column.getIndex()));
					}
				}else{
					state.addException("Order key '"+orderKey+"' is not specified in SELECT clause");
				}
			}
		}
		if(state.hasException()) return new Object[]{state};
		
		buildQuery(searchReq, heading, relations, query, aggregation, having, orderings, limit, useCache, requestScore) ;
		return new Object[]{heading, having, orderings, limit};
	}

	/**
	 * Builds the actual Elasticsearch request using all the information provided
	 * @param searchReq
	 * @param heading
	 * @param relations
	 * @param query
	 * @param aggregation
	 * @param having
	 * @param orderings
	 * @param limit
	 * @param useCache
	 */
	@SuppressWarnings("rawtypes")
	private void buildQuery(SearchRequestBuilder searchReq, Heading heading, List<String> relations,
			QueryBuilder query, AggregationBuilder aggregation, IComparison having, List<OrderBy> orderings,
			int limit, boolean useCache, boolean requestScore) {
		String[] types = new String[relations.size()];
		SearchRequestBuilder req = searchReq.setTypes(relations.toArray(types));
		
		// add filters and aggregations
		if(aggregation != null){
			// when aggregating the query must be a query and not a filter
			if(query != null)	req.setQuery(query);
			req.addAggregation(aggregation);
			
		// ordering does not work on aggregations (has to be done in client)
		}else if(query != null){
			if(requestScore) req.setQuery(query); // use query instead of filter to get a score
			else req.setPostFilter(query);
			
			// add order
			for(OrderBy ob : orderings){
				req.addSort(ob.getField(), ob.getOrder());
			}
		} else req.setQuery(QueryBuilders.matchAllQuery());
		
		int fetchSize = Utils.getIntProp(props, Utils.PROP_FETCH_SIZE, 1000);
		// add limit and determine to use scroll
		if(aggregation != null) {
			req = req.setSize(0);
		} else if(determineLimit(limit) > 0 && determineLimit(limit)  < fetchSize){
			req.setSize(determineLimit(limit) );
		} else if (orderings.isEmpty()){ // scrolling does not work well with sort
			req.setSize(fetchSize/5); // scanning results in 5 * size results... huh!?!
			req.setSearchType(SearchType.SCAN);
			req.setScroll(new TimeValue(Utils.getIntProp(props, Utils.PROP_SCROLL_TIMEOUT_SEC, 60)));
		}
		
		// use query cache when this was indicated in FROM clause
		if(useCache) req.setRequestCache(true);
		req.setTimeout(TimeValue.timeValueMillis(Utils.getIntProp(props, Utils.PROP_QUERY_TIMEOUT_MS, 10000)));
	}

	/**
	 * Gets SQL column types for the provided tables as a map from colname->java.sql.Types
	 * @param tables
	 * @return
	 */
	public Map<String, Integer> typesForColumns(List<String> tables){
		HashMap<String, Integer> colType = new HashMap<String, Integer>();
		colType.put(Heading.ID, Types.VARCHAR);
		colType.put(Heading.TYPE, Types.VARCHAR);
		colType.put(Heading.INDEX, Types.VARCHAR);
		for(String table : tables){
			if(!tableColumnInfo.containsKey(table)) continue;
			colType.putAll( tableColumnInfo.get(table) );
		}
		return colType;
	}

	/**
	 * Gets a property from the connection
	 * @param name
	 * @return
	 */
	/*
	public Object getProperty(String name){
		return this.props.get(name);
	}
	*/
	public int determineLimit(int limit){
		if(limit <= -1 ) return this.maxRows;
		if(maxRows <= -1) return limit;
		return Math.min(limit, maxRows);
	}

}
