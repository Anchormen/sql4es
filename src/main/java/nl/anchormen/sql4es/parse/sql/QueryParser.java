package nl.anchormen.sql4es.parse.sql;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SortItem;

import nl.anchormen.sql4es.model.BasicQueryState;
import nl.anchormen.sql4es.model.Heading;
import nl.anchormen.sql4es.model.OrderBy;
import nl.anchormen.sql4es.model.QuerySource;
import nl.anchormen.sql4es.model.Utils;
import nl.anchormen.sql4es.model.expression.IComparison;

/**
 * Interprets the parsed query and build the appropriate ES query (a {@link SearchRequestBuilder} instance). 
 * The other parses within this package are used to parse their speicific clause (WHERE, HAVING etc)
 *  
 * @author cversloot
 *
 */
public class QueryParser extends AstVisitor<ParseResult, Object>{
	
	private final static SelectParser selectParser = new SelectParser();
	private final static WhereParser whereParser = new WhereParser();
	private final static HavingParser havingParser = new HavingParser();
	private final static RelationParser relationParser = new RelationParser();
	private final static GroupParser groupParser = new GroupParser();
	private final static OrderByParser orderOarser = new OrderByParser();
	
	private String sql;
	private int maxRows = -1;
	private Heading heading = new Heading();
	private Properties props;
	private Map<String, Map<String, Integer>> tableColumnInfo;
	
	/**
	 * Builds the provided {@link SearchRequestBuilder} by parsing the {@link Query} using the properties provided.
	 * @param sql the original sql statement
	 * @param queryBody the Query parsed from the sql
	 * @param searchReq the request to build
	 * @param props a set of properties to use in certain cases
	 * @param tableColumnInfo mapping from available tables to columns and their typesd
	 * @return an array containing [ {@link Heading}, {@link IComparison} having, List&lt;{@link OrderBy}&gt; orderings, Integer limit]
	 * @throws SQLException
	 */
	public ParseResult parse(String sql, QueryBody queryBody, int maxRows, 
			Properties props, Map<String, Map<String, Integer>> tableColumnInfo) throws SQLException{
		this.sql = sql.replace("\r", " ").replace("\n", " ");// TODO: this removes linefeeds from string literals as well!
		this.props = props;
		this.maxRows = maxRows;
		this.tableColumnInfo = tableColumnInfo;
		
		if(queryBody instanceof QuerySpecification){
			ParseResult result = queryBody.accept(this, null);
			if(result.getException() != null) throw result.getException();
			return result;
		}
		throw new SQLException("The provided query does not contain a QueryBody");
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	protected ParseResult visitQuerySpecification(QuerySpecification node, Object obj){
		this.heading = new Heading();
		BasicQueryState state = new BasicQueryState(sql, heading, props);
		int limit = -1;
		AggregationBuilder aggregation = null;
		QueryBuilder query = null;
		IComparison having = null;
		List<OrderBy> orderings = new ArrayList<OrderBy>();
		boolean useCache = false;
		
		// check for distinct in combination with group by
		if(node.getSelect().isDistinct() && !node.getGroupBy().isEmpty()){
			state.addException("Unable to combine DISTINCT and GROUP BY within a single query");
			return new ParseResult(state.getException());
		};
		
		// get limit (possibly used by other parsers)
		if(node.getLimit().isPresent()){
			limit = Integer.parseInt(node.getLimit().get());
		}
		if(state.hasException()) return new ParseResult(state.getException());
		
		// get sources to fetch data from
		if(node.getFrom().isPresent()){
			useCache = getSources(node.getFrom().get(), state);
		}
		
		// get columns to fetch (builds the header)
		for(SelectItem si : node.getSelect().getSelectItems()){
			si.accept(selectParser, state);
		}
		if(state.hasException()) return new ParseResult(state.getException());
		boolean requestScore = heading.hasLabel("_score");
		
		// Translate column references and their aliases back to their case sensitive forms
		heading.reorderAndFixColumns(this.sql, "select.+", ".+from");
		//heading.setTypes(this.typesForColumns(state.getSources()));
		
		// create aggregation in case of DISTINCT
		if(node.getSelect().isDistinct()){
			aggregation = groupParser.addDistinctAggregation(state);
		}

		// add a Query
		query = QueryBuilders.matchAllQuery();
		if(node.getWhere().isPresent()){
			query = node.getWhere().get().accept(whereParser, state);
		}
		if(state.hasException()) return new ParseResult(state.getException());
		
		// parse group by and create aggregations accordingly
		if(node.getGroupBy() != null && node.getGroupBy().size() > 0){
			aggregation = groupParser.parse(node.getGroupBy(), state);
		}else if(heading.aggregateOnly()){
			aggregation = groupParser.buildFilterAggregation(query, heading);
		}
		if(state.hasException()) return new ParseResult(state.getException());
		
		// parse Having (is executed client side after results have been fetched)
		if(node.getHaving().isPresent()){
			having = node.getHaving().get().accept(havingParser, state);
		}

		// parse ORDER BY
		if(!node.getOrderBy().isEmpty()){
			for(SortItem si : node.getOrderBy()){
				OrderBy ob = si.accept(orderOarser, state);
				if(state.hasException()) return new ParseResult(state.getException());
				orderings.add(ob);
			}
		}
		if(state.hasException()) return new ParseResult(state.getException());
		
		//buildQuery(searchReq, heading, state.getSources(), query, aggregation, having, orderings, limit, useCache, requestScore) ;
		return new ParseResult(heading, state.getSources(), query, aggregation, having, orderings, limit, useCache, requestScore);
	}

	/**
	 * Gets the sources to query from the provided Relation. Parsed relations are put inside the state
	 * @param relation
	 * @param state
	 * @param searchReq
	 * @return if the set with relations contains the query cache identifier
	 */
	private boolean getSources(Relation relation, BasicQueryState state){
		List<QuerySource> sources = relation.accept(relationParser, state);
		boolean useCache = false;
		if(state.hasException()) return false;
		if(sources.size() < 1) {
			state.addException("Specify atleast one valid table to execute the query on!");
			return false;
		}
		for(int i=0; i<sources.size(); i++){
			if(sources.get(i).getSource().toLowerCase().equals(props.getProperty(Utils.PROP_QUERY_CACHE_TABLE, "query_cache"))){
				useCache = true;
				sources.remove(i);
				i--;
			}else if(sources.get(i).isSubQuery()){
				QuerySource qs = sources.get(i);
				QueryParser subQueryParser = new QueryParser();
				try {
					subQueryParser.parse(qs.getSource(), qs.getQuery(), maxRows, props, tableColumnInfo);
				} catch (SQLException e) {
					state.addException("Unable to parse sub-query due to: "+e.getMessage());
				}
				sources.remove(i);
				i--;
			}
		}
		heading.setTypes(this.typesForColumns(sources));
		state.setRelations(sources);
		return useCache;
	}

	/**
	 * Gets SQL column types for the provided tables as a map from colname to java.sql.Types
	 * @param tables
	 * @return
	 */
	public Map<String, Integer> typesForColumns(List<QuerySource> relations){
		HashMap<String, Integer> colType = new HashMap<String, Integer>();
		colType.put(Heading.ID, Types.VARCHAR);
		colType.put(Heading.TYPE, Types.VARCHAR);
		colType.put(Heading.INDEX, Types.VARCHAR);
		for(QuerySource table : relations){
			if(!tableColumnInfo.containsKey(table.getSource())) continue;
			colType.putAll( tableColumnInfo.get(table.getSource()) );
		}
		return colType;
	}

}
