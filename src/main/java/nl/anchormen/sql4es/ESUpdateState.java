package nl.anchormen.sql4es;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.query.QueryBuilder;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Delete;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.DropView;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.TableElement;
import com.facebook.presto.sql.tree.Values;

import nl.anchormen.sql4es.model.BasicQueryState;
import nl.anchormen.sql4es.model.Column;
import nl.anchormen.sql4es.model.Heading;
import nl.anchormen.sql4es.parse.sql.RelationParser;
import nl.anchormen.sql4es.parse.sql.SelectParser;
import nl.anchormen.sql4es.parse.sql.UpdateParser;
import nl.anchormen.sql4es.parse.sql.WhereParser;

public class ESUpdateState {

	private final UpdateParser updateParser = new UpdateParser();
	private Client client;
	private Properties props;
	private List<String> bulkList = new ArrayList<String>();
	private ESQueryState queryState;
	private Statement statement;
	
	public ESUpdateState(Client client, Statement statement) throws SQLException{
		this.client = client;
		this.props = statement.getConnection().getClientInfo();
		this.statement = statement;
		this.queryState = new ESQueryState(client, statement);
	}

	/**
	 * Parses the given name to extract the type and optionally the index in format (index.)type.
	 * The case sensitive name is extracted from the sql using the provided prefix and suffix.	 
	 * @param name
	 * @param sql
	 * @param prefix
	 * @param suffix
	 * @param defaultIndex
	 * @return array {index, type} where index may be the default provided 
	 */
	private String[] getIndexAndType(String name, String sql, String prefix, String suffix, String defaultIndex){
		String target = Heading.findOriginal(sql.trim()+";", name, prefix, suffix);
		int idx = target.indexOf('.');
		if(idx > 0){
			return new String[]{target.substring(0, idx), target.substring(idx+1)};
		}
		return new String[]{defaultIndex, target};
	}
	
	/**
	 * Parses and executes the provided insert statement and returns 1 if execution was successful
	 * @param sql
	 * @param insert
	 * @param index
	 * @return the number of executed inserts
	 * @throws SQLException
	 */
	public int execute(String sql, Insert insert, String index) throws SQLException{
		if(insert.getQuery().getQueryBody() instanceof Values){
			// parse one or multiple value sets (... VALUES (1,2,'a'), (2,4,'b'), ...)
			List<IndexRequestBuilder> requests = this.insertValues(sql, insert, index);
			return execute(requests, 2500);
		}else if(insert.getQuery().getQueryBody() instanceof QuerySpecification){
			// insert data based on a SELECT statement
			List<IndexRequestBuilder> requests = this.requestsForInsert(sql, insert, index);
			return execute(requests, 2500);
		}else throw new SQLException("Unknown set of values to insert ("+insert.getQuery().getQueryBody()+")");
		
	}
	
	/**
	 * Executes the list with requests as a bulk with maximum number of requests per bulk
	 * @param requests
	 * @param maxRequestsPerBulk
	 * @return
	 * @throws SQLException
	 */
	private int execute(List<?> requests, int maxRequestsPerBulk) throws SQLException{
		int result = 0;
		BulkRequestBuilder bulkReq = client.prepareBulk();
		for(Object req : requests){
			if(req instanceof IndexRequest)	bulkReq.add((IndexRequest)req);
			else if(req instanceof UpdateRequest) bulkReq.add((UpdateRequest)req);
			else if(req instanceof DeleteRequest) bulkReq.add((DeleteRequest)req);
			else if(req instanceof IndexRequestBuilder) bulkReq.add((IndexRequestBuilder)req);
			else if(req instanceof UpdateRequestBuilder) bulkReq.add((UpdateRequestBuilder)req);
			else if(req instanceof DeleteRequestBuilder) bulkReq.add((DeleteRequestBuilder)req);
			else throw new SQLException("Type "+req.getClass()+" cannot be added to a bulk request");
			
			if(bulkReq.numberOfActions() > maxRequestsPerBulk){
				result += bulkReq.get().getItems().length;
				bulkReq = client.prepareBulk();
			}
		}
		
		if(bulkReq.numberOfActions() > 0){
			result += bulkReq.get().getItems().length;
		}
		return result;
	}
	
	
	/**
	 * Creates a set of indexrequests based on the result of a query
	 * @param sql
	 * @param insert
	 * @param index
	 * @return
	 * @throws SQLException
	 */
	private List<IndexRequestBuilder> requestsForInsert(String sql, Insert insert, String index) throws SQLException {
		queryState.buildRequest(sql, insert.getQuery().getQueryBody(), index);
		String[] indexAndType = this.getIndexAndType(insert.getTarget().toString(), sql, "into\\s+", "\\s+select", index);
		index = indexAndType[0];
		String type = indexAndType[1];
		
		// execute query using nested resultsets
		ResultSet rs = queryState.execute(false);
		Heading headingToInsert = this.headingFromResultSet(rs.getMetaData());

		// read the resultset (recursively if nested)
		HashMap<String, Object> fieldValues = new HashMap<String, Object>();
		List<IndexRequestBuilder> indexReqs = new ArrayList<IndexRequestBuilder>();
		while(rs != null){
			while(rs.next()){
				for(Column col : headingToInsert.columns()){
					String label = col.getLabel();
					Object value = rs.getObject(label);
					if(value == null) continue;
					if(value instanceof ResultSet){
						value = buildSource((ResultSet)value);
					}else if(value instanceof Array){
						Object[] arrayVal = (Object[])((Array)value).getArray();
						if(arrayVal.length > 0 && arrayVal[0] instanceof ResultSet){
							for(int i=0; i<arrayVal.length; i++){
								arrayVal[i] = buildSource((ResultSet)arrayVal[i]); 
							}
						}
						value = arrayVal;
					}
					fieldValues.put(label, value);
				}
				IndexRequestBuilder indexReq = client.prepareIndex().setIndex(index)
						.setType(type)
						.setSource(fieldValues);
				indexReqs.add(indexReq);
				fieldValues = new HashMap<String, Object>();
			}
			rs.close();
			rs = queryState.moreResutls();
		}
		return indexReqs;
	}
	
	/**
	 * creates a set of index requests based on a set of explicit VALUES  
	 * @param insert
	 * @param index
	 * @return
	 * @throws SQLException
	 */
	private List<IndexRequestBuilder> insertValues(String sql, Insert insert, String index) throws SQLException {
		Heading heading = new Heading();
		QueryState state = new BasicQueryState(sql, heading, this.props);
		List<Object> values = updateParser.parse(insert, state);
		if(state.hasException()) throw state.getException();
		if(heading.hasLabel("_index") || heading.hasLabel("_type")) throw new SQLException("Not possible to set _index and _type fields");

		String[] indexAndType = this.getIndexAndType(insert.getTarget().toString(), sql, "into\\s+", "\\s+values", index);
		index = indexAndType[0];
		String type = indexAndType[1];
		
		if(values.size() % heading.getColumnCount() != 0) throw new SQLException("Number of columns does not match number of values for one of the inserts");
		HashMap<String, Object> fieldValues = new HashMap<String, Object>(heading.getColumnCount());
		String id = null;
		List<IndexRequestBuilder> indexReqs = new ArrayList<IndexRequestBuilder>();
		for(int i=0; i<values.size(); i++){
			Object value = values.get(i);
			Column col = heading.getColumn(i%heading.getColumnCount());
			if(col.getColumn().equals("_id")){
				id = value.toString();
			}else{
				fieldValues.put(col.getColumn(), value);
			}
			
			if(fieldValues.size() == (id == null ? heading.getColumnCount() : heading.getColumnCount() -1)){
				IndexRequestBuilder indexReq = client.prepareIndex().setIndex(index).setType(type);
				if(id != null) indexReq.setId(id);
				indexReq.setSource(fieldValues);
				indexReqs.add(indexReq);
				id = null;
				fieldValues = new HashMap<String, Object>(heading.getColumnCount());
			}
		}
		return indexReqs;
	}

	/**
	 * Converts a ResultSet into a (nested) Map to be used as a source within an Index operation.
	 * @param rs
	 * @return
	 * @throws SQLException
	 */
	private  Map<String, Object> buildSource(ResultSet rs) throws SQLException{
		Heading heading = this.headingFromResultSet(rs.getMetaData());
		HashMap<String, Object> source = new HashMap<String, Object>();
		while(rs.next()){
			for(Column col : heading.columns()){
				String label = col.getLabel();
				Object value = rs.getObject(label);
				if(value == null) continue;
				if(value instanceof ResultSet){
					value = buildSource((ResultSet)value);
				}else if(value instanceof Array){
					Object[] arrayVal = (Object[])((Array)value).getArray();
					if(arrayVal.length > 0 && arrayVal[0] instanceof ResultSet){
						for(int i=0; i<arrayVal.length; i++){
							arrayVal[i] = buildSource((ResultSet)arrayVal[i]); 
						}
					}
				}
				source.put(label, value);
			}
		}
		if(source.size() == 0) return null;
		return source;
	}
	
	/**
	 * Builds a Heading object from a {@link ResultSetMetaData}
	 * @param rsm
	 * @return
	 * @throws SQLException
	 */
	private Heading headingFromResultSet(ResultSetMetaData rsm) throws SQLException{
		Heading heading = new Heading();
		for(int i=1; i<=rsm.getColumnCount(); i++){
			String column = rsm.getColumnLabel(i);
			if(!column.equals("_id") && !column.equals("_index") && !column.equals("_type")){ 
				heading.add(new Column(column, heading.getColumnCount()));
			}
		}
		return heading;
	}
	
	/**
	 * Adds the provided sql (must be an INDEX or DELETE) to the bulk being held by this state
	 * @param sql
	 * @param index
	 * @throws SQLException
	 */
	public void addToBulk(String sql, String index) throws SQLException{
		String sqlNorm = sql.trim().toLowerCase();
		if(sqlNorm.startsWith("select")) throw new SQLException("It is not possible to add a SELECT statement to a bulk");
		this.bulkList.add(sql);
	}
	
	/**
	 * Executes the {@link BulkRequest} being hold by this state.
	 * @return an integer indicator for each executed request: Statement.SUCCESS_NO_INFO for success, 
	 * else Statement.EXECUTE_FAILED)
	 */
	public int[] executeBulk(){
		int[] result = new int[bulkList.size()];
		SqlParser parser = new SqlParser();
		for(int i=0; i<bulkList.size(); i++) try{
			String sql = bulkList.get(i);
			com.facebook.presto.sql.tree.Statement st = parser.createStatement(sql);
			if(st instanceof DropTable){
				this.execute(sql, (DropTable)st);
			}else if(st instanceof DropView){
				this.execute(sql, (DropView)st);
			}else if(st instanceof CreateTable){
				this.execute(sql, (CreateTable)st, this.statement.getConnection().getSchema());
			}else if(st instanceof CreateTableAsSelect){
				this.execute(sql, (CreateTableAsSelect)st, this.statement.getConnection().getSchema());
			}else if(st instanceof CreateView){
				this.execute(sql, (CreateView)st, this.statement.getConnection().getSchema());
			}else if(st instanceof Delete){
				this.execute(sql, (Delete)st, this.statement.getConnection().getSchema());
			}else  if(st instanceof Insert){
				this.execute(sql, (Insert)st, this.statement.getConnection().getSchema());
			}
			result[i]= Statement.SUCCESS_NO_INFO;
		}catch (Exception e){
			result[i] = Statement.EXECUTE_FAILED;
		}
		this.clearBulk();
		return result;
	}
	
	/**
	 * Clears the {@link BulkRequest} held by this state.
	 */
	public void clearBulk(){
		bulkList.clear();
	}

	// ---------------------------------------[ DELETE ]-----------------------------------------
	/**
	 * Deletes documents from elasticsearch using the predicate provided in the query. This request is 
	 * executed in atleast two steps:
	 * <ol>
	 * <li>fetch document _id's that match the query</li>
	 * <li>add deletion of each id to a bulk request</li>
	 * <li>execute bulk request when it contains 2500 items (and continue)</li>
	 * <li>execute bulk when all _id's have been added</li>
	 * </ol>
	 * @param sql
	 * @param delete
	 * @param index
	 * @return
	 * @throws SQLException
	 */
	public int execute(String sql, Delete delete, String index) throws SQLException {
		List<DeleteRequestBuilder> requests = requestsForDelete(sql, delete, index);
		return this.execute(requests, 2500);
	}
	
	private List<DeleteRequestBuilder> requestsForDelete(String sql, Delete delete, String index) throws SQLException{
		String type = delete.getTable().getName().toString();
		String select = "SELECT _id FROM "+type;
		if(delete.getWhere().isPresent()){
			select += sql.substring(sql.toLowerCase().indexOf(" where "));
		}
		
		Query query = (Query)new SqlParser().createStatement(select);
		this.queryState.buildRequest(select, query.getQueryBody(), index);
		ResultSet rs = this.queryState.execute();
		List<DeleteRequestBuilder> requests = new ArrayList<DeleteRequestBuilder>();
		while(rs != null){
			while(rs.next()){
				requests.add(client.prepareDelete(index, type, rs.getString("_id")));
			}
			rs.close();
			rs = queryState.moreResutls();
		}
		return requests;
	}

	// ------------------------------------[ CREATE TABLE / VIEW ]--------------------------------------
	
	/**
	 * Creates a type (and possibly a new index) using the CREATE TABLE definition. If the table to be created contains a dot
	 * it is assumed that the first part refers to an index (existing or not) and the second part refers to the type to be created. 
	 * Fields must be defined with their definition within double quotes. 
	 * It is possibly to specify dynamic_templates as a WITH property. For example: 
	 * create table newIndex.newType (myString "type:String, index:not_analyzed", myInt "type:Integer") WITH (dynamic_templates=
	 * "[{default_mapping: {match: *,match_mapping_type: string, "mapping: {type: string, index: not_analyzed	}}}]")
	 * The elements within the json strings should not be quoted since it breaks presto's parsing of the statement
	 * @param sql
	 * @param create
	 * @param index
	 * @return
	 * @throws SQLException
	 */
	public int execute(String sql, CreateTable create, String index) throws SQLException {
		
		String[] indexAndType = this.getIndexAndType(create.getName().toString(), sql, "table\\s+", "\\s+\\(", index);
		index = indexAndType[0];
		String type = indexAndType[1];
		
		StringBuilder sb = new StringBuilder();
		sb.append("{");
		boolean templatesAdded = false;
		if(create.getProperties().size() >= 0){
			Map<String, Expression> props = create.getProperties();
			if(props.containsKey("dynamic_templates")){ 
				sb.append("dynamic_templates:"+removeEnclosingQuotes( props.get("dynamic_templates").toString()));
				templatesAdded = true;
			}
			// add other 'index global' stuff
		}
		if(templatesAdded) sb.append(", ");
		sb.append("properties:{");
		List<TableElement> fields = create.getElements();
		for(int i=0; i<fields.size(); i++){
			TableElement field = fields.get(i);
			if(field.getName().equals("_id") || field.getName().equals("_type")) continue; // skip protected fields
			sb.append(field.getName()+":{"+field.getType()+"}");
			if(i<fields.size()-1) sb.append(", ");
		}
		sb.append("}}"); // close type and properties blocks
		String json = sb.toString().replaceAll("([\\[|{|,|:]\\s)*(\\w+|\\*)(\\s*[\\]|}|:|,])", "$1\"$2\"$3");
		
		// create index if it does not yet exist
		boolean indexExists = client.admin().indices().exists(new IndicesExistsRequest(index)).actionGet().isExists();
		if(!indexExists){
			CreateIndexResponse response = client.admin().indices().prepareCreate(index).addMapping(type, json).get();
			if(!response.isAcknowledged()) throw new SQLException("Table creation failed because database '"+index+"' could not be created");
		}else{
			PutMappingResponse response = client.admin().indices().preparePutMapping(index).setType(type).setSource(json).execute().actionGet();
			if(!response.isAcknowledged()) throw new SQLException("Table creation failed due to unknown reason");
		}
		this.statement.getConnection().getTypeMap(); // trigger a reload of the table&column set for the connection
		return 0; // default SQL result for a statement that does not manipulate any rows
	}
	
	private String removeEnclosingQuotes(String text){
		String trimmed = text.trim();
		if(trimmed.startsWith("\"")) return trimmed.substring(1, trimmed.length()-1);
		return text;
	}

	/**
	 * Creates a table based on a select on another type and inserts the data (currently not possible to
	 * perform this action 'WITH NO DATA'.
	 * @param sql
	 * @param createAsSelect
	 * @param index
	 * @return the number of rows inserted
	 * @throws SQLException
	 */
	public int execute(String sql, CreateTableAsSelect createAsSelect, String index) throws SQLException {
		if(!createAsSelect.isWithData()) throw new SQLException("Not yet possible to create table as select without data (create emtpy table, "
				+ "insert data and delete it will have the same effect");
		// first create the index
		SqlParser parser = new SqlParser();
		int queryIdx = sql.toLowerCase().indexOf(" as ");
		try{
			String createSql = sql.substring(0, queryIdx)+" (_id String)" ;
			CreateTable create = (CreateTable)parser.createStatement(createSql);
			this.execute(createSql, create, index);
			
		}catch(SQLException sqle) {
			throw sqle;
		}catch(Exception e){
			throw new SQLException("Unable to create table due to: "+e.getMessage(), e);
		}
		
		// secondly add the documents from the query
		String insertSql = "INSERT INTO "+createAsSelect.getName().toString()+" "+sql.substring(queryIdx+4);
		Insert insert = (Insert)parser.createStatement(insertSql);
		int res = this.execute(insertSql, insert, index);
		this.statement.getConnection().getTypeMap(); // trigger a reload of the table&column set for the connection
		return res;
	}

	/**
	 * Creates a view (elasticsearch alias) with given name and query
	 * @param sql
	 * @param create
	 * @param index
	 * @return
	 * @throws SQLException
	 */
	public int execute(String sql, CreateView create, String index) throws SQLException{
		
		String alias = create.getName().toString();
		alias = Heading.findOriginal(sql, alias, "\\s+view\\s+", "\\s+as\\s+");
		
		QueryBody queryBody = create.getQuery().getQueryBody();
		if(!(queryBody instanceof QuerySpecification)) throw new SQLException("Statement does not contain expected query specifiction");
		QuerySpecification querySpec = (QuerySpecification)queryBody;
		if(!querySpec.getFrom().isPresent()) throw new SQLException("Add atleast one INDEX to the query to create the view from");
		
		QueryState state = new BasicQueryState(sql, new Heading(), props);
		List<String> indices = new RelationParser().process(querySpec.getFrom().get(), null);
		new SelectParser().process(querySpec.getSelect(), state);
		
		IndicesAliasesResponse response;
		if(querySpec.getWhere().isPresent()){
			QueryBuilder query = new WhereParser().process(querySpec.getWhere().get(), state);
			response = client.admin().indices().prepareAliases().addAlias(indices.toArray(new String[indices.size()]), alias, query).execute().actionGet();
		}else{
			response = client.admin().indices().prepareAliases().addAlias(indices.toArray(new String[indices.size()]), alias).execute().actionGet();
		}
		if(!response.isAcknowledged()) throw new SQLException("Elasticsearch failed to create the specified alias");
		this.statement.getConnection().getTypeMap(); // trigger a reload of the table&column set for the connection
		return 0; // the number of altered rows
	}

	
	// ------------------------------------[ DROP TABLE / VIEW ]--------------------------------------
	
	/**
	 * Deletes the INDEX with the specified name 
	 * @param sql
	 * @param drop
	 * @return
	 * @throws SQLException 
	 */
	public int execute(String sql, DropTable drop) throws SQLException {
		String index = drop.getTableName().toString();
		index = Heading.findOriginal(sql.trim()+";", index, "table\\s+",";");
		DeleteIndexResponse response = client.admin().indices().prepareDelete(index).execute().actionGet();
		if(!response.isAcknowledged()) throw new SQLException("Elasticsearch failed to delete the specified index");
		return 0;
	}

	/**
	 * Deletes an Alias with all its indices: 'DROP VIEW alias' 
	 * Or removes a specific alias from an index : 'DROP VIEW alias.index'
	 * @param sql the original sql executed
	 * @param drop the parsed AST of the sql
	 * @return 0 (the number of rows affected)
	 * @throws SQLException
	 */
	public int execute(String sql, DropView drop) throws SQLException {
		String alias = drop.getName().toString();
		String[] aliasAndIndex = this.getIndexAndType(alias, sql.trim()+";", "view\\s+",";", null);
		alias = aliasAndIndex[0];
		String index = aliasAndIndex[1];
		if(alias == null){
			alias = index;
			index = null;
		}
		alias = Heading.findOriginal(sql.trim()+";", alias, "view\\s+",";");
		GetAliasesResponse aliases = client.admin().indices().prepareGetAliases(alias).get();
		ImmutableOpenMap<String, List<AliasMetaData>> aliasMd = aliases.getAliases();
		List<String> indices = new ArrayList<String>();
		if(index != null){
			// if index specified than just remove that index
			indices.add(index);
		}else for(ObjectCursor<String> key : aliasMd.keys()){
			// else remove all indexes attached to the alias
			for(AliasMetaData amd : aliasMd.get(key.value)){
				if(amd.alias().equals(alias)) indices.add(key.value);
			}
		}
		IndicesAliasesResponse response = client.admin().indices().prepareAliases().removeAlias(indices.toArray(new String[indices.size()]), alias).get();
		if(!response.isAcknowledged()) throw new SQLException("Elasticsearch failed to delete the specified alias");
		return 0;
	}
	
}
