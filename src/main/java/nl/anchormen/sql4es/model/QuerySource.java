package nl.anchormen.sql4es.model;

import com.facebook.presto.sql.tree.QueryBody;

public class QuerySource {

	private String index;
	private String source;
	private String alias;
	private QueryBody query;
	
	public QuerySource(String source) {
		this.source = source;
	}

	public QuerySource(String index, String source) {
		this.index = index;
		this.source = source;
	}
	
	public QuerySource(String source, QueryBody query) {
		this.source = source;
		this.query = query;
	}

	public QuerySource(String index, String source, QueryBody query) {
		this.index = index;
		this.source = source;
		this.query = query;
	}

	public String getAlias() {
		return alias;
	}

	public QuerySource setAlias(String alias) {
		this.alias = alias;
		return this;
	}

	public String getIndex() {
		return index;
	}

	public String getSource() {
		return source;
	}
	
	public String toString(){
		return (index == null ? "" : index+".")+ source+(alias == null ? "" : " AS "+alias);
	}

	public QueryBody getQuery() {
		return query;
	}

	public boolean isSubQuery(){
		return this.query != null;
	}
	
}
