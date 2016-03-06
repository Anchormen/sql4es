package nl.anchormen.sql4es.model;

public class TableRelation {

	private String table;
	private String alias;
	
	public TableRelation(String table) {
		this.table = table;
	}

	public String getAlias() {
		return alias;
	}

	public TableRelation setAlias(String alias) {
		this.alias = alias;
		return this;
	}

	public String getTable() {
		return table;
	}
	
	public String toString(){
		return table+(alias == null ? "" : " as "+alias);
	}
	
}
