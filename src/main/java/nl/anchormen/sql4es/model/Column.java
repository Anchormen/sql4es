package nl.anchormen.sql4es.model;

import java.sql.Types;

import nl.anchormen.sql4es.model.expression.ICalculation;

public class Column implements Comparable<Column>{
	
	public enum Operation {NONE, AVG, SUM, MIN, MAX, COUNT}
	
	private String columnName;
	private Operation op = Operation.NONE;
	private String alias = null;
	private int index = -1;
	private int sqlType = Types.OTHER;
	private ICalculation calculation = null;
	private boolean isVisible = true;
	
	public Column(String name, Operation op, int index) {
		this.columnName = name;
		this.op = op;
		this.index = index;
		if(name.equals("*") && op == Operation.COUNT) alias = "count(*)";
		else if(name.equals("1") && op == Operation.COUNT) alias = "count(1)";
		
		switch(this.op){
			case COUNT: sqlType = Types.BIGINT; break;
			case NONE: sqlType = Types.VARCHAR; break;
			case MIN: sqlType = Types.DOUBLE; break;
			case MAX: sqlType = Types.DOUBLE; break;
			case SUM: sqlType = Types.DOUBLE; break;
			case AVG: sqlType = Types.DOUBLE; break;
			default: sqlType = Types.OTHER;
		}
		if(calculation != null) sqlType = Types.DOUBLE;
	}	
	
	public Column(String column, int index) {
		this.columnName = column;
		this.index = index;
	}
	
	/**
	 * Sets the index this column refers to in the final result.
	 * Use with caution, setting a wrong index will screw up the resultset
	 * @param index
	 * @return
	 */
	public Column setIndex(int index){
		this.index = index;
		return this;
	}
	
	public int getIndex(){
		return this.index;
	}
	
	public String toString(){
		return ("col: "+columnName+", fullN: "+getFullName()+" as alias:"+getLabel() +" vis: "+isVisible+" index: "+index);
	}

	/**
	 * Gets the column this column refers to
	 * @return the name of the column
	 */
	public String getColumn() {
		return columnName;
	}

	/**
	 * Gets the operation of this column
	 * @return any of {NONE, AVG, SUM, MIN, MAX, COUNT}
	 */
	public Operation getOp() {
		return op;
	}
	
	/**
	 * Gets the alias of this column
	 * @return the alias or NULL if not set
	 */
	public String getAlias() {
		return alias;
	}

	/**
	 * Sets the alias of this column
	 * @param alias
	 */
	public Column setAlias(String alias) {
		this.alias = alias;
		return this;
	}
	
	/**
	 * Provides the preferred label of this column.
	 * This is the alias if this was set, else the fullName 
	 * @return
	 */
	public String getLabel(){
		if(alias != null) return alias;
		else return getFullName();
	}
	
	/**
	 * @return the full name including the operator (if not NONE). For example 'SUM(column)'
	 */
	public String getFullName(){
		switch(op){
			case AVG: return "avg("+getColumn()+")";
			case COUNT: return "count("+getColumn()+")";
			case MAX: return "max("+getColumn()+")";
			case MIN: return "min("+getColumn()+")";
			case SUM: return "sum("+getColumn()+")";
			default : return getColumn();
		}
	}
	
	public int getSqlType() {
		return sqlType;
	}

	/**
	 * Sets the type of this column. Only executed when it is not a function
	 * @param sqlType
	 */
	public Column setSqlType(int sqlType) {
		if(this.op != Operation.AVG)	this.sqlType = sqlType;
		return this;
	}

	/**
	 * Changes the column originally provided
	 * (currently used to fix case issues induced by the parser).
	 * @param newColumn
	 */
	public Column setColumn(String newColumn) {
		this.columnName = newColumn;
		if(columnName.equals("*") && op == Operation.COUNT && alias == null) alias = "count(*)";
		return this;
	}
	
	public int hashCode(){
		return (columnName+alias+op).hashCode();
	}
	
	public boolean equals(Object o){
		if(o instanceof Column){
			Column oc = (Column)o;
			return oc.getColumn().equals(columnName) && oc.getAlias().equals(this.alias) && this.op == oc.getOp();
		}
		return false;
	}

	public Column setCalculation(ICalculation calculation) {
		this.calculation  = calculation;
		this.sqlType = Types.DOUBLE;
		return this;
	}
	
	public ICalculation getCalculation(){
		return this.calculation;
	}
	
	public boolean hasCalculation(){
		return this.calculation != null;
	}

	public boolean isVisible() {
		return isVisible;
	}

	public Column setVisible(boolean isVisible) {
		this.isVisible = isVisible;
		return this;
	}

	@Override
	public int compareTo(Column o) {
		if(this.isVisible && !o.isVisible) return -1;
		if(!this.isVisible && o.isVisible) return 1;
		return this.getIndex() - o.getIndex();
	}
}
