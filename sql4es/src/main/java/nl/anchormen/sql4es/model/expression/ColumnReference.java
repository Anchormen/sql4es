package nl.anchormen.sql4es.model.expression;

import java.util.List;

import com.facebook.presto.sql.tree.ArithmeticUnaryExpression.Sign;

import nl.anchormen.sql4es.model.Column;

public class ColumnReference implements ICalculation{

	private Column column;
	private Sign sign = Sign.PLUS;
	
	public ColumnReference(Column column) {
		this.column = column;
	}

	@Override
	public Number evaluate(List<Object> row) {
		Object value = row.get(column.getIndex());
		if(value instanceof Boolean){
			if(((Boolean)value).booleanValue()) value = 1;
			else value = 0;
		}
		if(sign == Sign.MINUS) return -1 * ((Number)value).doubleValue();
		return (Number)value;
	}

	public Column getColumn(){
		return column;
	}
	
	public String toString(){
		return column.getFullName();
	}

	@Override
	public void setSign(Sign sign) {
		this.sign = sign;
	}
	
}
