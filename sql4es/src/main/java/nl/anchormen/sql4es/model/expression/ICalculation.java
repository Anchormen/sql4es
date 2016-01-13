package nl.anchormen.sql4es.model.expression;

import java.util.List;

import com.facebook.presto.sql.tree.ArithmeticUnaryExpression.Sign;

public interface ICalculation {

	public Number evaluate(List<Object> row);
	
	public void setSign(Sign sign);
	
}
