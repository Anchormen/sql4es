package nl.anchormen.sql4es.model.expression;

import java.util.List;

import com.facebook.presto.sql.tree.ArithmeticUnaryExpression.Sign;

public class SingleValue implements ICalculation{

	private Number value;
	
	public SingleValue(Number value) {
		this.value = value;
	}
	
	public void invertSign(){
		this.value = value.doubleValue() * -1;
	}
	
	@Override
	public void setSign(Sign sign) {
		if(sign == Sign.MINUS) this.value = value.doubleValue() * -1;
	}

	@Override
	public Number evaluate(List<Object> row){
		return this.value;
	}

	public String toString(){
		return ""+value;
	}

}
