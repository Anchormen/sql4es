package nl.anchormen.sql4es.parse.sql;

import java.util.List;

import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression.Sign;

import nl.anchormen.sql4es.ESQueryState;
import nl.anchormen.sql4es.QueryState;
import nl.anchormen.sql4es.model.Column;
import nl.anchormen.sql4es.model.Heading;
import nl.anchormen.sql4es.model.TableRelation;
import nl.anchormen.sql4es.model.Column.Operation;
import nl.anchormen.sql4es.model.expression.ColumnReference;
import nl.anchormen.sql4es.model.expression.ICalculation;
import nl.anchormen.sql4es.model.expression.SimpleCalculation;
import nl.anchormen.sql4es.model.expression.SingleValue;

import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;

/**
 * Parses the SELECT part of the SQL and is responsible to fill the {@link Heading} object within the passed {@link ESQueryState}.
 * The heading is used throughout the parsing phase and also to interpret the response from elasticsearch. The return type of the AstVisitor 
 * (Object type) is not used.
 * 
 * @author cversloot
 *
 */
public class SelectParser extends AstVisitor<Object, QueryState>{

	@Override
	protected Object visitSelectItem(SelectItem node, QueryState state){
		if(node instanceof SingleColumn){
			SingleColumn sc = (SingleColumn)node;
			Column column = (Column)sc.getExpression().accept(this, state);
			if(column != null){
				String alias = null;
				if(sc.getAlias().isPresent()) alias = sc.getAlias().get();
				Column col2 = state.getHeading().getColumnByNameAndOp(column.getColumn(), column.getOp());
				column.setAlias(alias);
				if(col2 != null){
					if(col2.getAlias() != null && alias != null && !col2.getAlias().equals(alias)){
						state.getHeading().add(column);
					}else {
						state.getHeading().add(column);
						if(col2.getAlias() == null) col2.setAlias(alias);
						col2.setVisible(true);
						col2.setIndex(column.getIndex());
					}
				}else{
					state.getHeading().add(column);
				}
			}
		}else{
			state.getHeading().add(createColumn(node.toString(), state.getRelations(), null));
		}
		return true;
	}
	
	@Override
	protected Column visitExpression(Expression node, QueryState state){
		if( node instanceof QualifiedNameReference){
			return createColumn( ((QualifiedNameReference)node).getName().toString(), state.getRelations(), null);
		}else if(node instanceof DereferenceExpression){
			// parse columns like 'reference.field'
			String column = visitDereferenceExpression((DereferenceExpression)node);
			return createColumn( column, state.getRelations(), null);
		}else if(node instanceof FunctionCall){
			FunctionCall fc = (FunctionCall)node;
			String operator = fc.getName().toString();
			
			String column;
			if(fc.getArguments().size() == 0) column = "*";
			else if(fc.getArguments().get(0) instanceof LongLiteral) 
				column = ""+((LongLiteral)fc.getArguments().get(0)).getValue();  
			else if(fc.getArguments().get(0) instanceof DereferenceExpression)
				column = visitDereferenceExpression((DereferenceExpression)fc.getArguments().get(0) );
			else {
				 column = ((QualifiedNameReference)fc.getArguments().get(0)).getName().toString();
			}
			try{
				return createColumn(column, state.getRelations(), Operation.valueOf(operator.trim().toUpperCase()));
			}catch(Exception e){
				state.addException("Unable to parse function due to: "+e.getMessage());
				return null;
			}
		}else if(node instanceof ArithmeticBinaryExpression){
			// resolve expressions within select such as (sum(x)/10)%3
			String colName = ((ArithmeticBinaryExpression)node).toString().trim().replaceAll("\"", "");
			colName = colName.substring(1, colName.lastIndexOf(')'));
			ICalculation calc = new ArithmeticParser().visitArithmeticBinary((ArithmeticBinaryExpression)node, state);
			if(state.hasException()) return null;
			Column calcCol = new Column(colName);
			calcCol.setCalculation(calc);
			return calcCol;
		}else{
			state.addException("Unable to parse type "+node.getClass()+" in Select");
		}
		return null;
	}
	
	public static String visitDereferenceExpression(DereferenceExpression node){
		if(node.getBase() instanceof QualifiedNameReference) {
			return ((QualifiedNameReference)node.getBase()).getName().toString()+"."+node.getFieldName();
		}else return visitDereferenceExpression((DereferenceExpression)node.getBase())+"."+node.getFieldName();
	}
	
	private class ArithmeticParser extends AstVisitor<ICalculation, QueryState>{
		
		@Override
		protected ICalculation visitArithmeticBinary(ArithmeticBinaryExpression node, QueryState state){
			ICalculation left = visitExpression(node.getLeft(), state);
			ICalculation right = visitExpression(node.getRight(), state);
			return new SimpleCalculation(left, right, node.getType());
		}
		
		@Override
		protected ICalculation visitExpression(Expression node, QueryState state){
			if(node instanceof LongLiteral){
				return new SingleValue(((LongLiteral)node).getValue());
			}else if(node instanceof DoubleLiteral){
				return new SingleValue(((DoubleLiteral)node).getValue());
			}else if (node instanceof ArithmeticBinaryExpression){
				return visitArithmeticBinary((ArithmeticBinaryExpression)node, state);
			}else if(node instanceof ArithmeticUnaryExpression){
				ArithmeticUnaryExpression unaryExp = (ArithmeticUnaryExpression)node;
				Sign sign = unaryExp.getSign();
				Expression value = unaryExp.getValue();
				ICalculation calc = value.accept(this, state);
				calc.setSign(sign);
				return calc;
			}else if(node instanceof Expression){
				Column column = new SelectParser().visitExpression((Expression)node, state);
				Column col2 = state.getHeading().getColumnByNameAndOp(column.getColumn(), column.getOp());
				if(col2 != null) return new ColumnReference(col2);
				else {
					state.getHeading().add(column);
					column = column.setVisible(false);
					return new ColumnReference(column);
				}
			}else state.addException("Unknown expression '"+node.getClass().getName()+"' encountered in ArithmeticExpression"); 
			return null;
		}
	}
	
	/**
	 * Create's a Column for the provided name and list with tables. This 
	 * @param name
	 * @param tables
	 * @return
	 */
	private Column createColumn(String name, List<TableRelation> tables, Operation op){
		if(name.contains(".")){
			String prefix = name.split("\\.")[0];
			for(TableRelation tr : tables){
				if(tr.getAlias() != null && prefix.equals(tr.getAlias())){
					if(op != null) return new Column(name.substring(name.indexOf('.')+1), op).setTable(tr.getTable(), tr.getAlias());
					return new Column(name.substring(name.indexOf('.')+1)).setTable(tr.getTable(), tr.getAlias());
				}else if (prefix.equals(tr.getTable())){
					if(op != null) return new Column(name.substring(name.indexOf('.')+1), op).setTable(tr.getTable(), tr.getAlias());
					return new Column(name.substring(name.indexOf('.')+1)).setTable(tr.getTable(), tr.getAlias());
				}
			}
		}
		if(op != null) return new Column(name, op);
		return new Column(name);
	}
	
}
