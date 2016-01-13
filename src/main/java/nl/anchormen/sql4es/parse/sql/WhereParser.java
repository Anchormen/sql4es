package nl.anchormen.sql4es.parse.sql;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression.Type;

import nl.anchormen.sql4es.QueryState;
import nl.anchormen.sql4es.model.Column;
import nl.anchormen.sql4es.model.Heading;

import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression.Sign;

public class WhereParser extends AstVisitor<QueryBuilder, QueryState>{
	
	@SuppressWarnings("deprecation")
	@Override
	protected QueryBuilder visitExpression(Expression node, QueryState state) {
		if( node instanceof LogicalBinaryExpression){
			LogicalBinaryExpression boolExp = (LogicalBinaryExpression)node;
			BoolQueryBuilder bqb = QueryBuilders.boolQuery();
			QueryBuilder leftQ = boolExp.getLeft().accept(this, state);
			QueryBuilder rightQ = boolExp.getRight().accept(this, state);
			if(boolExp.getType() == Type.AND){
				bqb.must(leftQ);
				bqb.must(rightQ);
			}else if(boolExp.getType() == Type.OR){
				bqb.should(leftQ);
				bqb.should(rightQ);
			}
			return bqb;
		}else if( node instanceof ComparisonExpression){
			ComparisonExpression compareExp = (ComparisonExpression)node;
			String variable = getVariableName(compareExp.getLeft());
			variable = getFieldName(variable, state);

			if(compareExp.getRight() instanceof QualifiedNameReference || compareExp.getRight() instanceof DereferenceExpression){
				state.addException("Matching two columns is not supported : "+compareExp);
				return null;
			}
			// get value of the expression
			Object value = getLiteralValue(compareExp.getRight(), state);
			if(state.hasException()) return null;
			
			QueryBuilder comparison = null;
			if(compareExp.getType() == ComparisonExpression.Type.EQUAL){
				if(value instanceof String) comparison = queryForString(variable, (String)value);
				else comparison = QueryBuilders.termQuery(variable, value);
			}else if(compareExp.getType() == ComparisonExpression.Type.GREATER_THAN_OR_EQUAL){
				comparison = QueryBuilders.rangeQuery(variable).from(value);
			}else if(compareExp.getType() == ComparisonExpression.Type.LESS_THAN_OR_EQUAL){
				comparison = QueryBuilders.rangeQuery(variable).to(value);
			}else if(compareExp.getType() == ComparisonExpression.Type.GREATER_THAN){
				comparison = QueryBuilders.rangeQuery(variable).gt(value);
			}else if(compareExp.getType() == ComparisonExpression.Type.LESS_THAN){
				comparison = QueryBuilders.rangeQuery(variable).lt(value);
			}else if(compareExp.getType() == ComparisonExpression.Type.NOT_EQUAL){
				comparison = QueryBuilders.notQuery(QueryBuilders.termQuery(variable, value));
			};
			return comparison;
		}else if( node instanceof NotExpression){
			state.addException("NOT is currently not supported, use '<>' instead");
			return null;
		}else if (node instanceof LikePredicate){
			String field = getVariableName(((LikePredicate)node).getValue());
			field = getFieldName(field, state);
			String query = ((StringLiteral)((LikePredicate)node).getPattern()).getValue();
			return queryForString(field, query);
		}else if (node instanceof InPredicate){
			String field = getVariableName(((InPredicate)node).getValue());
			field = getFieldName(field, state);
			
			InListExpression list = (InListExpression)((InPredicate)node).getValueList();
			List<Object> values = new ArrayList<Object>();
			for(Expression listItem : list.getValues()){
				Object value = this.getLiteralValue(listItem, state);
				if(state.hasException()) return null;
				values.add(value);
			}
			return QueryBuilders.termsQuery(field, values);
		}else 
			state.addException("Unable to parse "+node+" ("+node.getClass().getName()+") is not a supported expression");
		return null;
	}
	
	/**
	 * extracts a variable name from the provided expression
	 * @param e
	 * @return
	 */
	private String getVariableName(Expression e){
		if(e instanceof DereferenceExpression){
			// parse columns like 'reference.field'
			return SelectParser.visitDereferenceExpression((DereferenceExpression)e);
		}else{
			return ((QualifiedNameReference)e).getName().toString();
		}
	}
	
	/**
	 * Extracts the literal value from an expression (if expression is supported)
	 * @param expression
	 * @param state
	 * @return a Long, Boolean, Double or String object
	 */
	private Object getLiteralValue(Expression expression, QueryState state){
		if(expression instanceof LongLiteral) return ((LongLiteral)expression).getValue();
		else if(expression instanceof BooleanLiteral) return ((BooleanLiteral)expression).getValue();
		else if(expression instanceof DoubleLiteral) return ((DoubleLiteral)expression).getValue();
		else if(expression instanceof StringLiteral) return ((StringLiteral)expression).getValue();
		else if(expression instanceof ArithmeticUnaryExpression){
			ArithmeticUnaryExpression unaryExp = (ArithmeticUnaryExpression)expression;
			Sign sign = unaryExp.getSign();
			Number num = (Number)getLiteralValue(unaryExp.getValue(), state);
			if(sign == Sign.MINUS){
				if(num instanceof Long) return -1*num.longValue();
				else if(num instanceof Double) return -1*num.doubleValue();
				else {
					state.addException("Unsupported numeric literal expression encountered : "+num.getClass());
					return null;
				}
			}
			return num;
		} else state.addException("Literal type "+expression.getClass().getSimpleName()+" is not supported");
		return null;
	}
	
	/**
	 * Returns the case sensitive fieldName matching the (potentially lowercased) column
	 * @param colName
	 * @param state
	 * @return
	 */
	private String getFieldName(String colName, QueryState state){
		colName = Heading.findOriginal(state.originalSql(), colName, "where.+", "\\W");
		Column column = state.getHeading().getColumnByAlias(colName);
		if(column != null)return column.getColumn();
		else return colName;
	}
	
	/**
	 * Interprets the string term and returns an appropriate Query (wildcard, phrase or term)
	 * @param field
	 * @param term
	 * @return
	 */
	private QueryBuilder queryForString(String field, String term){
		if(term.contains("%") || term.contains("_")){
			return QueryBuilders.wildcardQuery(field, term.replaceAll("%", "*").replaceAll("_", "?"));
		}else if  (term.contains(" ") ){
			return QueryBuilders.matchPhraseQuery(field, term);
		}else return QueryBuilders.termQuery(field, term);
	}
	
}
