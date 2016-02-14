package nl.anchormen.sql4es.parse.sql;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
import nl.anchormen.sql4es.model.Utils;

import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression.Sign;

/**
 * A Presto {@link AstVisitor} implementation that parses WHERE clauses
 * 
 * @author cversloot
 *
 */
public class WhereParser extends AstVisitor<QueryBuilder, QueryState>{
	
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
			return this.processComparison(compareExp, state);
		}else if( node instanceof NotExpression){
			state.addException("NOT is currently not supported, use '<>' instead");
			return null;
		}else if (node instanceof LikePredicate){
			String field = getVariableName(((LikePredicate)node).getValue());
			FieldAndType fat = getFieldAndType(field, state);
			field = fat.getFieldName();
			if(field.equals(Heading.ID)){
				state.addException("Matching document _id using LIKE is not supported");
				return null;
			}
			String query = ((StringLiteral)((LikePredicate)node).getPattern()).getValue();
			if(fat.getFieldType() == Types.REF) 
				return QueryBuilders.nestedQuery(field.split("\\.")[0], queryForString(field, query));
			return queryForString(field, query);
		}else if (node instanceof InPredicate){
			return this.processIn((InPredicate)node, state);
			
		}else 
			state.addException("Unable to parse "+node+" ("+node.getClass().getName()+") is not a supported expression");
		return null;
	}
	

	/**
	 * Parses predicats of types =, >, >=, <, <= and <>
	 * @param compareExp
	 * @param state
	 * @return
	 */
	private QueryBuilder processComparison(ComparisonExpression compareExp, QueryState state) {
		String field = getVariableName(compareExp.getLeft());
		FieldAndType fat = getFieldAndType(field, state);
		field = fat.getFieldName();

		if(compareExp.getRight() instanceof QualifiedNameReference || compareExp.getRight() instanceof DereferenceExpression){
			state.addException("Matching two columns is not supported : "+compareExp);
			return null;
		}
		// get value of the expression
		Object value = getLiteralValue(compareExp.getRight(), state);
		if(state.hasException()) return null;
		
		QueryBuilder comparison = null;
		if(compareExp.getType() == ComparisonExpression.Type.EQUAL){
			if(field.equals(Heading.ID)) comparison = QueryBuilders.idsQuery(
						state.getRelations().toArray(new String[state.getRelations().size()])
					).ids((String)value);
			else if(field.equals(Heading.SEARCH)) comparison = QueryBuilders.queryStringQuery((String)value);
			else if(value instanceof String) comparison = queryForString(field, (String)value);
			else comparison = QueryBuilders.termQuery(field, value);
		}else if(compareExp.getType() == ComparisonExpression.Type.GREATER_THAN_OR_EQUAL){
			comparison = QueryBuilders.rangeQuery(field).from(value);
		}else if(compareExp.getType() == ComparisonExpression.Type.LESS_THAN_OR_EQUAL){
			comparison = QueryBuilders.rangeQuery(field).to(value);
		}else if(compareExp.getType() == ComparisonExpression.Type.GREATER_THAN){
			comparison = QueryBuilders.rangeQuery(field).gt(value);
		}else if(compareExp.getType() == ComparisonExpression.Type.LESS_THAN){
			comparison = QueryBuilders.rangeQuery(field).lt(value);
		}else if(compareExp.getType() == ComparisonExpression.Type.NOT_EQUAL){
			if(field.equals(Heading.ID)){
				state.addException("Matching document _id using '<>' is not supported");
				return null;
			}
			comparison = QueryBuilders.notQuery(QueryBuilders.termQuery(field, value));
		};
		if(fat.getFieldType() == Types.REF) 
			return QueryBuilders.nestedQuery(field.split("\\.")[0], comparison);
		return comparison;
	}

	/**
	 * Parses predicates of type IN (...)
	 * @param node
	 * @param state
	 * @return
	 */
	private QueryBuilder processIn(InPredicate node, QueryState state) {
		String field = getVariableName(node.getValue());
		FieldAndType fat = getFieldAndType(field, state);
		field = fat.getFieldName();
		
		if(!(node.getValueList() instanceof InListExpression)){
			state.addException("SELECT ... IN can only be used with a list of values");
			return null;
		}
		InListExpression list = (InListExpression)(node).getValueList();
		List<Object> values = new ArrayList<Object>();
		for(Expression listItem : list.getValues()){
			Object value = this.getLiteralValue(listItem, state);
			if(state.hasException()) return null;
			values.add(value);
		}
		if(field.equals(Heading.ID)) {
			String[] ids = new String[values.size()];
			return QueryBuilders.idsQuery(state.getRelations().toArray(new String[state.getRelations().size()])).addIds(values.toArray(ids));
		}
		if(fat.getFieldType() == Types.REF) 
			return QueryBuilders.nestedQuery(field.split("\\.")[0], QueryBuilders.termsQuery(field, values));
		return QueryBuilders.termsQuery(field, values);
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
	
	/**
	 * Returns the case sensitive fieldName matching the (potentially lowercased) column
	 * @param colName
	 * @param state
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private FieldAndType getFieldAndType(String colName, QueryState state){
		colName = Heading.findOriginal(state.originalSql(), colName, "where.+", "\\W");
		Column column = state.getHeading().getColumnByAlias(colName);
		String properName = colName; 		
		if(column != null) properName = column.getColumn();
		String[] parts = properName.split("\\.");
		for(int i=1; i<parts.length; i++) parts[i] = parts[i-1]+"."+parts[i];
		
		// check if the requested column has nested type
		Map<String, Map<String, Integer>> tableColumnInfo = (Map<String, Map<String, Integer>>)state.getProperty(Utils.PROP_TABLE_COLUMN_MAP);
		for(String relation : state.getRelations()){
			for(String parentCol : parts){
				Integer type = tableColumnInfo.get(relation).get(parentCol);
				if(type != null) return new FieldAndType(properName, type);
			}
		}
		// in rare instances the type is unknown (added recently?)
		return new FieldAndType(properName, Types.OTHER);
	}
	
	private class FieldAndType{
		
		private String fieldName;
		private int fieldType;
		
		public FieldAndType(String fieldName, int fieldType) {
			this.fieldName = fieldName;
			this.fieldType = fieldType;
		}

		public String getFieldName() {
			return fieldName;
		}

		public int getFieldType() {
			return fieldType;
		}
		
	}
}
