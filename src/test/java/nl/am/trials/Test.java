package nl.am.trials;

import java.net.URISyntaxException;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.Delete;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.DropView;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.TableElement;


public class Test extends AstVisitor<Boolean, String> {

	public static void main(String[] args) throws URISyntaxException {
		SqlParser sp = new SqlParser();
		/*
		Query q = (Query)sp.createStatement("SELECT field, count(*) as cnt, sum(col) FROM some_table "
				+ "WHERE id > 10 AND id < 100 AND (name = 'some name' OR NOT address = 'bullshit') "
				+ "GROUP BY field ORDER BY sum(col) DESC LIMIT 10");
		System.out.println(q);
		Object res = q.accept(new Test(), "");
		System.out.println(res);
		*/
		DropView drop = (DropView)sp.createStatement("DROP VIEW mytaBDle.bla");
		System.out.println("Table: "+drop.getName().toString());
	}

	@Override
	protected Boolean visitQuery(Query node, String s){
		System.out.println(s+"Query: "+node);
		if(node.getQueryBody() instanceof QuerySpecification){
			return ((QuerySpecification)node.getQueryBody()).accept(this, s+"\t");
		}else return node.getQueryBody().accept(this, s+"\t");
	}
	
	@Override
	protected Boolean visitQuerySpecification(QuerySpecification node, String s){
		System.out.println(s+"Queryspecification: "+node);
		System.out.println(s+"Select:"+node.getSelect());
		System.out.println(s+"From:"+node.getFrom());
		System.out.println(s+"Where:"+node.getWhere());
		System.out.println(s+"Group by:"+node.getGroupBy());
		System.out.println(s+"Having:"+node.getHaving());
		System.out.println(s+"Order by:"+node.getOrderBy());
		System.out.println(s+"Limit:"+node.getLimit());
		//for(SelectItem si : node.getSelect().getSelectItems()){
		//	si.accept(this, s+"\t");
		//}
		if(node.getWhere().isPresent()) node.getWhere().get().accept(this, s+"\t");
		return true;
	}
	
	@Override
	protected Boolean visitSelectItem(SelectItem node, String s){
		if(node instanceof SingleColumn){
			SingleColumn si = (SingleColumn)node;
			if(si.getAlias().isPresent()) System.out.println(s+"alias:"+si.getAlias().get());
			si.getExpression().accept(this, s+"\t");
		}else{
			// all columns
		}
		return true;
	}
	
	@Override
	protected Boolean visitExpression(Expression node, String s){
		System.out.println(s+node.getClass().getName());
		if(node instanceof FunctionCall){
			System.out.println(s+"\tname: "+((FunctionCall)node).getName());
			System.out.println(s+"\targ(0): "+((FunctionCall)node).getArguments().get(0));
		}else if( node instanceof LogicalBinaryExpression){
			LogicalBinaryExpression lbe = (LogicalBinaryExpression)node;
		}else if( node instanceof QualifiedNameReference){
			System.out.println("name: "+((QualifiedNameReference)node).getName());
		}
		return true;
	}
	
}
