package nl.anchormen.sql4es.parse.sql;

import java.util.ArrayList;
import java.util.List;

import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.Table;

import nl.anchormen.sql4es.QueryState;

public class RelationParser extends AstVisitor<List<String> , QueryState>{
	
	@Override
	protected List<String>  visitRelation(Relation node, QueryState state){
		if(node instanceof Join){
			return node.accept(this, state);
		}else if( node instanceof SampledRelation){
			state.addException("Sampled relations are not supported");
			return new ArrayList<String>();
		}else if( node instanceof AliasedRelation){
			state.addException("Aliasses for tables are not supported, please remove alias '"+((AliasedRelation)node).getAlias()+"'");
			return new ArrayList<String>();
		}else if( node instanceof QueryBody){
			return node.accept(this, state);
		}else{
			state.addException("Unable to parse node because it has an unknown type :"+node.getClass());
			return new ArrayList<String>();
		}
	}
	
	@Override
	protected List<String>  visitJoin(Join node, QueryState state){
		//req.addException("Querying multiple tables is not (yet) supported");
		// possible to parse multiple tables but it is not supported
		List<String> relations = node.getLeft().accept(this,state);
		relations.addAll( node.getRight().accept(this, state) );
		return relations;
	}
	
	@Override
	protected List<String> visitQueryBody(QueryBody node, QueryState state){
		ArrayList<String> relations = new ArrayList<String>();
		if(node instanceof Table){
			String table = ((Table)node).getName().toString();
			// resolve relations provided in dot notation (schema.index.type) and just get the type for now
			String[] catIndexType = table.split("\\.");
			if(catIndexType.length == 1) {
				relations.add(table);
			}else{
				relations.add(catIndexType[catIndexType.length-1]);
			}
		}else{
			state.addException(node.getClass().getName()+" is not supported ("+node.getClass().getName()+")");
		}
		return relations;
	}

}
