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
import nl.anchormen.sql4es.model.TableRelation;

/**
 * A Presto {@link AstVisitor} implementation that parses FROM clauses
 * @author cversloot
 *
 */
public class RelationParser extends AstVisitor<List<TableRelation> , QueryState>{
	
	@Override
	protected List<TableRelation> visitRelation(Relation node, QueryState state){
		if(node instanceof Join){
			return node.accept(this, state);
		}else if( node instanceof SampledRelation){
			state.addException("Sampled relations are not supported");
			return null;
		}else if( node instanceof AliasedRelation){
			AliasedRelation ar = (AliasedRelation)node;
			List<TableRelation> relations = ar.getRelation().accept(this, state);
			for(TableRelation rr : relations) rr.setAlias(ar.getAlias());
			return relations;
		}else if( node instanceof QueryBody){
			return node.accept(this, state);
		}else{
			state.addException("Unable to parse node because it has an unknown type :"+node.getClass());
			return null;
		}
	}
	
	@Override
	protected List<TableRelation>  visitJoin(Join node, QueryState state){
		// possible to parse multiple tables but it is not supported
		List<TableRelation> relations = node.getLeft().accept(this,state);
		relations.addAll( node.getRight().accept(this, state) );
		return relations;
	}
	
	@Override
	protected List<TableRelation> visitQueryBody(QueryBody node, QueryState state){
		ArrayList<TableRelation> relations = new ArrayList<TableRelation>();
		if(node instanceof Table){
			String table = ((Table)node).getName().toString();
			// resolve relations provided in dot notation (schema.index.type) and just get the type for now
			String[] catIndexType = table.split("\\.");
			if(catIndexType.length == 1) {
				relations.add(new TableRelation(table));
			}else{
				relations.add(new TableRelation(catIndexType[catIndexType.length-1]));
			}
		}else{
			state.addException(node.getClass().getName()+" is not supported ("+node.getClass().getName()+")");
		}
		return relations;
	}

}
