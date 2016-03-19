package nl.anchormen.sql4es.parse.sql;

import java.sql.SQLException;
import java.util.List;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;

import nl.anchormen.sql4es.model.Heading;
import nl.anchormen.sql4es.model.OrderBy;
import nl.anchormen.sql4es.model.QuerySource;
import nl.anchormen.sql4es.model.expression.IComparison;

public class ParseResult {

	private Heading heading;
	private List<QuerySource> sources;
	private QueryBuilder query;
	private AggregationBuilder aggregation;
	private IComparison having;
	private List<OrderBy> sorts;
	private int limit = -1;
	private Boolean useCache = false;
	private Boolean requestScore = false;
	private SQLException exception;
	
	public ParseResult(Heading heading, List<QuerySource> sources, QueryBuilder query, AggregationBuilder aggregation,
			IComparison having, List<OrderBy> sorts, Integer limit, Boolean useCache, Boolean requestScore) {
		super();
		this.heading = heading;
		this.sources = sources;
		this.query = query;
		this.aggregation = aggregation;
		this.having = having;
		this.sorts = sorts;
		this.limit = limit;
		this.useCache = useCache;
		this.requestScore = requestScore;
	}
	
	public ParseResult(SQLException exception){
		this.exception = exception;
	}

	public Heading getHeading() {
		return heading;
	}

	public List<QuerySource> getSources() {
		return sources;
	}

	public QueryBuilder getQuery() {
		return query;
	}

	public AggregationBuilder getAggregation() {
		return aggregation;
	}

	public IComparison getHaving() {
		return having;
	}

	public List<OrderBy> getSorts() {
		return sorts;
	}

	public Integer getLimit() {
		return limit;
	}

	public Boolean getUseCache() {
		return useCache;
	}

	public Boolean getRequestScore() {
		return requestScore;
	}

	public SQLException getException() {
		return exception;
	}

}
