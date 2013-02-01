package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.tree.SortItem;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.concurrent.Immutable;
import java.util.List;
import java.util.Map;

@Immutable
public class TopNNode
        extends PlanNode
{
    private final PlanNode source;
    private final long count;
    private final List<Symbol> orderBy;
    private final Map<Symbol, SortItem.Ordering> orderings;

    @JsonCreator
    public TopNNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("count") long count,
            @JsonProperty("orderBy") List<Symbol> orderBy,
            @JsonProperty("orderings") Map<Symbol, SortItem.Ordering> orderings)
    {
        super(id);

        Preconditions.checkNotNull(source, "source is null");
        Preconditions.checkArgument(count > 0, "count must be positive");
        Preconditions.checkNotNull(orderBy, "orderBy is null");
        Preconditions.checkArgument(!orderBy.isEmpty(), "orderBy is empty");
        Preconditions.checkArgument(orderings.size() == orderBy.size(), "orderBy and orderings sizes don't match");

        this.source = source;
        this.count = count;
        this.orderBy = ImmutableList.copyOf(orderBy);
        this.orderings = ImmutableMap.copyOf(orderings);
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @JsonProperty("source")
    public PlanNode getSource()
    {
        return source;
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return source.getOutputSymbols();
    }

    @JsonProperty("count")
    public long getCount()
    {
        return count;
    }

    @JsonProperty("orderBy")
    public List<Symbol> getOrderBy()
    {
        return orderBy;
    }

    @JsonProperty("orderings")
    public Map<Symbol, SortItem.Ordering> getOrderings()
    {
        return orderings;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitTopN(this, context);
    }
}
