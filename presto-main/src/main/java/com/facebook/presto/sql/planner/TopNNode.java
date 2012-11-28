package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.compiler.Symbol;
import com.facebook.presto.sql.tree.SortItem;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

public class TopNNode
        extends PlanNode
{
    private final PlanNode source;
    private final long count;
    private final List<Symbol> orderBy;
    private final Map<Symbol, SortItem.Ordering> orderings;

    public TopNNode(PlanNode source,
            long count,
            List<Symbol> orderBy,
            Map<Symbol, SortItem.Ordering> orderings)
    {
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

    public PlanNode getSource()
    {
        return source;
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return source.getOutputSymbols();
    }

    public long getCount()
    {
        return count;
    }

    public List<Symbol> getOrderBy()
    {
        return orderBy;
    }

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
