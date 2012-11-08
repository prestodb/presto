package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.compiler.Slot;
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
    private final List<Slot> orderBy;
    private final Map<Slot, SortItem.Ordering> orderings;

    public TopNNode(PlanNode source, long count, List<Slot> orderBy, Map<Slot, SortItem.Ordering> orderings)
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
    public List<Slot> getOutputs()
    {
        return source.getOutputs();
    }

    public long getCount()
    {
        return count;
    }

    public List<Slot> getOrderBy()
    {
        return orderBy;
    }

    public Map<Slot, SortItem.Ordering> getOrderings()
    {
        return orderings;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitTopN(this, context);
    }
}
