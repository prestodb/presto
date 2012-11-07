package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.compiler.Slot;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class LimitNode
        extends PlanNode
{
    private final PlanNode source;
    private final long count;

    public LimitNode(PlanNode source, long count)
    {
        this.source = source;
        this.count = count;
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

    public long getCount()
    {
        return count;
    }

    @Override
    public List<Slot> getOutputs()
    {
        return source.getOutputs();
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitLimit(this, context);
    }
}
