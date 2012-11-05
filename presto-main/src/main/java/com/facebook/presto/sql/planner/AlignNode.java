package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.compiler.Slot;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class AlignNode
    extends PlanNode
{
    private final List<PlanNode> sources;
    private final List<Slot> outputs;

    public AlignNode(List<PlanNode> sources, List<Slot> outputs)
    {
        this.outputs = outputs;
        this.sources = ImmutableList.copyOf(sources);
    }

    public List<Slot> getOutputs()
    {
        return outputs;
    }

    public List<PlanNode> getSources()
    {
        return sources;
    }

    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitAlign(this, context);
    }

}
