package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.compiler.Slot;

import java.util.List;

public abstract class PlanNode
{
    public abstract List<PlanNode> getSources();
    public abstract List<Slot> getOutputs();

    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitPlan(this, context);
    }
}
