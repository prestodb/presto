package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.compiler.Symbol;

import java.util.List;

public abstract class PlanNode
{
    public abstract List<PlanNode> getSources();
    public abstract List<Symbol> getOutputSymbols();

    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitPlan(this, context);
    }
}
