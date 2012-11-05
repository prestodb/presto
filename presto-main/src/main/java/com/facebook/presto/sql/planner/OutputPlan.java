package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.compiler.Slot;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class OutputPlan
    extends PlanNode
{
    private final PlanNode source;
    private final List<String> columnNames;

    public OutputPlan(PlanNode source, List<String> columnNames)
    {
        this.source = source;
        this.columnNames = columnNames;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<Slot> getOutputs()
    {
        return source.getOutputs();
    }

    public List<String> getColumnNames()
    {
        return columnNames;
    }

    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitOutput(this, context);
    }

}
