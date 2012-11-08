package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.compiler.Slot;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Functions.forMap;

public class OutputPlan
    extends PlanNode
{
    private final PlanNode source;
    private final List<String> columnNames;
    private final Map<String, Slot> assignments;

    public OutputPlan(PlanNode source, List<String> columnNames, Map<String, Slot> assignments)
    {
        Preconditions.checkNotNull(source, "source is null");
        Preconditions.checkNotNull(columnNames, "columnNames is null");
        Preconditions.checkArgument(columnNames.size() == assignments.size(), "columnNames and assignments sizes don't match");

        this.source = source;
        this.columnNames = columnNames;
        this.assignments = ImmutableMap.copyOf(assignments);
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<Slot> getOutputs()
    {
        return ImmutableList.copyOf(Iterables.transform(columnNames, forMap(assignments)));
    }

    public List<String> getColumnNames()
    {
        return columnNames;
    }

    public Map<String, Slot> getAssignments()
    {
        return assignments;
    }

    public PlanNode getSource()
    {
        return source;
    }

    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitOutput(this, context);
    }

}
