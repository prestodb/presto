package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.sql.planner.Symbol;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Functions.forMap;

@Immutable
public class OutputNode
    extends PlanNode
{
    private final PlanNode source;
    private final List<String> columnNames;
    private final Map<String, Symbol> assignments; // column name = symbol

    @JsonCreator
    public OutputNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("columns") List<String> columnNames,
            @JsonProperty("assignments") Map<String, Symbol> assignments)
    {
        super(id);

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
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.copyOf(Iterables.transform(columnNames, forMap(assignments)));
    }

    @JsonProperty("columns")
    public List<String> getColumnNames()
    {
        return columnNames;
    }

    @JsonProperty
    public Map<String, Symbol> getAssignments()
    {
        return assignments;
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitOutput(this, context);
    }

}
