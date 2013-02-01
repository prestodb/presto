package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.sql.analyzer.Symbol;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;

@Immutable
public class TableScanNode
    extends PlanNode
{
    private final TableHandle table;
    private final Map<Symbol, ColumnHandle> attributes; // symbol -> column

    @JsonCreator
    public TableScanNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("table") TableHandle table,
            @JsonProperty("assignments") Map<Symbol, ColumnHandle> assignments)
    {
        super(id);

        Preconditions.checkNotNull(table, "table is null");
        Preconditions.checkNotNull(assignments, "assignments is null");
        Preconditions.checkArgument(!assignments.isEmpty(), "assignments is empty");

        this.table = table;
        this.attributes = ImmutableMap.copyOf(assignments);
    }

    @JsonProperty("table")
    public TableHandle getTable()
    {
        return table;
    }

    @JsonProperty("assignments")
    public Map<Symbol, ColumnHandle> getAssignments()
    {
        return attributes;
    }

    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.copyOf(attributes.keySet());
    }

    public List<PlanNode> getSources()
    {
        return ImmutableList.of();
    }

    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitTableScan(this, context);
    }
}
