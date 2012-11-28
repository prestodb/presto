package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.compiler.Symbol;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.TableHandle;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

public class TableScan
    extends PlanNode
{
    private final TableHandle table;
    private final Map<Symbol, ColumnHandle> attributes; // symbol -> column

    public TableScan(TableHandle table, Map<Symbol, ColumnHandle> assignments)
    {
        Preconditions.checkNotNull(table, "table is null");
        Preconditions.checkNotNull(assignments, "assignments is null");
        Preconditions.checkArgument(!assignments.isEmpty(), "assignments is empty");

        this.table = table;
        this.attributes = ImmutableMap.copyOf(assignments);
    }

    public TableHandle getTable()
    {
        return table;
    }

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
