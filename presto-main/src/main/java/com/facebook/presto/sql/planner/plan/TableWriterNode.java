package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.sql.analyzer.Symbol;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class TableWriterNode
        extends PlanNode
{
    private final PlanNode source;
    private final TableHandle tableHandle;
    private final Symbol output;
    private final Map<Symbol, ColumnHandle> columns;

    @JsonCreator
    public TableWriterNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("table") TableHandle table,
            @JsonProperty("columns") Map<Symbol, ColumnHandle> columns,
            @JsonProperty("output") Symbol output)
    {
        super(id);

        this.columns = columns;
        this.output = output;
        this.source = checkNotNull(source, "source is null");
        this.tableHandle = table;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public TableHandle getTable()
    {
        return tableHandle;
    }

    @JsonProperty
    public Map<Symbol, ColumnHandle> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public Symbol getOutput()
    {
        return output;
    }

    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.of(output);
    }

    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitTableWriter(this, context);
    }
}
