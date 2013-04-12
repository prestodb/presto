package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.analyzer.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

@Immutable
public class TableWriterNode
        extends PlanNode
{
    private final PlanNode source;
    private final TableHandle tableHandle;
    private final List<Symbol> inputSymbols;
    private final Map<Symbol, Type> inputTypes;
    private final Map<Symbol, ColumnHandle> columnHandles;
    private final Map<Symbol, Type> outputTypes;

    @JsonCreator
    public TableWriterNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("tableHandle") TableHandle tableHandle,
            @JsonProperty("inputSymbols") List<Symbol> inputSymbols,
            @JsonProperty("inputTypes") Map<Symbol, Type> inputTypes,
            @JsonProperty("columnHandles") Map<Symbol, ColumnHandle> columnHandles,
            @JsonProperty("outputTypes") Map<Symbol, Type> outputTypes)
    {
        super(id);

        this.source = checkNotNull(source, "source is null");
        this.tableHandle = tableHandle;
        this.inputSymbols = checkNotNull(inputSymbols, "inputSymbols is null");
        this.inputTypes = checkNotNull(inputTypes, "inputTypes is null");
        this.columnHandles = checkNotNull(columnHandles, "columnHandles is null");
        this.outputTypes = checkNotNull(outputTypes, "outputTypes is null");

        checkState(inputSymbols.size() == inputTypes.size(), "Got %s symbols and %s types", inputSymbols.size(), inputTypes.size());
        checkState(inputSymbols.size() == columnHandles.size(), "Got %s symbols and %s column handles", inputSymbols.size(), columnHandles.size());
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
    public TableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public List<Symbol> getInputSymbols()
    {
        return inputSymbols;
    }

    @JsonProperty
    public Map<Symbol, Type> getInputTypes()
    {
        return inputTypes;
    }

    @JsonProperty
    public Map<Symbol, ColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }

    @JsonProperty
    public Map<Symbol, Type> getOutputTypes()
    {
        return outputTypes;
    }

    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.copyOf(outputTypes.keySet());
    }

    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitTableWriter(this, context);
    }
}
