/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.OutputTableHandle;
import com.facebook.presto.sql.planner.Symbol;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class TableWriterNode
        extends PlanNode
{
    private final PlanNode source;
    private final OutputTableHandle target;
    private final List<Symbol> outputs;
    private final List<Symbol> columns;
    private final List<String> columnNames;
    private final Optional<Symbol> sampleWeightSymbol;
    private final String catalog;
    private final TableMetadata tableMetadata;
    private final boolean sampleWeightSupported;

    @JsonCreator
    public TableWriterNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("target") OutputTableHandle target,
            @JsonProperty("columns") List<Symbol> columns,
            @JsonProperty("columnNames") List<String> columnNames,
            @JsonProperty("outputs") List<Symbol> outputs,
            @JsonProperty("sampleWeightSymbol") Optional<Symbol> sampleWeightSymbol)
    {
        this(id, source, target, columns, columnNames, outputs, sampleWeightSymbol, null, null, false);
    }

    public TableWriterNode(
            PlanNodeId id,
            PlanNode source,
            OutputTableHandle target,
            List<Symbol> columns,
            List<String> columnNames,
            List<Symbol> outputs,
            Optional<Symbol> sampleWeightSymbol,
            String catalog,
            TableMetadata tableMetadata,
            boolean sampleWeightSupported)
    {
        super(id);

        checkNotNull(columns, "columns is null");
        checkNotNull(columnNames, "columnNames is null");
        checkArgument(columns.size() == columnNames.size(), "columns and columnNames sizes don't match");
        checkArgument((target == null) ^ (catalog == null && tableMetadata == null), "exactly one of target or (catalog, tableMetadata) must be set");

        this.source = checkNotNull(source, "source is null");
        this.target = target;
        this.columns = ImmutableList.copyOf(columns);
        this.columnNames = ImmutableList.copyOf(columnNames);
        this.outputs = ImmutableList.copyOf(checkNotNull(outputs, "outputs is null"));
        this.sampleWeightSymbol = checkNotNull(sampleWeightSymbol, "sampleWeightSymbol is null");
        this.catalog = catalog;
        this.tableMetadata = tableMetadata;
        this.sampleWeightSupported = sampleWeightSupported;
    }

    public String getCatalog()
    {
        return catalog;
    }

    public TableMetadata getTableMetadata()
    {
        return tableMetadata;
    }

    public boolean isSampleWeightSupported()
    {
        return sampleWeightSupported;
    }

    @JsonProperty
    public Optional<Symbol> getSampleWeightSymbol()
    {
        return sampleWeightSymbol;
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public OutputTableHandle getTarget()
    {
        return target;
    }

    @JsonProperty
    public List<Symbol> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public List<String> getColumnNames()
    {
        return columnNames;
    }

    @JsonProperty("outputs")
    @Override
    public List<Symbol> getOutputSymbols()
    {
        return outputs;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitTableWriter(this, context);
    }
}
