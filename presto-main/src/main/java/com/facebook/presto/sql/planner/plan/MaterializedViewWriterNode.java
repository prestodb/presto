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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.planner.Symbol;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class MaterializedViewWriterNode
        extends PlanNode
{
    private final PlanNode source;
    private final TableHandle tableHandle;
    private final Symbol output;
    private final Map<Symbol, ColumnHandle> columns;

    @JsonCreator
    public MaterializedViewWriterNode(
            @JsonProperty("id") PlanNodeId id,
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

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.of(output);
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitMaterializedViewWriter(this, context);
    }
}
