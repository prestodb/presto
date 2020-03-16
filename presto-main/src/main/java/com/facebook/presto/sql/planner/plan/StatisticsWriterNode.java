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

import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class StatisticsWriterNode
        extends InternalPlanNode
{
    private final PlanNode source;
    private final TableHandle tableHandle;
    private final VariableReferenceExpression rowCountVariable;
    private final boolean rowCountEnabled;
    private final StatisticAggregationsDescriptor<VariableReferenceExpression> descriptor;

    @JsonCreator
    public StatisticsWriterNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("tableHandle") TableHandle tableHandle,
            @JsonProperty("rowCountVariable") VariableReferenceExpression rowCountVariable,
            @JsonProperty("rowCountEnabled") boolean rowCountEnabled,
            @JsonProperty("descriptor") StatisticAggregationsDescriptor<VariableReferenceExpression> descriptor)
    {
        super(id);
        this.source = requireNonNull(source, "source is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.rowCountVariable = requireNonNull(rowCountVariable, "rowCountVariable is null");
        this.rowCountEnabled = rowCountEnabled;
        this.descriptor = requireNonNull(descriptor, "descriptor is null");
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
    public StatisticAggregationsDescriptor<VariableReferenceExpression> getDescriptor()
    {
        return descriptor;
    }

    @JsonProperty
    public VariableReferenceExpression getRowCountVariable()
    {
        return rowCountVariable;
    }

    @JsonProperty
    public boolean isRowCountEnabled()
    {
        return rowCountEnabled;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return ImmutableList.of(rowCountVariable);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new StatisticsWriterNode(
                getId(),
                Iterables.getOnlyElement(newChildren),
                tableHandle,
                rowCountVariable,
                rowCountEnabled,
                descriptor);
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitStatisticsWriterNode(this, context);
    }
}
