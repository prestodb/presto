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

import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Immutable
public class UpdateNode
        extends InternalPlanNode
{
    private final PlanNode source;
    private final VariableReferenceExpression rowId;
    private final List<VariableReferenceExpression> columnValueAndRowIdSymbols;
    private final List<VariableReferenceExpression> outputVariables;

    @JsonCreator
    public UpdateNode(
            Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("rowId") VariableReferenceExpression rowId,
            @JsonProperty("columnValueAndRowIdSymbols") List<VariableReferenceExpression> columnValueAndRowIdSymbols,
            @JsonProperty("outputVariables") List<VariableReferenceExpression> outputVariables)
    {
        this(source.getSourceLocation(), id, Optional.empty(), source, rowId, columnValueAndRowIdSymbols, outputVariables);
    }

    public UpdateNode(
            Optional<SourceLocation> sourceLocation,
            PlanNodeId id,
            Optional<PlanNode> statsEquivalentPlanNode,
            PlanNode source,
            VariableReferenceExpression rowId,
            List<VariableReferenceExpression> columnValueAndRowIdSymbols,
            List<VariableReferenceExpression> outputVariables)
    {
        super(sourceLocation, id, statsEquivalentPlanNode);

        this.source = requireNonNull(source, "source is null");
        this.rowId = requireNonNull(rowId, "rowId is null");
        this.columnValueAndRowIdSymbols = ImmutableList.copyOf(requireNonNull(columnValueAndRowIdSymbols, "columnValueAndRowIdSymbols is null"));
        this.outputVariables = ImmutableList.copyOf(requireNonNull(outputVariables, "outputVariables is null"));
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public VariableReferenceExpression getRowId()
    {
        return rowId;
    }

    @JsonProperty
    public List<VariableReferenceExpression> getColumnValueAndRowIdSymbols()
    {
        return columnValueAndRowIdSymbols;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitUpdate(this, context);
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new UpdateNode(getSourceLocation(), getId(), statsEquivalentPlanNode, source, rowId, columnValueAndRowIdSymbols, outputVariables);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new UpdateNode(getSourceLocation(), getId(), Optional.of(Iterables.getOnlyElement(newChildren)), source, rowId, columnValueAndRowIdSymbols, outputVariables);
    }
}
