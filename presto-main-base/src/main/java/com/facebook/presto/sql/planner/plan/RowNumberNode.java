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
import com.google.errorprone.annotations.Immutable;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@Immutable
public final class RowNumberNode
        extends InternalPlanNode
{
    private final PlanNode source;
    private final List<VariableReferenceExpression> partitionBy;
    private final Optional<Integer> maxRowCountPerPartition;
    private final VariableReferenceExpression rowNumberVariable;
    private final boolean partial;
    private final Optional<VariableReferenceExpression> hashVariable;

    @JsonCreator
    public RowNumberNode(
            Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("partitionBy") List<VariableReferenceExpression> partitionBy,
            @JsonProperty("rowNumberVariable") VariableReferenceExpression rowNumberVariable,
            @JsonProperty("maxRowCountPerPartition") Optional<Integer> maxRowCountPerPartition,
            @JsonProperty("partial") boolean partial,
            @JsonProperty("hashVariable") Optional<VariableReferenceExpression> hashVariable)
    {
        this(sourceLocation, id, Optional.empty(), source, partitionBy, rowNumberVariable, maxRowCountPerPartition, partial, hashVariable);
    }

    public RowNumberNode(
            Optional<SourceLocation> sourceLocation,
            PlanNodeId id,
            Optional<PlanNode> statsEquivalentPlanNode,
            PlanNode source,
            List<VariableReferenceExpression> partitionBy,
            VariableReferenceExpression rowNumberVariable,
            Optional<Integer> maxRowCountPerPartition,
            boolean partial,
            Optional<VariableReferenceExpression> hashVariable)
    {
        super(sourceLocation, id, statsEquivalentPlanNode);

        requireNonNull(source, "source is null");
        requireNonNull(partitionBy, "partitionBy is null");
        requireNonNull(rowNumberVariable, "rowNumberVariable is null");
        requireNonNull(maxRowCountPerPartition, "maxRowCountPerPartition is null");
        requireNonNull(hashVariable, "hashVariable is null");

        checkState(!partial || maxRowCountPerPartition.isPresent(), "RowNumberNode cannot be partial when maxRowCountPerPartition is not present");

        this.source = source;
        this.partitionBy = ImmutableList.copyOf(partitionBy);
        this.rowNumberVariable = rowNumberVariable;
        this.maxRowCountPerPartition = maxRowCountPerPartition;
        this.partial = partial;
        this.hashVariable = hashVariable;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        ImmutableList.Builder<VariableReferenceExpression> output = ImmutableList.builder();
        output.addAll(source.getOutputVariables());
        if (!partial) {
            output.add(rowNumberVariable);
        }
        return output.build();
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public List<VariableReferenceExpression> getPartitionBy()
    {
        return partitionBy;
    }

    @JsonProperty
    public VariableReferenceExpression getRowNumberVariable()
    {
        return rowNumberVariable;
    }

    @JsonProperty
    public Optional<Integer> getMaxRowCountPerPartition()
    {
        return maxRowCountPerPartition;
    }

    @JsonProperty
    public boolean isPartial()
    {
        return partial;
    }

    @JsonProperty
    public Optional<VariableReferenceExpression> getHashVariable()
    {
        return hashVariable;
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitRowNumber(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new RowNumberNode(getSourceLocation(), getId(), getStatsEquivalentPlanNode(), Iterables.getOnlyElement(newChildren), partitionBy, rowNumberVariable, maxRowCountPerPartition, partial, hashVariable);
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new RowNumberNode(getSourceLocation(), getId(), statsEquivalentPlanNode, source, partitionBy, rowNumberVariable, maxRowCountPerPartition, partial, hashVariable);
    }
}
