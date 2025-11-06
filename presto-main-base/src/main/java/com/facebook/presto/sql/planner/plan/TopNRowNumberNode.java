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
import com.facebook.presto.spi.plan.DataOrganizationSpecification;
import com.facebook.presto.spi.plan.OrderingScheme;
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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public final class TopNRowNumberNode
        extends InternalPlanNode
{
    public enum RankingFunction
    {
        ROW_NUMBER,
        RANK,
        DENSE_RANK
    }

    private final PlanNode source;
    private final DataOrganizationSpecification specification;
    private final RankingFunction rankingFunction;
    private final VariableReferenceExpression rowNumberVariable;
    private final int maxRowCountPerPartition;
    private final boolean partial;
    private final Optional<VariableReferenceExpression> hashVariable;

    @JsonCreator
    public TopNRowNumberNode(
            Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("specification") DataOrganizationSpecification specification,
            @JsonProperty("rankingType") RankingFunction rankingFunction,
            @JsonProperty("rowNumberVariable") VariableReferenceExpression rowNumberVariable,
            @JsonProperty("maxRowCountPerPartition") int maxRowCountPerPartition,
            @JsonProperty("partial") boolean partial,
            @JsonProperty("hashVariable") Optional<VariableReferenceExpression> hashVariable)
    {
        this(sourceLocation, id, Optional.empty(), source, specification, rankingFunction, rowNumberVariable, maxRowCountPerPartition, partial, hashVariable);
    }

    public TopNRowNumberNode(
            Optional<SourceLocation> sourceLocation,
            PlanNodeId id,
            Optional<PlanNode> statsEquivalentPlanNode,
            PlanNode source,
            DataOrganizationSpecification specification,
            RankingFunction rankingFunction,
            VariableReferenceExpression rowNumberVariable,
            int maxRowCountPerPartition,
            boolean partial,
            Optional<VariableReferenceExpression> hashVariable)
    {
        super(sourceLocation, id, statsEquivalentPlanNode);

        requireNonNull(source, "source is null");
        requireNonNull(specification, "specification is null");
        checkArgument(specification.getOrderingScheme().isPresent(), "specification orderingScheme is absent");
        requireNonNull(rowNumberVariable, "rowNumberVariable is null");
        checkArgument(maxRowCountPerPartition > 0, "maxRowCountPerPartition must be > 0");
        requireNonNull(hashVariable, "hashVariable is null");

        this.source = source;
        this.specification = specification;
        this.rankingFunction = rankingFunction;
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
        ImmutableList.Builder<VariableReferenceExpression> builder = ImmutableList.<VariableReferenceExpression>builder().addAll(source.getOutputVariables());

        if (!partial) {
            builder.add(rowNumberVariable);
        }
        return builder.build();
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public DataOrganizationSpecification getSpecification()
    {
        return specification;
    }

    @JsonProperty
    public RankingFunction getRankingFunction()
    {
        return rankingFunction;
    }

    public List<VariableReferenceExpression> getPartitionBy()
    {
        return specification.getPartitionBy();
    }

    public OrderingScheme getOrderingScheme()
    {
        return specification.getOrderingScheme().get();
    }

    @JsonProperty
    public VariableReferenceExpression getRowNumberVariable()
    {
        return rowNumberVariable;
    }

    @JsonProperty
    public int getMaxRowCountPerPartition()
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
        return visitor.visitTopNRowNumber(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new TopNRowNumberNode(getSourceLocation(), getId(), getStatsEquivalentPlanNode(), Iterables.getOnlyElement(newChildren), specification, rankingFunction, rowNumberVariable, maxRowCountPerPartition, partial, hashVariable);
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new TopNRowNumberNode(getSourceLocation(), getId(), statsEquivalentPlanNode, source, specification, rankingFunction, rowNumberVariable, maxRowCountPerPartition, partial, hashVariable);
    }
}
