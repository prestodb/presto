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
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class IndexJoinNode
        extends InternalPlanNode
{
    private final JoinType type;
    private final PlanNode probeSource;
    private final PlanNode indexSource;
    private final List<EquiJoinClause> criteria;
    private final Optional<RowExpression> filter;
    private final Optional<VariableReferenceExpression> probeHashVariable;
    private final Optional<VariableReferenceExpression> indexHashVariable;

    @JsonCreator
    public IndexJoinNode(
            Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("type") JoinType type,
            @JsonProperty("probeSource") PlanNode probeSource,
            @JsonProperty("indexSource") PlanNode indexSource,
            @JsonProperty("criteria") List<EquiJoinClause> criteria,
            @JsonProperty("filter") Optional<RowExpression> filter,
            @JsonProperty("probeHashVariable") Optional<VariableReferenceExpression> probeHashVariable,
            @JsonProperty("indexHashVariable") Optional<VariableReferenceExpression> indexHashVariable)
    {
        this(sourceLocation,
                id,
                Optional.empty(),
                type,
                probeSource,
                indexSource,
                criteria,
                filter,
                probeHashVariable,
                indexHashVariable);
    }

    public IndexJoinNode(
            Optional<SourceLocation> sourceLocation,
            PlanNodeId id,
            Optional<PlanNode> statsEquivalentPlanNode,
            JoinType type,
            PlanNode probeSource,
            PlanNode indexSource,
            List<EquiJoinClause> criteria,
            Optional<RowExpression> filter,
            Optional<VariableReferenceExpression> probeHashVariable,
            Optional<VariableReferenceExpression> indexHashVariable)
    {
        super(sourceLocation, id, statsEquivalentPlanNode);
        this.type = requireNonNull(type, "type is null");
        this.probeSource = requireNonNull(probeSource, "probeSource is null");
        this.indexSource = requireNonNull(indexSource, "indexSource is null");
        this.criteria = ImmutableList.copyOf(requireNonNull(criteria, "criteria is null"));
        this.filter = requireNonNull(filter, "filter is null");
        this.probeHashVariable = requireNonNull(probeHashVariable, "probeHashVariable is null");
        this.indexHashVariable = requireNonNull(indexHashVariable, "indexHashVariable is null");
    }

    @JsonProperty
    public JoinType getType()
    {
        return type;
    }

    @JsonProperty
    public PlanNode getProbeSource()
    {
        return probeSource;
    }

    @JsonProperty
    public PlanNode getIndexSource()
    {
        return indexSource;
    }

    @JsonProperty
    public List<EquiJoinClause> getCriteria()
    {
        return criteria;
    }

    @JsonProperty
    public Optional<RowExpression> getFilter()
    {
        return filter;
    }

    @JsonProperty
    public Optional<VariableReferenceExpression> getProbeHashVariable()
    {
        return probeHashVariable;
    }

    @JsonProperty
    public Optional<VariableReferenceExpression> getIndexHashVariable()
    {
        return indexHashVariable;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(probeSource, indexSource);
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return ImmutableList.<VariableReferenceExpression>builder()
                .addAll(probeSource.getOutputVariables())
                .addAll(indexSource.getOutputVariables())
                .build();
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitIndexJoin(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 2, "expected newChildren to contain 2 nodes");
        return new IndexJoinNode(
                getSourceLocation(),
                getId(),
                getStatsEquivalentPlanNode(),
                type,
                newChildren.get(0),
                newChildren.get(1),
                criteria,
                filter,
                probeHashVariable,
                indexHashVariable);
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new IndexJoinNode(
                getSourceLocation(),
                getId(),
                statsEquivalentPlanNode,
                type,
                probeSource,
                indexSource,
                criteria,
                filter,
                probeHashVariable,
                indexHashVariable);
    }

    public static class EquiJoinClause
    {
        private final VariableReferenceExpression probe;
        private final VariableReferenceExpression index;

        @JsonCreator
        public EquiJoinClause(@JsonProperty("left") VariableReferenceExpression probe, @JsonProperty("right") VariableReferenceExpression index)
        {
            this.probe = requireNonNull(probe, "probe is null");
            this.index = requireNonNull(index, "index is null");
        }

        @JsonProperty("left")
        public VariableReferenceExpression getProbe()
        {
            return probe;
        }

        @JsonProperty("right")
        public VariableReferenceExpression getIndex()
        {
            return index;
        }
    }
}
