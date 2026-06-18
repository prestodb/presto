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
package com.facebook.presto.spi.plan;

import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.Utils.checkArgument;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

@Immutable
public final class IndexJoinNode
        extends PlanNode
{
    private final JoinType type;
    private final PlanNode probeSource;
    private final PlanNode indexSource;
    private final List<EquiJoinClause> criteria;
    private final Optional<RowExpression> filter;
    private final Optional<VariableReferenceExpression> probeHashVariable;
    private final Optional<VariableReferenceExpression> indexHashVariable;
    private final List<VariableReferenceExpression> lookupVariables;

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
            @JsonProperty("indexHashVariable") Optional<VariableReferenceExpression> indexHashVariable,
            @JsonProperty("lookupVariables") List<VariableReferenceExpression> lookupVariables)
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
                indexHashVariable,
                lookupVariables);
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
            Optional<VariableReferenceExpression> indexHashVariable,
            List<VariableReferenceExpression> lookupVariables)
    {
        super(sourceLocation, id, statsEquivalentPlanNode);
        this.type = requireNonNull(type, "type is null");
        this.probeSource = requireNonNull(probeSource, "probeSource is null");
        this.indexSource = requireNonNull(indexSource, "indexSource is null");
        this.criteria = unmodifiableList(requireNonNull(criteria, "criteria is null"));
        this.filter = requireNonNull(filter, "filter is null");
        this.probeHashVariable = requireNonNull(probeHashVariable, "probeHashVariable is null");
        this.indexHashVariable = requireNonNull(indexHashVariable, "indexHashVariable is null");
        this.lookupVariables = requireNonNull(lookupVariables, "lookupVariables is null");
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

    @JsonProperty
    public List<VariableReferenceExpression> getLookupVariables()
    {
        return lookupVariables;
    }

    @Override
    public List<PlanNode> getSources()
    {
        List<PlanNode> sources = new ArrayList<>();
        sources.add(probeSource);
        sources.add(indexSource);
        return unmodifiableList(sources);
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        List<VariableReferenceExpression> outputVariables = new ArrayList<VariableReferenceExpression>(probeSource.getOutputVariables());
        outputVariables.addAll(indexSource.getOutputVariables());
        return unmodifiableList(outputVariables);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
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
                indexHashVariable,
                lookupVariables);
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
                indexHashVariable,
                lookupVariables);
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
