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

import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
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
    private final Type type;
    private final PlanNode probeSource;
    private final PlanNode indexSource;
    private final List<EquiJoinClause> criteria;
    private final Optional<VariableReferenceExpression> probeHashVariable;
    private final Optional<VariableReferenceExpression> indexHashVariable;

    @JsonCreator
    public IndexJoinNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("type") Type type,
            @JsonProperty("probeSource") PlanNode probeSource,
            @JsonProperty("indexSource") PlanNode indexSource,
            @JsonProperty("criteria") List<EquiJoinClause> criteria,
            @JsonProperty("probeHashVariable") Optional<VariableReferenceExpression> probeHashVariable,
            @JsonProperty("indexHashVariable") Optional<VariableReferenceExpression> indexHashVariable)
    {
        super(id);
        this.type = requireNonNull(type, "type is null");
        this.probeSource = requireNonNull(probeSource, "probeSource is null");
        this.indexSource = requireNonNull(indexSource, "indexSource is null");
        this.criteria = ImmutableList.copyOf(requireNonNull(criteria, "criteria is null"));
        this.probeHashVariable = requireNonNull(probeHashVariable, "probeHashVariable is null");
        this.indexHashVariable = requireNonNull(indexHashVariable, "indexHashVariable is null");
    }

    public enum Type
    {
        INNER("Inner"),
        SOURCE_OUTER("SourceOuter");

        private final String joinLabel;

        private Type(String joinLabel)
        {
            this.joinLabel = joinLabel;
        }

        public String getJoinLabel()
        {
            return joinLabel;
        }
    }

    @JsonProperty
    public Type getType()
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
        return new IndexJoinNode(getId(), type, newChildren.get(0), newChildren.get(1), criteria, probeHashVariable, indexHashVariable);
    }

    public static class EquiJoinClause
    {
        private final VariableReferenceExpression probe;
        private final VariableReferenceExpression index;

        @JsonCreator
        public EquiJoinClause(@JsonProperty("probe") VariableReferenceExpression probe, @JsonProperty("index") VariableReferenceExpression index)
        {
            this.probe = requireNonNull(probe, "probe is null");
            this.index = requireNonNull(index, "index is null");
        }

        @JsonProperty("probe")
        public VariableReferenceExpression getProbe()
        {
            return probe;
        }

        @JsonProperty("index")
        public VariableReferenceExpression getIndex()
        {
            return index;
        }
    }
}
