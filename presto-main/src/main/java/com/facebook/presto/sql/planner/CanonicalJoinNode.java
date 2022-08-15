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
package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class CanonicalJoinNode
        extends PlanNode
{
    private final List<PlanNode> sources;
    private final JoinNode.Type type;
    private final Set<JoinNode.EquiJoinClause> criteria;
    private final Set<RowExpression> filters;
    private final List<VariableReferenceExpression> outputVariables;

    @JsonCreator
    public CanonicalJoinNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("sources") List<PlanNode> sources,
            @JsonProperty("type") JoinNode.Type type,
            @JsonProperty("criteria") Set<JoinNode.EquiJoinClause> criteria,
            @JsonProperty("filter") Set<RowExpression> filters,
            @JsonProperty("outputVariables") List<VariableReferenceExpression> outputVariables)
    {
        super(Optional.empty(), id, Optional.empty());
        this.sources = requireNonNull(sources, "sources is null");
        this.type = requireNonNull(type, "type is null");
        this.criteria = requireNonNull(criteria, "criteria is null");
        this.filters = requireNonNull(filters, "filters is null");
        this.outputVariables = requireNonNull(outputVariables, "outputVariables is null");
    }

    @Override
    @JsonProperty
    public List<PlanNode> getSources()
    {
        return sources;
    }

    @JsonProperty
    public JoinNode.Type getType()
    {
        return type;
    }

    @JsonProperty
    public Set<JoinNode.EquiJoinClause> getCriteria()
    {
        return criteria;
    }

    @JsonProperty
    public Set<RowExpression> getFilters()
    {
        return filters;
    }

    @Override
    @JsonProperty
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }

    @Override
    @JsonProperty
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new CanonicalJoinNode(getId(), newChildren, type, criteria, filters, outputVariables);
    }

    @Override
    @JsonProperty
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Cannot assign canonical plan id to Canonical join node: %s", this));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CanonicalJoinNode that = (CanonicalJoinNode) o;
        return Objects.equals(sources, that.sources) &&
                Objects.equals(type, that.type) &&
                Objects.equals(criteria, that.criteria) &&
                Objects.equals(filters, that.filters) &&
                Objects.equals(outputVariables, that.outputVariables);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sources, type, criteria, filters, outputVariables);
    }
}
