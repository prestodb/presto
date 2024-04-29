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

import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class ConnectorJoinNode
        extends PlanNode
{
    private final List<PlanNode> sources;
    private final JoinType type;
    private final Set<EquiJoinClause> criteria;
    private final Set<RowExpression> filters;
    private final Optional<JoinDistributionType> distributionType;
    private final List<VariableReferenceExpression> outputVariables;

    public ConnectorJoinNode(
            PlanNodeId id,
            List<PlanNode> sources,
            Optional<PlanNode> statsEquivalentPlanNode,
            JoinType type,
            Set<EquiJoinClause> criteria,
            Set<RowExpression> filters,
            Optional<JoinDistributionType> distributionType,
            List<VariableReferenceExpression> outputVariables)
    {
        super(Optional.empty(), id, statsEquivalentPlanNode);
        this.sources = requireNonNull(sources, "sources is null");
        this.type = requireNonNull(type, "type is null");
        this.criteria = requireNonNull(criteria, "criteria is null");
        this.filters = requireNonNull(filters, "filters is null");
        this.distributionType = requireNonNull(distributionType, "distributionType is null");
        this.outputVariables = requireNonNull(outputVariables, "outputVariables is null");
    }

    @Override
    public List<PlanNode> getSources()
    {
        return sources;
    }

    public JoinType getType()
    {
        return type;
    }

    public Set<EquiJoinClause> getCriteria()
    {
        return criteria;
    }

    public Set<RowExpression> getFilters()
    {
        return filters;
    }

    public Optional<JoinDistributionType> getDistributionType()
    {
        return distributionType;
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new ConnectorJoinNode(getId(), newChildren, getStatsEquivalentPlanNode(), type, criteria, filters, distributionType, outputVariables);
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new ConnectorJoinNode(getId(), getSources(), getStatsEquivalentPlanNode(), getType(), getCriteria(), getFilters(), getDistributionType(), outputVariables);
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
        ConnectorJoinNode that = (ConnectorJoinNode) o;
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

    @Override
    public String toString()
    {
        return "ConnectorJoinNode{" +
                "sources=" + sources +
                ", type=" + type +
                ", criteria=" + criteria +
                ", filters=" + filters +
                ", outputVariables=" + outputVariables +
                '}';
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitConnectorJoinNode(this, context);
    }
}
