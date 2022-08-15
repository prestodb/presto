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
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.plan.InternalPlanNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Used to represent a stats equivalent plan which has a downstream Limit node.
 * We include the Limit node in the definition as it can affect its stats.
 */
public class StatsEquivalentPlanNodeWithLimit
        extends InternalPlanNode
{
    // TODO: We are storing duplicated information at multiple levels. Look into if we can optimize it.
    private final PlanNode plan;
    private final LimitNode limit;

    @JsonCreator
    public StatsEquivalentPlanNodeWithLimit(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("plan") PlanNode plan,
            @JsonProperty("limit") LimitNode limit)
    {
        super(Optional.empty(), id, Optional.empty());
        this.plan = requireNonNull(plan, "plan is null");
        this.limit = requireNonNull(limit, "limit is null");
    }

    @Override
    public List<PlanNode> getSources()
    {
        return plan.getSources();
    }

    @JsonProperty
    public PlanNode getPlan()
    {
        return plan;
    }

    @JsonProperty
    public PlanNode getLimit()
    {
        return limit;
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return plan.getOutputVariables();
    }

    @Override
    @JsonProperty
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Unexpected call replaceChildren for %s", this));
    }

    @Override
    @JsonProperty
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Cannot assign canonical plan id for: %s", this));
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitStatsEquivalentPlanNodeWithLimit(this, context);
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
        StatsEquivalentPlanNodeWithLimit that = (StatsEquivalentPlanNodeWithLimit) o;
        return Objects.equals(plan, that.plan) &&
                Objects.equals(limit, that.limit);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(plan, limit);
    }
}
