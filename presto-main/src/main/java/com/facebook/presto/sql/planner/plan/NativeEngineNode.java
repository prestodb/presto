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

import javax.annotation.concurrent.Immutable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Immutable
public class NativeEngineNode
        extends InternalPlanNode
{
    private final PlanNode subPlan;

    @JsonCreator
    public NativeEngineNode(Optional<SourceLocation> sourceLocation, @JsonProperty("id") PlanNodeId id, @JsonProperty("subPlan") PlanNode subPlan)
    {
        this(sourceLocation, id, Optional.empty(), subPlan);
    }

    public NativeEngineNode(Optional<SourceLocation> sourceLocation, PlanNodeId id, Optional<PlanNode> statsEquivalentPlanNode, PlanNode subPlan)
    {
        super(sourceLocation, id, statsEquivalentPlanNode);

        this.subPlan = requireNonNull(subPlan, "subPlan is null");
    }

    @Override
    public List<PlanNode> getSources()
    {
        //return subPlan.getSources();
        return Collections.emptyList();
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return subPlan.getOutputVariables();
    }

    @JsonProperty
    public PlanNode getSubPlan()
    {
        return subPlan;
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        // return new NativeEngineNode(getSourceLocation(), getId(), getStatsEquivalentPlanNode(), Iterables.getOnlyElement(newChildren));
        return new NativeEngineNode(getSourceLocation(), getId(), getStatsEquivalentPlanNode(), subPlan.replaceChildren(newChildren));
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new NativeEngineNode(getSourceLocation(), getId(), statsEquivalentPlanNode, subPlan);
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitNativeEngine(this, context);
    }
}
