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
import com.facebook.presto.spi.plan.PlanVisitor;

import java.util.Optional;

public abstract class InternalPlanNode
        extends PlanNode
{
    protected InternalPlanNode(Optional<SourceLocation> sourceLocation, PlanNodeId planNodeId, Optional<PlanNode> statsEquivalentPlanNode)
    {
        super(sourceLocation, planNodeId, statsEquivalentPlanNode);
    }

    public final <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        if (visitor instanceof InternalPlanVisitor) {
            return accept((InternalPlanVisitor<R, C>) visitor, context);
        }
        return visitor.visitPlan(this, context);
    }

    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitPlan(this, context);
    }
}
