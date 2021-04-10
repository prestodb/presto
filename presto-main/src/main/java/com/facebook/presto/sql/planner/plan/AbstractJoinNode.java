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

import java.util.Map;

public abstract class AbstractJoinNode
        extends InternalPlanNode
{
    protected AbstractJoinNode(PlanNodeId planNodeId)
    {
        super(planNodeId);
    }

    public abstract Map<String, VariableReferenceExpression> getDynamicFilters();

    public abstract PlanNode getProbe();

    public abstract PlanNode getBuild();
}
