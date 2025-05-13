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

package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.cost.PlanCostEstimate;
import com.facebook.presto.spi.plan.PlanNode;

import static java.util.Objects.requireNonNull;

public class PlanNodeWithCost
{
    private final PlanNode planNode;
    private final PlanCostEstimate cost;

    public PlanNodeWithCost(PlanCostEstimate cost, PlanNode planNode)
    {
        this.cost = requireNonNull(cost, "cost is null");
        this.planNode = requireNonNull(planNode, "planNode is null");
    }

    public PlanNode getPlanNode()
    {
        return planNode;
    }

    public PlanCostEstimate getCost()
    {
        return cost;
    }
}
