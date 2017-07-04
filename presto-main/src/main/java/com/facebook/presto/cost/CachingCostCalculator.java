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

package com.facebook.presto.cost;

import com.facebook.presto.Session;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class CachingCostCalculator
        implements CostCalculator
{
    private final CostCalculator costCalculator;
    private final Map<PlanNode, PlanNodeCostEstimate> costs = new HashMap<>();
    private final Map<PlanNode, PlanNodeCostEstimate> cummulativeCosts = new HashMap<>();

    public CachingCostCalculator(CostCalculator costCalculator)
    {
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
    }

    @Override
    public PlanNodeCostEstimate calculateCost(PlanNode planNode, Lookup lookup, Session session, Map<Symbol, Type> types)
    {
        if (!costs.containsKey(planNode)) {
            // cannot use Map.computeIfAbsent due to costs map modification in the mappingFunction callback
            PlanNodeCostEstimate cost = costCalculator.calculateCost(planNode, lookup, session, types);
            requireNonNull(costs, "computed cost can not be null");
            checkState(costs.put(planNode, cost) == null, "cost for " + planNode + " already computed");
        }
        return costs.get(planNode);
    }

    @Override
    public PlanNodeCostEstimate calculateCumulativeCost(PlanNode planNode, Lookup lookup, Session session, Map<Symbol, Type> types)
    {
        if (!cummulativeCosts.containsKey(planNode)) {
            // cannot use Map.computeIfAbsent due to costs map modification in the mappingFunction callback
            PlanNodeCostEstimate cost = costCalculator.calculateCumulativeCost(planNode, lookup, session, types);
            requireNonNull(cummulativeCosts, "computed cost can not be null");
            checkState(cummulativeCosts.put(planNode, cost) == null, "cost for " + planNode + " already computed");
        }
        return cummulativeCosts.get(planNode);
    }
}
