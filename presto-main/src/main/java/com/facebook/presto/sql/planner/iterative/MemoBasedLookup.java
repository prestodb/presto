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

package com.facebook.presto.sql.planner.iterative;

import com.facebook.presto.Session;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.PlanNodeCostEstimate;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class MemoBasedLookup
        implements Lookup
{
    private final Memo memo;
    private final Map<PlanNode, PlanNodeStatsEstimate> stats = new HashMap<>();
    private final Map<PlanNode, PlanNodeCostEstimate> costs = new HashMap<>();
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;

    public MemoBasedLookup(Memo memo, StatsCalculator statsCalculator, CostCalculator costCalculator)
    {
        this.memo = memo;
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
    }

    @Override
    public PlanNode resolve(PlanNode node)
    {
        if (node instanceof GroupReference) {
            return memo.getNode(((GroupReference) node).getGroupId());
        }
        return node;
    }

    // todo[LO] maybe lookup passed to stats calculator should be constrained so only
    //          methods for obtaining traits and only for self/sources would work?

    @Override
    public PlanNodeStatsEstimate getStats(PlanNode planNode, Session session, Map<Symbol, Type> types)
    {
        // cannot use Map.computeIfAbsent due to stats map modification in the mappingFunction callback
        PlanNode key = resolve(planNode);
        if (!stats.containsKey(key)) {
            PlanNodeStatsEstimate statsEstimate = statsCalculator.calculateStats(key, this, session, types);
            stats.put(key, statsEstimate);
        }
        return stats.get(key);
    }

    @Override
    public PlanNodeCostEstimate getCumulativeCost(PlanNode planNode, Session session, Map<Symbol, Type> types)
    {
        // cannot use Map.computeIfAbsent due to costs map modification in the mappingFunction callback
        PlanNode key = resolve(planNode);
        if (!costs.containsKey(key)) {
            PlanNodeCostEstimate cost = costCalculator.calculateCumulativeCost(key, this, session, types);
            costs.put(key, cost);
        }
        return costs.get(key);
    }
}
