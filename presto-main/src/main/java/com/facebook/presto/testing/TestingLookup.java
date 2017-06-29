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

package com.facebook.presto.testing;

import com.facebook.presto.Session;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.PlanNodeCostEstimate;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class TestingLookup
        implements Lookup
{
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;
    private final Map<PlanNode, PlanNodeStatsEstimate> stats = new HashMap<>();
    private final Map<PlanNode, PlanNodeCostEstimate> costs = new HashMap<>();
    private final Function<GroupReference, PlanNode> resolver;

    public TestingLookup(StatsCalculator statsCalculator, CostCalculator costCalculator, Function<GroupReference, PlanNode> resolver)
    {
        this(statsCalculator, costCalculator, ImmutableMap.of(), ImmutableMap.of(), resolver);
    }

    private TestingLookup(StatsCalculator statsCalculator, CostCalculator costCalculator, Map<PlanNode, PlanNodeStatsEstimate> stats, Map<PlanNode, PlanNodeCostEstimate> costs, Function<GroupReference, PlanNode> resolver)
    {
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
        this.stats.putAll(stats);
        this.costs.putAll(costs);
        this.resolver = requireNonNull(resolver, "resolver is null");
    }

    public TestingLookup withStats(Map<PlanNode, PlanNodeStatsEstimate> stats)
    {
        return new TestingLookup(statsCalculator, costCalculator, stats, ImmutableMap.of(), resolver);
    }

    @Override
    public PlanNode resolve(PlanNode node)
    {
        if (node instanceof GroupReference) {
            return resolver.apply((GroupReference) node);
        }
        return node;
    }

    @Override
    public PlanNodeStatsEstimate getStats(PlanNode planNode, Session session, Map<Symbol, Type> types)
    {
        PlanNode resolved = resolve(planNode);
        PlanNodeStatsEstimate statsEstimate = stats.get(resolved);
        if (statsEstimate == null) {
            statsEstimate = statsCalculator.calculateStats(resolved, this, session, types);
            stats.put(resolved, statsEstimate);
        }
        return statsEstimate;
    }

    @Override
    public PlanNodeCostEstimate getCumulativeCost(PlanNode planNode, Session session, Map<Symbol, Type> types)
    {
        PlanNode resolved = resolve(planNode);
        PlanNodeCostEstimate costEstimate = costs.get(resolved);
        if (costEstimate == null) {
            costEstimate = costCalculator.calculateCumulativeCost(resolved, this, session, types);
            costs.put(resolved, costEstimate);
        }
        return costEstimate;
    }
}
