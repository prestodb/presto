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

import java.util.Map;
import java.util.function.Function;

import static com.facebook.presto.cost.PlanNodeCostEstimate.INFINITE_COST;
import static com.facebook.presto.cost.PlanNodeStatsEstimate.UNKNOWN_STATS;
import static com.google.common.base.Verify.verify;

public interface Lookup
{
    /**
     * Resolves a node by materializing GroupReference nodes
     * representing symbolic references to other nodes.
     * <p>
     * If the node is not a GroupReference, it returns the
     * argument as is.
     */
    PlanNode resolve(PlanNode node);

    PlanNodeStatsEstimate getStats(PlanNode node, Session session, Map<Symbol, Type> types);

    PlanNodeCostEstimate getCumulativeCost(PlanNode node, Session session, Map<Symbol, Type> types);

    /**
     * A Lookup implementation that does not perform lookup. It satisfies contract
     * by rejecting {@link GroupReference}-s.
     */
    static Lookup noLookup()
    {
        return new Lookup()
        {
            @Override
            public PlanNode resolve(PlanNode node)
            {
                verify(!(node instanceof GroupReference), "Unexpected GroupReference");
                return node;
            }

            @Override
            public PlanNodeStatsEstimate getStats(PlanNode node, Session session, Map<Symbol, Type> types)
            {
                return UNKNOWN_STATS;
            }

            @Override
            public PlanNodeCostEstimate getCumulativeCost(PlanNode node, Session session, Map<Symbol, Type> types)
            {
                return INFINITE_COST;
            }
        };
    }

    static Lookup from(Function<GroupReference, PlanNode> resolver)
    {
        return from(resolver,
                (planNode, lookup, session, types) -> UNKNOWN_STATS,
                (planNode, lookup, session, types) -> INFINITE_COST);
    }

    static Lookup from(Function<GroupReference, PlanNode> resolver, StatsCalculator statsCalculator, CostCalculator costCalculator)
    {
        return new Lookup()
        {
            @Override
            public PlanNode resolve(PlanNode node)
            {
                if (node instanceof GroupReference) {
                    return resolver.apply((GroupReference) node);
                }

                return node;
            }

            @Override
            public PlanNodeStatsEstimate getStats(PlanNode node, Session session, Map<Symbol, Type> types)
            {
                return statsCalculator.calculateStats(resolve(node), this, session, types);
            }

            @Override
            public PlanNodeCostEstimate getCumulativeCost(PlanNode node, Session session, Map<Symbol, Type> types)
            {
                return costCalculator.calculateCumulativeCost(node, this, session, types);
            }
        };
    }
}
