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
import com.facebook.presto.sql.planner.iterative.rule.ReorderJoins;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Specific {@link StatsCalculator} for {@link ReorderJoins} that recognizes joins and
 * caches join stats by join node id. This is true for {@link ReorderJoins} since
 * it only changes join sides and partitioning.
 */
public class JoinNodeCachingStatsCalculator
        implements StatsCalculator
{
    private final StatsCalculator statsCalculator;
    private final Map<PlanNodeId, PlanNodeStatsEstimate> stats = new HashMap<>();

    public JoinNodeCachingStatsCalculator(StatsCalculator statsCalculator)
    {
        this.statsCalculator = statsCalculator;
    }

    @Override
    public PlanNodeStatsEstimate calculateStats(PlanNode planNode, Lookup lookup, Session session, Map<Symbol, Type> types)
    {
        if (!(planNode instanceof JoinNode)) {
            return statsCalculator.calculateStats(planNode, lookup, session, types);
        }

        PlanNodeId key = planNode.getId();
        if (!stats.containsKey(key)) {
            // cannot use Map.computeIfAbsent due to stats map modification in the mappingFunction callback
            PlanNodeStatsEstimate statsEstimate = statsCalculator.calculateStats(planNode, lookup, session, types);
            requireNonNull(stats, "computed stats can not be null");
            checkState(stats.put(key, statsEstimate) == null, "statistics for " + planNode + " already computed");
        }
        return stats.get(key);
    }
}
