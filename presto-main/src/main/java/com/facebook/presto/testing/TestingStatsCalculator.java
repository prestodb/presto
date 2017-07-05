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
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class TestingStatsCalculator
        implements StatsCalculator
{
    private final StatsCalculator statsCalculator;
    private final Map<PlanNodeId, PlanNodeStatsEstimate> stats;

    public TestingStatsCalculator(StatsCalculator statsCalculator, Map<PlanNodeId, PlanNodeStatsEstimate> stats)
    {
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.stats = ImmutableMap.copyOf(requireNonNull(stats, "stats is null"));
    }

    @Override
    public PlanNodeStatsEstimate calculateStats(PlanNode planNode, Lookup lookup, Session session, Map<Symbol, Type> types)
    {
        if (stats.containsKey(planNode.getId())) {
            return stats.get(planNode.getId());
        }

        return statsCalculator.calculateStats(planNode, lookup, session, types);
    }
}
