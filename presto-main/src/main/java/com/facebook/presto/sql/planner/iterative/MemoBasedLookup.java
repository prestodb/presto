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
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class MemoBasedLookup
        implements Lookup
{
    private final Memo memo;
    private final Map<PlanNode, PlanNodeStatsEstimate> statsMap = new HashMap<>();
    private final StatsCalculator statsCalculator;

    public MemoBasedLookup(Memo memo, StatsCalculator statsCalculator)
    {
        this.memo = requireNonNull(memo, "memo can not be null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
    }

    @Override
    public PlanNode resolve(PlanNode node)
    {
        if (node instanceof GroupReference) {
            return memo.getNode(((GroupReference) node).getGroupId());
        }
        return node;
    }

    // todo[LO] maybe lookup passed to stats/cost calculator should be constrained so only
    //          methods for obtaining traits and only for self and sources would be allowed?

    @Override
    public PlanNodeStatsEstimate getStats(PlanNode planNode, Session session, Map<Symbol, Type> types)
    {
        PlanNode key = resolve(planNode);
        if (!statsMap.containsKey(key)) {
            // cannot use Map.computeIfAbsent due to stats map modification in the mappingFunction callback
            PlanNodeStatsEstimate stats = statsCalculator.calculateStats(key, this, session, types);
            requireNonNull(stats, "returned stats can not be null");
            checkState(statsMap.put(key, stats) == null, "statistics for " + key + "already computed");
        }
        return statsMap.get(key);
    }
}
