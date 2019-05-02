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
import com.facebook.presto.cost.ComposableStatsCalculator.Rule;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;

import java.util.Optional;

import static com.facebook.presto.sql.planner.plan.Patterns.semiJoin;

public class SemiJoinStatsRule
        implements Rule<SemiJoinNode>
{
    private static final Pattern<SemiJoinNode> PATTERN = semiJoin();

    @Override
    public Pattern<SemiJoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<PlanNodeStatsEstimate> calculate(SemiJoinNode node, StatsProvider statsProvider, Lookup lookup, Session session, TypeProvider types)
    {
        PlanNodeStatsEstimate sourceStats = statsProvider.getStats(node.getSource());

        // For now we just propagate statistics for source symbols.
        // Handling semiJoinOutput symbols requires support for correlation for boolean columns.

        return Optional.of(sourceStats);
    }
}
