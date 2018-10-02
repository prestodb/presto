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
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class PassthroughStatsRule<T extends PlanNode>
        implements Rule<T>
{
    private final Pattern<T> pattern;

    public PassthroughStatsRule(Class<T> clazz)
    {
        this.pattern = Pattern.typeOf(clazz);
    }

    @Override
    public final Optional<PlanNodeStatsEstimate> calculate(T node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types)
    {
        List<PlanNodeStatsEstimate> planNodeStatsEstimates = node.getSources().stream()
                .map(sourceStats::getStats)
                .collect(toImmutableList());

        double outputRowCount = planNodeStatsEstimates.stream()
                .map(PlanNodeStatsEstimate::getOutputRowCount)
                .reduce(1.0, (totalRowCount, nodeRowCount) -> totalRowCount * nodeRowCount);

        PlanNodeStatsEstimate.Builder outputStats = PlanNodeStatsEstimate.builder();
        outputStats.setOutputRowCount(outputRowCount);
        planNodeStatsEstimates.forEach(estimate -> estimate.getSymbolsWithKnownStatistics().stream()
                .filter(symbol -> node.getOutputSymbols().contains(symbol))
                .forEach(symbol -> outputStats.addSymbolStatistics(symbol, estimate.getSymbolStatistics(symbol))));

        return Optional.of(outputStats.build());
    }

    @Override
    public Pattern<T> getPattern()
    {
        return pattern;
    }
}
