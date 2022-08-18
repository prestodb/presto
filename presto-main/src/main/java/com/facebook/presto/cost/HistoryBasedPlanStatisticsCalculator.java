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
import com.facebook.presto.common.plan.PlanCanonicalizationStrategy;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeWithHash;
import com.facebook.presto.spi.statistics.HistoricalPlanStatistics;
import com.facebook.presto.spi.statistics.HistoryBasedPlanStatisticsProvider;
import com.facebook.presto.spi.statistics.HistoryBasedSourceInfo;
import com.facebook.presto.sql.planner.PlanHasher;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.facebook.presto.SystemSessionProperties.useHistoryBasedPlanStatisticsEnabled;
import static com.facebook.presto.common.plan.PlanCanonicalizationStrategy.historyBasedPlanCanonicalizationStrategyList;
import static com.facebook.presto.sql.planner.iterative.Plans.resolveGroupReferences;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class HistoryBasedPlanStatisticsCalculator
        implements StatsCalculator
{
    private final Supplier<HistoryBasedPlanStatisticsProvider> historyBasedPlanStatisticsProvider;
    private final StatsCalculator delegate;
    private final PlanHasher planHasher;

    public HistoryBasedPlanStatisticsCalculator(
            Supplier<HistoryBasedPlanStatisticsProvider> historyBasedPlanStatisticsProvider,
            StatsCalculator delegate,
            PlanHasher planHasher)
    {
        this.historyBasedPlanStatisticsProvider = requireNonNull(historyBasedPlanStatisticsProvider, "historyBasedPlanStatisticsProvider is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.planHasher = requireNonNull(planHasher, "planHasher is null");
    }

    @Override
    public PlanNodeStatsEstimate calculateStats(PlanNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types)
    {
        PlanNodeStatsEstimate delegateStats = delegate.calculateStats(node, sourceStats, lookup, session, types);
        return getStatistics(node, session, lookup, delegateStats);
    }

    @VisibleForTesting
    public PlanHasher getPlanHasher()
    {
        return planHasher;
    }

    private PlanNodeStatsEstimate getStatistics(PlanNode planNode, Session session, Lookup lookup, PlanNodeStatsEstimate delegateStats)
    {
        PlanNode plan = resolveGroupReferences(planNode, lookup);
        if (!useHistoryBasedPlanStatisticsEnabled(session)) {
            return delegateStats;
        }

        ImmutableMap.Builder<PlanCanonicalizationStrategy, String> allHashesBuilder = ImmutableMap.builder();

        for (PlanCanonicalizationStrategy strategy : historyBasedPlanCanonicalizationStrategyList()) {
            Optional<String> hash = plan.getStatsEquivalentPlanNode().flatMap(node -> planHasher.hash(node, strategy));
            hash.ifPresent(string -> allHashesBuilder.put(strategy, string));
        }

        Map<PlanCanonicalizationStrategy, String> allHashes = allHashesBuilder.build();
        List<PlanNodeWithHash> planNodeWithHashes = allHashes.values().stream()
                .distinct()
                .map(hash -> new PlanNodeWithHash(plan, Optional.of(hash)))
                .collect(toImmutableList());

        // If no hashes are found, try to fetch statistics without hash.
        if (planNodeWithHashes.isEmpty()) {
            planNodeWithHashes = ImmutableList.of(new PlanNodeWithHash(plan, Optional.empty()));
        }

        Map<PlanNodeWithHash, HistoricalPlanStatistics> statistics = historyBasedPlanStatisticsProvider.get().getStats(planNodeWithHashes);

        // Return statistics corresponding to first strategy that we find, in order specified by `historyBasedPlanCanonicalizationStrategyList`
        for (PlanCanonicalizationStrategy strategy : historyBasedPlanCanonicalizationStrategyList()) {
            for (Map.Entry<PlanNodeWithHash, HistoricalPlanStatistics> entry : statistics.entrySet()) {
                if (allHashes.containsKey(strategy) && Optional.of(allHashes.get(strategy)).equals(entry.getKey().getHash())) {
                    // TODO: Use better historical statistics
                    return delegateStats.combineStats(entry.getValue().getLastRunStatistics(), new HistoryBasedSourceInfo(entry.getKey().getHash()));
                }
            }
        }

        return Optional.ofNullable(statistics.get(new PlanNodeWithHash(plan, Optional.empty())))
                .map(HistoricalPlanStatistics::getLastRunStatistics)
                .map(planStatistics -> delegateStats.combineStats(planStatistics, new HistoryBasedSourceInfo(Optional.empty())))
                .orElse(delegateStats);
    }
}
