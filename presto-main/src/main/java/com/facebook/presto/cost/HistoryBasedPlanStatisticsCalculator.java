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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeWithHash;
import com.facebook.presto.spi.statistics.HistoricalPlanStatistics;
import com.facebook.presto.spi.statistics.HistoryBasedPlanStatisticsProvider;
import com.facebook.presto.spi.statistics.PlanStatistics;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.facebook.presto.SystemSessionProperties.useHistoryBasedPlanStatisticsEnabled;
import static com.facebook.presto.common.plan.PlanCanonicalizationStrategy.historyBasedPlanCanonicalizationStrategyList;
import static com.facebook.presto.spi.StandardErrorCode.PLAN_SERIALIZATION_ERROR;
import static com.facebook.presto.sql.planner.CanonicalPlanGenerator.generateCanonicalPlan;
import static com.facebook.presto.sql.planner.iterative.Plans.resolveGroupReferences;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.hash.Hashing.sha256;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class HistoryBasedPlanStatisticsCalculator
        implements StatsCalculator
{
    private final Supplier<HistoryBasedPlanStatisticsProvider> historyBasedPlanStatisticsProvider;
    private final StatsCalculator delegate;
    private final ObjectMapper objectMapper;

    public HistoryBasedPlanStatisticsCalculator(
            Supplier<HistoryBasedPlanStatisticsProvider> historyBasedPlanStatisticsProvider,
            StatsCalculator delegate,
            ObjectMapper objectMapper)
    {
        this.historyBasedPlanStatisticsProvider = requireNonNull(historyBasedPlanStatisticsProvider, "historyBasedPlanStatisticsProvider is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
    }

    @Override
    public PlanNodeStatsEstimate calculateStats(PlanNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types)
    {
        return delegate.calculateStats(node, sourceStats, lookup, session, types)
                .combineStats(getStatistics(node, session, lookup));
    }

    private PlanStatistics getStatistics(PlanNode planNode, Session session, Lookup lookup)
    {
        PlanNode plan = resolveGroupReferences(planNode, lookup);
        if (!useHistoryBasedPlanStatisticsEnabled(session)) {
            return PlanStatistics.empty();
        }

        // TODO: generateCanonicalPlan() iterates through whole plan subtree. Consider caching/precomputing hashes.
        Map<PlanCanonicalizationStrategy, String> allHashes = historyBasedPlanCanonicalizationStrategyList().stream()
                .map(strategy -> generateCanonicalPlan(planNode, strategy))
                .filter(canonicalPlan -> canonicalPlan.isPresent())
                .collect(toImmutableMap(canonicalPlan -> canonicalPlan.get().getStrategy(), canonicalPlan -> hashPlan(canonicalPlan.get().getPlan())));

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
                    return entry.getValue().getLastRunStatistics();
                }
            }
        }

        return Optional.ofNullable(statistics.get(new PlanNodeWithHash(plan, Optional.empty())))
                .map(HistoricalPlanStatistics::getLastRunStatistics)
                .orElseGet(PlanStatistics::empty);
    }

    private String hashPlan(PlanNode plan)
    {
        try {
            return sha256().hashString(objectMapper.writeValueAsString(plan), UTF_8).toString();
        }
        catch (JsonProcessingException e) {
            throw new PrestoException(PLAN_SERIALIZATION_ERROR, "Cannot serialize plan to JSON", e);
        }
    }
}
