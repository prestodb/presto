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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeWithHash;
import com.facebook.presto.spi.statistics.ExternalPlanStatisticsProvider;
import com.facebook.presto.spi.statistics.HistoricalPlanStatistics;
import com.facebook.presto.spi.statistics.PlanStatistics;
import com.facebook.presto.sql.planner.CanonicalPlanHashes;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import static com.facebook.presto.SystemSessionProperties.useExternalPlanStatisticsEnabled;
import static com.facebook.presto.common.plan.PlanCanonicalizationStrategy.historyBasedPlanCanonicalizationStrategyList;
import static com.facebook.presto.sql.planner.iterative.Plans.resolveGroupReferences;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class HistoryBasedPlanStatisticsCalculator
        implements StatsCalculator
{
    private final Supplier<ExternalPlanStatisticsProvider> externalPlanStatisticsProvider;
    private final Metadata metadata;
    private final StatsCalculator delegate;
    private final ObjectMapper objectMapper;

    // TODO: Evict plan node hashes once a query finishes.
    private final ConcurrentMap<QueryId, CanonicalPlanHashes> planNodeHashes;

    public HistoryBasedPlanStatisticsCalculator(
            Supplier<ExternalPlanStatisticsProvider> externalPlanStatisticsProvider,
            Metadata metadata,
            StatsCalculator delegate,
            ObjectMapper objectMapper)
    {
        this.externalPlanStatisticsProvider = requireNonNull(externalPlanStatisticsProvider, "externalPlanStatisticsProvider is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        this.planNodeHashes = new ConcurrentHashMap<>();
    }

    @Override
    public PlanNodeStatsEstimate calculateStats(PlanNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types)
    {
        return delegate.calculateStats(node, sourceStats, lookup, session, types)
                .combineStats(getStatistics(node, session, lookup));
    }

    public void cacheHashBasedStatistics(Session session, PlanNode root, TypeProvider types)
    {
        if (!useExternalPlanStatisticsEnabled(session)) {
            return;
        }
        CanonicalPlanHashes canonicalPlanHashes = new CanonicalPlanHashes(root, objectMapper);
        planNodeHashes.put(session.getQueryId(), canonicalPlanHashes);

        ImmutableList.Builder<PlanNodeWithHash> hashes = new ImmutableList.Builder<>();
        Stack<PlanNode> stack = new Stack();
        stack.push(root);
        while (!stack.isEmpty()) {
            PlanNode plan = stack.pop();
            plan.getSources().forEach(source -> stack.push(source));
            for (String hash : canonicalPlanHashes.getCanonicalPlanHashes(plan.getId()).values()) {
                hashes.add(new PlanNodeWithHash(plan, Optional.of(hash)));
            }
        }
        // Cache statistics for future calls
        // TODO: Right now, we assume that underlying implementation will cache statistics
        // when called. We should make this explicit by adding a cache here or through API.
        externalPlanStatisticsProvider.get().getStats(hashes.build());
    }

    @VisibleForTesting
    public Optional<CanonicalPlanHashes> getCanonicalPlanHashes(QueryId queryId)
    {
        return Optional.ofNullable(planNodeHashes.get(queryId));
    }

    private PlanStatistics getStatistics(PlanNode planNode, Session session, Lookup lookup)
    {
        final PlanNode plan = resolveGroupReferences(planNode, lookup);
        if (!useExternalPlanStatisticsEnabled(session)) {
            return PlanStatistics.empty();
        }

        Map<PlanCanonicalizationStrategy, String> allHashes = getCanonicalPlanHashes(session.getQueryId())
                .map(hashes -> hashes.getCanonicalPlanHashes(planNode.getId()))
                .orElseGet(ImmutableMap::of);

        List<PlanNodeWithHash> planNodeWithHashes = allHashes.values().stream()
                .map(hash -> new PlanNodeWithHash(plan, Optional.of(hash)))
                .collect(toImmutableList());

        // If no hashes are found, try to fetch statistics without hash.
        if (planNodeWithHashes.isEmpty()) {
            planNodeWithHashes = ImmutableList.of(new PlanNodeWithHash(plan, Optional.empty()));
        }

        Map<PlanNodeWithHash, HistoricalPlanStatistics> statistics = externalPlanStatisticsProvider.get().getStats(planNodeWithHashes);

        // Return statistics corresponding to first strategy that we find, in order specified by `historyBasedPlanCanonicalizationStrategyList`
        for (PlanCanonicalizationStrategy strategy : historyBasedPlanCanonicalizationStrategyList()) {
            for (Map.Entry<PlanNodeWithHash, HistoricalPlanStatistics> entry : statistics.entrySet()) {
                if (allHashes.containsKey(strategy) && Optional.of(allHashes.get(strategy)).equals(entry.getKey().getHash())) {
                    return entry.getValue().getLastRunStatistics();
                }
            }
        }

        return Optional.ofNullable(statistics.get(new PlanNodeWithHash(plan, Optional.empty())))
                .map(stats -> stats.getLastRunStatistics())
                .orElseGet(PlanStatistics::empty);
    }
}
