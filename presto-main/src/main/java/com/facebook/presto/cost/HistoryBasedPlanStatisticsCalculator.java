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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.common.plan.PlanCanonicalizationStrategy;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeWithHash;
import com.facebook.presto.spi.statistics.CostBasedSourceInfo;
import com.facebook.presto.spi.statistics.ExternalPlanStatisticsProvider;
import com.facebook.presto.spi.statistics.HistoricalPlanStatistics;
import com.facebook.presto.spi.statistics.HistoryBasedPlanStatisticsProvider;
import com.facebook.presto.spi.statistics.HistoryBasedSourceInfo;
import com.facebook.presto.spi.statistics.PlanStatistics;
import com.facebook.presto.sql.planner.PlanCanonicalInfoProvider;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.sql.planner.plan.AbstractJoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.facebook.presto.SystemSessionProperties.useExternalPlanStatisticsEnabled;
import static com.facebook.presto.SystemSessionProperties.useHistoryBasedPlanStatisticsEnabled;
import static com.facebook.presto.common.plan.PlanCanonicalizationStrategy.historyBasedPlanCanonicalizationStrategyList;
import static com.facebook.presto.cost.HistoricalPlanStatisticsUtil.getPredictedPlanStatistics;
import static com.facebook.presto.cost.TableStatisticsExtractor.extractTableStatistics;
import static com.facebook.presto.sql.planner.iterative.Plans.resolveGroupReferences;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.jsonLogicalPlan;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.graph.Traverser.forTree;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HistoryBasedPlanStatisticsCalculator
        implements StatsCalculator
{
    private static final List<Class<? extends PlanNode>> PRECOMPUTE_PLAN_NODES = ImmutableList.of(JoinNode.class, SemiJoinNode.class, AggregationNode.class);
    private static final DataSize CACHE_SIZE_BYTES = new DataSize(10, DataSize.Unit.MEGABYTE);

    // For weight, we only consider size of hash, as PlanNodes are already in memory for running queries.
    // We use length of hash + 20 bytes to account for stats.
    private final LoadingCache<PlanNodeWithHash, HistoricalPlanStatistics> cache = CacheBuilder.newBuilder()
            .maximumWeight(CACHE_SIZE_BYTES.toBytes())
            .weigher((Weigher<PlanNodeWithHash, HistoricalPlanStatistics>) (key, statistics) -> key.getHash().orElse("").length() + 20)
            .expireAfterWrite(5, TimeUnit.MINUTES)
            .build(new CacheLoader<PlanNodeWithHash, HistoricalPlanStatistics>()
            {
                @Override
                public HistoricalPlanStatistics load(PlanNodeWithHash key)
                {
                    return loadAll(Collections.singleton(key)).values().stream().findAny().orElseGet(HistoricalPlanStatistics::empty);
                }

                @Override
                public Map<PlanNodeWithHash, HistoricalPlanStatistics> loadAll(Iterable<? extends PlanNodeWithHash> keys)
                {
                    Map<PlanNodeWithHash, HistoricalPlanStatistics> statistics = new HashMap<>(historyBasedPlanStatisticsProvider.get().getStats(ImmutableList.copyOf(keys)));
                    // loadAll excepts all keys to be written
                    for (PlanNodeWithHash key : keys) {
                        statistics.putIfAbsent(key, HistoricalPlanStatistics.empty());
                    }
                    return ImmutableMap.copyOf(statistics);
                }
            });

    private final Supplier<HistoryBasedPlanStatisticsProvider> historyBasedPlanStatisticsProvider;
    private static final Logger log = Logger.get(HistoryBasedPlanStatisticsCalculator.class);
    private final Supplier<ExternalPlanStatisticsProvider> externalPlanStatisticsProvider;
    private final Metadata metadata;
    private final StatsCalculator delegate;
    private final PlanCanonicalInfoProvider planCanonicalInfoProvider;
    private final HistoryBasedOptimizationConfig config;

    public HistoryBasedPlanStatisticsCalculator(
            Supplier<HistoryBasedPlanStatisticsProvider> historyBasedPlanStatisticsProvider,
            StatsCalculator delegate,
            PlanCanonicalInfoProvider planCanonicalInfoProvider,
            HistoryBasedOptimizationConfig config,
            Supplier<ExternalPlanStatisticsProvider> externalPlanStatisticsProvider,
            Metadata metadata)
    {
        this.historyBasedPlanStatisticsProvider = requireNonNull(historyBasedPlanStatisticsProvider, "historyBasedPlanStatisticsProvider is null");
        this.externalPlanStatisticsProvider = requireNonNull(externalPlanStatisticsProvider, "externalPlanStatisticsProvider is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.planCanonicalInfoProvider = requireNonNull(planCanonicalInfoProvider, "planHasher is null");
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public PlanNodeStatsEstimate calculateStats(PlanNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types)
    {
        PlanNodeStatsEstimate delegateStats = delegate.calculateStats(node, sourceStats, lookup, session, types);
        return getStatistics(node, session, lookup, delegateStats, types);
    }

    @Override
    public void registerPlan(PlanNode root, Session session)
    {
        // Only precompute history based stats when plan has a join/aggregation.
        if (!PlanNodeSearcher.searchFrom(root).where(node -> PRECOMPUTE_PLAN_NODES.stream().anyMatch(clazz -> clazz.isInstance(node))).matches()) {
            return;
        }
        ImmutableList.Builder<PlanNodeWithHash> planNodesWithHash = ImmutableList.builder();
        forTree(PlanNode::getSources).depthFirstPreOrder(root).forEach(plan -> {
            if (plan.getStatsEquivalentPlanNode().isPresent()) {
                planNodesWithHash.addAll(getPlanNodeHashes(plan, session).values());
            }
        });
        try {
            cache.getAll(planNodesWithHash.build());
        }
        catch (ExecutionException e) {
            throw new RuntimeException("Unable to register plan: ", e.getCause());
        }
    }

    @VisibleForTesting
    public PlanCanonicalInfoProvider getPlanCanonicalInfoProvider()
    {
        return planCanonicalInfoProvider;
    }

    @VisibleForTesting
    public void invalidateCache()
    {
        cache.invalidateAll();
    }

    private Map<PlanCanonicalizationStrategy, PlanNodeWithHash> getPlanNodeHashes(PlanNode plan, Session session)
    {
        if (!useHistoryBasedPlanStatisticsEnabled(session) || !plan.getStatsEquivalentPlanNode().isPresent()) {
            return ImmutableMap.of();
        }

        PlanNode statsEquivalentPlanNode = plan.getStatsEquivalentPlanNode().get();
        ImmutableMap.Builder<PlanCanonicalizationStrategy, PlanNodeWithHash> allHashesBuilder = ImmutableMap.builder();

        for (PlanCanonicalizationStrategy strategy : historyBasedPlanCanonicalizationStrategyList()) {
            Optional<String> hash = planCanonicalInfoProvider.hash(session, statsEquivalentPlanNode, strategy);
            allHashesBuilder.put(strategy, new PlanNodeWithHash(statsEquivalentPlanNode, hash));
        }

        return allHashesBuilder.build();
    }

    private PlanNodeStatsEstimate getStatistics(PlanNode planNode, Session session, Lookup lookup, PlanNodeStatsEstimate delegateStats, TypeProvider types)
    {
        PlanNode plan = resolveGroupReferences(planNode, lookup);

        if (shouldUseExternalPlanStatistics(planNode, session)) {
            return delegateStats.combineStats(getExternalStatistics(plan, session, types), new CostBasedSourceInfo());
        }

        if (!useHistoryBasedPlanStatisticsEnabled(session)) {
            return delegateStats;
        }

        Map<PlanCanonicalizationStrategy, PlanNodeWithHash> allHashes = getPlanNodeHashes(plan, session);

        Map<PlanNodeWithHash, HistoricalPlanStatistics> statistics = ImmutableMap.of();
        try {
            statistics = cache.getAll(allHashes.values().stream().distinct().collect(toImmutableList()));
        }
        catch (ExecutionException e) {
            throw new RuntimeException(format("Unable to get plan statistics for %s", planNode), e.getCause());
        }
        // Return statistics corresponding to first strategy that we find, in order specified by `historyBasedPlanCanonicalizationStrategyList`
        for (PlanCanonicalizationStrategy strategy : historyBasedPlanCanonicalizationStrategyList()) {
            for (Map.Entry<PlanNodeWithHash, HistoricalPlanStatistics> entry : statistics.entrySet()) {
                if (allHashes.containsKey(strategy) && entry.getKey().getHash().isPresent() && allHashes.get(strategy).equals(entry.getKey())) {
                    Optional<List<PlanStatistics>> inputTableStatistics = getPlanNodeInputTableStatistics(plan, session);
                    if (inputTableStatistics.isPresent()) {
                        PlanStatistics predictedPlanStatistics = getPredictedPlanStatistics(entry.getValue(), inputTableStatistics.get(), config);
                        if (predictedPlanStatistics.getConfidence() > 0) {
                            return delegateStats.combineStats(
                                    predictedPlanStatistics,
                                    new HistoryBasedSourceInfo(entry.getKey().getHash(), inputTableStatistics));
                        }
                    }
                }
            }
        }

        return delegateStats;
    }

    private Optional<List<PlanStatistics>> getPlanNodeInputTableStatistics(PlanNode plan, Session session)
    {
        if (!useHistoryBasedPlanStatisticsEnabled(session) || !plan.getStatsEquivalentPlanNode().isPresent()) {
            return Optional.empty();
        }

        PlanNode statsEquivalentPlanNode = plan.getStatsEquivalentPlanNode().get();
        return planCanonicalInfoProvider.getInputTableStatistics(session, statsEquivalentPlanNode);
    }

    private PlanStatistics getExternalStatistics(PlanNode planNode, Session session, TypeProvider types)
    {
        ExternalPlanStatisticsProvider externalStatisticsProvider = externalPlanStatisticsProvider.get();
        try {
            return externalStatisticsProvider.getStats(
                    planNode,
                    session.getQueryId(),
                    node -> jsonLogicalPlan(node, types, metadata.getFunctionAndTypeManager(), StatsAndCosts.empty(), session),
                    Optional.of(node -> extractTableStatistics(node,
                            tableScanNode -> metadata.getTableStatistics(
                                    session,
                                    tableScanNode.getTable(),
                                    ImmutableList.copyOf(tableScanNode.getAssignments().values()),
                                    new Constraint<>(tableScanNode.getCurrentConstraint())))));
        }
        catch (Exception e) {
            log.error(e, "Error calling externalStatisticsProvider.getStats");
            return PlanStatistics.empty();
        }
    }

    // contains PlanNode types for which we should call external stats provider
    private static final Set<Class<? extends PlanNode>> EXTERNAL_STATS_PROVIDER_ALLOWED_NODE_TYPES = ImmutableSet.of(AbstractJoinNode.class);

    private boolean shouldUseExternalPlanStatistics(PlanNode planNode, Session session)
    {
        return useExternalPlanStatisticsEnabled(session) &&
                EXTERNAL_STATS_PROVIDER_ALLOWED_NODE_TYPES.stream().anyMatch(t -> t.isInstance(planNode));
    }
}
