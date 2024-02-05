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
import com.facebook.presto.spi.statistics.PlanStatistics;
import com.facebook.presto.sql.planner.PlanCanonicalInfoProvider;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static com.facebook.presto.SystemSessionProperties.getHistoryBasedOptimizerTimeoutLimit;
import static com.facebook.presto.SystemSessionProperties.getHistoryInputTableStatisticsMatchingThreshold;
import static com.facebook.presto.SystemSessionProperties.isVerboseRuntimeStatsEnabled;
import static com.facebook.presto.SystemSessionProperties.useHistoryBasedPlanStatisticsEnabled;
import static com.facebook.presto.common.RuntimeMetricName.HISTORY_OPTIMIZER_QUERY_REGISTRATION_GET_PLAN_NODE_HASHES;
import static com.facebook.presto.common.RuntimeMetricName.HISTORY_OPTIMIZER_QUERY_REGISTRATION_GET_STATISTICS;
import static com.facebook.presto.common.RuntimeUnit.NANO;
import static com.facebook.presto.common.plan.PlanCanonicalizationStrategy.historyBasedPlanCanonicalizationStrategyList;
import static com.facebook.presto.cost.HistoricalPlanStatisticsUtil.getPredictedPlanStatistics;
import static com.facebook.presto.sql.planner.iterative.Plans.resolveGroupReferences;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.graph.Traverser.forTree;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class HistoryBasedPlanStatisticsCalculator
        implements StatsCalculator
{
    private final Supplier<HistoryBasedPlanStatisticsProvider> historyBasedPlanStatisticsProvider;
    private final HistoryBasedStatisticsCacheManager historyBasedStatisticsCacheManager;
    private final StatsCalculator delegate;
    private final PlanCanonicalInfoProvider planCanonicalInfoProvider;
    private final HistoryBasedOptimizationConfig config;

    public HistoryBasedPlanStatisticsCalculator(
            Supplier<HistoryBasedPlanStatisticsProvider> historyBasedPlanStatisticsProvider,
            HistoryBasedStatisticsCacheManager historyBasedStatisticsCacheManager,
            StatsCalculator delegate,
            PlanCanonicalInfoProvider planCanonicalInfoProvider,
            HistoryBasedOptimizationConfig config)
    {
        this.historyBasedPlanStatisticsProvider = requireNonNull(historyBasedPlanStatisticsProvider, "historyBasedPlanStatisticsProvider is null");
        this.historyBasedStatisticsCacheManager = requireNonNull(historyBasedStatisticsCacheManager, "historyBasedStatisticsCacheManager is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.planCanonicalInfoProvider = requireNonNull(planCanonicalInfoProvider, "planHasher is null");
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public PlanNodeStatsEstimate calculateStats(PlanNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types)
    {
        PlanNodeStatsEstimate delegateStats = delegate.calculateStats(node, sourceStats, lookup, session, types);
        return getStatistics(node, session, lookup, delegateStats);
    }

    @Override
    public boolean registerPlan(PlanNode root, Session session, long startTimeInNano, long timeoutInMilliseconds)
    {
        // If previous registration timeout for this query, this run is likely to timeout too, return false.
        if (historyBasedStatisticsCacheManager.historyBasedQueryRegistrationTimeout(session.getQueryId())) {
            return false;
        }
        ImmutableList.Builder<PlanNodeWithHash> planNodesWithHash = ImmutableList.builder();
        Iterable<PlanNode> planNodeIterable = forTree(PlanNode::getSources).depthFirstPreOrder(root);
        boolean enableVerboseRuntimeStats = isVerboseRuntimeStatsEnabled(session);
        long profileStartTime = 0;
        for (PlanNode plan : planNodeIterable) {
            if (checkTimeOut(startTimeInNano, timeoutInMilliseconds)) {
                historyBasedStatisticsCacheManager.setHistoryBasedQueryRegistrationTimeout(session.getQueryId());
                return false;
            }
            if (plan.getStatsEquivalentPlanNode().isPresent()) {
                if (enableVerboseRuntimeStats) {
                    profileStartTime = System.nanoTime();
                }
                planNodesWithHash.addAll(getPlanNodeHashes(plan, session, false).values());
                if (enableVerboseRuntimeStats) {
                    session.getRuntimeStats().addMetricValue(HISTORY_OPTIMIZER_QUERY_REGISTRATION_GET_PLAN_NODE_HASHES, NANO, System.nanoTime() - profileStartTime);
                }
            }
        }
        try {
            if (enableVerboseRuntimeStats) {
                profileStartTime = System.nanoTime();
            }
            historyBasedStatisticsCacheManager.getStatisticsCache(session.getQueryId(), historyBasedPlanStatisticsProvider, getHistoryBasedOptimizerTimeoutLimit(session).toMillis()).getAll(planNodesWithHash.build());
            if (enableVerboseRuntimeStats) {
                session.getRuntimeStats().addMetricValue(HISTORY_OPTIMIZER_QUERY_REGISTRATION_GET_STATISTICS, NANO, System.nanoTime() - profileStartTime);
            }
        }
        catch (ExecutionException e) {
            throw new RuntimeException("Unable to register plan: ", e.getCause());
        }

        if (checkTimeOut(startTimeInNano, timeoutInMilliseconds)) {
            historyBasedStatisticsCacheManager.setHistoryBasedQueryRegistrationTimeout(session.getQueryId());
        }
        // Return true even if get empty history statistics, so that HistoricalStatisticsEquivalentPlanMarkingOptimizer still return the plan with StatsEquivalentPlanNode which
        // will be used in populating history statistics
        return true;
    }

    private boolean checkTimeOut(long startTimeInNano, long timeoutInMilliseconds)
    {
        return NANOSECONDS.toMillis(System.nanoTime() - startTimeInNano) > timeoutInMilliseconds;
    }

    @VisibleForTesting
    public PlanCanonicalInfoProvider getPlanCanonicalInfoProvider()
    {
        return planCanonicalInfoProvider;
    }

    @VisibleForTesting
    public StatsCalculator getDelegate()
    {
        return delegate;
    }

    @VisibleForTesting
    public Supplier<HistoryBasedPlanStatisticsProvider> getHistoryBasedPlanStatisticsProvider()
    {
        return historyBasedPlanStatisticsProvider;
    }

    private Map<PlanCanonicalizationStrategy, PlanNodeWithHash> getPlanNodeHashes(PlanNode plan, Session session, boolean cacheOnly)
    {
        if (!useHistoryBasedPlanStatisticsEnabled(session) || !plan.getStatsEquivalentPlanNode().isPresent()) {
            return ImmutableMap.of();
        }

        PlanNode statsEquivalentPlanNode = plan.getStatsEquivalentPlanNode().get();
        ImmutableMap.Builder<PlanCanonicalizationStrategy, PlanNodeWithHash> allHashesBuilder = ImmutableMap.builder();

        for (PlanCanonicalizationStrategy strategy : historyBasedPlanCanonicalizationStrategyList()) {
            Optional<String> hash = planCanonicalInfoProvider.hash(session, statsEquivalentPlanNode, strategy, cacheOnly);
            if (hash.isPresent()) {
                allHashesBuilder.put(strategy, new PlanNodeWithHash(statsEquivalentPlanNode, hash));
            }
        }

        return allHashesBuilder.build();
    }

    private PlanNodeStatsEstimate getStatistics(PlanNode planNode, Session session, Lookup lookup, PlanNodeStatsEstimate delegateStats)
    {
        if (!useHistoryBasedPlanStatisticsEnabled(session)) {
            return delegateStats;
        }

        PlanNode plan = resolveGroupReferences(planNode, lookup);
        Map<PlanCanonicalizationStrategy, PlanNodeWithHash> allHashes = getPlanNodeHashes(plan, session, true);

        Map<PlanNodeWithHash, HistoricalPlanStatistics> statistics = ImmutableMap.of();
        try {
            statistics = historyBasedStatisticsCacheManager
                    .getStatisticsCache(session.getQueryId(), historyBasedPlanStatisticsProvider, getHistoryBasedOptimizerTimeoutLimit(session).toMillis())
                    .getAll(allHashes.values().stream().distinct().collect(toImmutableList()));
        }
        catch (ExecutionException e) {
            throw new RuntimeException(format("Unable to get plan statistics for %s", planNode), e.getCause());
        }
        double historyMatchingThreshold = getHistoryInputTableStatisticsMatchingThreshold(session) > 0 ? getHistoryInputTableStatisticsMatchingThreshold(session) : config.getHistoryMatchingThreshold();
        // Return statistics corresponding to first strategy that we find, in order specified by `historyBasedPlanCanonicalizationStrategyList`
        for (PlanCanonicalizationStrategy strategy : historyBasedPlanCanonicalizationStrategyList()) {
            for (Map.Entry<PlanNodeWithHash, HistoricalPlanStatistics> entry : statistics.entrySet()) {
                if (allHashes.containsKey(strategy) && entry.getKey().getHash().isPresent() && allHashes.get(strategy).equals(entry.getKey())) {
                    Optional<List<PlanStatistics>> inputTableStatistics = getPlanNodeInputTableStatistics(plan, session, true);
                    if (inputTableStatistics.isPresent()) {
                        PlanStatistics predictedPlanStatistics = getPredictedPlanStatistics(entry.getValue(), inputTableStatistics.get(), historyMatchingThreshold);
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

    private Optional<List<PlanStatistics>> getPlanNodeInputTableStatistics(PlanNode plan, Session session, boolean cacheOnly)
    {
        if (!useHistoryBasedPlanStatisticsEnabled(session) || !plan.getStatsEquivalentPlanNode().isPresent()) {
            return Optional.empty();
        }

        PlanNode statsEquivalentPlanNode = plan.getStatsEquivalentPlanNode().get();
        return planCanonicalInfoProvider.getInputTableStatistics(session, statsEquivalentPlanNode, cacheOnly);
    }
}
