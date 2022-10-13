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
import com.facebook.presto.common.resourceGroups.QueryType;
import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeWithHash;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.HistoricalPlanStatistics;
import com.facebook.presto.spi.statistics.HistoryBasedPlanStatisticsProvider;
import com.facebook.presto.spi.statistics.HistoryBasedSourceInfo;
import com.facebook.presto.spi.statistics.PlanStatistics;
import com.facebook.presto.spi.statistics.PlanStatisticsWithSourceInfo;
import com.facebook.presto.sql.planner.CanonicalPlan;
import com.facebook.presto.sql.planner.PlanNodeCanonicalInfo;
import com.facebook.presto.sql.planner.planPrinter.PlanNodeStats;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.facebook.presto.SystemSessionProperties.trackHistoryBasedPlanStatisticsEnabled;
import static com.facebook.presto.common.plan.PlanCanonicalizationStrategy.historyBasedPlanCanonicalizationStrategyList;
import static com.facebook.presto.common.resourceGroups.QueryType.INSERT;
import static com.facebook.presto.common.resourceGroups.QueryType.SELECT;
import static com.facebook.presto.cost.HistoricalPlanStatisticsUtil.updatePlanStatistics;
import static com.facebook.presto.sql.planner.planPrinter.PlanNodeStatsSummarizer.aggregateStageStats;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.graph.Traverser.forTree;
import static java.util.Objects.requireNonNull;

public class HistoryBasedPlanStatisticsTracker
{
    private static final Logger LOG = Logger.get(HistoryBasedPlanStatisticsTracker.class);
    private static final Set<QueryType> ALLOWED_QUERY_TYPES = ImmutableSet.of(SELECT, INSERT);

    private final Supplier<HistoryBasedPlanStatisticsProvider> historyBasedPlanStatisticsProvider;
    private final SessionPropertyManager sessionPropertyManager;
    private final HistoryBasedOptimizationConfig config;

    public HistoryBasedPlanStatisticsTracker(
            Supplier<HistoryBasedPlanStatisticsProvider> historyBasedPlanStatisticsProvider,
            SessionPropertyManager sessionPropertyManager,
            HistoryBasedOptimizationConfig config)
    {
        this.historyBasedPlanStatisticsProvider = requireNonNull(historyBasedPlanStatisticsProvider, "historyBasedPlanStatisticsProvider is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.config = requireNonNull(config, "config is null");
    }

    public void updateStatistics(QueryExecution queryExecution)
    {
        queryExecution.addFinalQueryInfoListener(this::updateStatistics);
    }

    @VisibleForTesting
    public HistoryBasedPlanStatisticsProvider getHistoryBasedPlanStatisticsProvider()
    {
        return historyBasedPlanStatisticsProvider.get();
    }

    public Map<PlanNodeWithHash, PlanStatisticsWithSourceInfo> getQueryStats(QueryInfo queryInfo)
    {
        Session session = queryInfo.getSession().toSession(sessionPropertyManager);
        if (!trackHistoryBasedPlanStatisticsEnabled(session)) {
            return ImmutableMap.of();
        }

        // Only update statistics for successful queries
        if (queryInfo.getFailureInfo() != null ||
                !queryInfo.getOutputStage().isPresent() ||
                !queryInfo.getOutputStage().get().getPlan().isPresent()) {
            return ImmutableMap.of();
        }

        // Only update statistics for SELECT/INSERT queries
        if (!queryInfo.getQueryType().isPresent() || !ALLOWED_QUERY_TYPES.contains(queryInfo.getQueryType().get())) {
            return ImmutableMap.of();
        }

        if (!queryInfo.isFinalQueryInfo()) {
            LOG.error("Expected final query info when updating history based statistics: %s", queryInfo);
            return ImmutableMap.of();
        }

        StageInfo outputStage = queryInfo.getOutputStage().get();
        List<StageInfo> allStages = outputStage.getAllStages();

        Map<PlanNodeId, PlanNodeStats> planNodeStatsMap = aggregateStageStats(allStages);
        Map<PlanNodeWithHash, PlanStatisticsWithSourceInfo> planStatistics = new HashMap<>();
        Map<CanonicalPlan, PlanNodeCanonicalInfo> canonicalInfoMap = new HashMap<>();
        queryInfo.getPlanCanonicalInfo().forEach(canonicalPlanWithInfo -> {
            // We can have duplicate stats equivalent plan nodes. It's ok to use any stats in this case
            canonicalInfoMap.putIfAbsent(canonicalPlanWithInfo.getCanonicalPlan(), canonicalPlanWithInfo.getInfo());
        });

        for (StageInfo stageInfo : allStages) {
            if (!stageInfo.getPlan().isPresent()) {
                continue;
            }
            PlanNode root = stageInfo.getPlan().get().getRoot();
            for (PlanNode planNode : forTree(PlanNode::getSources).depthFirstPreOrder(root)) {
                if (!planNode.getStatsEquivalentPlanNode().isPresent()) {
                    continue;
                }
                PlanNodeStats planNodeStats = planNodeStatsMap.get(planNode.getId());
                if (planNodeStats == null) {
                    continue;
                }
                PlanNode statsEquivalentPlanNode = planNode.getStatsEquivalentPlanNode().get();
                for (PlanCanonicalizationStrategy strategy : historyBasedPlanCanonicalizationStrategyList()) {
                    Optional<PlanNodeCanonicalInfo> planNodeCanonicalInfo = Optional.ofNullable(
                            canonicalInfoMap.get(new CanonicalPlan(statsEquivalentPlanNode, strategy)));
                    if (planNodeCanonicalInfo.isPresent()) {
                        String hash = planNodeCanonicalInfo.get().getHash();
                        List<PlanStatistics> inputTableStatistics = planNodeCanonicalInfo.get().getInputTableStatistics();

                        double outputPositions = planNodeStats.getPlanNodeOutputPositions();
                        double outputBytes = planNodeStats.getPlanNodeOutputDataSize().toBytes();
                        planStatistics.putIfAbsent(
                                new PlanNodeWithHash(statsEquivalentPlanNode, Optional.of(hash)),
                                new PlanStatisticsWithSourceInfo(
                                        planNode.getId(),
                                        new PlanStatistics(
                                                Estimate.of(outputPositions),
                                                Double.isNaN(outputBytes) ? Estimate.unknown() : Estimate.of(outputBytes),
                                                1.0),
                                        new HistoryBasedSourceInfo(Optional.of(hash), Optional.of(inputTableStatistics))));
                    }
                }
            }
        }
        return ImmutableMap.copyOf(planStatistics);
    }

    private void updateStatistics(QueryInfo queryInfo)
    {
        Map<PlanNodeWithHash, PlanStatisticsWithSourceInfo> planStatistics = getQueryStats(queryInfo);
        Map<PlanNodeWithHash, HistoricalPlanStatistics> historicalPlanStatisticsMap =
                historyBasedPlanStatisticsProvider.get().getStats(planStatistics.keySet().stream().collect(toImmutableList()));
        Map<PlanNodeWithHash, HistoricalPlanStatistics> newPlanStatistics = planStatistics.entrySet().stream()
                .filter(entry -> entry.getKey().getHash().isPresent() &&
                        entry.getValue().getSourceInfo() instanceof HistoryBasedSourceInfo &&
                        ((HistoryBasedSourceInfo) entry.getValue().getSourceInfo()).getInputTableStatistics().isPresent())
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> {
                            HistoricalPlanStatistics historicalPlanStatistics = Optional.ofNullable(historicalPlanStatisticsMap.get(entry.getKey()))
                                    .orElseGet(HistoricalPlanStatistics::empty);
                            HistoryBasedSourceInfo historyBasedSourceInfo = (HistoryBasedSourceInfo) entry.getValue().getSourceInfo();
                            return updatePlanStatistics(
                                    historicalPlanStatistics,
                                    historyBasedSourceInfo.getInputTableStatistics().get(),
                                    entry.getValue().getPlanStatistics(),
                                    config);
                        }));

        if (newPlanStatistics.isEmpty()) {
            return;
        }
        historyBasedPlanStatisticsProvider.get().putStats(ImmutableMap.copyOf(newPlanStatistics));
    }
}
