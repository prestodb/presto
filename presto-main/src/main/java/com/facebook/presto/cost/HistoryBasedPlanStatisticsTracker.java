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
import com.facebook.presto.common.type.FixedWidthType;
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
import com.facebook.presto.spi.statistics.PlanAnalyticsSourceInfo;
import com.facebook.presto.spi.statistics.PlanStatistics;
import com.facebook.presto.spi.statistics.PlanStatisticsWithSourceInfo;
import com.facebook.presto.sql.planner.CanonicalPlan;
import com.facebook.presto.sql.planner.PlanNodeCanonicalInfo;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.planPrinter.PlanNodeStats;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.facebook.presto.SystemSessionProperties.isPlanAnalyticsEnabled;
import static com.facebook.presto.SystemSessionProperties.trackHistoryBasedPlanStatisticsEnabled;
import static com.facebook.presto.common.plan.PlanCanonicalizationStrategy.EXACT;
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
    private final HistoryBasedStatisticsCacheManager historyBasedStatisticsCacheManager;
    private final SessionPropertyManager sessionPropertyManager;
    private final HistoryBasedOptimizationConfig config;

    public HistoryBasedPlanStatisticsTracker(
            Supplier<HistoryBasedPlanStatisticsProvider> historyBasedPlanStatisticsProvider,
            HistoryBasedStatisticsCacheManager historyBasedStatisticsCacheManager,
            SessionPropertyManager sessionPropertyManager,
            HistoryBasedOptimizationConfig config)
    {
        this.historyBasedPlanStatisticsProvider = requireNonNull(historyBasedPlanStatisticsProvider, "historyBasedPlanStatisticsProvider is null");
        this.historyBasedStatisticsCacheManager = requireNonNull(historyBasedStatisticsCacheManager, "historyBasedStatisticsCacheManager is null");
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
                double outputPositions = planNodeStats.getPlanNodeOutputPositions();
                double outputBytes = adjustedOutputBytes(planNode, planNodeStats);

                PlanNode statsEquivalentPlanNode = planNode.getStatsEquivalentPlanNode().get();
                for (PlanCanonicalizationStrategy strategy : historyBasedPlanCanonicalizationStrategyList()) {
                    Optional<PlanNodeCanonicalInfo> planNodeCanonicalInfo = Optional.ofNullable(
                            canonicalInfoMap.get(new CanonicalPlan(statsEquivalentPlanNode, strategy)));
                    if (planNodeCanonicalInfo.isPresent()) {
                        String hash = planNodeCanonicalInfo.get().getHash();
                        List<PlanStatistics> inputTableStatistics = planNodeCanonicalInfo.get().getInputTableStatistics();
                        planStatistics.putIfAbsent(
                                new PlanNodeWithHash(statsEquivalentPlanNode, Optional.of(hash)),
                                new PlanStatisticsWithSourceInfo(
                                        planNode.getId(),
                                        new PlanStatistics(
                                                Estimate.of(outputPositions),
                                                Double.isNaN(outputBytes) ? Estimate.unknown() : Estimate.of(outputBytes),
                                                1.0, Estimate.unknown()),
                                        new HistoryBasedSourceInfo(Optional.of(hash), Optional.of(inputTableStatistics))));
                    }
                }
            }
        }
        return ImmutableMap.copyOf(planStatistics);
    }

    // After we assign stats equivalent plan node, additional variables may be introduced by optimizer, for example
    // hash variables in HashGenerationOptimizer. We should discount these variables from output sizes to make
    // stats more accurate, and similar to stats from Cost Based Optimizer. Here, we approximate this by only adjusting for types
    // with fixed width, for others we will let the sizes be so, and assume the size differences are small when using variable
    // sized types.
    private double adjustedOutputBytes(PlanNode planNode, PlanNodeStats planNodeStats)
    {
        double outputPositions = planNodeStats.getPlanNodeOutputPositions();
        double outputBytes = planNodeStats.getPlanNodeOutputDataSize().toBytes();
        outputBytes -= planNode.getOutputVariables().stream()
                .mapToDouble(variable -> variable.getType() instanceof FixedWidthType ? outputPositions * ((FixedWidthType) variable.getType()).getFixedSize() : 0)
                .sum();
        outputBytes += planNode.getStatsEquivalentPlanNode().get().getOutputVariables().stream()
                .mapToDouble(variable -> variable.getType() instanceof FixedWidthType ? outputPositions * ((FixedWidthType) variable.getType()).getFixedSize() : 0)
                .sum();
        if (outputBytes < 0 || (outputPositions > 0 && outputBytes < 1)) {
            outputBytes = Double.NaN;
        }
        return outputBytes;
    }

    public Map<PlanNodeWithHash, PlanStatisticsWithSourceInfo> getPlanAnalyticsQueryStats(QueryInfo queryInfo)
    {
        Session session = queryInfo.getSession().toSession(sessionPropertyManager);
        if (!isPlanAnalyticsEnabled(session)) {
            return ImmutableMap.of();
        }

        // Only update statistics for successful queries
        if (queryInfo.getFailureInfo() != null || !queryInfo.getOutputStage().isPresent() || !queryInfo.getOutputStage().get().getPlan().isPresent()) {
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
        List<StageInfo> allStages = new ArrayList<>(outputStage.getAllStages());

        Map<PlanNodeId, PlanNodeStats> planNodeStatsMap = aggregateStageStats(allStages);
        Map<PlanNodeWithHash, PlanStatisticsWithSourceInfo> intermediatePlanStatisticMap = new HashMap<>();
        Map<CanonicalPlan, PlanNodeCanonicalInfo> canonicalInfoMap = new HashMap<>();
        queryInfo.getPlanAnalyticsCanonicalInfo().forEach(canonicalPlanWithInfo -> {
            // We can have duplicate stats equivalent plan nodes. It's ok to use any stats in this case
            canonicalInfoMap.putIfAbsent(canonicalPlanWithInfo.getCanonicalPlan(), canonicalPlanWithInfo.getInfo());
        });
        Map<PlanNodeWithHash, Long> cpuTimeMap = new HashMap<>();
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
                Optional<PlanNodeCanonicalInfo> planNodeCanonicalInfo = Optional.ofNullable(canonicalInfoMap.get(new CanonicalPlan(statsEquivalentPlanNode, EXACT)));
                if (planNodeCanonicalInfo.isPresent()) {
                    String hash = planNodeCanonicalInfo.get().getHash();
                    long execTime = planNodeStats.getPlanNodeCpuTime().toMillis();
                    double outputPositions = planNodeStats.getPlanNodeOutputPositions();
                    double outputBytes = adjustedOutputBytes(planNode, planNodeStats);
                    cpuTimeMap.put(new PlanNodeWithHash(statsEquivalentPlanNode, Optional.of(hash)), execTime);
                    intermediatePlanStatisticMap.putIfAbsent(
                            new PlanNodeWithHash(statsEquivalentPlanNode, Optional.of(hash)),
                            new PlanStatisticsWithSourceInfo(planNode.getId(),
                                    new PlanStatistics(Estimate.of(outputPositions), Double.isNaN(outputBytes) ? Estimate.unknown() : Estimate.of(outputBytes),
                                            1.0, Estimate.unknown()),
                                    new PlanAnalyticsSourceInfo(Optional.of(hash))));
                }
            }
        }
        PlanNode planNode = outputStage.getPlan().get().getRoot();

        Map<PlanNodeWithHash, Long> cumulativeCpuTimeMap = new HashMap<>();

        // get Cumulative Cpu Time
        planNode.accept(new InternalPlanVisitor<Long, Void>()
        {
            @Override
            public Long visitPlan(PlanNode node, Void context)
            {
                Long sumCpu = 0L;
                if (!node.getStatsEquivalentPlanNode().isPresent()) {
                    // No Stats equivalent node present for this node (likely an exchange)
                    for (PlanNode source : node.getSources()) {
                        sumCpu += source.accept(this, context);
                    }
                    return sumCpu;
                }
                PlanNode statsEquivalentPlanNode = node.getStatsEquivalentPlanNode().get();
                Optional<PlanNodeCanonicalInfo> planNodeCanonicalInfo = Optional.ofNullable(canonicalInfoMap.get(new CanonicalPlan(statsEquivalentPlanNode, EXACT)));
                if (!planNodeCanonicalInfo.isPresent()) {
                    // No Stats present for this node ( can happen when all work is done by a single operator in a stage)
                    for (PlanNode source : statsEquivalentPlanNode.getSources()) {
                        sumCpu += source.accept(this, context);
                    }
                    return sumCpu;
                }
                // Stats Present
                PlanNodeWithHash planNodeWithHash = new PlanNodeWithHash(statsEquivalentPlanNode, Optional.ofNullable(planNodeCanonicalInfo.get().getHash()));
                sumCpu = cpuTimeMap.getOrDefault(planNodeWithHash, 0L);
                for (PlanNode source : statsEquivalentPlanNode.getSources()) {
                    sumCpu += source.accept(this, context);
                }
                cumulativeCpuTimeMap.put(planNodeWithHash, sumCpu);
                return sumCpu;
            }
        }, null);

        Map<PlanNodeWithHash, PlanStatisticsWithSourceInfo> finalPlanStatisticMap = new HashMap<>();
        // Update previously stored statistics with cumulative statistic info
        for (Map.Entry<PlanNodeWithHash, PlanStatisticsWithSourceInfo> ent : intermediatePlanStatisticMap.entrySet()) {
            PlanStatistics prevStats = ent.getValue().getPlanStatistics();
            finalPlanStatisticMap.put(ent.getKey(),
                    new PlanStatisticsWithSourceInfo(
                            ent.getValue().getId(),
                            new PlanStatistics(prevStats.getRowCount(),
                                    prevStats.getOutputSize(), prevStats.getConfidence(),
                                    Estimate.of(cumulativeCpuTimeMap.getOrDefault(ent.getKey(), 0L))),
                            ent.getValue().getSourceInfo()));
        }
        return ImmutableMap.copyOf(finalPlanStatisticMap);
    }

    public void updateStatistics(QueryInfo queryInfo)
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

        if (!newPlanStatistics.isEmpty()) {
            historyBasedPlanStatisticsProvider.get().putStats(ImmutableMap.copyOf(newPlanStatistics));
        }
        historyBasedStatisticsCacheManager.invalidate(queryInfo.getQueryId());
    }
}
