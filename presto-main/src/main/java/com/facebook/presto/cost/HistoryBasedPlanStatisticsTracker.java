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
import com.facebook.presto.execution.StageExecutionState;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeWithHash;
import com.facebook.presto.spi.plan.TableWriterNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.HistoricalPlanStatistics;
import com.facebook.presto.spi.statistics.HistoricalPlanStatisticsEntryInfo;
import com.facebook.presto.spi.statistics.HistoryBasedPlanStatisticsProvider;
import com.facebook.presto.spi.statistics.HistoryBasedSourceInfo;
import com.facebook.presto.spi.statistics.JoinNodeStatistics;
import com.facebook.presto.spi.statistics.PartialAggregationStatistics;
import com.facebook.presto.spi.statistics.PlanStatistics;
import com.facebook.presto.spi.statistics.PlanStatisticsWithSourceInfo;
import com.facebook.presto.spi.statistics.TableWriterNodeStatistics;
import com.facebook.presto.sql.planner.CanonicalPlan;
import com.facebook.presto.sql.planner.PlanNodeCanonicalInfo;
import com.facebook.presto.sql.planner.planPrinter.PlanNodeStats;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.facebook.presto.SystemSessionProperties.getHistoryBasedOptimizerTimeoutLimit;
import static com.facebook.presto.SystemSessionProperties.trackHistoryBasedPlanStatisticsEnabled;
import static com.facebook.presto.SystemSessionProperties.trackHistoryStatsFromFailedQuery;
import static com.facebook.presto.SystemSessionProperties.trackPartialAggregationHistory;
import static com.facebook.presto.common.resourceGroups.QueryType.INSERT;
import static com.facebook.presto.common.resourceGroups.QueryType.SELECT;
import static com.facebook.presto.cost.HistoricalPlanStatisticsUtil.updatePlanStatistics;
import static com.facebook.presto.cost.HistoryBasedPlanStatisticsManager.historyBasedPlanCanonicalizationStrategyList;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static com.facebook.presto.sql.planner.planPrinter.PlanNodeStatsSummarizer.aggregateStageStats;
import static com.google.common.base.Preconditions.checkState;
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
    private final boolean isNativeExecution;
    private final String serverVersion;

    public HistoryBasedPlanStatisticsTracker(
            Supplier<HistoryBasedPlanStatisticsProvider> historyBasedPlanStatisticsProvider,
            HistoryBasedStatisticsCacheManager historyBasedStatisticsCacheManager,
            SessionPropertyManager sessionPropertyManager,
            HistoryBasedOptimizationConfig config,
            boolean isNativeExecution,
            String serverVersion)
    {
        this.historyBasedPlanStatisticsProvider = requireNonNull(historyBasedPlanStatisticsProvider, "historyBasedPlanStatisticsProvider is null");
        this.historyBasedStatisticsCacheManager = requireNonNull(historyBasedStatisticsCacheManager, "historyBasedStatisticsCacheManager is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.config = requireNonNull(config, "config is null");
        this.isNativeExecution = isNativeExecution;
        this.serverVersion = serverVersion;
    }

    public void updateStatistics(QueryExecution queryExecution)
    {
        queryExecution.addFinalQueryInfoListener(this::updateStatistics);
    }

    public Map<PlanCanonicalizationStrategy, String> getCanonicalPlan(QueryId queryId)
    {
        return historyBasedStatisticsCacheManager.getCanonicalPlan(queryId);
    }

    public Optional<PlanNode> getStatsEquivalentPlanRootNode(QueryId queryId)
    {
        return historyBasedStatisticsCacheManager.getStatsEquivalentPlanRootNode(queryId);
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

        // If track_history_stats_from_failed_queries is set to true, we do not require that the query is successful
        boolean trackStatsForFailedQueries = trackHistoryStatsFromFailedQuery(session);
        boolean querySucceed = queryInfo.getFailureInfo() == null;
        if ((!querySucceed && !trackStatsForFailedQueries) || !queryInfo.getOutputStage().isPresent() || !queryInfo.getOutputStage().get().getPlan().isPresent()) {
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
        List<StageInfo> allStages = ImmutableList.of();
        if (querySucceed) {
            allStages = outputStage.getAllStages();
        }
        else if (trackStatsForFailedQueries) {
            allStages = outputStage.getAllStages().stream().filter(x -> x.getLatestAttemptExecutionInfo().getState().equals(StageExecutionState.FINISHED)).collect(toImmutableList());
        }

        if (allStages.isEmpty()) {
            return ImmutableMap.of();
        }

        HistoricalPlanStatisticsEntryInfo historicalPlanStatisticsEntryInfo = new HistoricalPlanStatisticsEntryInfo(
                isNativeExecution ? HistoricalPlanStatisticsEntryInfo.WorkerType.CPP : HistoricalPlanStatisticsEntryInfo.WorkerType.JAVA, queryInfo.getQueryId(), serverVersion);

        Map<PlanNodeId, PlanNodeStats> planNodeStatsMap = aggregateStageStats(allStages);
        Map<PlanNodeWithHash, PlanStatisticsWithSourceInfo> planStatisticsMap = new HashMap<>();
        Map<CanonicalPlan, PlanNodeCanonicalInfo> canonicalInfoMap = new HashMap<>();
        Map<Integer, FinalAggregationStatsInfo> aggregationNodeMap = new HashMap<>();
        Set<PlanNodeId> planNodeIdsDynamicFilter = getPlanNodeAppliedDynamicFilter(planNodeStatsMap, allStages);

        queryInfo.getPlanCanonicalInfo().forEach(canonicalPlanWithInfo -> {
            // We can have duplicate stats equivalent plan nodes. It's ok to use any stats in this case
            canonicalInfoMap.putIfAbsent(canonicalPlanWithInfo.getCanonicalPlan(), canonicalPlanWithInfo.getInfo());
        });

        for (StageInfo stageInfo : allStages) {
            if (!stageInfo.getPlan().isPresent()) {
                continue;
            }
            boolean isScaledWriterStage = stageInfo.getPlan().isPresent() && stageInfo.getPlan().get().getPartitioning().equals(SCALED_WRITER_DISTRIBUTION);
            PlanNode root = stageInfo.getPlan().get().getRoot();
            for (PlanNode planNode : forTree(PlanNode::getSources).depthFirstPreOrder(root)) {
                if ((!planNode.getStatsEquivalentPlanNode().isPresent() && !isAggregation(planNode, AggregationNode.Step.PARTIAL)) || planNodeIdsDynamicFilter.contains(planNode.getId())) {
                    continue;
                }
                PlanNodeStats planNodeStats = planNodeStatsMap.get(planNode.getId());
                if (planNodeStats == null) {
                    continue;
                }

                double outputPositions = planNodeStats.getPlanNodeOutputPositions();
                double outputBytes = adjustedOutputBytes(planNode, planNodeStats);
                double nullJoinBuildKeyCount = planNodeStats.getPlanNodeNullJoinBuildKeyCount();
                double joinBuildKeyCount = planNodeStats.getPlanNodeJoinBuildKeyCount();
                double nullJoinProbeKeyCount = planNodeStats.getPlanNodeNullJoinProbeKeyCount();
                double joinProbeKeyCount = planNodeStats.getPlanNodeJoinProbeKeyCount();
                PartialAggregationStatistics partialAggregationStatistics = PartialAggregationStatistics.empty();

                if (isAggregation(planNode, AggregationNode.Step.PARTIAL) && trackPartialAggregationHistory(session)) {
                    // we're doing a depth-first traversal of the plan tree so we must have seen the corresponding final agg already:
                    // find it and update its partial agg stats
                    partialAggregationStatistics = constructAggregationNodeStatistics(planNode, planNodeStatsMap, outputBytes, outputPositions);
                    updatePartialAggregationStatistics((AggregationNode) planNode, aggregationNodeMap, partialAggregationStatistics, planStatisticsMap);
                }

                if (!planNode.getStatsEquivalentPlanNode().isPresent()) {
                    continue;
                }

                JoinNodeStatistics joinNodeStatistics = JoinNodeStatistics.empty();
                if (planNode instanceof JoinNode) {
                    joinNodeStatistics = new JoinNodeStatistics(Estimate.of(nullJoinBuildKeyCount), Estimate.of(joinBuildKeyCount), Estimate.of(nullJoinProbeKeyCount), Estimate.of(joinProbeKeyCount));
                }

                TableWriterNodeStatistics tableWriterNodeStatistics = TableWriterNodeStatistics.empty();
                if (isScaledWriterStage && planNode instanceof TableWriterNode) {
                    tableWriterNodeStatistics = new TableWriterNodeStatistics(Estimate.of(stageInfo.getLatestAttemptExecutionInfo().getStats().getTotalTasks()));
                }

                PlanNode statsEquivalentPlanNode = planNode.getStatsEquivalentPlanNode().get();
                for (PlanCanonicalizationStrategy strategy : historyBasedPlanCanonicalizationStrategyList(session)) {
                    Optional<PlanNodeCanonicalInfo> planNodeCanonicalInfo = Optional.ofNullable(
                            canonicalInfoMap.get(new CanonicalPlan(statsEquivalentPlanNode, strategy)));
                    if (planNodeCanonicalInfo.isPresent()) {
                        String hash = planNodeCanonicalInfo.get().getHash();
                        List<PlanStatistics> inputTableStatistics = planNodeCanonicalInfo.get().getInputTableStatistics();
                        PlanNodeWithHash planNodeWithHash = new PlanNodeWithHash(statsEquivalentPlanNode, Optional.of(hash));
                        // Plan node added after HistoricalStatisticsEquivalentPlanMarkingOptimizer will have the same hash as its source node. If the source node is not join or
                        // table writer node, the newly added node will have the same hash but no join/table writer statistics, hence we need to overwrite in this case.
                        PlanStatistics newPlanNodeStats = new PlanStatistics(
                                Estimate.of(outputPositions),
                                Double.isNaN(outputBytes) ? Estimate.unknown() : Estimate.of(outputBytes),
                                1.0,
                                joinNodeStatistics,
                                tableWriterNodeStatistics,
                                partialAggregationStatistics);
                        if (planStatisticsMap.containsKey(planNodeWithHash)) {
                            newPlanNodeStats = planStatisticsMap.get(planNodeWithHash).getPlanStatistics().update(newPlanNodeStats);
                        }
                        PlanStatisticsWithSourceInfo planStatsWithSourceInfo = new PlanStatisticsWithSourceInfo(
                                planNode.getId(),
                                newPlanNodeStats,
                                new HistoryBasedSourceInfo(Optional.of(hash), Optional.of(inputTableStatistics), Optional.of(historicalPlanStatisticsEntryInfo)));
                        planStatisticsMap.put(planNodeWithHash, planStatsWithSourceInfo);

                        if (isAggregation(planNode, AggregationNode.Step.FINAL) && ((AggregationNode) planNode).getAggregationId().isPresent() && trackPartialAggregationHistory(session)) {
                            // we're doing a depth-first traversal of the plan tree: cache the final agg so that when we encounter the partial agg we can come back
                            // and update the partial agg statistics
                            aggregationNodeMap.put(((AggregationNode) planNode).getAggregationId().get(), new FinalAggregationStatsInfo(planNodeWithHash, planStatsWithSourceInfo));
                        }
                    }
                }
            }
        }
        return ImmutableMap.copyOf(planStatisticsMap);
    }

    private static Set<PlanNodeId> getPlanNodeAppliedDynamicFilter(Map<PlanNodeId, PlanNodeStats> planNodeStatsMap, List<StageInfo> allStages)
    {
        Map<PlanNodeId, Set<PlanNodeId>> dynamicFilterNodeMap = new HashMap<>();
        planNodeStatsMap.forEach((planNodeId, planNodeStats) -> {
            if (planNodeStats.getDynamicFilterStats().isPresent()) {
                if (!dynamicFilterNodeMap.containsKey(planNodeId)) {
                    dynamicFilterNodeMap.put(planNodeId, new HashSet<>());
                }
                dynamicFilterNodeMap.get(planNodeId).addAll(planNodeStats.getDynamicFilterStats().get().getProducerNodeIds());
            }
        });
        if (dynamicFilterNodeMap.isEmpty()) {
            return ImmutableSet.of();
        }
        // Now find the path between producer and child node having dynamic filter applied. Reverse the tree so that all nodes' out degree is at most 1 and easier to find path between nodes
        MutableGraph<PlanNodeId> reversePlanTree = GraphBuilder.directed().allowsSelfLoops(false).build();
        for (StageInfo stageInfo : allStages) {
            if (!stageInfo.getPlan().isPresent()) {
                continue;
            }
            PlanNode root = stageInfo.getPlan().get().getRoot();
            for (PlanNode planNode : forTree(PlanNode::getSources).depthFirstPreOrder(root)) {
                for (PlanNode child : planNode.getSources()) {
                    reversePlanTree.putEdge(child.getId(), planNode.getId());
                }
            }
        }
        Set<PlanNodeId> planNodeIdsDynamicFilter = new HashSet<>();
        dynamicFilterNodeMap.forEach((destNode, producerNodes) -> {
            for (PlanNodeId producerNode : producerNodes) {
                PlanNodeId rootNode = destNode;
                Set<PlanNodeId> visitedNodes = new HashSet<>();
                while (!rootNode.equals(producerNode)) {
                    visitedNodes.add(rootNode);
                    if (reversePlanTree.successors(rootNode).isEmpty()) {
                        break;
                    }
                    checkState(reversePlanTree.successors(rootNode).size() == 1);
                    rootNode = reversePlanTree.successors(rootNode).stream().findFirst().orElse(null);
                }
                if (rootNode.equals(producerNode)) {
                    planNodeIdsDynamicFilter.addAll(visitedNodes);
                }
            }
        });
        return planNodeIdsDynamicFilter;
    }

    private static void updatePartialAggregationStatistics(
            AggregationNode partialAggregationNode,
            Map<Integer, FinalAggregationStatsInfo> aggregationNodeStats,
            PartialAggregationStatistics partialAggregationStatistics,
            Map<PlanNodeWithHash, PlanStatisticsWithSourceInfo> planStatisticsMap)
    {
        if (!partialAggregationNode.getAggregationId().isPresent() || !aggregationNodeStats.containsKey(partialAggregationNode.getAggregationId().get())) {
            return;
        }

        // find the stats for the matching final aggregation node (the partial and the final node share the same aggregationId)
        FinalAggregationStatsInfo finalAggregationStatsInfo = aggregationNodeStats.get(partialAggregationNode.getAggregationId().get());
        PlanStatisticsWithSourceInfo planStatisticsWithSourceInfo = finalAggregationStatsInfo.getPlanStatisticsWithSourceInfo();
        PlanStatistics planStatisticsFinalAgg = planStatisticsWithSourceInfo.getPlanStatistics();
        planStatisticsFinalAgg = planStatisticsFinalAgg.updateAggregationStatistics(partialAggregationStatistics);

        planStatisticsMap.put(
                finalAggregationStatsInfo.getPlanNodeWithHash(),
                new PlanStatisticsWithSourceInfo(
                        planStatisticsWithSourceInfo.getId(),
                        planStatisticsFinalAgg,
                        planStatisticsWithSourceInfo.getSourceInfo()));
    }

    private PartialAggregationStatistics constructAggregationNodeStatistics(PlanNode planNode, Map<PlanNodeId, PlanNodeStats> planNodeStatsMap, double outputBytes, double outputPositions)
    {
        PlanNode childNode = planNode.getSources().get(0);
        PlanNodeStats childNodeStats = planNodeStatsMap.get(childNode.getId());
        if (childNodeStats != null) {
            double partialAggregationInputBytes = adjustedOutputBytes(childNode, childNodeStats);
            return new PartialAggregationStatistics(
                    Double.isNaN(partialAggregationInputBytes) ? Estimate.unknown() : Estimate.of(partialAggregationInputBytes),
                    Double.isNaN(outputBytes) ? Estimate.unknown() : Estimate.of(outputBytes),
                    Estimate.of(childNodeStats.getPlanNodeOutputPositions()),
                    Estimate.of(outputPositions));
        }
        return PartialAggregationStatistics.empty();
    }

    private boolean isAggregation(PlanNode planNode, AggregationNode.Step step)
    {
        return planNode instanceof AggregationNode && ((AggregationNode) planNode).getStep() == step;
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
        // partial aggregation nodes have no stats equivalent plan node: use original output variables
        List<VariableReferenceExpression> outputVariables = planNode.getOutputVariables();
        if (planNode.getStatsEquivalentPlanNode().isPresent()) {
            outputVariables = planNode.getStatsEquivalentPlanNode().get().getOutputVariables();
        }
        outputBytes += outputVariables.stream()
                .mapToDouble(variable -> variable.getType() instanceof FixedWidthType ? outputPositions * ((FixedWidthType) variable.getType()).getFixedSize() : 0)
                .sum();
        // annotate illegal cases with NaN: if outputBytes is less than 0, or if there is at least 1 output row but less than 1 output byte
        // Note that this function may be called for partial aggs that produce no output columns (e.g. "select count(*)"), where 0 is a valid number of output bytes, in this case
        // the ouptput variables is an empty list
        if (outputBytes < 0 || (outputPositions > 0 && outputBytes < 1 && !outputVariables.isEmpty())) {
            outputBytes = Double.NaN;
        }
        return outputBytes;
    }

    public void updateStatistics(QueryInfo queryInfo)
    {
        Session session = queryInfo.getSession().toSession(sessionPropertyManager);
        if (!trackHistoryBasedPlanStatisticsEnabled(session)) {
            historyBasedStatisticsCacheManager.invalidate(queryInfo.getQueryId());
            return;
        }
        Map<PlanNodeWithHash, PlanStatisticsWithSourceInfo> planStatistics = getQueryStats(queryInfo);
        Map<PlanNodeWithHash, HistoricalPlanStatistics> historicalPlanStatisticsMap =
                historyBasedPlanStatisticsProvider.get().getStats(planStatistics.keySet().stream().collect(toImmutableList()), getHistoryBasedOptimizerTimeoutLimit(session).toMillis());
        Map<PlanNodeWithHash, HistoricalPlanStatistics> newPlanStatistics = planStatistics.entrySet().stream()
                .filter(entry -> entry.getKey().getHash().isPresent() &&
                        entry.getValue().getSourceInfo() instanceof HistoryBasedSourceInfo &&
                        ((HistoryBasedSourceInfo) entry.getValue().getSourceInfo()).getInputTableStatistics().isPresent() &&
                        ((HistoryBasedSourceInfo) entry.getValue().getSourceInfo()).getHistoricalPlanStatisticsEntryInfo().isPresent())
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
                                    config,
                                    historyBasedSourceInfo.getHistoricalPlanStatisticsEntryInfo().get());
                        }));

        if (!newPlanStatistics.isEmpty()) {
            historyBasedPlanStatisticsProvider.get().putStats(ImmutableMap.copyOf(newPlanStatistics));
        }
        historyBasedStatisticsCacheManager.invalidate(queryInfo.getQueryId());
    }

    private class FinalAggregationStatsInfo
    {
        private final PlanNodeWithHash planNodeWithHash;
        private final PlanStatisticsWithSourceInfo planStatisticsWithSourceInfo;

        FinalAggregationStatsInfo(PlanNodeWithHash planNodeWithHash, PlanStatisticsWithSourceInfo planStatisticsWithSourceInfo)
        {
            this.planNodeWithHash = requireNonNull(planNodeWithHash);
            this.planStatisticsWithSourceInfo = requireNonNull(planStatisticsWithSourceInfo);
        }

        public PlanNodeWithHash getPlanNodeWithHash()
        {
            return planNodeWithHash;
        }

        public PlanStatisticsWithSourceInfo getPlanStatisticsWithSourceInfo()
        {
            return planStatisticsWithSourceInfo;
        }
    }
}
