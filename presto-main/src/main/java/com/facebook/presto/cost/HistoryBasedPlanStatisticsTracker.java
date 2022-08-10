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
import com.facebook.presto.common.plan.PlanCanonicalizationStrategy;
import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeWithHash;
import com.facebook.presto.spi.resourceGroups.QueryType;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.HistoricalPlanStatistics;
import com.facebook.presto.spi.statistics.HistoryBasedPlanStatisticsProvider;
import com.facebook.presto.spi.statistics.PlanStatistics;
import com.facebook.presto.sql.planner.PlanHasher;
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
import static com.facebook.presto.spi.resourceGroups.QueryType.INSERT;
import static com.facebook.presto.spi.resourceGroups.QueryType.SELECT;
import static com.facebook.presto.sql.planner.planPrinter.PlanNodeStatsSummarizer.aggregateStageStats;
import static com.google.common.graph.Traverser.forTree;
import static java.util.Objects.requireNonNull;

public class HistoryBasedPlanStatisticsTracker
{
    private static final Logger LOG = Logger.get(HistoryBasedPlanStatisticsTracker.class);
    private static final Set<QueryType> ALLOWED_QUERY_TYPES = ImmutableSet.of(SELECT, INSERT);

    private final Supplier<HistoryBasedPlanStatisticsProvider> historyBasedPlanStatisticsProvider;
    private final SessionPropertyManager sessionPropertyManager;
    private final PlanHasher planHasher;

    public HistoryBasedPlanStatisticsTracker(
            Supplier<HistoryBasedPlanStatisticsProvider> historyBasedPlanStatisticsProvider,
            SessionPropertyManager sessionPropertyManager,
            PlanHasher planHasher)
    {
        this.historyBasedPlanStatisticsProvider = requireNonNull(historyBasedPlanStatisticsProvider, "historyBasedPlanStatisticsProvider is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.planHasher = requireNonNull(planHasher, "planHasher is null");
    }

    public void trackStatistics(QueryExecution queryExecution)
    {
        queryExecution.addFinalQueryInfoListener(this::trackStatistics);
    }

    @VisibleForTesting
    public HistoryBasedPlanStatisticsProvider getHistoryBasedPlanStatisticsProvider()
    {
        return historyBasedPlanStatisticsProvider.get();
    }

    private void trackStatistics(QueryInfo queryInfo)
    {
        if (!trackHistoryBasedPlanStatisticsEnabled(queryInfo.getSession().toSession(sessionPropertyManager))) {
            return;
        }

        // Only update statistics for successful queries
        if (queryInfo.getFailureInfo() != null ||
                !queryInfo.getOutputStage().isPresent() ||
                !queryInfo.getOutputStage().get().getPlan().isPresent()) {
            return;
        }

        // Only update statistics for SELECT/INSERT queries
        if (!queryInfo.getQueryType().isPresent() || !ALLOWED_QUERY_TYPES.contains(queryInfo.getQueryType().get())) {
            return;
        }

        if (!queryInfo.isFinalQueryInfo()) {
            LOG.error("Expected final query info when updating history based statistics: %s", queryInfo);
            return;
        }

        StageInfo outputStage = queryInfo.getOutputStage().get();
        List<StageInfo> allStages = outputStage.getAllStages();

        Map<PlanNodeId, PlanNodeStats> planNodeStatsMap = aggregateStageStats(allStages);
        Map<PlanNodeWithHash, HistoricalPlanStatistics> historicalPlanStats = new HashMap<>();

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
                    Optional<String> hash = planHasher.hash(statsEquivalentPlanNode, strategy);
                    if (hash.isPresent()) {
                        double outputPositions = planNodeStats.getPlanNodeOutputPositions();
                        double outputBytes = planNodeStats.getPlanNodeOutputDataSize().toBytes();
                        historicalPlanStats.put(
                                new PlanNodeWithHash(statsEquivalentPlanNode, hash),
                                new HistoricalPlanStatistics(new PlanStatistics(Estimate.of(outputPositions), Estimate.of(outputBytes), 1.0)));
                    }
                }
            }
        }
        historyBasedPlanStatisticsProvider.get().putStats(ImmutableMap.copyOf(historicalPlanStats));
    }
}
