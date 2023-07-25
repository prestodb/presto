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

package com.facebook.presto.spark.planner;

import com.facebook.presto.Session;
import com.facebook.presto.cost.HistoryBasedOptimizationConfig;
import com.facebook.presto.cost.HistoryBasedPlanStatisticsCalculator;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.statistics.HistoryBasedSourceInfo;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;

import java.util.List;

import static com.facebook.presto.cost.HistoricalPlanStatisticsUtil.similarStats;
import static java.util.Objects.requireNonNull;

/**
 * This StatsCalculator incorporates runtime statistics when available
 * and decides whether to use historical or runtime-based statistics.
 */
public class PrestoSparkStatsCalculator
        implements StatsCalculator

{
    private final HistoryBasedPlanStatisticsCalculator historyBasedPlanStatisticsCalculator;
    private final StatsCalculator delegate;
    private final HistoryBasedOptimizationConfig historyBasedOptimizationConfig;

    public PrestoSparkStatsCalculator(HistoryBasedPlanStatisticsCalculator historyBasedPlanStatisticsCalculator, StatsCalculator delegate, HistoryBasedOptimizationConfig historyBasedOptimizationConfig)
    {
        this.historyBasedPlanStatisticsCalculator = requireNonNull(historyBasedPlanStatisticsCalculator, "historyBasedPlanStatisticsCalculator is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.historyBasedOptimizationConfig = requireNonNull(historyBasedOptimizationConfig, "historyBasedOptimizationConfig");
    }

    @Override
    public PlanNodeStatsEstimate calculateStats(PlanNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types)
    {
        boolean shouldUseHistoricalStats = shouldUseHistoricalStats(node, sourceStats, lookup, session, types);
        if (shouldUseHistoricalStats) {
            return historyBasedPlanStatisticsCalculator.calculateStats(node, sourceStats, lookup, session, types);
        }

        return delegate.calculateStats(node, sourceStats, lookup, session, types);
    }

    private boolean shouldUseHistoricalStats(PlanNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types)
    {
        // RemoteSourceNode stats are computed at runtime.  If there are any
        // RemoteSourceNodes, check whether we should use historical or runtime stats
        // by comparing whether the runtime stats are similar to the historical stats
        // if they are similar, use historical stats. if they differ, use runtime stats.
        List<RemoteSourceNode> remoteSourceNodes = PlanNodeSearcher.searchFrom(node, lookup)
                .where(RemoteSourceNode.class::isInstance)
                .findAll();
        for (RemoteSourceNode remoteSourceNode : remoteSourceNodes) {
            PlanNodeStatsEstimate historicalStats = historyBasedPlanStatisticsCalculator.calculateStats(remoteSourceNode, sourceStats, lookup, session, types);
            PlanNodeStatsEstimate runtimeStats = delegate.calculateStats(remoteSourceNode, sourceStats, lookup, session, types);
            if (!runtimeStats.isTotalSizeUnknown() &&
                    (!(historicalStats.getSourceInfo() instanceof HistoryBasedSourceInfo) ||
                            !similarStats(historicalStats.getTotalSize(), runtimeStats.getTotalSize(), historyBasedOptimizationConfig.getHistoryMatchingThreshold()))) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean registerPlan(PlanNode root, Session session, long startTimeInNano, long timeoutInMilliseconds)
    {
        return historyBasedPlanStatisticsCalculator.registerPlan(root, session, startTimeInNano, timeoutInMilliseconds);
    }

    public HistoryBasedPlanStatisticsCalculator getHistoryBasedPlanStatisticsCalculator()
    {
        return historyBasedPlanStatisticsCalculator;
    }
}
