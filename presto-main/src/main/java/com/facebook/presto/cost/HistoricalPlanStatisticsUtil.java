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

import com.facebook.presto.spi.statistics.HistoricalPlanStatistics;
import com.facebook.presto.spi.statistics.HistoricalPlanStatisticsEntry;
import com.facebook.presto.spi.statistics.PlanStatistics;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.lang.Double.isNaN;

public class HistoricalPlanStatisticsUtil
{
    private HistoricalPlanStatisticsUtil() {}

    /**
     * Returns predicted plan statistics depending on historical runs
     */
    public static PlanStatistics getPredictedPlanStatistics(
            HistoricalPlanStatistics historicalPlanStatistics,
            List<PlanStatistics> inputTableStatistics,
            HistoryBasedOptimizationConfig config)
    {
        List<HistoricalPlanStatisticsEntry> lastRunsStatistics = historicalPlanStatistics.getLastRunsStatistics();
        if (lastRunsStatistics.isEmpty()) {
            return PlanStatistics.empty();
        }

        Optional<Integer> similarStatsIndex = getSimilarStatsIndex(historicalPlanStatistics, inputTableStatistics, config.getHistoryMatchingThreshold());

        if (similarStatsIndex.isPresent()) {
            return lastRunsStatistics.get(similarStatsIndex.get()).getPlanStatistics();
        }

        // TODO: Use linear regression to predict stats if we have only 1 table.
        return PlanStatistics.empty();
    }

    /**
     * Returns updated HistoricalPlanStatistics after an update with new PlanStatistics
     */
    public static HistoricalPlanStatistics updatePlanStatistics(
            HistoricalPlanStatistics historicalPlanStatistics,
            List<PlanStatistics> inputTableStatistics,
            PlanStatistics current,
            HistoryBasedOptimizationConfig config)
    {
        List<HistoricalPlanStatisticsEntry> lastRunsStatistics = historicalPlanStatistics.getLastRunsStatistics();

        List<HistoricalPlanStatisticsEntry> newLastRunsStatistics = new ArrayList<>(lastRunsStatistics);

        Optional<Integer> similarStatsIndex = getSimilarStatsIndex(historicalPlanStatistics, inputTableStatistics, config.getHistoryMatchingThreshold());
        if (similarStatsIndex.isPresent()) {
            newLastRunsStatistics.remove(similarStatsIndex.get().intValue());
        }

        newLastRunsStatistics.add(new HistoricalPlanStatisticsEntry(current, inputTableStatistics));
        int maxLastRuns = inputTableStatistics.isEmpty() ? 1 : config.getMaxLastRunsHistory();
        if (newLastRunsStatistics.size() > maxLastRuns) {
            newLastRunsStatistics.remove(0);
        }

        return new HistoricalPlanStatistics(newLastRunsStatistics);
    }

    private static Optional<Integer> getSimilarStatsIndex(
            HistoricalPlanStatistics historicalPlanStatistics,
            List<PlanStatistics> inputTableStatistics,
            double threshold)
    {
        List<HistoricalPlanStatisticsEntry> lastRunsStatistics = historicalPlanStatistics.getLastRunsStatistics();

        if (lastRunsStatistics.isEmpty()) {
            return Optional.empty();
        }

        for (int lastRunsIndex = 0; lastRunsIndex < lastRunsStatistics.size(); ++lastRunsIndex) {
            if (inputTableStatistics.size() != lastRunsStatistics.get(lastRunsIndex).getInputTableStatistics().size()) {
                // This is not expected, but may happen when changing thrift definitions.
                continue;
            }
            boolean rowSimilarity = true;
            boolean outputSizeSimilarity = true;

            for (int inputTablesIndex = 0; inputTablesIndex < inputTableStatistics.size(); ++inputTablesIndex) {
                PlanStatistics currentInputStatistics = inputTableStatistics.get(inputTablesIndex);
                PlanStatistics historicalInputStatistics = lastRunsStatistics.get(lastRunsIndex).getInputTableStatistics().get(inputTablesIndex);

                rowSimilarity = rowSimilarity && similarStats(currentInputStatistics.getRowCount().getValue(), historicalInputStatistics.getRowCount().getValue(), threshold);
                outputSizeSimilarity = outputSizeSimilarity && similarStats(currentInputStatistics.getOutputSize().getValue(), historicalInputStatistics.getOutputSize().getValue(), threshold);
            }
            // Write information if both rows and output size are similar.
            if (rowSimilarity && outputSizeSimilarity) {
                return Optional.of(lastRunsIndex);
            }
        }
        return Optional.empty();
    }

    private static boolean similarStats(double stats1, double stats2, double threshold)
    {
        if (isNaN(stats1) && isNaN(stats2)) {
            return true;
        }
        return stats1 >= (1 - threshold) * stats2 && stats1 <= (1 + threshold) * stats2;
    }
}
