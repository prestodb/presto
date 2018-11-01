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

import java.util.stream.Stream;

public class PlanNodeStatsEstimateMath
{
    private PlanNodeStatsEstimateMath()
    {}

    @FunctionalInterface
    private interface RangeAdditionStrategy
    {
        StatisticRange add(StatisticRange leftRange, StatisticRange rightRange);
    }

    public static PlanNodeStatsEstimate addStatsAndSumDistinctValues(PlanNodeStatsEstimate left, PlanNodeStatsEstimate right)
    {
        return addStats(left, right, StatisticRange::addAndSumDistinctValues);
    }

    public static PlanNodeStatsEstimate addStatsAndMaxDistinctValues(PlanNodeStatsEstimate left, PlanNodeStatsEstimate right)
    {
        return addStats(left, right, StatisticRange::addAndMaxDistinctValues);
    }

    public static PlanNodeStatsEstimate addStatsAndCollapseDistinctValues(PlanNodeStatsEstimate left, PlanNodeStatsEstimate right)
    {
        return addStats(left, right, StatisticRange::addAndCollapseDistinctValues);
    }

    private static PlanNodeStatsEstimate addStats(PlanNodeStatsEstimate left, PlanNodeStatsEstimate right, RangeAdditionStrategy strategy)
    {
        PlanNodeStatsEstimate.Builder statsBuilder = PlanNodeStatsEstimate.builder();
        double newRowCount = left.getOutputRowCount() + right.getOutputRowCount();

        Stream.concat(left.getSymbolsWithKnownStatistics().stream(), right.getSymbolsWithKnownStatistics().stream())
                .distinct()
                .forEach(symbol -> {
                    statsBuilder.addSymbolStatistics(symbol,
                            addColumnStats(
                                    left.getSymbolStatistics(symbol),
                                    left.getOutputRowCount(),
                                    right.getSymbolStatistics(symbol),
                                    right.getOutputRowCount(),
                                    newRowCount,
                                    strategy));
                });

        return statsBuilder.setOutputRowCount(newRowCount).build();
    }

    private static SymbolStatsEstimate addColumnStats(SymbolStatsEstimate leftStats, double leftRows, SymbolStatsEstimate rightStats, double rightRows, double newRowCount, RangeAdditionStrategy strategy)
    {
        StatisticRange leftRange = StatisticRange.from(leftStats);
        StatisticRange rightRange = StatisticRange.from(rightStats);

        StatisticRange sum = strategy.add(leftRange, rightRange);
        double nullsCountRight = rightStats.getNullsFraction() * rightRows;
        double nullsCountLeft = leftStats.getNullsFraction() * leftRows;
        double totalSizeLeft = (leftRows - nullsCountLeft) * leftStats.getAverageRowSize();
        double totalSizeRight = (rightRows - nullsCountRight) * rightStats.getAverageRowSize();
        double newNullsFraction = (nullsCountLeft + nullsCountRight) / newRowCount;
        double newNonNullsRowCount = newRowCount * (1.0 - newNullsFraction);

        return SymbolStatsEstimate.builder()
                .setStatisticsRange(sum)
                .setAverageRowSize((totalSizeLeft + totalSizeRight) / newNonNullsRowCount) // FIXME, weights to average. left and right should be equal in most cases anyway
                .setNullsFraction(newNullsFraction)
                .build();
    }
}
