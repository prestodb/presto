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

import com.facebook.presto.sql.planner.Symbol;

import java.util.HashSet;
import java.util.stream.Stream;

import static com.facebook.presto.cost.AggregationStatsRule.groupBy;
import static com.facebook.presto.util.MoreMath.min;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.emptyMap;

public class PlanNodeStatsEstimateMath
{
    private PlanNodeStatsEstimateMath()
    {
    }

    private interface SubtractRangeStrategy
    {
        StatisticRange range(StatisticRange leftRange, StatisticRange rightRange);
    }

    public static PlanNodeStatsEstimate differenceInStats(PlanNodeStatsEstimate left, PlanNodeStatsEstimate right)
    {
        return differenceInStatsWithRangeStrategy(left, right, StatisticRange::subtract);
    }

    public static PlanNodeStatsEstimate differenceInNonRangeStats(PlanNodeStatsEstimate left, PlanNodeStatsEstimate right)
    {
        return differenceInStatsWithRangeStrategy(left, right, ((leftRange, rightRange) -> leftRange));
    }

    private static PlanNodeStatsEstimate differenceInStatsWithRangeStrategy(PlanNodeStatsEstimate left, PlanNodeStatsEstimate right, SubtractRangeStrategy strategy)
    {
        PlanNodeStatsEstimate.Builder statsBuilder = PlanNodeStatsEstimate.builder();
        double newRowCount = left.getOutputRowCount() - right.getOutputRowCount();

        Stream.concat(left.getSymbolsWithKnownStatistics().stream(), right.getSymbolsWithKnownStatistics().stream())
                .forEach(symbol -> {
                    statsBuilder.addSymbolStatistics(
                            symbol,
                            subtractColumnStats(
                                    left.getSymbolStatistics(symbol),
                                    left.getOutputRowCount(),
                                    right.getSymbolStatistics(symbol),
                                    right.getOutputRowCount(),
                                    newRowCount,
                                    strategy));
                });

        return statsBuilder.setOutputRowCount(newRowCount).build();
    }

    private static SymbolStatsEstimate subtractColumnStats(
            SymbolStatsEstimate leftStats,
            double leftRowCount,
            SymbolStatsEstimate rightStats,
            double rightRowCount,
            double newRowCount,
            SubtractRangeStrategy strategy)
    {
        StatisticRange leftRange = StatisticRange.from(leftStats);
        StatisticRange rightRange = StatisticRange.from(rightStats);

        double nullsCountLeft = leftStats.getNullsFraction() * leftRowCount;
        double nullsCountRight = rightStats.getNullsFraction() * rightRowCount;
        double totalSizeLeft = leftRowCount * leftStats.getAverageRowSize();
        double totalSizeRight = rightRowCount * rightStats.getAverageRowSize();
        StatisticRange range = strategy.range(leftRange, rightRange);

        return SymbolStatsEstimate.builder()
                .setDistinctValuesCount(leftStats.getDistinctValuesCount() - rightStats.getDistinctValuesCount())
                .setHighValue(range.getHigh())
                .setLowValue(range.getLow())
                .setAverageRowSize((totalSizeLeft - totalSizeRight) / newRowCount)
                .setNullsFraction((nullsCountLeft - nullsCountRight) / newRowCount)
                .build();
    }

    @FunctionalInterface
    private interface RangeAdditionStrategy
    {
        StatisticRange add(StatisticRange leftRange, StatisticRange rightRange);
    }

    public static PlanNodeStatsEstimate addStatsAndSumDistinctValues(PlanNodeStatsEstimate left, PlanNodeStatsEstimate right)
    {
        return addStats(left, right, StatisticRange::addAndSumDistinctValues);
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
                .forEach(symbol -> {
                    statsBuilder.addSymbolStatistics(symbol,
                            addColumnStats(left.getSymbolStatistics(symbol),
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
        double totalSizeLeft = leftRows * leftStats.getAverageRowSize();
        double totalSizeRight = rightRows * rightStats.getAverageRowSize();

        return SymbolStatsEstimate.builder()
                .setStatisticsRange(sum)
                .setAverageRowSize((totalSizeLeft + totalSizeRight) / newRowCount) // FIXME, weights to average. left and right should be equal in most cases anyway
                .setNullsFraction((nullsCountLeft + nullsCountRight) / newRowCount)
                .build();
    }

    public static PlanNodeStatsEstimate intersect(PlanNodeStatsEstimate left, PlanNodeStatsEstimate right)
    {
        checkArgument(new HashSet<>(left.getSymbolsWithKnownStatistics()).equals(new HashSet<>(right.getSymbolsWithKnownStatistics())));

        PlanNodeStatsEstimate.Builder statsBuilder = PlanNodeStatsEstimate.builder();

        for (Symbol symbol : left.getSymbolsWithKnownStatistics()) {
            SymbolStatsEstimate leftSymbolStats = left.getSymbolStatistics(symbol);
            SymbolStatsEstimate rightSymbolStats = right.getSymbolStatistics(symbol);
            StatisticRange leftRange = StatisticRange.from(leftSymbolStats);
            StatisticRange rightRange = StatisticRange.from(rightSymbolStats);
            StatisticRange intersection = leftRange.intersect(rightRange);

            statsBuilder.addSymbolStatistics(
                    symbol,
                    SymbolStatsEstimate.builder()
                            .setStatisticsRange(intersection)
                            // it does matter how many nulls are preserved, the intersting point is the fact if there are nulls both sides or not
                            // this will be normalized later by groupBy
                            .setNullsFraction(min(leftSymbolStats.getNullsFraction(), rightSymbolStats.getNullsFraction()))
                            .build());
        }
        statsBuilder.setOutputRowCount(left.getOutputRowCount() + right.getOutputRowCount());  // this is the maximum row count;
        PlanNodeStatsEstimate intermediateResult = statsBuilder.build();
        return groupBy(intermediateResult, intermediateResult.getSymbolsWithKnownStatistics(), emptyMap());
    }
}
