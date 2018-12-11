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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Double.isNaN;
import static java.lang.Double.max;
import static java.lang.Double.min;
import static java.util.stream.Stream.concat;

public class PlanNodeStatsEstimateMath
{
    private PlanNodeStatsEstimateMath()
    {}

    public static PlanNodeStatsEstimate subtractStats(PlanNodeStatsEstimate superset, PlanNodeStatsEstimate subset)
    {
        if (superset.isOutputRowCountUnknown() || subset.isOutputRowCountUnknown()) {
            return PlanNodeStatsEstimate.unknown();
        }

        double supersetRowCount = superset.getOutputRowCount();
        double subsetRowCount = subset.getOutputRowCount();
        double outputRowCount = supersetRowCount - subsetRowCount;
        verify(outputRowCount >= 0, "outputRowCount must be greater than or equal to zero: %s", outputRowCount);

        // everything will be filtered out after applying negation
        if (outputRowCount == 0) {
            return createZeroStats(superset);
        }

        PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.builder();
        result.setOutputRowCount(outputRowCount);

        superset.getSymbolsWithKnownStatistics().forEach(symbol -> {
            SymbolStatsEstimate supersetSymbolStats = superset.getSymbolStatistics(symbol);
            SymbolStatsEstimate subsetSymbolStats = subset.getSymbolStatistics(symbol);

            SymbolStatsEstimate.Builder newSymbolStats = SymbolStatsEstimate.builder();

            // for simplicity keep the average row size the same as in the input
            // in most cases the average row size doesn't change after applying filters
            newSymbolStats.setAverageRowSize(supersetSymbolStats.getAverageRowSize());

            // nullsCount
            double supersetNullsCount = supersetSymbolStats.getNullsFraction() * supersetRowCount;
            double subsetNullsCount = subsetSymbolStats.getNullsFraction() * subsetRowCount;
            double newNullsCount = supersetNullsCount - subsetNullsCount;
            verify(isNaN(newNullsCount) || newNullsCount >= 0, "newNullsCount must greater than or equal to zero: %s", newNullsCount);
            newSymbolStats.setNullsFraction(min(newNullsCount, outputRowCount) / outputRowCount);

            // distinctValuesCount
            double supersetDistinctValues = supersetSymbolStats.getDistinctValuesCount();
            double subsetDistinctValues = subsetSymbolStats.getDistinctValuesCount();
            double newDistinctValuesCount;
            if (isNaN(supersetDistinctValues) || isNaN(subsetDistinctValues)) {
                newDistinctValuesCount = NaN;
            }
            else if (supersetDistinctValues == 0) {
                newDistinctValuesCount = 0;
            }
            else if (subsetDistinctValues == 0) {
                newDistinctValuesCount = supersetDistinctValues;
            }
            else {
                double supersetNonNullsCount = supersetRowCount - supersetNullsCount;
                double subsetNonNullsCount = subsetRowCount - subsetNullsCount;
                double supersetValuesPerDistinctValue = supersetNonNullsCount / supersetDistinctValues;
                double subsetValuesPerDistinctValue = subsetNonNullsCount / subsetDistinctValues;
                if (supersetValuesPerDistinctValue <= subsetValuesPerDistinctValue) {
                    newDistinctValuesCount = supersetDistinctValues - subsetDistinctValues;
                }
                else {
                    newDistinctValuesCount = supersetDistinctValues;
                }
            }
            verify(isNaN(newDistinctValuesCount) || newDistinctValuesCount >= 0, "newDistinctValuesCount must be greater or equal than zero: %s", newDistinctValuesCount);
            newSymbolStats.setDistinctValuesCount(newDistinctValuesCount);

            // range
            double supersetLow = supersetSymbolStats.getLowValue();
            double supersetHigh = supersetSymbolStats.getHighValue();
            double subsetLow = subsetSymbolStats.getLowValue();
            double subsetHigh = subsetSymbolStats.getHighValue();
            if (isEmptyRange(supersetLow, supersetHigh)) {
                newSymbolStats.setLowValue(NaN);
                newSymbolStats.setHighValue(NaN);
            }
            else if (isEmptyRange(subsetLow, subsetHigh) || isUnknownRange(subsetLow, subsetHigh)) {
                newSymbolStats.setLowValue(supersetLow);
                newSymbolStats.setHighValue(supersetHigh);
            }
            else {
                verify(supersetLow <= subsetLow, "supersetLow [%s] must be less than or equal to subsetLow [%s]", supersetLow, subsetLow);
                verify(supersetHigh >= subsetHigh, "supersetHigh [%s] must be greater than or equal to subsetHigh [%s]", supersetHigh, subsetHigh);

                double newLow = supersetLow;
                double newHigh = supersetHigh;

                if (supersetLow == subsetLow && supersetHigh != subsetHigh) {
                    newLow = subsetHigh;
                }
                if (supersetHigh == subsetHigh && supersetLow != subsetLow) {
                    newHigh = subsetLow;
                }

                newSymbolStats.setHighValue(newHigh);
                newSymbolStats.setLowValue(newLow);
            }

            result.addSymbolStatistics(symbol, newSymbolStats.build());
        });

        return result.build();
    }

    private static boolean isEmptyRange(double low, double high)
    {
        if (isNaN(low) || isNaN(high)) {
            checkArgument(isNaN(low) && isNaN(high), "low and high are both expected to be NaN. low: %s, high: %s.", low, high);
            return true;
        }
        return false;
    }

    private static boolean isUnknownRange(double low, double high)
    {
        return low == NEGATIVE_INFINITY && high == POSITIVE_INFINITY;
    }

    public static PlanNodeStatsEstimate computeSymmetricDifferenceStats(
            PlanNodeStatsEstimate superset,
            PlanNodeStatsEstimate left,
            PlanNodeStatsEstimate right,
            PlanNodeStatsEstimate intersect)
    {
        if (superset.isOutputRowCountUnknown() || left.isOutputRowCountUnknown() || right.isOutputRowCountUnknown() || intersect.isOutputRowCountUnknown()) {
            return PlanNodeStatsEstimate.unknown();
        }

        PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.builder();
        double outputRowCount = min(left.getOutputRowCount() + right.getOutputRowCount() - intersect.getOutputRowCount(), superset.getOutputRowCount());
        verify(outputRowCount >= 0, "outputRowCount must be greater than or equal to zero: %s", outputRowCount);
        result.setOutputRowCount(outputRowCount);

        if (outputRowCount == 0) {
            return createZeroStats(superset);
        }

        superset.getSymbolsWithKnownStatistics().forEach(symbol -> {
            SymbolStatsEstimate supersetSymbolStats = superset.getSymbolStatistics(symbol);
            SymbolStatsEstimate leftSymbolStats = left.getSymbolStatistics(symbol);
            SymbolStatsEstimate rightSymbolStats = right.getSymbolStatistics(symbol);
            SymbolStatsEstimate intersectSymbolStats = intersect.getSymbolStatistics(symbol);

            SymbolStatsEstimate.Builder newSymbolStats = SymbolStatsEstimate.builder();

            // for simplicity keep the average row size the same as in the input
            // in most cases the average row size doesn't change after applying filters
            newSymbolStats.setAverageRowSize(supersetSymbolStats.getAverageRowSize());

            // distinctValuesCount, range
            // assuming distinct values are non-overlapping
            StatisticRange range = leftSymbolStats.statisticRange().addAndSumDistinctValues(rightSymbolStats.statisticRange());
            newSymbolStats.setStatisticsRange(range);
            double supersetDistinctValuesCount = supersetSymbolStats.getDistinctValuesCount();
            if (range.getDistinctValuesCount() > supersetDistinctValuesCount) {
                newSymbolStats.setDistinctValuesCount(supersetDistinctValuesCount);
            }

            // nullsCount
            double leftNumberOfNulls = leftSymbolStats.getNullsFraction() * left.getOutputRowCount();
            double rightNumberOfNulls = rightSymbolStats.getNullsFraction() * right.getOutputRowCount();
            double andNumberOfNulls = intersectSymbolStats.getNullsFraction() * intersect.getOutputRowCount();
            double supersetNumberOfNulls = supersetSymbolStats.getNullsFraction() * superset.getOutputRowCount();
            double newNumberOfNulls = max(leftNumberOfNulls + rightNumberOfNulls - andNumberOfNulls, 0);
            double newNullsFraction = min(min(newNumberOfNulls, supersetNumberOfNulls), outputRowCount) / outputRowCount;
            newSymbolStats.setNullsFraction(newNullsFraction);

            result.addSymbolStatistics(symbol, newSymbolStats.build());
        });

        return result.build();
    }

    private static PlanNodeStatsEstimate createZeroStats(PlanNodeStatsEstimate stats)
    {
        PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.builder();
        result.setOutputRowCount(0);
        stats.getSymbolsWithKnownStatistics().forEach(symbol -> result.addSymbolStatistics(symbol, SymbolStatsEstimate.zero()));
        return result.build();
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
        if (left.isOutputRowCountUnknown() || right.isOutputRowCountUnknown()) {
            return PlanNodeStatsEstimate.unknown();
        }

        PlanNodeStatsEstimate.Builder statsBuilder = PlanNodeStatsEstimate.builder();
        double newRowCount = left.getOutputRowCount() + right.getOutputRowCount();

        concat(left.getSymbolsWithKnownStatistics().stream(), right.getSymbolsWithKnownStatistics().stream())
                .distinct()
                .forEach(symbol -> {
                    SymbolStatsEstimate symbolStats = SymbolStatsEstimate.zero();
                    if (newRowCount > 0) {
                        symbolStats = addColumnStats(
                                left.getSymbolStatistics(symbol),
                                left.getOutputRowCount(),
                                right.getSymbolStatistics(symbol),
                                right.getOutputRowCount(),
                                newRowCount,
                                strategy);
                    }
                    statsBuilder.addSymbolStatistics(symbol, symbolStats);
                });

        return statsBuilder.setOutputRowCount(newRowCount).build();
    }

    private static SymbolStatsEstimate addColumnStats(SymbolStatsEstimate leftStats, double leftRows, SymbolStatsEstimate rightStats, double rightRows, double newRowCount, RangeAdditionStrategy strategy)
    {
        checkArgument(newRowCount > 0, "newRowCount must be greater than zero");

        StatisticRange leftRange = StatisticRange.from(leftStats);
        StatisticRange rightRange = StatisticRange.from(rightStats);

        StatisticRange sum = strategy.add(leftRange, rightRange);
        double nullsCountRight = rightStats.getNullsFraction() * rightRows;
        double nullsCountLeft = leftStats.getNullsFraction() * leftRows;
        double totalSizeLeft = (leftRows - nullsCountLeft) * leftStats.getAverageRowSize();
        double totalSizeRight = (rightRows - nullsCountRight) * rightStats.getAverageRowSize();
        double newNullsFraction = (nullsCountLeft + nullsCountRight) / newRowCount;
        double newNonNullsRowCount = newRowCount * (1.0 - newNullsFraction);

        // FIXME, weights to average. left and right should be equal in most cases anyway
        double newAverageRowSize = newNonNullsRowCount == 0 ? 0 : ((totalSizeLeft + totalSizeRight) / newNonNullsRowCount);

        return SymbolStatsEstimate.builder()
                .setStatisticsRange(sum)
                .setAverageRowSize(newAverageRowSize)
                .setNullsFraction(newNullsFraction)
                .build();
    }
}
