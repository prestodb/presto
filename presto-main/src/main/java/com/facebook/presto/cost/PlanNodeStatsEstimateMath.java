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

import com.facebook.presto.spi.statistics.ConnectorHistogram;
import com.facebook.presto.spi.statistics.DisjointRangeDomainHistogram;

import java.util.Optional;

import static com.facebook.presto.spi.statistics.DisjointRangeDomainHistogram.addConjunction;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;
import static java.lang.Double.max;
import static java.lang.Double.min;
import static java.util.stream.Stream.concat;

public class PlanNodeStatsEstimateMath
{
    private final boolean shouldUseHistograms;
    public PlanNodeStatsEstimateMath(boolean shouldUseHistograms)
    {
        this.shouldUseHistograms = shouldUseHistograms;
    }

    /**
     * Subtracts subset stats from supersets stats.
     * It is assumed that each NDV from subset has a matching NDV in superset.
     */
    public PlanNodeStatsEstimate subtractSubsetStats(PlanNodeStatsEstimate superset, PlanNodeStatsEstimate subset)
    {
        if (superset.isOutputRowCountUnknown() || subset.isOutputRowCountUnknown()) {
            return PlanNodeStatsEstimate.unknown();
        }

        double supersetRowCount = superset.getOutputRowCount();
        double subsetRowCount = subset.getOutputRowCount();
        double outputRowCount = max(supersetRowCount - subsetRowCount, 0);

        // everything will be filtered out after applying negation
        if (outputRowCount == 0) {
            return createZeroStats(superset);
        }

        PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.builder();
        result.setOutputRowCount(outputRowCount);

        superset.getVariablesWithKnownStatistics().forEach(symbol -> {
            VariableStatsEstimate supersetSymbolStats = superset.getVariableStatistics(symbol);
            VariableStatsEstimate subsetSymbolStats = subset.getVariableStatistics(symbol);

            VariableStatsEstimate.Builder newSymbolStats = VariableStatsEstimate.builder();

            // for simplicity keep the average row size the same as in the input
            // in most cases the average row size doesn't change after applying filters
            newSymbolStats.setAverageRowSize(supersetSymbolStats.getAverageRowSize());

            // nullsCount
            double supersetNullsCount = supersetSymbolStats.getNullsFraction() * supersetRowCount;
            double subsetNullsCount = subsetSymbolStats.getNullsFraction() * subsetRowCount;
            double newNullsCount = max(supersetNullsCount - subsetNullsCount, 0);
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
                    newDistinctValuesCount = max(supersetDistinctValues - subsetDistinctValues, 0);
                }
                else {
                    newDistinctValuesCount = supersetDistinctValues;
                }
            }
            newSymbolStats.setDistinctValuesCount(newDistinctValuesCount);

            // range
            newSymbolStats.setLowValue(supersetSymbolStats.getLowValue());
            newSymbolStats.setHighValue(supersetSymbolStats.getHighValue());

            result.addVariableStatistics(symbol, newSymbolStats.build());
        });

        return result.build();
    }

    public PlanNodeStatsEstimate capStats(PlanNodeStatsEstimate stats, PlanNodeStatsEstimate cap)
    {
        if (stats.isOutputRowCountUnknown() || cap.isOutputRowCountUnknown()) {
            return PlanNodeStatsEstimate.unknown();
        }

        PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.builder();
        double cappedRowCount = min(stats.getOutputRowCount(), cap.getOutputRowCount());
        result.setOutputRowCount(cappedRowCount);

        stats.getVariablesWithKnownStatistics().forEach(symbol -> {
            VariableStatsEstimate symbolStats = stats.getVariableStatistics(symbol);
            VariableStatsEstimate capSymbolStats = cap.getVariableStatistics(symbol);

            VariableStatsEstimate.Builder newSymbolStats = VariableStatsEstimate.builder();

            // for simplicity keep the average row size the same as in the input
            // in most cases the average row size doesn't change after applying filters
            newSymbolStats.setAverageRowSize(symbolStats.getAverageRowSize());
            newSymbolStats.setDistinctValuesCount(min(symbolStats.getDistinctValuesCount(), capSymbolStats.getDistinctValuesCount()));
            double newLow = max(symbolStats.getLowValue(), capSymbolStats.getLowValue());
            double newHigh = min(symbolStats.getHighValue(), capSymbolStats.getHighValue());
            newSymbolStats.setLowValue(newLow);
            newSymbolStats.setHighValue(newHigh);

            double numberOfNulls = stats.getOutputRowCount() * symbolStats.getNullsFraction();
            double capNumberOfNulls = cap.getOutputRowCount() * capSymbolStats.getNullsFraction();
            double cappedNumberOfNulls = min(numberOfNulls, capNumberOfNulls);
            double cappedNullsFraction = cappedRowCount == 0 ? 1 : cappedNumberOfNulls / cappedRowCount;
            newSymbolStats.setNullsFraction(cappedNullsFraction);
            if (shouldUseHistograms) {
                newSymbolStats.setHistogram(symbolStats.getHistogram().map(symbolHistogram -> addConjunction(symbolHistogram, new StatisticRange(newLow, newHigh, 0).toPrestoRange())));
            }

            result.addVariableStatistics(symbol, newSymbolStats.build());
        });

        return result.build();
    }

    private static PlanNodeStatsEstimate createZeroStats(PlanNodeStatsEstimate stats)
    {
        PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.builder();
        result.setOutputRowCount(0);
        stats.getVariablesWithKnownStatistics().forEach(symbol -> result.addVariableStatistics(symbol, VariableStatsEstimate.zero()));
        return result.build();
    }

    protected enum RangeAdditionStrategy
    {
        ADD_AND_SUM_DISTINCT(StatisticRange::addAndSumDistinctValues),
        ADD_AND_MAX_DISTINCT(StatisticRange::addAndMaxDistinctValues),
        ADD_AND_COLLAPSE_DISTINCT(StatisticRange::addAndCollapseDistinctValues),
        INTERSECT(StatisticRange::intersect);
        private final RangeAdditionFunction rangeAdditionFunction;

        RangeAdditionStrategy(RangeAdditionFunction rangeAdditionFunction)
        {
            this.rangeAdditionFunction = rangeAdditionFunction;
        }

        public RangeAdditionFunction getRangeAdditionFunction()
        {
            return rangeAdditionFunction;
        }
    }

    @FunctionalInterface
    protected interface RangeAdditionFunction
    {
        StatisticRange add(StatisticRange leftRange, StatisticRange rightRange);
    }

    public PlanNodeStatsEstimate addStatsAndSumDistinctValues(PlanNodeStatsEstimate left, PlanNodeStatsEstimate right)
    {
        return addStats(left, right, RangeAdditionStrategy.ADD_AND_SUM_DISTINCT);
    }

    public PlanNodeStatsEstimate addStatsAndMaxDistinctValues(PlanNodeStatsEstimate left, PlanNodeStatsEstimate right)
    {
        return addStats(left, right, RangeAdditionStrategy.ADD_AND_MAX_DISTINCT);
    }

    public PlanNodeStatsEstimate addStatsAndCollapseDistinctValues(PlanNodeStatsEstimate left, PlanNodeStatsEstimate right)
    {
        return addStats(left, right, RangeAdditionStrategy.ADD_AND_COLLAPSE_DISTINCT);
    }

    public PlanNodeStatsEstimate addStatsAndIntersect(PlanNodeStatsEstimate left, PlanNodeStatsEstimate right)
    {
        if (left.isOutputRowCountUnknown() || right.isOutputRowCountUnknown()) {
            return PlanNodeStatsEstimate.unknown();
        }

        PlanNodeStatsEstimate.Builder statsBuilder = PlanNodeStatsEstimate.builder();
        double estimatedRowCount = Math.min(left.getOutputRowCount(), right.getOutputRowCount());
        double rowCount = concat(
                left.getVariablesWithKnownStatistics().stream(),
                right.getVariablesWithKnownStatistics().stream())
                .distinct()
                .map(symbol -> {
                    StatisticRange lstats = StatisticRange.from(left.getVariableStatistics(symbol));
                    StatisticRange rstats = StatisticRange.from(right.getVariableStatistics(symbol));
                    return Math.min(
                            left.getOutputRowCount() * lstats.overlapPercentWith(rstats),
                            right.getOutputRowCount() * rstats.overlapPercentWith(lstats));
                }).reduce(Math::min).orElse(estimatedRowCount);

        buildVariableStatistics(left, right, statsBuilder, rowCount, RangeAdditionStrategy.INTERSECT);

        return statsBuilder.setOutputRowCount(rowCount).build();
    }

    private PlanNodeStatsEstimate addStats(
            PlanNodeStatsEstimate left,
            PlanNodeStatsEstimate right,
            RangeAdditionStrategy strategy)
    {
        double rowCount = left.getOutputRowCount() + right.getOutputRowCount();
        double totalSize = left.getTotalSize() + right.getTotalSize();

        if (isNaN(rowCount) && isNaN(totalSize)) {
            return PlanNodeStatsEstimate.unknown();
        }

        PlanNodeStatsEstimate.Builder statsBuilder = PlanNodeStatsEstimate.builder();
        buildVariableStatistics(left, right, statsBuilder, rowCount, strategy);

        return statsBuilder.setOutputRowCount(rowCount)
                .setTotalSize(totalSize).build();
    }

    private void buildVariableStatistics(
            PlanNodeStatsEstimate left,
            PlanNodeStatsEstimate right,
            PlanNodeStatsEstimate.Builder statsBuilder,
            double estimatedRowCount,
            RangeAdditionStrategy strategy)
    {
        concat(left.getVariablesWithKnownStatistics().stream(), right.getVariablesWithKnownStatistics().stream())
                .distinct()
                .forEach(symbol -> {
                    VariableStatsEstimate symbolStats = VariableStatsEstimate.unknown();
                    if (estimatedRowCount <= 0) {
                        symbolStats = VariableStatsEstimate.zero();
                    }
                    else if (estimatedRowCount > 0) {
                        symbolStats = addColumnStats(
                                left.getVariableStatistics(symbol),
                                left.getOutputRowCount(),
                                right.getVariableStatistics(symbol),
                                right.getOutputRowCount(),
                                estimatedRowCount,
                                strategy);
                    }
                    statsBuilder.addVariableStatistics(symbol, symbolStats);
                });
    }

    private VariableStatsEstimate addColumnStats(
            VariableStatsEstimate leftStats,
            double leftRows,
            VariableStatsEstimate rightStats,
            double rightRows,
            double newRowCount,
            RangeAdditionStrategy strategy)
    {
        checkArgument(newRowCount > 0, "newRowCount must be greater than zero");

        StatisticRange leftRange = StatisticRange.from(leftStats);
        StatisticRange rightRange = StatisticRange.from(rightStats);

        StatisticRange sum = strategy.getRangeAdditionFunction().add(leftRange, rightRange);
        double nullsCountRight = rightStats.getNullsFraction() * rightRows;
        double nullsCountLeft = leftStats.getNullsFraction() * leftRows;
        double totalSizeLeft = (leftRows - nullsCountLeft) * leftStats.getAverageRowSize();
        double totalSizeRight = (rightRows - nullsCountRight) * rightStats.getAverageRowSize();
        double newNullsFraction = Math.min((nullsCountLeft + nullsCountRight) / newRowCount, 1);
        double newNonNullsRowCount = newRowCount * (1.0 - newNullsFraction);

        // FIXME, weights to average. left and right should be equal in most cases anyway
        double newAverageRowSize = newNonNullsRowCount == 0 ? 0 : ((totalSizeLeft + totalSizeRight) / newNonNullsRowCount);
        VariableStatsEstimate.Builder statistics = VariableStatsEstimate.builder()
                .setStatisticsRange(sum)
                .setAverageRowSize(newAverageRowSize)
                .setNullsFraction(newNullsFraction);
        if (shouldUseHistograms) {
            Optional<ConnectorHistogram> newHistogram = RangeAdditionStrategy.INTERSECT == strategy ?
                    leftStats.getHistogram().map(leftHistogram -> DisjointRangeDomainHistogram.addConjunction(leftHistogram, rightRange.toPrestoRange())) :
                    leftStats.getHistogram().map(leftHistogram -> DisjointRangeDomainHistogram.addDisjunction(leftHistogram, rightRange.toPrestoRange()));
            statistics.setHistogram(newHistogram);
        }

        return statistics.build();
    }
}
