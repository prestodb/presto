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
import org.testng.annotations.Test;

import static com.facebook.presto.cost.PlanNodeStatsEstimateMath.addNonOverlapping;
import static com.facebook.presto.cost.PlanNodeStatsEstimateMath.addStatsAndMaxDistinctValues;
import static com.facebook.presto.cost.PlanNodeStatsEstimateMath.addStatsAndSumDistinctValues;
import static com.facebook.presto.cost.PlanNodeStatsEstimateMath.cap;
import static com.facebook.presto.cost.PlanNodeStatsEstimateMath.subtractSubset;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;

public class TestPlanNodeStatsEstimateMath
{
    private static final Symbol SYMBOL = new Symbol("symbol");
    private static final StatisticRange NON_EMPTY_RANGE = openRange(1);

    @Test
    public void testAddRowCount()
    {
        PlanNodeStatsEstimate unknownStats = statistics(NaN, NaN, NaN, StatisticRange.empty());
        PlanNodeStatsEstimate first = statistics(10, NaN, NaN, StatisticRange.empty());
        PlanNodeStatsEstimate second = statistics(20, NaN, NaN, StatisticRange.empty());

        assertEquals(addStatsAndSumDistinctValues(unknownStats, unknownStats), PlanNodeStatsEstimate.unknown());
        assertEquals(addStatsAndSumDistinctValues(first, unknownStats), PlanNodeStatsEstimate.unknown());
        assertEquals(addStatsAndSumDistinctValues(unknownStats, second), PlanNodeStatsEstimate.unknown());
        assertEquals(addStatsAndSumDistinctValues(first, second).getOutputRowCount(), 30.0);
    }

    @Test
    public void testAddNullsFraction()
    {
        PlanNodeStatsEstimate unknownRowCount = statistics(NaN, 0.1, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate unknownNullsFraction = statistics(10, NaN, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate first = statistics(10, 0.1, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate second = statistics(20, 0.2, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate fractionalRowCountFirst = statistics(0.1, 0.1, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate fractionalRowCountSecond = statistics(0.2, 0.3, NaN, NON_EMPTY_RANGE);

        assertAddNullsFraction(unknownRowCount, unknownRowCount, NaN);
        assertAddNullsFraction(unknownNullsFraction, unknownNullsFraction, NaN);
        assertAddNullsFraction(unknownRowCount, unknownNullsFraction, NaN);
        assertAddNullsFraction(first, unknownNullsFraction, NaN);
        assertAddNullsFraction(unknownRowCount, second, NaN);
        assertAddNullsFraction(first, second, 0.16666666666666666);
        assertAddNullsFraction(fractionalRowCountFirst, fractionalRowCountSecond, 0.2333333333333333);
    }

    private static void assertAddNullsFraction(PlanNodeStatsEstimate first, PlanNodeStatsEstimate second, double expected)
    {
        assertEquals(addStatsAndSumDistinctValues(first, second).getSymbolStatistics(SYMBOL).getNullsFraction(), expected);
    }

    @Test
    public void testAddAverageRowSize()
    {
        PlanNodeStatsEstimate unknownRowCount = statistics(NaN, 0.1, 10, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate unknownNullsFraction = statistics(10, NaN, 10, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate unknownAverageRowSize = statistics(10, 0.1, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate first = statistics(10, 0.1, 15, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate second = statistics(20, 0.2, 20, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate fractionalRowCountFirst = statistics(0.1, 0.1, 0.3, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate fractionalRowCountSecond = statistics(0.2, 0.3, 0.4, NON_EMPTY_RANGE);

        assertAddAverageRowSize(unknownRowCount, unknownRowCount, NaN);
        assertAddAverageRowSize(unknownNullsFraction, unknownNullsFraction, NaN);
        assertAddAverageRowSize(unknownAverageRowSize, unknownAverageRowSize, NaN);
        assertAddAverageRowSize(first, unknownRowCount, NaN);
        assertAddAverageRowSize(unknownNullsFraction, second, NaN);
        assertAddAverageRowSize(first, unknownAverageRowSize, NaN);
        assertAddAverageRowSize(first, second, 18.2);
        assertAddAverageRowSize(fractionalRowCountFirst, fractionalRowCountSecond, 0.3608695652173913);
    }

    private static void assertAddAverageRowSize(PlanNodeStatsEstimate first, PlanNodeStatsEstimate second, double expected)
    {
        assertEquals(addStatsAndSumDistinctValues(first, second).getSymbolStatistics(SYMBOL).getAverageRowSize(), expected);
    }

    @Test
    public void testSumNumberOfDistinctValues()
    {
        PlanNodeStatsEstimate unknownRowCount = statistics(NaN, NaN, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate emptyRange = statistics(10, NaN, NaN, StatisticRange.empty());
        PlanNodeStatsEstimate unknownRange = statistics(10, NaN, NaN, openRange(NaN));
        PlanNodeStatsEstimate first = statistics(10, NaN, NaN, openRange(2));
        PlanNodeStatsEstimate second = statistics(10, NaN, NaN, openRange(3));

        assertSumNumberOfDistinctValues(unknownRowCount, unknownRowCount, NaN);
        assertSumNumberOfDistinctValues(unknownRowCount, second, NaN);
        assertSumNumberOfDistinctValues(first, emptyRange, 2);
        assertSumNumberOfDistinctValues(first, unknownRange, NaN);
        assertSumNumberOfDistinctValues(first, second, 5);
    }

    private static void assertSumNumberOfDistinctValues(PlanNodeStatsEstimate first, PlanNodeStatsEstimate second, double expected)
    {
        assertEquals(addStatsAndSumDistinctValues(first, second).getSymbolStatistics(SYMBOL).getDistinctValuesCount(), expected);
    }

    @Test
    public void testMaxNumberOfDistinctValues()
    {
        PlanNodeStatsEstimate unknownRowCount = statistics(NaN, NaN, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate emptyRange = statistics(10, NaN, NaN, StatisticRange.empty());
        PlanNodeStatsEstimate unknownRange = statistics(10, NaN, NaN, openRange(NaN));
        PlanNodeStatsEstimate first = statistics(10, NaN, NaN, openRange(2));
        PlanNodeStatsEstimate second = statistics(10, NaN, NaN, openRange(3));

        assertMaxNumberOfDistinctValues(unknownRowCount, unknownRowCount, NaN);
        assertMaxNumberOfDistinctValues(unknownRowCount, second, NaN);
        assertMaxNumberOfDistinctValues(first, emptyRange, 2);
        assertMaxNumberOfDistinctValues(first, unknownRange, NaN);
        assertMaxNumberOfDistinctValues(first, second, 3);
    }

    private static void assertMaxNumberOfDistinctValues(PlanNodeStatsEstimate first, PlanNodeStatsEstimate second, double expected)
    {
        assertEquals(addStatsAndMaxDistinctValues(first, second).getSymbolStatistics(SYMBOL).getDistinctValuesCount(), expected);
    }

    @Test
    public void testAddRange()
    {
        PlanNodeStatsEstimate unknownRowCount = statistics(NaN, NaN, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate emptyRange = statistics(10, NaN, NaN, StatisticRange.empty());
        PlanNodeStatsEstimate unknownRange = statistics(10, NaN, NaN, openRange(NaN));
        PlanNodeStatsEstimate first = statistics(10, NaN, NaN, new StatisticRange(12, 100, 2));
        PlanNodeStatsEstimate second = statistics(10, NaN, NaN, new StatisticRange(101, 200, 3));

        assertAddRange(unknownRange, unknownRange, NEGATIVE_INFINITY, POSITIVE_INFINITY);
        assertAddRange(unknownRowCount, second, NEGATIVE_INFINITY, POSITIVE_INFINITY);
        assertAddRange(unknownRange, second, NEGATIVE_INFINITY, POSITIVE_INFINITY);
        assertAddRange(emptyRange, second, 101, 200);
        assertAddRange(first, second, 12, 200);
    }

    private static void assertAddRange(PlanNodeStatsEstimate first, PlanNodeStatsEstimate second, double expectedLow, double expectedHigh)
    {
        SymbolStatsEstimate statistics = addStatsAndMaxDistinctValues(first, second).getSymbolStatistics(SYMBOL);
        assertEquals(statistics.getLowValue(), expectedLow);
        assertEquals(statistics.getHighValue(), expectedHigh);
    }

    @Test
    public void testSubtractRowCount()
    {
        PlanNodeStatsEstimate unknownStats = statistics(NaN, NaN, NaN, StatisticRange.empty());
        PlanNodeStatsEstimate first = statistics(40, NaN, NaN, StatisticRange.empty());
        PlanNodeStatsEstimate second = statistics(10, NaN, NaN, StatisticRange.empty());

        assertEquals(subtractSubset(unknownStats, unknownStats), PlanNodeStatsEstimate.unknown());
        assertEquals(subtractSubset(first, unknownStats), PlanNodeStatsEstimate.unknown());
        assertEquals(subtractSubset(unknownStats, second), PlanNodeStatsEstimate.unknown());
        assertEquals(subtractSubset(first, second).getOutputRowCount(), 30.0);
    }

    @Test
    public void testSubtractNullsFraction()
    {
        PlanNodeStatsEstimate unknownRowCount = statistics(NaN, 0.1, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate unknownNullsFraction = statistics(10, NaN, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate first = statistics(50, 0.1, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate second = statistics(20, 0.2, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate fractionalRowCountFirst = statistics(0.7, 0.1, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate fractionalRowCountSecond = statistics(0.2, 0.3, NaN, NON_EMPTY_RANGE);

        assertSubtractNullsFraction(unknownRowCount, unknownRowCount, NaN);
        assertSubtractNullsFraction(unknownRowCount, unknownNullsFraction, NaN);
        assertSubtractNullsFraction(first, unknownNullsFraction, NaN);
        assertSubtractNullsFraction(unknownRowCount, second, NaN);
        assertSubtractNullsFraction(first, second, 0.03333333333333333);
        assertSubtractNullsFraction(fractionalRowCountFirst, fractionalRowCountSecond, 0.019999999999999993);
    }

    private static void assertSubtractNullsFraction(PlanNodeStatsEstimate first, PlanNodeStatsEstimate second, double expected)
    {
        assertEquals(subtractSubset(first, second).getSymbolStatistics(SYMBOL).getNullsFraction(), expected);
    }

    @Test
    public void testSubtractNumberOfDistinctValues()
    {
        PlanNodeStatsEstimate unknownRowCount = statistics(NaN, NaN, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate unknownDistinctValues = statistics(100, 0.1, NaN, openRange(NaN));
        PlanNodeStatsEstimate zero = statistics(0, 0.1, NaN, openRange(0));
        PlanNodeStatsEstimate first = statistics(30, 0.1, NaN, openRange(10));
        PlanNodeStatsEstimate second = statistics(20, 0.1, NaN, openRange(5));
        PlanNodeStatsEstimate third = statistics(10, 0.1, NaN, openRange(3));

        assertSubtractNumberOfDistinctValues(unknownRowCount, unknownRowCount, NaN);
        assertSubtractNumberOfDistinctValues(unknownRowCount, second, NaN);
        assertSubtractNumberOfDistinctValues(unknownDistinctValues, second, NaN);
        assertSubtractNumberOfDistinctValues(first, zero, 10);
        assertSubtractNumberOfDistinctValues(zero, zero, 0);
        assertSubtractNumberOfDistinctValues(first, second, 5);
        assertSubtractNumberOfDistinctValues(second, third, 5);
    }

    private static void assertSubtractNumberOfDistinctValues(PlanNodeStatsEstimate first, PlanNodeStatsEstimate second, double expected)
    {
        assertEquals(subtractSubset(first, second).getSymbolStatistics(SYMBOL).getDistinctValuesCount(), expected);
    }

    @Test
    public void testSubtractRange()
    {
        assertSubtractRange(NEGATIVE_INFINITY, POSITIVE_INFINITY, NEGATIVE_INFINITY, POSITIVE_INFINITY, NEGATIVE_INFINITY, POSITIVE_INFINITY);
        assertSubtractRange(0, 1, NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 1);
        assertSubtractRange(NaN, NaN, 0, 1, NaN, NaN);
        assertSubtractRange(0, 1, NaN, NaN, 0, 1);
        assertSubtractRange(0, 2, 0, 1, 0, 2);
        assertSubtractRange(0, 2, 1, 2, 0, 2);
        assertSubtractRange(0, 2, 0.5, 1, 0, 2);
    }

    private static void assertSubtractRange(double supersetLow, double supersetHigh, double subsetLow, double subsetHigh, double expectedLow, double expectedHigh)
    {
        PlanNodeStatsEstimate first = statistics(30, NaN, NaN, new StatisticRange(supersetLow, supersetHigh, 10));
        PlanNodeStatsEstimate second = statistics(20, NaN, NaN, new StatisticRange(subsetLow, subsetHigh, 5));
        SymbolStatsEstimate statistics = subtractSubset(first, second).getSymbolStatistics(SYMBOL);
        assertEquals(statistics.getLowValue(), expectedLow);
        assertEquals(statistics.getHighValue(), expectedHigh);
    }

    @Test
    public void testSymmetricDifferenceRowCount()
    {
        PlanNodeStatsEstimate unknownRowCount = statistics(NaN, NaN, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate left = statistics(10, NaN, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate right = statistics(20, NaN, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate intersection = statistics(5, NaN, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate uncappingSuperset = statistics(50, NaN, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate cappingSuperset = statistics(10, NaN, NaN, NON_EMPTY_RANGE);

        assertEquals(computeSymmetricDifferenceStats(unknownRowCount, left, right, intersection).getOutputRowCount(), NaN);
        assertEquals(computeSymmetricDifferenceStats(uncappingSuperset, unknownRowCount, right, intersection).getOutputRowCount(), NaN);
        assertEquals(computeSymmetricDifferenceStats(uncappingSuperset, left, unknownRowCount, intersection).getOutputRowCount(), NaN);
        assertEquals(computeSymmetricDifferenceStats(uncappingSuperset, left, right, unknownRowCount).getOutputRowCount(), NaN);
        assertEquals(computeSymmetricDifferenceStats(uncappingSuperset, left, right, intersection).getOutputRowCount(), 25.0);
        assertEquals(computeSymmetricDifferenceStats(cappingSuperset, left, right, intersection).getOutputRowCount(), 10.0);
    }

    @Test
    public void testSymmetricDifferenceNullsFraction()
    {
        PlanNodeStatsEstimate left = statistics(10, 0.1, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate right = statistics(20, 0.1, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate intersection = statistics(5, 0.2, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate superset = statistics(40, 0.1, NaN, NON_EMPTY_RANGE);
        assertEquals(computeSymmetricDifferenceStats(superset, left, right, intersection).getSymbolStatistics(SYMBOL).getNullsFraction(), 0.08);

        left = statistics(10, 0.1, NaN, NON_EMPTY_RANGE);
        right = statistics(20, 0.1, NaN, NON_EMPTY_RANGE);
        intersection = statistics(5, 0.6, NaN, NON_EMPTY_RANGE);
        superset = statistics(40, 0.1, NaN, NON_EMPTY_RANGE);
        assertEquals(computeSymmetricDifferenceStats(superset, left, right, intersection).getSymbolStatistics(SYMBOL).getNullsFraction(), 0.0);

        left = statistics(10, 0.3, NaN, NON_EMPTY_RANGE);
        right = statistics(20, 0.4, NaN, NON_EMPTY_RANGE);
        intersection = statistics(5, 0.0, NaN, NON_EMPTY_RANGE);
        superset = statistics(40, 0.1, NaN, NON_EMPTY_RANGE);
        assertEquals(computeSymmetricDifferenceStats(superset, left, right, intersection).getSymbolStatistics(SYMBOL).getNullsFraction(), 0.16);
    }

    private PlanNodeStatsEstimate computeSymmetricDifferenceStats(PlanNodeStatsEstimate superset, PlanNodeStatsEstimate left, PlanNodeStatsEstimate right, PlanNodeStatsEstimate intersect)
    {
        return cap(
                subtractSubset(
                        addNonOverlapping(superset, left, right),
                        intersect,
                        // for OR no NDVs should be removed
                        false),
                superset);
    }

    private static PlanNodeStatsEstimate statistics(double rowCount, double nullsFraction, double averageRowSize, StatisticRange range)
    {
        return PlanNodeStatsEstimate.builder()
                .setOutputRowCount(rowCount)
                .addSymbolStatistics(SYMBOL, SymbolStatsEstimate.builder()
                        .setNullsFraction(nullsFraction)
                        .setAverageRowSize(averageRowSize)
                        .setStatisticsRange(range)
                        .build())
                .build();
    }

    private static StatisticRange openRange(double distinctValues)
    {
        return new StatisticRange(NEGATIVE_INFINITY, POSITIVE_INFINITY, distinctValues);
    }
}
