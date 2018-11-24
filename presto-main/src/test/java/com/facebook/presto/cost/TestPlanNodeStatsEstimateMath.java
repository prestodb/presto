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

import static com.facebook.presto.cost.PlanNodeStatsEstimateMath.addStatsAndMaxDistinctValues;
import static com.facebook.presto.cost.PlanNodeStatsEstimateMath.addStatsAndSumDistinctValues;
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
