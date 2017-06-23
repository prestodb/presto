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
package com.facebook.presto.orc.metadata.statistics;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static com.facebook.presto.orc.metadata.statistics.ColumnStatistics.mergeColumnStatistics;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public abstract class AbstractStatisticsBuilderTest<B extends StatisticsBuilder, T>
{
    public enum StatisticsType
    {
        NONE, BOOLEAN, INTEGER, DOUBLE, STRING, DATE, DECIMAL
    }

    private final StatisticsType statisticsType;
    private final Supplier<B> statisticsBuilderSupplier;
    private final BiConsumer<B, T> adder;

    public AbstractStatisticsBuilderTest(StatisticsType statisticsType, Supplier<B> statisticsBuilderSupplier, BiConsumer<B, T> adder)
    {
        this.statisticsType = statisticsType;
        this.statisticsBuilderSupplier = statisticsBuilderSupplier;
        this.adder = adder;
    }

    @Test
    public void testNoValue()
    {
        B statisticsBuilder = statisticsBuilderSupplier.get();
        AggregateColumnStatistics aggregateColumnStatistics = new AggregateColumnStatistics();

        assertNoColumnStatistics(statisticsBuilder.buildColumnStatistics(), 0);
        aggregateColumnStatistics.add(statisticsBuilder.buildColumnStatistics());
        assertNoColumnStatistics(aggregateColumnStatistics.getMergedColumnStatistics(), 0);

        assertNoColumnStatistics(statisticsBuilder.buildColumnStatistics(), 0);
        aggregateColumnStatistics.add(statisticsBuilder.buildColumnStatistics());
        assertNoColumnStatistics(aggregateColumnStatistics.getMergedColumnStatistics(), 0);

        assertNoColumnStatistics(statisticsBuilder.buildColumnStatistics(), 0);
        aggregateColumnStatistics.add(statisticsBuilder.buildColumnStatistics());
        assertNoColumnStatistics(aggregateColumnStatistics.getMergedColumnStatistics(), 0);
    }

    public void assertMinMaxValues(T expectedMin, T expectedMax)
    {
        // add min then max
        B statisticsBuilder = statisticsBuilderSupplier.get();
        AggregateColumnStatistics aggregateColumnStatistics = new AggregateColumnStatistics();
        aggregateColumnStatistics.add(statisticsBuilder.buildColumnStatistics());
        assertColumnStatistics(statisticsBuilder.buildColumnStatistics(), 0, null, null, aggregateColumnStatistics);

        adder.accept(statisticsBuilder, expectedMin);
        aggregateColumnStatistics.add(statisticsBuilder.buildColumnStatistics());
        assertColumnStatistics(statisticsBuilder.buildColumnStatistics(), 1, expectedMin, expectedMin, aggregateColumnStatistics);

        adder.accept(statisticsBuilder, expectedMin);
        aggregateColumnStatistics.add(statisticsBuilder.buildColumnStatistics());
        assertColumnStatistics(statisticsBuilder.buildColumnStatistics(), 2, expectedMin, expectedMin, aggregateColumnStatistics);

        adder.accept(statisticsBuilder, expectedMax);
        aggregateColumnStatistics.add(statisticsBuilder.buildColumnStatistics());
        assertColumnStatistics(statisticsBuilder.buildColumnStatistics(), 3, expectedMin, expectedMax, aggregateColumnStatistics);

        adder.accept(statisticsBuilder, expectedMax);
        aggregateColumnStatistics.add(statisticsBuilder.buildColumnStatistics());
        assertColumnStatistics(statisticsBuilder.buildColumnStatistics(), 4, expectedMin, expectedMax, aggregateColumnStatistics);

        // add max then min
        statisticsBuilder = statisticsBuilderSupplier.get();
        aggregateColumnStatistics = new AggregateColumnStatistics();

        adder.accept(statisticsBuilder, expectedMax);
        aggregateColumnStatistics.add(statisticsBuilder.buildColumnStatistics());
        assertColumnStatistics(statisticsBuilder.buildColumnStatistics(), 1, expectedMax, expectedMax, aggregateColumnStatistics);

        adder.accept(statisticsBuilder, expectedMax);
        aggregateColumnStatistics.add(statisticsBuilder.buildColumnStatistics());
        assertColumnStatistics(statisticsBuilder.buildColumnStatistics(), 2, expectedMax, expectedMax, aggregateColumnStatistics);

        adder.accept(statisticsBuilder, expectedMin);
        aggregateColumnStatistics.add(statisticsBuilder.buildColumnStatistics());
        assertColumnStatistics(statisticsBuilder.buildColumnStatistics(), 3, expectedMin, expectedMax, aggregateColumnStatistics);

        adder.accept(statisticsBuilder, expectedMin);
        aggregateColumnStatistics.add(statisticsBuilder.buildColumnStatistics());
        assertColumnStatistics(statisticsBuilder.buildColumnStatistics(), 4, expectedMin, expectedMax, aggregateColumnStatistics);
    }

    public static void assertNoColumnStatistics(ColumnStatistics columnStatistics, int expectedNumberOfValues)
    {
        assertEquals(columnStatistics.getNumberOfValues(), expectedNumberOfValues);
        assertNull(columnStatistics.getBooleanStatistics());
        assertNull(columnStatistics.getIntegerStatistics());
        assertNull(columnStatistics.getDoubleStatistics());
        assertNull(columnStatistics.getStringStatistics());
        assertNull(columnStatistics.getDateStatistics());
        assertNull(columnStatistics.getDecimalStatistics());
        assertNull(columnStatistics.getBloomFilter());
    }

    private void assertColumnStatistics(
            ColumnStatistics columnStatistics,
            int expectedNumberOfValues,
            T expectedMin,
            T expectedMax,
            AggregateColumnStatistics aggregateColumnStatistics)
    {
        assertColumnStatistics(columnStatistics, expectedNumberOfValues, expectedMin, expectedMax);
        assertColumnStatistics(aggregateColumnStatistics.getMergedColumnStatistics(), aggregateColumnStatistics.getTotalCount(), expectedMin, expectedMax);

        List<ColumnStatistics> statisticsList = aggregateColumnStatistics.getStatisticsList();
        assertNoColumnStatistics(mergeColumnStatistics(insertEmptyColumnStatisticsAt(statisticsList, 0, 10)), aggregateColumnStatistics.getTotalCount() + 10);
        assertNoColumnStatistics(mergeColumnStatistics(insertEmptyColumnStatisticsAt(statisticsList, statisticsList.size(), 10)), aggregateColumnStatistics.getTotalCount() + 10);
        assertNoColumnStatistics(mergeColumnStatistics(insertEmptyColumnStatisticsAt(statisticsList, statisticsList.size() / 2, 10)), aggregateColumnStatistics.getTotalCount() + 10);
    }

    static List<ColumnStatistics> insertEmptyColumnStatisticsAt(List<ColumnStatistics> statisticsList, int index, long numberOfValues)
    {
        List<ColumnStatistics> newStatisticsList = new ArrayList<>(statisticsList);
        newStatisticsList.add(index, new ColumnStatistics(numberOfValues, null, null, null, null, null, null, null));
        return newStatisticsList;
    }

    public void assertColumnStatistics(ColumnStatistics columnStatistics, int expectedNumberOfValues, T expectedMin, T expectedMax)
    {
        assertEquals(columnStatistics.getNumberOfValues(), expectedNumberOfValues);

        if (statisticsType == StatisticsType.BOOLEAN && expectedNumberOfValues > 0) {
            assertNotNull(columnStatistics.getBooleanStatistics());
        }
        else {
            assertNull(columnStatistics.getBooleanStatistics());
        }

        if (statisticsType == StatisticsType.INTEGER && expectedNumberOfValues > 0) {
            assertRangeStatistics(columnStatistics.getIntegerStatistics(), expectedMin, expectedMax);
        }
        else {
            assertNull(columnStatistics.getIntegerStatistics());
        }

        if (statisticsType == StatisticsType.DOUBLE && expectedNumberOfValues > 0) {
            assertRangeStatistics(columnStatistics.getDoubleStatistics(), expectedMin, expectedMax);
        }
        else {
            assertNull(columnStatistics.getDoubleStatistics());
        }

        if (statisticsType == StatisticsType.STRING && expectedNumberOfValues > 0) {
            assertRangeStatistics(columnStatistics.getStringStatistics(), expectedMin, expectedMax);
        }
        else {
            assertNull(columnStatistics.getStringStatistics());
        }

        if (statisticsType == StatisticsType.DATE && expectedNumberOfValues > 0) {
            assertRangeStatistics(columnStatistics.getDateStatistics(), expectedMin, expectedMax);
        }
        else {
            assertNull(columnStatistics.getDateStatistics());
        }

        if (statisticsType == StatisticsType.DECIMAL && expectedNumberOfValues > 0) {
            assertRangeStatistics(columnStatistics.getDecimalStatistics(), expectedMin, expectedMax);
        }
        else {
            assertNull(columnStatistics.getDecimalStatistics());
        }

        assertNull(columnStatistics.getBloomFilter());
    }

    void assertRangeStatistics(RangeStatistics<?> rangeStatistics, T expectedMin, T expectedMax)
    {
        assertNotNull(rangeStatistics);
        assertEquals(rangeStatistics.getMin(), expectedMin);
        assertEquals(rangeStatistics.getMax(), expectedMax);
    }

    public static class AggregateColumnStatistics
    {
        private int totalCount;
        private final ImmutableList.Builder<ColumnStatistics> statisticsList = ImmutableList.builder();

        public void add(ColumnStatistics columnStatistics)
        {
            totalCount += columnStatistics.getNumberOfValues();
            statisticsList.add(columnStatistics);
        }

        public int getTotalCount()
        {
            return totalCount;
        }

        public List<ColumnStatistics> getStatisticsList()
        {
            return statisticsList.build();
        }

        public ColumnStatistics getMergedColumnStatistics()
        {
            return mergeColumnStatistics(statisticsList.build());
        }
    }
}
