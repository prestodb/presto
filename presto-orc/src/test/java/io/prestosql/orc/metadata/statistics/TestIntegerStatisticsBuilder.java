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
package io.prestosql.orc.metadata.statistics;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static io.prestosql.orc.metadata.statistics.AbstractStatisticsBuilderTest.StatisticsType.INTEGER;
import static io.prestosql.orc.metadata.statistics.ColumnStatistics.mergeColumnStatistics;
import static io.prestosql.orc.metadata.statistics.IntegerStatistics.INTEGER_VALUE_BYTES;
import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.MIN_VALUE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestIntegerStatisticsBuilder
        extends AbstractStatisticsBuilderTest<IntegerStatisticsBuilder, Long>
{
    public TestIntegerStatisticsBuilder()
    {
        super(INTEGER, IntegerStatisticsBuilder::new, IntegerStatisticsBuilder::addValue);
    }

    @Test
    public void testMinMaxValues()
    {
        assertMinMaxValues(0L, 0L);
        assertMinMaxValues(42L, 42L);
        assertMinMaxValues(MIN_VALUE, MIN_VALUE);
        assertMinMaxValues(MAX_VALUE, MAX_VALUE);

        assertMinMaxValues(0L, 42L);
        assertMinMaxValues(42L, 42L);
        assertMinMaxValues(MIN_VALUE, 42L);
        assertMinMaxValues(42L, MAX_VALUE);
        assertMinMaxValues(MIN_VALUE, MAX_VALUE);

        assertValues(-42L, 0L, ContiguousSet.create(Range.closed(-42L, 0L), DiscreteDomain.longs()).asList());
        assertValues(-42L, 42L, ContiguousSet.create(Range.closed(-42L, 42L), DiscreteDomain.longs()).asList());
        assertValues(0L, 42L, ContiguousSet.create(Range.closed(0L, 42L), DiscreteDomain.longs()).asList());
        assertValues(MIN_VALUE, MIN_VALUE + 42, ContiguousSet.create(Range.closed(MIN_VALUE, MIN_VALUE + 42), DiscreteDomain.longs()).asList());
        assertValues(MAX_VALUE - 42L, MAX_VALUE, ContiguousSet.create(Range.closed(MAX_VALUE - 42L, MAX_VALUE), DiscreteDomain.longs()).asList());
    }

    @Test
    public void testMinAverageValueBytes()
    {
        assertMinAverageValueBytes(0L, ImmutableList.of());
        assertMinAverageValueBytes(INTEGER_VALUE_BYTES, ImmutableList.of(42L));
        assertMinAverageValueBytes(INTEGER_VALUE_BYTES, ImmutableList.of(0L));
        assertMinAverageValueBytes(INTEGER_VALUE_BYTES, ImmutableList.of(0L, 42L, 42L, 43L));
    }

    @Test
    public void testSum()
    {
        int values = 0;
        long expectedSum = 0;
        IntegerStatisticsBuilder integerStatisticsBuilder = new IntegerStatisticsBuilder();
        for (int value = -100_000; value < 500_000; value++) {
            values++;
            expectedSum += value;
            integerStatisticsBuilder.addValue(value);
        }
        assertIntegerStatistics(integerStatisticsBuilder.buildColumnStatistics(), values, expectedSum);
    }

    @Test
    public void testSumOverflow()
    {
        IntegerStatisticsBuilder integerStatisticsBuilder = new IntegerStatisticsBuilder();

        integerStatisticsBuilder.addValue(MAX_VALUE);
        assertIntegerStatistics(integerStatisticsBuilder.buildColumnStatistics(), 1, MAX_VALUE);

        integerStatisticsBuilder.addValue(10);
        assertIntegerStatistics(integerStatisticsBuilder.buildColumnStatistics(), 2, null);
    }

    @Test
    public void testSumUnderflow()
    {
        IntegerStatisticsBuilder integerStatisticsBuilder = new IntegerStatisticsBuilder();

        integerStatisticsBuilder.addValue(MIN_VALUE);
        assertIntegerStatistics(integerStatisticsBuilder.buildColumnStatistics(), 1, MIN_VALUE);

        integerStatisticsBuilder.addValue(-10);
        assertIntegerStatistics(integerStatisticsBuilder.buildColumnStatistics(), 2, null);
    }

    @Test
    public void testMerge()
    {
        List<ColumnStatistics> statisticsList = new ArrayList<>();

        IntegerStatisticsBuilder statisticsBuilder = new IntegerStatisticsBuilder();
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMergedIntegerStatistics(statisticsList, 0, 0L);

        statisticsBuilder.addValue(0);
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMergedIntegerStatistics(statisticsList, 1, 0L);

        statisticsBuilder.addValue(-44);
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMergedIntegerStatistics(statisticsList, 3, -44L);

        statisticsBuilder.addValue(100);
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMergedIntegerStatistics(statisticsList, 6, (-44L * 2) + 100);

        statisticsBuilder.addValue(MAX_VALUE);
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMergedIntegerStatistics(statisticsList, 10, null);
    }

    @Test
    public void testMergeOverflow()
    {
        List<ColumnStatistics> statisticsList = new ArrayList<>();

        statisticsList.add(new IntegerStatisticsBuilder().buildColumnStatistics());
        assertMergedIntegerStatistics(statisticsList, 0, 0L);

        statisticsList.add(singleValueIntegerStatistics(MAX_VALUE));
        assertMergedIntegerStatistics(statisticsList, 1, MAX_VALUE);

        statisticsList.add(singleValueIntegerStatistics(1));
        assertMergedIntegerStatistics(statisticsList, 2, null);
    }

    private static ColumnStatistics singleValueIntegerStatistics(long value)
    {
        IntegerStatisticsBuilder statisticsBuilder = new IntegerStatisticsBuilder();
        statisticsBuilder.addValue(value);
        return statisticsBuilder.buildColumnStatistics();
    }

    private static void assertMergedIntegerStatistics(List<ColumnStatistics> statisticsList, int expectedNumberOfValues, Long expectedSum)
    {
        assertIntegerStatistics(mergeColumnStatistics(statisticsList), expectedNumberOfValues, expectedSum);

        assertNoColumnStatistics(mergeColumnStatistics(insertEmptyColumnStatisticsAt(statisticsList, 0, 10)), expectedNumberOfValues + 10);
        assertNoColumnStatistics(mergeColumnStatistics(insertEmptyColumnStatisticsAt(statisticsList, statisticsList.size(), 10)), expectedNumberOfValues + 10);
        assertNoColumnStatistics(mergeColumnStatistics(insertEmptyColumnStatisticsAt(statisticsList, statisticsList.size() / 2, 10)), expectedNumberOfValues + 10);
    }

    private static void assertIntegerStatistics(ColumnStatistics columnStatistics, int expectedNumberOfValues, Long expectedSum)
    {
        if (expectedNumberOfValues > 0) {
            assertEquals(columnStatistics.getNumberOfValues(), expectedNumberOfValues);
            assertEquals(columnStatistics.getIntegerStatistics().getSum(), expectedSum);
        }
        else {
            assertNull(columnStatistics.getIntegerStatistics());
            assertEquals(columnStatistics.getNumberOfValues(), 0);
        }
    }
}
