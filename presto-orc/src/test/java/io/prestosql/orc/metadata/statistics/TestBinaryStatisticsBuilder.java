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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.orc.metadata.statistics.AbstractStatisticsBuilderTest.StatisticsType.NONE;
import static io.prestosql.orc.metadata.statistics.BinaryStatistics.BINARY_VALUE_BYTES_OVERHEAD;
import static io.prestosql.orc.metadata.statistics.ColumnStatistics.mergeColumnStatistics;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestBinaryStatisticsBuilder
        extends AbstractStatisticsBuilderTest<BinaryStatisticsBuilder, Slice>
{
    private static final Slice FIRST_VALUE = utf8Slice("apple");
    private static final Slice SECOND_VALUE = utf8Slice("banana");

    public TestBinaryStatisticsBuilder()
    {
        super(NONE, BinaryStatisticsBuilder::new, BinaryStatisticsBuilder::addValue);
    }

    @Test
    public void testMinMaxValues()
    {
        // order doesn't matter because there is no min and max for binary
        assertMinMaxValues(EMPTY_SLICE, EMPTY_SLICE);
        assertMinMaxValues(FIRST_VALUE, SECOND_VALUE);
        assertMinMaxValues(SECOND_VALUE, FIRST_VALUE);
    }

    @Test
    public void testSum()
    {
        BinaryStatisticsBuilder binaryStatisticsBuilder = new BinaryStatisticsBuilder();
        for (Slice value : ImmutableList.of(EMPTY_SLICE, FIRST_VALUE, SECOND_VALUE)) {
            binaryStatisticsBuilder.addValue(value);
        }
        assertBinaryStatistics(binaryStatisticsBuilder.buildColumnStatistics(), 3, EMPTY_SLICE.length() + FIRST_VALUE.length() + SECOND_VALUE.length());
    }

    @Test
    public void testMerge()
    {
        List<ColumnStatistics> statisticsList = new ArrayList<>();

        BinaryStatisticsBuilder statisticsBuilder = new BinaryStatisticsBuilder();
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMergedBinaryStatistics(statisticsList, 0, 0);

        statisticsBuilder.addValue(EMPTY_SLICE);
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMergedBinaryStatistics(statisticsList, 1, 0);

        statisticsBuilder.addValue(FIRST_VALUE);
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMergedBinaryStatistics(statisticsList, 3, FIRST_VALUE.length());

        statisticsBuilder.addValue(SECOND_VALUE);
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMergedBinaryStatistics(statisticsList, 6, FIRST_VALUE.length() * 2 + SECOND_VALUE.length());
    }

    @Test
    public void testMinAverageValueBytes()
    {
        assertMinAverageValueBytes(0L, ImmutableList.of());
        assertMinAverageValueBytes(BINARY_VALUE_BYTES_OVERHEAD, ImmutableList.of(EMPTY_SLICE));
        assertMinAverageValueBytes(FIRST_VALUE.length() + BINARY_VALUE_BYTES_OVERHEAD, ImmutableList.of(FIRST_VALUE));
        assertMinAverageValueBytes((FIRST_VALUE.length() + SECOND_VALUE.length()) / 2 + BINARY_VALUE_BYTES_OVERHEAD, ImmutableList.of(FIRST_VALUE, SECOND_VALUE));
    }

    private void assertMergedBinaryStatistics(List<ColumnStatistics> statisticsList, int expectedNumberOfValues, long expectedSum)
    {
        assertBinaryStatistics(mergeColumnStatistics(statisticsList), expectedNumberOfValues, expectedSum);

        assertNoColumnStatistics(mergeColumnStatistics(insertEmptyColumnStatisticsAt(statisticsList, 0, 10)), expectedNumberOfValues + 10);
        assertNoColumnStatistics(mergeColumnStatistics(insertEmptyColumnStatisticsAt(statisticsList, statisticsList.size(), 10)), expectedNumberOfValues + 10);
        assertNoColumnStatistics(mergeColumnStatistics(insertEmptyColumnStatisticsAt(statisticsList, statisticsList.size() / 2, 10)), expectedNumberOfValues + 10);
    }

    private void assertBinaryStatistics(ColumnStatistics columnStatistics, int expectedNumberOfValues, long expectedSum)
    {
        if (expectedNumberOfValues > 0) {
            assertEquals(columnStatistics.getNumberOfValues(), expectedNumberOfValues);
            assertEquals(columnStatistics.getBinaryStatistics().getSum(), expectedSum);
        }
        else {
            assertNull(columnStatistics.getBinaryStatistics());
            assertEquals(columnStatistics.getNumberOfValues(), 0);
        }
    }
}
