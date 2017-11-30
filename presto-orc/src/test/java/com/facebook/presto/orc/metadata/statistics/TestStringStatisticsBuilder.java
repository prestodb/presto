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
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.orc.metadata.statistics.AbstractStatisticsBuilderTest.StatisticsType.STRING;
import static com.facebook.presto.orc.metadata.statistics.ColumnStatistics.mergeColumnStatistics;
import static com.facebook.presto.orc.metadata.statistics.StringStatistics.STRING_VALUE_BYTES_OVERHEAD;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestStringStatisticsBuilder
        extends AbstractStatisticsBuilderTest<StringStatisticsBuilder, Slice>
{
    // U+0000 to U+D7FF
    private static final Slice LOW_BOTTOM_VALUE = utf8Slice("foo \u0000");
    private static final Slice LOW_TOP_VALUE = utf8Slice("foo \uD7FF");
    // U+E000 to U+FFFF
    private static final Slice MEDIUM_BOTTOM_VALUE = utf8Slice("foo \uE000");
    private static final Slice MEDIUM_TOP_VALUE = utf8Slice("foo \uFFFF");
    // U+10000 to U+10FFFF
    private static final Slice HIGH_BOTTOM_VALUE = utf8Slice("foo \uD800\uDC00");
    private static final Slice HIGH_TOP_VALUE = utf8Slice("foo \uDBFF\uDFFF");

    public TestStringStatisticsBuilder()
    {
        super(STRING, StringStatisticsBuilder::new, StringStatisticsBuilder::addValue);
    }

    @Test
    public void testMinMaxValues()
    {
        assertMinMaxValues(EMPTY_SLICE, EMPTY_SLICE);
        assertMinMaxValues(LOW_BOTTOM_VALUE, LOW_BOTTOM_VALUE);
        assertMinMaxValues(LOW_TOP_VALUE, LOW_TOP_VALUE);
        assertMinMaxValues(MEDIUM_BOTTOM_VALUE, MEDIUM_BOTTOM_VALUE);
        assertMinMaxValues(MEDIUM_TOP_VALUE, MEDIUM_TOP_VALUE);
        assertMinMaxValues(HIGH_BOTTOM_VALUE, HIGH_BOTTOM_VALUE);
        assertMinMaxValues(HIGH_TOP_VALUE, HIGH_TOP_VALUE);

        assertMinMaxValues(EMPTY_SLICE, LOW_BOTTOM_VALUE);
        assertMinMaxValues(EMPTY_SLICE, LOW_TOP_VALUE);
        assertMinMaxValues(EMPTY_SLICE, MEDIUM_BOTTOM_VALUE);
        assertMinMaxValues(EMPTY_SLICE, MEDIUM_TOP_VALUE);
        assertMinMaxValues(EMPTY_SLICE, HIGH_BOTTOM_VALUE);
        assertMinMaxValues(EMPTY_SLICE, HIGH_TOP_VALUE);

        assertMinMaxValues(LOW_BOTTOM_VALUE, LOW_TOP_VALUE);
        assertMinMaxValues(LOW_BOTTOM_VALUE, MEDIUM_BOTTOM_VALUE);
        assertMinMaxValues(LOW_BOTTOM_VALUE, MEDIUM_TOP_VALUE);
        assertMinMaxValues(LOW_BOTTOM_VALUE, HIGH_BOTTOM_VALUE);
        assertMinMaxValues(LOW_BOTTOM_VALUE, HIGH_TOP_VALUE);

        assertMinMaxValues(LOW_TOP_VALUE, MEDIUM_BOTTOM_VALUE);
        assertMinMaxValues(LOW_TOP_VALUE, MEDIUM_TOP_VALUE);
        assertMinMaxValues(LOW_TOP_VALUE, HIGH_BOTTOM_VALUE);
        assertMinMaxValues(LOW_TOP_VALUE, HIGH_TOP_VALUE);

        assertMinMaxValues(MEDIUM_BOTTOM_VALUE, MEDIUM_TOP_VALUE);
        assertMinMaxValues(MEDIUM_BOTTOM_VALUE, HIGH_BOTTOM_VALUE);
        assertMinMaxValues(MEDIUM_BOTTOM_VALUE, HIGH_TOP_VALUE);

        assertMinMaxValues(MEDIUM_TOP_VALUE, HIGH_BOTTOM_VALUE);
        assertMinMaxValues(MEDIUM_TOP_VALUE, HIGH_TOP_VALUE);

        assertMinMaxValues(HIGH_BOTTOM_VALUE, HIGH_TOP_VALUE);
    }

    @Test
    public void testSum()
    {
        StringStatisticsBuilder stringStatisticsBuilder = new StringStatisticsBuilder();
        for (Slice value : ImmutableList.of(EMPTY_SLICE, LOW_BOTTOM_VALUE, LOW_TOP_VALUE)) {
            stringStatisticsBuilder.addValue(value);
        }
        assertStringStatistics(stringStatisticsBuilder.buildColumnStatistics(), 3, EMPTY_SLICE.length() + LOW_BOTTOM_VALUE.length() + LOW_TOP_VALUE.length());
    }

    @Test
    public void testMerge()
    {
        List<ColumnStatistics> statisticsList = new ArrayList<>();

        StringStatisticsBuilder statisticsBuilder = new StringStatisticsBuilder();
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMergedStringStatistics(statisticsList, 0, 0);

        statisticsBuilder.addValue(EMPTY_SLICE);
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMergedStringStatistics(statisticsList, 1, 0);

        statisticsBuilder.addValue(LOW_BOTTOM_VALUE);
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMergedStringStatistics(statisticsList, 3, LOW_BOTTOM_VALUE.length());

        statisticsBuilder.addValue(LOW_TOP_VALUE);
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMergedStringStatistics(statisticsList, 6, LOW_BOTTOM_VALUE.length() * 2 + LOW_TOP_VALUE.length());
    }

    @Test
    public void testMinAverageValueBytes()
    {
        assertMinAverageValueBytes(0L, ImmutableList.of());
        assertMinAverageValueBytes(STRING_VALUE_BYTES_OVERHEAD, ImmutableList.of(EMPTY_SLICE));
        assertMinAverageValueBytes(LOW_BOTTOM_VALUE.length() + STRING_VALUE_BYTES_OVERHEAD, ImmutableList.of(LOW_BOTTOM_VALUE));
        assertMinAverageValueBytes((LOW_BOTTOM_VALUE.length() + LOW_TOP_VALUE.length()) / 2 + STRING_VALUE_BYTES_OVERHEAD, ImmutableList.of(LOW_BOTTOM_VALUE, LOW_TOP_VALUE));
    }

    private void assertMergedStringStatistics(List<ColumnStatistics> statisticsList, int expectedNumberOfValues, long expectedSum)
    {
        assertStringStatistics(mergeColumnStatistics(statisticsList), expectedNumberOfValues, expectedSum);

        assertNoColumnStatistics(mergeColumnStatistics(insertEmptyColumnStatisticsAt(statisticsList, 0, 10)), expectedNumberOfValues + 10);
        assertNoColumnStatistics(mergeColumnStatistics(insertEmptyColumnStatisticsAt(statisticsList, statisticsList.size(), 10)), expectedNumberOfValues + 10);
        assertNoColumnStatistics(mergeColumnStatistics(insertEmptyColumnStatisticsAt(statisticsList, statisticsList.size() / 2, 10)), expectedNumberOfValues + 10);
    }

    private void assertStringStatistics(ColumnStatistics columnStatistics, int expectedNumberOfValues, long expectedSum)
    {
        if (expectedNumberOfValues > 0) {
            assertEquals(columnStatistics.getNumberOfValues(), expectedNumberOfValues);
            assertEquals(columnStatistics.getStringStatistics().getSum(), expectedSum);
        }
        else {
            assertNull(columnStatistics.getStringStatistics());
            assertEquals(columnStatistics.getNumberOfValues(), 0);
        }
    }
}
