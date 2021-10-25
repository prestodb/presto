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
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.orc.metadata.statistics.AbstractStatisticsBuilderTest.StatisticsType.STRING;
import static com.facebook.presto.orc.metadata.statistics.ColumnStatistics.mergeColumnStatistics;
import static com.facebook.presto.orc.metadata.statistics.StringStatistics.STRING_VALUE_BYTES_OVERHEAD;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
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

    private static final Slice LONG_BOTTOM_VALUE = utf8Slice("aaaaaaaaaaaaaaaaaaaa");

    public TestStringStatisticsBuilder()
    {
        super(STRING, () -> new StringStatisticsBuilder(Integer.MAX_VALUE), StringStatisticsBuilder::addValue);
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
        StringStatisticsBuilder stringStatisticsBuilder = new StringStatisticsBuilder(Integer.MAX_VALUE);
        for (Slice value : ImmutableList.of(EMPTY_SLICE, LOW_BOTTOM_VALUE, LOW_TOP_VALUE)) {
            stringStatisticsBuilder.addValue(value);
        }
        assertStringStatistics(stringStatisticsBuilder.buildColumnStatistics(), 3, EMPTY_SLICE.length() + LOW_BOTTOM_VALUE.length() + LOW_TOP_VALUE.length());
    }

    @Test
    public void testMerge()
    {
        List<ColumnStatistics> statisticsList = new ArrayList<>();

        StringStatisticsBuilder statisticsBuilder = new StringStatisticsBuilder(Integer.MAX_VALUE);
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
    public void testMinMaxValuesWithLimit()
    {
        assertMinMaxValuesWithLimit(MEDIUM_TOP_VALUE, null, ImmutableList.of(MEDIUM_TOP_VALUE, HIGH_BOTTOM_VALUE), 7);
        assertMinMaxValuesWithLimit(null, MEDIUM_TOP_VALUE, ImmutableList.of(LONG_BOTTOM_VALUE, MEDIUM_TOP_VALUE), 7);
        assertMinMaxValuesWithLimit(null, null, ImmutableList.of(LONG_BOTTOM_VALUE), 6, 20);
    }

    @Test
    public void testMergeWithLimit()
    {
        List<ColumnStatistics> statisticsList = new ArrayList<>();
        statisticsList.add(stringColumnStatistics(MEDIUM_BOTTOM_VALUE, MEDIUM_BOTTOM_VALUE));
        assertMinMax(mergeColumnStatistics(statisticsList).getStringStatistics(), MEDIUM_BOTTOM_VALUE, MEDIUM_BOTTOM_VALUE);
        statisticsList.add(stringColumnStatistics(null, MEDIUM_BOTTOM_VALUE));
        assertMinMax(mergeColumnStatistics(statisticsList).getStringStatistics(), null, MEDIUM_BOTTOM_VALUE);
        statisticsList.add(stringColumnStatistics(null, MEDIUM_TOP_VALUE));
        assertMinMax(mergeColumnStatistics(statisticsList).getStringStatistics(), null, MEDIUM_TOP_VALUE);
        statisticsList.add(stringColumnStatistics(MEDIUM_TOP_VALUE, null));
        assertMinMax(mergeColumnStatistics(statisticsList).getStringStatistics(), null, null, 400);
        statisticsList.add(stringColumnStatistics(MEDIUM_BOTTOM_VALUE, MEDIUM_BOTTOM_VALUE));
        assertMinMax(mergeColumnStatistics(statisticsList).getStringStatistics(), null, null, 500);
    }

    @Test
    public void testMergeWithNullMinMaxValues()
    {
        List<ColumnStatistics> statisticsList = new ArrayList<>();
        statisticsList.add(stringColumnStatistics(MEDIUM_BOTTOM_VALUE, MEDIUM_BOTTOM_VALUE));
        assertMinMax(mergeColumnStatistics(statisticsList).getStringStatistics(), MEDIUM_BOTTOM_VALUE, MEDIUM_BOTTOM_VALUE);
        statisticsList.add(stringColumnStatistics(null, null));
        assertMinMax(mergeColumnStatistics(statisticsList).getStringStatistics(), null, null);
    }

    @Test
    public void testMixingAddValueAndMergeWithLimit()
    {
        // max merged to null
        List<ColumnStatistics> statisticsList = new ArrayList<>();

        StringStatisticsBuilder statisticsBuilder = new StringStatisticsBuilder(7);
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMergedStringStatistics(statisticsList, 0, 0);

        statisticsBuilder.addValue(LOW_BOTTOM_VALUE);
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMergedStringStatistics(statisticsList, 1, LOW_BOTTOM_VALUE.length());
        assertMinMax(mergeColumnStatistics(statisticsList).getStringStatistics(), LOW_BOTTOM_VALUE, LOW_BOTTOM_VALUE);

        statisticsBuilder.addValue(LOW_TOP_VALUE);
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMergedStringStatistics(statisticsList, 3, LOW_BOTTOM_VALUE.length() * 2 + LOW_TOP_VALUE.length());
        assertMinMax(mergeColumnStatistics(statisticsList).getStringStatistics(), LOW_BOTTOM_VALUE, LOW_TOP_VALUE);

        statisticsBuilder.addValue(HIGH_BOTTOM_VALUE);
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMergedStringStatistics(statisticsList, 6, LOW_BOTTOM_VALUE.length() * 3 + LOW_TOP_VALUE.length() * 2 + HIGH_BOTTOM_VALUE.length());
        assertMinMax(mergeColumnStatistics(statisticsList).getStringStatistics(), LOW_BOTTOM_VALUE, null);

        statisticsBuilder.addValue(HIGH_BOTTOM_VALUE);
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMergedStringStatistics(statisticsList, 10, LOW_BOTTOM_VALUE.length() * 4 + LOW_TOP_VALUE.length() * 3 + HIGH_BOTTOM_VALUE.length() * 3);
        assertMinMax(mergeColumnStatistics(statisticsList).getStringStatistics(), LOW_BOTTOM_VALUE, null);

        // min merged to null
        statisticsList = new ArrayList<>();

        statisticsBuilder = new StringStatisticsBuilder(7);
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMergedStringStatistics(statisticsList, 0, 0);

        statisticsBuilder.addValue(MEDIUM_TOP_VALUE);
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMergedStringStatistics(statisticsList, 1, MEDIUM_TOP_VALUE.length());
        assertMinMax(mergeColumnStatistics(statisticsList).getStringStatistics(), MEDIUM_TOP_VALUE, MEDIUM_TOP_VALUE);

        statisticsBuilder.addValue(MEDIUM_BOTTOM_VALUE);
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMergedStringStatistics(statisticsList, 3, MEDIUM_TOP_VALUE.length() * 2 + MEDIUM_BOTTOM_VALUE.length());
        assertMinMax(mergeColumnStatistics(statisticsList).getStringStatistics(), MEDIUM_BOTTOM_VALUE, MEDIUM_TOP_VALUE);

        statisticsBuilder.addValue(LONG_BOTTOM_VALUE);
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMergedStringStatistics(statisticsList, 6, MEDIUM_TOP_VALUE.length() * 3 + MEDIUM_BOTTOM_VALUE.length() * 2 + LONG_BOTTOM_VALUE.length());
        assertMinMax(mergeColumnStatistics(statisticsList).getStringStatistics(), null, MEDIUM_TOP_VALUE);

        statisticsBuilder.addValue(LONG_BOTTOM_VALUE);
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMergedStringStatistics(statisticsList, 10, MEDIUM_TOP_VALUE.length() * 4 + MEDIUM_BOTTOM_VALUE.length() * 3 + LONG_BOTTOM_VALUE.length() * 3);
        assertMinMax(mergeColumnStatistics(statisticsList).getStringStatistics(), null, MEDIUM_TOP_VALUE);

        // min and max both merged to null
        statisticsList = new ArrayList<>();

        statisticsBuilder = new StringStatisticsBuilder(7);
        statisticsBuilder.addValue(MEDIUM_BOTTOM_VALUE);
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMinMax(mergeColumnStatistics(statisticsList).getStringStatistics(), MEDIUM_BOTTOM_VALUE, MEDIUM_BOTTOM_VALUE);

        statisticsBuilder.addValue(LONG_BOTTOM_VALUE);
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMinMax(mergeColumnStatistics(statisticsList).getStringStatistics(), null, MEDIUM_BOTTOM_VALUE);

        statisticsBuilder.addValue(HIGH_TOP_VALUE);
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMinMax(mergeColumnStatistics(statisticsList).getStringStatistics(), null, null);

        statisticsBuilder.addValue(HIGH_BOTTOM_VALUE);
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMinMax(mergeColumnStatistics(statisticsList).getStringStatistics(), null, null);
    }

    @Test
    public void testCopyStatsToSaveMemory()
    {
        StringStatisticsBuilder statisticsBuilder = new StringStatisticsBuilder(Integer.MAX_VALUE);
        Slice shortSlice = Slices.wrappedBuffer(LONG_BOTTOM_VALUE.getBytes(), 0, 1);
        statisticsBuilder.addValue(shortSlice);
        Slice stats = statisticsBuilder.buildColumnStatistics().getStringStatistics().getMax();

        // assert we only spend 1 byte for stats
        assertNotNull(stats);
        assertEquals(stats.getRetainedSize(), Slices.wrappedBuffer(new byte[1]).getRetainedSize());
    }

    @Test
    public void testMinAverageValueBytes()
    {
        assertMinAverageValueBytes(0L, ImmutableList.of());
        assertMinAverageValueBytes(STRING_VALUE_BYTES_OVERHEAD, ImmutableList.of(EMPTY_SLICE));
        assertMinAverageValueBytes(LOW_BOTTOM_VALUE.length() + STRING_VALUE_BYTES_OVERHEAD, ImmutableList.of(LOW_BOTTOM_VALUE));
        assertMinAverageValueBytes((LOW_BOTTOM_VALUE.length() + LOW_TOP_VALUE.length()) / 2 + STRING_VALUE_BYTES_OVERHEAD, ImmutableList.of(LOW_BOTTOM_VALUE, LOW_TOP_VALUE));
    }

    @Test
    public void testSliceWithIndexLength()
    {
        StringStatisticsBuilder builder = new StringStatisticsBuilder(10);
        Slice slice = utf8Slice("abcdefghijklmnopqrstuvwxyz");
        for (int i = 0; i < slice.length(); i++) {
            builder.addValue(slice, i, 1);
        }

        StringStatistics stringStatistics = builder.buildColumnStatistics().getStringStatistics();
        assertEquals(stringStatistics.getMin(), slice.slice(0, 1));
        assertEquals(stringStatistics.getMax(), slice.slice(slice.length() - 1, 1));
        assertEquals(stringStatistics.getSum(), slice.length());
    }

    private void assertMergedStringStatistics(List<ColumnStatistics> statisticsList, int expectedNumberOfValues, long expectedSum)
    {
        assertStringStatistics(mergeColumnStatistics(statisticsList), expectedNumberOfValues, expectedSum);

        assertNoColumnStatistics(mergeColumnStatistics(insertEmptyColumnStatisticsAt(statisticsList, 0, 10)), expectedNumberOfValues + 10);
        assertNoColumnStatistics(mergeColumnStatistics(insertEmptyColumnStatisticsAt(statisticsList, statisticsList.size(), 10)), expectedNumberOfValues + 10);
        assertNoColumnStatistics(mergeColumnStatistics(insertEmptyColumnStatisticsAt(statisticsList, statisticsList.size() / 2, 10)), expectedNumberOfValues + 10);
    }

    private static void assertMinMaxValuesWithLimit(Slice expectedMin, Slice expectedMax, List<Slice> values, int limit)
    {
        checkArgument(values != null && values.size() > 0);
        StringStatisticsBuilder builder = new StringStatisticsBuilder(limit);
        for (Slice value : values) {
            builder.addValue(value);
        }
        assertMinMax(builder.buildColumnStatistics().getStringStatistics(), expectedMin, expectedMax);
    }

    private static void assertMinMaxValuesWithLimit(Slice expectedMin, Slice expectedMax, List<Slice> values, int limit, long expectedSum)
    {
        checkArgument(values != null && values.size() > 0);
        StringStatisticsBuilder builder = new StringStatisticsBuilder(limit);
        for (Slice value : values) {
            builder.addValue(value);
        }
        assertMinMax(builder.buildColumnStatistics().getStringStatistics(), expectedMin, expectedMax, expectedSum);
    }

    private static void assertMinMax(StringStatistics actualStringStatistics, Slice expectedMin, Slice expectedMax)
    {
        if (expectedMax == null && expectedMin == null) {
            assertNull(actualStringStatistics);
            return;
        }

        assertNotNull(actualStringStatistics);
        assertEquals(actualStringStatistics.getMin(), expectedMin);
        assertEquals(actualStringStatistics.getMax(), expectedMax);
    }

    private static void assertMinMax(StringStatistics actualStringStatistics, Slice expectedMin, Slice expectedMax, long expectedSum)
    {
        assertNotNull(actualStringStatistics);
        assertEquals(actualStringStatistics.getMin(), expectedMin);
        assertEquals(actualStringStatistics.getMax(), expectedMax);
        assertEquals(actualStringStatistics.getSum(), expectedSum);
    }

    private static ColumnStatistics stringColumnStatistics(Slice minimum, Slice maximum)
    {
        if (minimum == null && maximum == null) {
            return new ColumnStatistics(100L, null);
        }
        return new StringColumnStatistics(
                100L,
                null,
                new StringStatistics(minimum, maximum, 100));
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
