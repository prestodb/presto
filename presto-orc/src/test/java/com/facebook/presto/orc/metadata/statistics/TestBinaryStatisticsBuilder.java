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

import com.facebook.presto.common.block.VariableWidthBlockBuilder;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.orc.metadata.statistics.AbstractStatisticsBuilderTest.StatisticsType.NONE;
import static com.facebook.presto.orc.metadata.statistics.BinaryStatistics.BINARY_VALUE_BYTES_OVERHEAD;
import static com.facebook.presto.orc.metadata.statistics.ColumnStatistics.mergeColumnStatistics;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestBinaryStatisticsBuilder
        extends AbstractStatisticsBuilderTest<BinaryStatisticsBuilder, Slice>
{
    private static final Slice FIRST_VALUE = utf8Slice("apple");
    private static final Slice SECOND_VALUE = utf8Slice("banana");

    public TestBinaryStatisticsBuilder()
    {
        super(NONE, BinaryStatisticsBuilder::new, TestBinaryStatisticsBuilder::addValue);
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
            addValue(binaryStatisticsBuilder, value);
        }
        assertBinaryStatistics(binaryStatisticsBuilder.buildColumnStatistics(), 3, EMPTY_SLICE.length() + FIRST_VALUE.length() + SECOND_VALUE.length());
    }

    @Test
    public void testBlockBinaryStatistics()
    {
        String alphabets = "abcdefghijklmnopqrstuvwxyz";
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, alphabets.length(), alphabets.length());
        Slice slice = utf8Slice(alphabets);
        for (int i = 0; i < slice.length(); i++) {
            VARBINARY.writeSlice(blockBuilder, slice, i, 1);
        }
        blockBuilder.appendNull();

        BinaryStatisticsBuilder binaryStatisticsBuilder = new BinaryStatisticsBuilder();
        binaryStatisticsBuilder.addBlock(VARBINARY, blockBuilder);

        BinaryStatistics binaryStatistics = binaryStatisticsBuilder.buildColumnStatistics().getBinaryStatistics();
        assertEquals(binaryStatistics.getSum(), slice.length());
    }

    @Test
    public void testAddValueByPosition()
    {
        String alphabet = "abcdefghijklmnopqrstuvwxyz";
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, alphabet.length(), alphabet.length());
        Slice slice = utf8Slice(alphabet);
        for (int i = 0; i < slice.length(); i++) {
            VARBINARY.writeSlice(blockBuilder, slice, i, 1);
        }
        blockBuilder.appendNull();

        BinaryStatisticsBuilder statisticsBuilder = new BinaryStatisticsBuilder();
        int positionCount = blockBuilder.getPositionCount();
        for (int position = 0; position < positionCount; position++) {
            statisticsBuilder.addValue(VARBINARY, blockBuilder, position);
        }

        ColumnStatistics columnStatistics = statisticsBuilder.buildColumnStatistics();
        assertEquals(columnStatistics.getNumberOfValues(), positionCount - 1);

        BinaryStatistics binaryStatistics = columnStatistics.getBinaryStatistics();
        assertEquals(binaryStatistics.getSum(), slice.length());
    }

    @Test
    public void testMerge()
    {
        List<ColumnStatistics> statisticsList = new ArrayList<>();

        BinaryStatisticsBuilder statisticsBuilder = new BinaryStatisticsBuilder();
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMergedBinaryStatistics(statisticsList, 0, 0);

        addValue(statisticsBuilder, EMPTY_SLICE);
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMergedBinaryStatistics(statisticsList, 1, 0);

        addValue(statisticsBuilder, FIRST_VALUE);
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMergedBinaryStatistics(statisticsList, 3, FIRST_VALUE.length());

        addValue(statisticsBuilder, SECOND_VALUE);
        statisticsList.add(statisticsBuilder.buildColumnStatistics());
        assertMergedBinaryStatistics(statisticsList, 6, FIRST_VALUE.length() * 2 + SECOND_VALUE.length());
    }

    @Test
    public void testTotalValueBytes()
    {
        assertTotalValueBytes(0L, ImmutableList.of());
        assertTotalValueBytes(BINARY_VALUE_BYTES_OVERHEAD, ImmutableList.of(EMPTY_SLICE));
        assertTotalValueBytes(FIRST_VALUE.length() + BINARY_VALUE_BYTES_OVERHEAD, ImmutableList.of(FIRST_VALUE));
        assertTotalValueBytes((FIRST_VALUE.length() + SECOND_VALUE.length()) + 2 * BINARY_VALUE_BYTES_OVERHEAD, ImmutableList.of(FIRST_VALUE, SECOND_VALUE));
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

    public static void addValue(BinaryStatisticsBuilder binaryStatisticsBuilder, Slice slice)
    {
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, 1, slice.length());
        blockBuilder.writeBytes(slice, 0, slice.length()).closeEntry();

        binaryStatisticsBuilder.addBlock(VARBINARY, blockBuilder);
    }
}
