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
package io.prestosql.block;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.VariableWidthBlock;
import io.prestosql.spi.block.VariableWidthBlockBuilder;
import io.prestosql.spi.type.VarcharType;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.String.format;
import static java.util.Arrays.copyOfRange;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestVariableWidthBlock
        extends AbstractTestBlock
{
    @Test
    public void test()
    {
        Slice[] expectedValues = createExpectedValues(100);
        assertVariableWithValues(expectedValues);
        assertVariableWithValues(alternatingNullValues(expectedValues));
    }

    @Test
    public void testCopyRegion()
    {
        Slice[] expectedValues = createExpectedValues(100);
        Block block = createBlockBuilderWithValues(expectedValues).build();
        Block actual = block.copyRegion(10, 10);
        Block expected = createBlockBuilderWithValues(copyOfRange(expectedValues, 10, 20)).build();
        assertEquals(actual.getPositionCount(), expected.getPositionCount());
        assertEquals(actual.getSizeInBytes(), expected.getSizeInBytes());
    }

    @Test
    public void testCopyPositions()
    {
        Slice[] expectedValues = alternatingNullValues(createExpectedValues(100));
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), () -> blockBuilder.newBlockBuilderLike(null), 0, 2, 4, 6, 7, 9, 10, 16);
    }

    @Test
    public void testLazyBlockBuilderInitialization()
    {
        Slice[] expectedValues = createExpectedValues(100);
        BlockBuilder emptyBlockBuilder = new VariableWidthBlockBuilder(null, 0, 0);

        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, expectedValues.length, 32 * expectedValues.length);
        assertEquals(blockBuilder.getSizeInBytes(), emptyBlockBuilder.getSizeInBytes());
        assertEquals(blockBuilder.getRetainedSizeInBytes(), emptyBlockBuilder.getRetainedSizeInBytes());

        writeValues(expectedValues, blockBuilder);
        assertTrue(blockBuilder.getSizeInBytes() > emptyBlockBuilder.getSizeInBytes());
        assertTrue(blockBuilder.getRetainedSizeInBytes() > emptyBlockBuilder.getRetainedSizeInBytes());

        blockBuilder = blockBuilder.newBlockBuilderLike(null);
        assertEquals(blockBuilder.getSizeInBytes(), emptyBlockBuilder.getSizeInBytes());
        assertEquals(blockBuilder.getRetainedSizeInBytes(), emptyBlockBuilder.getRetainedSizeInBytes());
    }

    @Test
    private void testGetSizeInBytes()
    {
        int numEntries = 1000;
        VarcharType unboundedVarcharType = createUnboundedVarcharType();
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, numEntries, 20 * numEntries);
        for (int i = 0; i < numEntries; i++) {
            unboundedVarcharType.writeString(blockBuilder, String.valueOf(ThreadLocalRandom.current().nextLong()));
        }
        Block block = blockBuilder.build();

        List<Block> splitQuarter = splitBlock(block, 4);
        long sizeInBytes = block.getSizeInBytes();
        long quarter1size = splitQuarter.get(0).getSizeInBytes();
        long quarter2size = splitQuarter.get(1).getSizeInBytes();
        long quarter3size = splitQuarter.get(2).getSizeInBytes();
        long quarter4size = splitQuarter.get(3).getSizeInBytes();
        double expectedQuarterSizeMin = sizeInBytes * 0.2;
        double expectedQuarterSizeMax = sizeInBytes * 0.3;
        assertTrue(quarter1size > expectedQuarterSizeMin && quarter1size < expectedQuarterSizeMax, format("quarter1size is %s, should be between %s and %s", quarter1size, expectedQuarterSizeMin, expectedQuarterSizeMax));
        assertTrue(quarter2size > expectedQuarterSizeMin && quarter2size < expectedQuarterSizeMax, format("quarter2size is %s, should be between %s and %s", quarter2size, expectedQuarterSizeMin, expectedQuarterSizeMax));
        assertTrue(quarter3size > expectedQuarterSizeMin && quarter3size < expectedQuarterSizeMax, format("quarter3size is %s, should be between %s and %s", quarter3size, expectedQuarterSizeMin, expectedQuarterSizeMax));
        assertTrue(quarter4size > expectedQuarterSizeMin && quarter4size < expectedQuarterSizeMax, format("quarter4size is %s, should be between %s and %s", quarter4size, expectedQuarterSizeMin, expectedQuarterSizeMax));
        assertEquals(quarter1size + quarter2size + quarter3size + quarter4size, sizeInBytes);
    }

    @Test
    public void testEstimatedDataSizeForStats()
    {
        Slice[] expectedValues = createExpectedValues(100);
        assertEstimatedDataSizeForStats(createBlockBuilderWithValues(expectedValues), expectedValues);
    }

    @Test
    public void testCompactBlock()
    {
        Slice compactSlice = Slices.copyOf(createExpectedValue(16));
        Slice incompactSlice = Slices.copyOf(createExpectedValue(20)).slice(0, 16);
        int[] offsets = {0, 1, 1, 2, 4, 8, 16};
        boolean[] valueIsNull = {false, true, false, false, false, false};

        testCompactBlock(new VariableWidthBlock(0, EMPTY_SLICE, new int[1], Optional.empty()));
        testCompactBlock(new VariableWidthBlock(valueIsNull.length, compactSlice, offsets, Optional.of(valueIsNull)));

        testIncompactBlock(new VariableWidthBlock(valueIsNull.length - 1, compactSlice, offsets, Optional.of(valueIsNull)));
        // underlying slice is not compact
        testIncompactBlock(new VariableWidthBlock(valueIsNull.length, incompactSlice, offsets, Optional.of(valueIsNull)));
    }

    private void assertVariableWithValues(Slice[] expectedValues)
    {
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);
        assertBlock(blockBuilder, () -> blockBuilder.newBlockBuilderLike(null), expectedValues);
        assertBlock(blockBuilder.build(), () -> blockBuilder.newBlockBuilderLike(null), expectedValues);
    }

    private static BlockBuilder createBlockBuilderWithValues(Slice[] expectedValues)
    {
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, expectedValues.length, 32 * expectedValues.length);
        return writeValues(expectedValues, blockBuilder);
    }

    private static BlockBuilder writeValues(Slice[] expectedValues, BlockBuilder blockBuilder)
    {
        for (Slice expectedValue : expectedValues) {
            if (expectedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeBytes(expectedValue, 0, expectedValue.length()).closeEntry();
            }
        }
        return blockBuilder;
    }
}
