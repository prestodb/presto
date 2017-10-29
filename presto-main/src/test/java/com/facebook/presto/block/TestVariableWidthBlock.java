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
package com.facebook.presto.block;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.VariableWidthBlock;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static io.airlift.slice.Slices.EMPTY_SLICE;
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
        assertVariableWithValues((Slice[]) alternatingNullValues(expectedValues));
    }

    @Test
    public void testCopyRegion()
            throws Exception
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
            throws Exception
    {
        Slice[] expectedValues = (Slice[]) alternatingNullValues(createExpectedValues(100));
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), 0, 2, 4, 6, 7, 9, 10, 16);
    }

    @Test
    public void testLazyBlockBuilderInitialization()
            throws Exception
    {
        Slice[] expectedValues = createExpectedValues(100);
        BlockBuilder emptyBlockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), 0, 0);

        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), expectedValues.length, 32 * expectedValues.length);
        assertEquals(blockBuilder.getSizeInBytes(), emptyBlockBuilder.getSizeInBytes());
        assertEquals(blockBuilder.getRetainedSizeInBytes(), emptyBlockBuilder.getRetainedSizeInBytes());

        writeValues(expectedValues, blockBuilder);
        assertTrue(blockBuilder.getSizeInBytes() > emptyBlockBuilder.getSizeInBytes());
        assertTrue(blockBuilder.getRetainedSizeInBytes() > emptyBlockBuilder.getRetainedSizeInBytes());

        blockBuilder = blockBuilder.newBlockBuilderLike(new BlockBuilderStatus());
        assertEquals(blockBuilder.getSizeInBytes(), emptyBlockBuilder.getSizeInBytes());
        assertEquals(blockBuilder.getRetainedSizeInBytes(), emptyBlockBuilder.getRetainedSizeInBytes());
    }

    @Test
    private void testGetSizeInBytes()
    {
        int numEntries = 1000;
        VarcharType unboundedVarcharType = createUnboundedVarcharType();
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), numEntries, 20 * numEntries);
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
    public void testCompactBlock()
    {
        // Test Constructor
        Slice slice = Slices.copyOf(createExpectedValue(16));
        Slice largerSlice = Slices.copyOf(createExpectedValue(20));
        int[] offsets = {0, 1, 1, 2, 4, 8, 16};
        boolean[] valueIsNull = {false, true, false, false, false, false};

        assertCompact(new VariableWidthBlock(0, EMPTY_SLICE, new int[1], new boolean[0]));
        assertCompact(new VariableWidthBlock(valueIsNull.length, slice, offsets, valueIsNull));

        assertNotCompact(new VariableWidthBlock(valueIsNull.length - 1, slice, offsets, valueIsNull));
        assertNotCompact(new VariableWidthBlock(valueIsNull.length, largerSlice, offsets, valueIsNull));
        assertNotCompact(new VariableWidthBlock(valueIsNull.length, largerSlice.slice(0, 16), offsets, valueIsNull));

        // Test getRegion and copyRegion
        Block block = new VariableWidthBlock(valueIsNull.length, slice, offsets, valueIsNull);
        assertGetRegionCompactness(block);
        assertCopyRegionCompactness(block);
        assertCopyRegionCompactness(new VariableWidthBlock(valueIsNull.length, largerSlice, offsets, valueIsNull));
        assertCopyRegionCompactness(new VariableWidthBlock(valueIsNull.length, largerSlice.slice(0, 16), offsets, valueIsNull));

        // Test BlockBuilder
        BlockBuilder emptyBlockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), 0, 0);
        assertNotCompact(emptyBlockBuilder);
        assertCompact(emptyBlockBuilder.build());

        BlockBuilder nonFullBlockBuilder = createNonFullBlockBuilderWithValues(createExpectedValues(17));
        assertNotCompact(nonFullBlockBuilder);
        assertNotCompact(nonFullBlockBuilder.build());
        assertCopyRegionCompactness(nonFullBlockBuilder);

        BlockBuilder fullBlockBuilder = createBlockBuilderWithValues(createExpectedValues(17));
        assertNotCompact(fullBlockBuilder);
        assertCompact(fullBlockBuilder.build());
        assertCopyRegionCompactness(fullBlockBuilder);

        assertCompact(fullBlockBuilder.getRegion(0, fullBlockBuilder.getPositionCount()));
        assertNotCompact(fullBlockBuilder.getRegion(0, fullBlockBuilder.getPositionCount() - 1));
        assertNotCompact(fullBlockBuilder.getRegion(1, fullBlockBuilder.getPositionCount() - 1));
    }

    private void assertVariableWithValues(Slice[] expectedValues)
    {
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);
        assertBlock(blockBuilder, expectedValues);
        assertBlock(blockBuilder.build(), expectedValues);
    }

    private static BlockBuilder createBlockBuilderWithValues(Slice[] expectedValues)
    {
        int totalBytes = Arrays.stream(expectedValues).mapToInt(slice -> slice == null ? 0 : slice.length()).sum();
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), expectedValues.length, totalBytes);
        return writeValues(expectedValues, blockBuilder);
    }

    private static BlockBuilder createNonFullBlockBuilderWithValues(Slice[] expectedValues)
    {
        int totalBytes = Arrays.stream(expectedValues).mapToInt(Slice::length).sum();
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), expectedValues.length * 2, totalBytes * 2);
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
