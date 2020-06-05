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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.Int128ArrayBlock;
import com.facebook.presto.common.block.Int128ArrayBlockBuilder;
import com.facebook.presto.common.block.VariableWidthBlockBuilder;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.block.BlockUtil.getNum128Integers;
import static com.facebook.presto.common.block.Int128ArrayBlock.INT128_BYTES;
import static com.facebook.presto.type.DecimalInequalityOperators.distinctBlockPositionLongLong;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestInt128ArrayBlock
        extends AbstractTestBlock
{
    @Test
    public void test()
    {
        Slice[] expectedValues = createTestValue(17);
        assertFixedWithValues(expectedValues);
        assertFixedWithValues(alternatingNullValues(expectedValues));
    }

    @Test
    public void testCopyPositions()
    {
        Slice[] expectedValues = alternatingNullValues(createTestValue(17));
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), () -> blockBuilder.newBlockBuilderLike(null), 0, 2, 4, 6, 7, 9, 10, 16);
    }

    @Test
    public void testLazyBlockBuilderInitialization()
    {
        Slice[] expectedValues = createTestValue(100);
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
    public void testEstimatedDataSizeForStats()
    {
        Slice[] expectedValues = createTestValue(100);
        assertEstimatedDataSizeForStats(createBlockBuilderWithValues(expectedValues), expectedValues);
    }

    @Test
    public void testCompactBlock()
    {
        long[] longArray = {0L, 0L, 0L, 0L, 0L, 1L, 0L, 2L, 0L, 3L, 0L, 4L};
        boolean[] valueIsNull = {false, true, false, false, false, false};

        testCompactBlock(new Int128ArrayBlock(0, Optional.empty(), new long[0]));
        testCompactBlock(new Int128ArrayBlock(valueIsNull.length, Optional.of(valueIsNull), longArray));
        testIncompactBlock(new Int128ArrayBlock(valueIsNull.length - 2, Optional.of(valueIsNull), longArray));
    }

    @Test
    public void testIsDistinctFrom()
    {
        Block left = new Int128ArrayBlock(1, Optional.empty(), new long[]{112L, 0L});
        Block right = new Int128ArrayBlock(1, Optional.empty(), new long[]{185L, 0L});

        assertFalse(distinctBlockPositionLongLong(left, 0, left, 0));
        assertTrue(distinctBlockPositionLongLong(left, 0, right, 0));
    }

    private void assertFixedWithValues(Slice[] expectedValues)
    {
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);
        assertBlock(blockBuilder, () -> blockBuilder.newBlockBuilderLike(null), expectedValues);
        assertBlock(blockBuilder.build(), () -> blockBuilder.newBlockBuilderLike(null), expectedValues);
    }

    private static BlockBuilder createBlockBuilderWithValues(Slice[] expectedValues)
    {
        Int128ArrayBlockBuilder blockBuilder = new Int128ArrayBlockBuilder(null, expectedValues.length);
        writeValues(expectedValues, blockBuilder);
        return blockBuilder;
    }

    private static void writeValues(Slice[] expectedValues, BlockBuilder blockBuilder)
    {
        for (Slice expectedValue : expectedValues) {
            if (expectedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeLong(expectedValue.getLong(0));
                blockBuilder.writeLong(expectedValue.getLong(8));
                blockBuilder.closeEntry();
            }
        }
    }

    private static Slice[] createTestValue(int positionCount)
    {
        Slice[] expectedValues = new Slice[positionCount];
        for (int position = 0; position < positionCount; position++) {
            expectedValues[position] = createExpectedValue(INT128_BYTES);
        }
        return expectedValues;
    }

    @Override
    protected boolean isByteAccessSupported()
    {
        return false;
    }

    @Override
    protected boolean isShortAccessSupported()
    {
        return false;
    }

    @Override
    protected boolean isIntAccessSupported()
    {
        return false;
    }

    @Override
    protected boolean isLongAccessSupported()
    {
        return false;
    }

    @Override
    protected boolean isAlignedLongAccessSupported()
    {
        return true;
    }

    @Override
    protected boolean isSliceAccessSupported()
    {
        return true;
    }

    @Override
    protected void assertSlicePosition(Block block, int position, Slice expectedSliceValue)
    {
        int num128Integers = Math.min(getNum128Integers(expectedSliceValue.length()), block.getPositionCount() - position);
        for (int offset = 0; offset < num128Integers; offset++) {
            assertEquals(expectedSliceValue.length(), SIZE_OF_LONG * 2);

            assertEquals(block.getSlice(position, offset, SIZE_OF_LONG * 2), expectedSliceValue.slice(offset, SIZE_OF_LONG * 2));
            assertEquals(block.getSliceLength(position), SIZE_OF_LONG * 2);

            assertTrue(block.bytesEqual(position, offset, expectedSliceValue, 0, SIZE_OF_LONG * 2));
            assertFalse(block.bytesEqual(position, offset, Slices.utf8Slice("XXXXXXXXXXXXXXXX"), 0, SIZE_OF_LONG * 2));
        }
    }

    @Override
    protected void assertSlicePositionUnchecked(Block block, int internalPosition, Slice expectedSliceValue)
    {
        assertSlicePosition(block, internalPosition - block.getOffsetBase(), expectedSliceValue);
    }
}
