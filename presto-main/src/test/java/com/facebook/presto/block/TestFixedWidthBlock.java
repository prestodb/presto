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
import com.facebook.presto.spi.block.FixedWidthBlock;
import com.facebook.presto.spi.block.FixedWidthBlockBuilder;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestFixedWidthBlock
        extends AbstractTestBlock
{
    @Test
    public void test()
    {
        for (int fixedSize = 0; fixedSize < 20; fixedSize++) {
            Slice[] expectedValues = createExpectedValues(17, fixedSize);
            assertFixedWithValues(expectedValues, fixedSize);
            assertFixedWithValues((Slice[]) alternatingNullValues(expectedValues), fixedSize);
        }
    }

    @Test
    public void testCopyPositions()
            throws Exception
    {
        for (int fixedSize = 0; fixedSize < 20; fixedSize++) {
            Slice[] expectedValues = (Slice[]) alternatingNullValues(createExpectedValues(17, fixedSize));
            BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues, fixedSize);
            assertBlockFilteredPositions(expectedValues, blockBuilder.build(), 0, 2, 4, 6, 7, 9, 10, 16);
        }
    }

    @Test
    public void testLazyBlockBuilderInitialization()
            throws Exception
    {
        for (int fixedSize = 0; fixedSize < 20; fixedSize++) {
            Slice[] expectedValues = (Slice[]) alternatingNullValues(createExpectedValues(17, fixedSize));
            BlockBuilder emptyBlockBuilder = new FixedWidthBlockBuilder(fixedSize, new BlockBuilderStatus(), 0);

            BlockBuilder blockBuilder = new FixedWidthBlockBuilder(fixedSize, new BlockBuilderStatus(), expectedValues.length);
            assertEquals(blockBuilder.getSizeInBytes(), emptyBlockBuilder.getSizeInBytes());
            assertEquals(blockBuilder.getRetainedSizeInBytes(), emptyBlockBuilder.getRetainedSizeInBytes());

            writeValues(expectedValues, blockBuilder);
            assertTrue(blockBuilder.getSizeInBytes() > emptyBlockBuilder.getSizeInBytes());
            assertTrue(blockBuilder.getRetainedSizeInBytes() > emptyBlockBuilder.getRetainedSizeInBytes());

            blockBuilder = blockBuilder.newBlockBuilderLike(new BlockBuilderStatus());
            assertEquals(blockBuilder.getSizeInBytes(), emptyBlockBuilder.getSizeInBytes());
            assertEquals(blockBuilder.getRetainedSizeInBytes(), emptyBlockBuilder.getRetainedSizeInBytes());
        }
    }

    @Test
    public void testCompactBlock()
    {
        // Test Constructor
        Slice slice = Slices.copyOf(createExpectedValue(24));
        Slice largerSlice = Slices.copyOf(createExpectedValue(30));
        boolean[] valueIsNull = {false, true, false, false, false, false};

        assertCompact(new FixedWidthBlock(4, 0, EMPTY_SLICE, Slices.wrappedBooleanArray(new boolean[0])));
        assertCompact(new FixedWidthBlock(4, valueIsNull.length, slice, Slices.wrappedBooleanArray(valueIsNull)));

        assertNotCompact(new FixedWidthBlock(4, valueIsNull.length - 1, slice, Slices.wrappedBooleanArray(valueIsNull)));
        assertNotCompact(new FixedWidthBlock(4, valueIsNull.length, largerSlice, Slices.wrappedBooleanArray(valueIsNull)));
        assertNotCompact(new FixedWidthBlock(4, valueIsNull.length, largerSlice.slice(0, 24), Slices.wrappedBooleanArray(valueIsNull)));

        // Test getRegion and copyRegion
        Block block = new FixedWidthBlock(4, valueIsNull.length, slice, Slices.wrappedBooleanArray(valueIsNull));
        assertGetRegionCompactness(block);
        assertCopyRegionCompactness(block);
        assertCopyRegionCompactness(new FixedWidthBlock(4, valueIsNull.length, largerSlice, Slices.wrappedBooleanArray(valueIsNull)));
        assertCopyRegionCompactness(new FixedWidthBlock(4, valueIsNull.length, largerSlice.slice(0, 24), Slices.wrappedBooleanArray(valueIsNull)));

        // Test BlockBuilder
        BlockBuilder emptyBlockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), 0, 0);
        assertNotCompact(emptyBlockBuilder);
        assertCompact(emptyBlockBuilder.build());

        BlockBuilder nonFullBlockBuilder = createNonFullBlockBuilderWithValues(createExpectedValues(17, 4), 4);
        assertNotCompact(nonFullBlockBuilder);
        assertNotCompact(nonFullBlockBuilder.build());
        assertCopyRegionCompactness(nonFullBlockBuilder);

        BlockBuilder fullBlockBuilder = createBlockBuilderWithValues(createExpectedValues(17, 4), 4);
        assertNotCompact(fullBlockBuilder);
        assertCompact(fullBlockBuilder.build());
        assertCopyRegionCompactness(fullBlockBuilder);

        assertCompact(fullBlockBuilder.getRegion(0, fullBlockBuilder.getPositionCount()));
        assertNotCompact(fullBlockBuilder.getRegion(0, fullBlockBuilder.getPositionCount() - 1));
        assertNotCompact(fullBlockBuilder.getRegion(1, fullBlockBuilder.getPositionCount() - 1));
    }

    private void assertFixedWithValues(Slice[] expectedValues, int fixedSize)
    {
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues, fixedSize);
        assertBlock(blockBuilder, expectedValues);
        assertBlock(blockBuilder.build(), expectedValues);
    }

    private static BlockBuilder createBlockBuilderWithValues(Slice[] expectedValues, int fixedSize)
    {
        FixedWidthBlockBuilder blockBuilder = new FixedWidthBlockBuilder(fixedSize, expectedValues.length);
        writeValues(expectedValues, blockBuilder);
        return blockBuilder;
    }

    private static BlockBuilder createNonFullBlockBuilderWithValues(Slice[] expectedValues, int fixedSize)
    {
        FixedWidthBlockBuilder blockBuilder = new FixedWidthBlockBuilder(fixedSize, expectedValues.length * 2);
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
                blockBuilder.writeBytes(expectedValue, 0, expectedValue.length()).closeEntry();
            }
        }
    }

    private static Slice[] createExpectedValues(int positionCount, int fixedSize)
    {
        Slice[] expectedValues = new Slice[positionCount];
        for (int position = 0; position < positionCount; position++) {
            expectedValues[position] = createExpectedValue(fixedSize);
        }
        return expectedValues;
    }
}
