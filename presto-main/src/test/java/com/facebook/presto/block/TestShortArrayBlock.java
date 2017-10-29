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
import com.facebook.presto.spi.block.ShortArrayBlock;
import com.facebook.presto.spi.block.ShortArrayBlockBuilder;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestShortArrayBlock
        extends AbstractTestBlock
{
    @Test
    public void test()
    {
        Slice[] expectedValues = createTestValue(17);
        assertFixedWithValues(expectedValues);
        assertFixedWithValues((Slice[]) alternatingNullValues(expectedValues));
    }

    @Test
    public void testCopyPositions()
            throws Exception
    {
        Slice[] expectedValues = (Slice[]) alternatingNullValues(createTestValue(17));
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), 0, 2, 4, 6, 7, 9, 10, 16);
    }

    @Test
    public void testLazyBlockBuilderInitialization()
            throws Exception
    {
        Slice[] expectedValues = createTestValue(100);
        BlockBuilder emptyBlockBuilder = new ShortArrayBlockBuilder(new BlockBuilderStatus(), 0);

        BlockBuilder blockBuilder = new ShortArrayBlockBuilder(new BlockBuilderStatus(), expectedValues.length);
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
    public void testCompactBlock()
    {
        // Test Constructor
        short[] shortArray = {(short) 0, (short) 0, (short) 1, (short) 2, (short) 3, (short) 4};
        boolean[] valueIsNull = {false, true, false, false, false, false};

        assertCompact(new ShortArrayBlock(0, new boolean[0], new short[0]));
        assertCompact(new ShortArrayBlock(shortArray.length, valueIsNull, shortArray));
        assertNotCompact(new ShortArrayBlock(shortArray.length - 1, valueIsNull, shortArray));

        // Test getRegion and copyRegion
        Block block = new ShortArrayBlock(shortArray.length, valueIsNull, shortArray);
        assertGetRegionCompactness(block);
        assertCopyRegionCompactness(block);

        // Test BlockBuilder
        BlockBuilder emptyBlockBuilder = new ShortArrayBlockBuilder(new BlockBuilderStatus(), 0);
        assertNotCompact(emptyBlockBuilder);
        assertCompact(emptyBlockBuilder.build());

        BlockBuilder nonFullBlockBuilder = createNonFullBlockBuilderWithValues(createTestValue(17));
        assertNotCompact(nonFullBlockBuilder);
        assertNotCompact(nonFullBlockBuilder.build());
        assertCopyRegionCompactness(nonFullBlockBuilder);

        BlockBuilder fullBlockBuilder = createBlockBuilderWithValues(createTestValue(17));
        assertNotCompact(fullBlockBuilder);
        assertCompact(fullBlockBuilder.build());
        assertCopyRegionCompactness(fullBlockBuilder);

        assertCompact(fullBlockBuilder.getRegion(0, fullBlockBuilder.getPositionCount()));
        assertNotCompact(fullBlockBuilder.getRegion(0, fullBlockBuilder.getPositionCount() - 1));
        assertNotCompact(fullBlockBuilder.getRegion(1, fullBlockBuilder.getPositionCount() - 1));
    }

    private void assertFixedWithValues(Slice[] expectedValues)
    {
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);
        assertBlock(blockBuilder, expectedValues);
        assertBlock(blockBuilder.build(), expectedValues);
    }

    private static BlockBuilder createBlockBuilderWithValues(Slice[] expectedValues)
    {
        ShortArrayBlockBuilder blockBuilder = new ShortArrayBlockBuilder(new BlockBuilderStatus(), expectedValues.length);
        writeValues(expectedValues, blockBuilder);
        return blockBuilder;
    }

    private static BlockBuilder createNonFullBlockBuilderWithValues(Slice[] expectedValues)
    {
        ShortArrayBlockBuilder blockBuilder = new ShortArrayBlockBuilder(new BlockBuilderStatus(), expectedValues.length * 2);
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
                blockBuilder.writeShort(expectedValue.getShort(0)).closeEntry();
            }
        }
    }

    private static Slice[] createTestValue(int positionCount)
    {
        Slice[] expectedValues = new Slice[positionCount];
        for (int position = 0; position < positionCount; position++) {
            expectedValues[position] = createExpectedValue(SIZE_OF_SHORT);
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
        return true;
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
    protected boolean isSliceAccessSupported()
    {
        return false;
    }
}
