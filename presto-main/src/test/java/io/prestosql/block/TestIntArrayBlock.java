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
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.IntArrayBlock;
import io.prestosql.spi.block.IntArrayBlockBuilder;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestIntArrayBlock
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
        BlockBuilder emptyBlockBuilder = new IntArrayBlockBuilder(null, 0);

        BlockBuilder blockBuilder = new IntArrayBlockBuilder(null, expectedValues.length);
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
        int[] intArray = {0, 0, 1, 2, 3, 4};
        boolean[] valueIsNull = {false, true, false, false, false, false};

        testCompactBlock(new IntArrayBlock(0, Optional.empty(), new int[0]));
        testCompactBlock(new IntArrayBlock(intArray.length, Optional.of(valueIsNull), intArray));
        testIncompactBlock(new IntArrayBlock(intArray.length - 1, Optional.of(valueIsNull), intArray));
    }

    private void assertFixedWithValues(Slice[] expectedValues)
    {
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);
        assertBlock(blockBuilder, () -> blockBuilder.newBlockBuilderLike(null), expectedValues);
        assertBlock(blockBuilder.build(), () -> blockBuilder.newBlockBuilderLike(null), expectedValues);
    }

    private static BlockBuilder createBlockBuilderWithValues(Slice[] expectedValues)
    {
        IntArrayBlockBuilder blockBuilder = new IntArrayBlockBuilder(null, expectedValues.length);
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
                blockBuilder.writeInt(expectedValue.getInt(0)).closeEntry();
            }
        }
    }

    private static Slice[] createTestValue(int positionCount)
    {
        Slice[] expectedValues = new Slice[positionCount];
        for (int position = 0; position < positionCount; position++) {
            expectedValues[position] = createExpectedValue(SIZE_OF_INT);
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
