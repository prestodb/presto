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

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.FixedWidthBlockBuilder;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

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
            assertBlockFilteredPositions(expectedValues, blockBuilder.build(), Ints.asList(0, 2, 4, 6, 7, 9, 10, 16));
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
