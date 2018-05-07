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

import com.facebook.presto.spi.block.ArrayBlockBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.ColumnarArray;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.lang.reflect.Array;
import java.util.Arrays;

import static com.facebook.presto.block.ColumnarTestUtils.alternatingNullValues;
import static com.facebook.presto.block.ColumnarTestUtils.assertBlock;
import static com.facebook.presto.block.ColumnarTestUtils.assertBlockPosition;
import static com.facebook.presto.block.ColumnarTestUtils.createTestDictionaryBlock;
import static com.facebook.presto.block.ColumnarTestUtils.createTestDictionaryExpectedValues;
import static com.facebook.presto.block.ColumnarTestUtils.createTestRleBlock;
import static com.facebook.presto.block.ColumnarTestUtils.createTestRleExpectedValues;
import static com.facebook.presto.spi.block.ColumnarArray.toColumnarArray;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestColumnarArray
{
    private static final int[] ARRAY_SIZES = new int[] {16, 0, 13, 1, 2, 11, 4, 7};

    @Test
    public void test()
    {
        Slice[][] expectedValues = new Slice[ARRAY_SIZES.length][];
        for (int i = 0; i < ARRAY_SIZES.length; i++) {
            expectedValues[i] = new Slice[ARRAY_SIZES[i]];
            for (int j = 0; j < ARRAY_SIZES[i]; j++) {
                if (j % 3 != 1) {
                    expectedValues[i][j] = Slices.utf8Slice(String.format("%d.%d", i, j));
                }
            }
        }
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);
        verifyBlock(blockBuilder, expectedValues);
        verifyBlock(blockBuilder.build(), expectedValues);

        Slice[][] expectedValuesWithNull = alternatingNullValues(expectedValues);
        BlockBuilder blockBuilderWithNull = createBlockBuilderWithValues(expectedValuesWithNull);
        verifyBlock(blockBuilderWithNull, expectedValuesWithNull);
        verifyBlock(blockBuilderWithNull.build(), expectedValuesWithNull);
    }

    private static <T> void verifyBlock(Block block, T[] expectedValues)
    {
        assertBlock(block, expectedValues);

        assertColumnarArray(block, expectedValues);
        assertDictionaryBlock(block, expectedValues);
        assertRunLengthEncodedBlock(block, expectedValues);

        int offset = 1;
        int length = expectedValues.length - 2;
        Block blockRegion = block.getRegion(offset, length);
        T[] expectedValuesRegion = Arrays.copyOfRange(expectedValues, offset, offset + length);

        assertBlock(blockRegion, expectedValuesRegion);

        assertColumnarArray(blockRegion, expectedValuesRegion);
        assertDictionaryBlock(blockRegion, expectedValuesRegion);
        assertRunLengthEncodedBlock(blockRegion, expectedValuesRegion);
    }

    private static <T> void assertDictionaryBlock(Block block, T[] expectedValues)
    {
        DictionaryBlock dictionaryBlock = createTestDictionaryBlock(block);
        T[] expectedDictionaryValues = createTestDictionaryExpectedValues(expectedValues);

        assertBlock(dictionaryBlock, expectedDictionaryValues);
        assertColumnarArray(dictionaryBlock, expectedDictionaryValues);
        assertRunLengthEncodedBlock(dictionaryBlock, expectedDictionaryValues);
    }

    private static <T> void assertRunLengthEncodedBlock(Block block, T[] expectedValues)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            RunLengthEncodedBlock runLengthEncodedBlock = createTestRleBlock(block, position);
            T[] expectedDictionaryValues = createTestRleExpectedValues(expectedValues, position);

            assertBlock(runLengthEncodedBlock, expectedDictionaryValues);
            assertColumnarArray(runLengthEncodedBlock, expectedDictionaryValues);
        }
    }

    private static <T> void assertColumnarArray(Block block, T[] expectedValues)
    {
        ColumnarArray columnarArray = toColumnarArray(block);
        assertEquals(columnarArray.getPositionCount(), expectedValues.length);

        Block elementsBlock = columnarArray.getElementsBlock();
        int elementsPosition = 0;
        for (int position = 0; position < expectedValues.length; position++) {
            T expectedArray = expectedValues[position];
            assertEquals(columnarArray.isNull(position), expectedArray == null);
            assertEquals(columnarArray.getLength(position), expectedArray == null ? 0 : Array.getLength(expectedArray));

            for (int i = 0; i < columnarArray.getLength(position); i++) {
                Object expectedElement = Array.get(expectedArray, i);
                assertBlockPosition(elementsBlock, elementsPosition, expectedElement);
                elementsPosition++;
            }
        }
    }

    public static BlockBuilder createBlockBuilderWithValues(Slice[][] expectedValues)
    {
        BlockBuilder blockBuilder = new ArrayBlockBuilder(VARCHAR, null, 100, 100);
        for (Slice[] expectedValue : expectedValues) {
            if (expectedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                BlockBuilder elementBlockBuilder = VARCHAR.createBlockBuilder(null, expectedValue.length);
                for (Slice v : expectedValue) {
                    if (v == null) {
                        elementBlockBuilder.appendNull();
                    }
                    else {
                        VARCHAR.writeSlice(elementBlockBuilder, v);
                    }
                }
                blockBuilder.appendStructure(elementBlockBuilder.build());
            }
        }
        return blockBuilder;
    }
}
