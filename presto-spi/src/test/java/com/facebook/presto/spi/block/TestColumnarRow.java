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
package com.facebook.presto.spi.block;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.lang.reflect.Array;
import java.util.Collections;

import static com.facebook.presto.spi.block.ColumnarRow.toColumnarRow;
import static com.facebook.presto.spi.block.ColumnarTestUtils.alternatingNullValues;
import static com.facebook.presto.spi.block.ColumnarTestUtils.assertBlock;
import static com.facebook.presto.spi.block.ColumnarTestUtils.assertBlockPosition;
import static com.facebook.presto.spi.block.ColumnarTestUtils.createTestDictionaryBlock;
import static com.facebook.presto.spi.block.ColumnarTestUtils.createTestDictionaryExpectedValues;
import static com.facebook.presto.spi.block.ColumnarTestUtils.createTestRleBlock;
import static com.facebook.presto.spi.block.ColumnarTestUtils.createTestRleExpectedValues;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestColumnarRow
{
    private static final int POSITION_COUNT = 5;
    private static final int FIELD_COUNT = 5;

    @Test
    public void test()
            throws Exception
    {
        Slice[][] expectedValues = new Slice[POSITION_COUNT][];
        for (int i = 0; i < POSITION_COUNT; i++) {
            expectedValues[i] = new Slice[FIELD_COUNT];
            for (int j = 0; j < FIELD_COUNT; j++) {
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

        assertColumnarRow(block, expectedValues);
        assertDictionaryBlock(block, expectedValues);
        assertRunLengthEncodedBlock(block, expectedValues);
    }

    private static <T> void assertDictionaryBlock(Block block, T[] expectedValues)
    {
        DictionaryBlock dictionaryBlock = createTestDictionaryBlock(block);
        T[] expectedDictionaryValues = createTestDictionaryExpectedValues(expectedValues);

        assertBlock(dictionaryBlock, expectedDictionaryValues);
        assertColumnarRow(dictionaryBlock, expectedDictionaryValues);
        assertRunLengthEncodedBlock(dictionaryBlock, expectedDictionaryValues);
    }

    private static <T> void assertRunLengthEncodedBlock(Block block, T[] expectedValues)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            RunLengthEncodedBlock runLengthEncodedBlock = createTestRleBlock(block, position);
            T[] expectedDictionaryValues = createTestRleExpectedValues(expectedValues, position);

            assertBlock(runLengthEncodedBlock, expectedDictionaryValues);
            assertColumnarRow(runLengthEncodedBlock, expectedDictionaryValues);
        }
    }

    private static <T> void assertColumnarRow(Block block, T[] expectedValues)
    {
        ColumnarRow columnarRow = toColumnarRow(block);
        assertEquals(columnarRow.getPositionCount(), expectedValues.length);

        for (int fieldId = 0; fieldId < FIELD_COUNT; fieldId++) {
            Block fieldBlock = columnarRow.getField(fieldId);
            int elementsPosition = 0;
            for (int position = 0; position < expectedValues.length; position++) {
                T expectedRow = expectedValues[position];
                assertEquals(columnarRow.isNull(position), expectedRow == null);
                if (expectedRow == null) {
                    continue;
                }

                Object expectedElement = Array.get(expectedRow, fieldId);
                assertBlockPosition(fieldBlock, elementsPosition, expectedElement);
                elementsPosition++;
            }
        }
    }

    public static BlockBuilder createBlockBuilderWithValues(Slice[][] expectedValues)
    {
        BlockBuilder blockBuilder = createBlockBuilder(new BlockBuilderStatus(), 100, 100);
        for (Slice[] expectedValue : expectedValues) {
            if (expectedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                BlockBuilder elementBlockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), expectedValue.length);
                for (Slice v : expectedValue) {
                    if (v == null) {
                        elementBlockBuilder.appendNull();
                    }
                    else {
                        VARCHAR.writeSlice(elementBlockBuilder, v);
                    }
                }
                blockBuilder.writeObject(elementBlockBuilder.build()).closeEntry();
            }
        }
        return blockBuilder;
    }

    private static BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        return new RowBlockBuilder(Collections.nCopies(FIELD_COUNT, VARCHAR), blockBuilderStatus, expectedEntries);
    }
}
