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
import com.facebook.presto.common.block.ColumnarMap;
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.block.MapBlockBuilder;
import com.facebook.presto.common.block.MethodHandleUtil;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.block.SingleMapBlockWriter;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.util.Arrays;

import static com.facebook.presto.block.ColumnarTestUtils.alternatingNullValues;
import static com.facebook.presto.block.ColumnarTestUtils.assertBlock;
import static com.facebook.presto.block.ColumnarTestUtils.assertBlockPosition;
import static com.facebook.presto.block.ColumnarTestUtils.createTestDictionaryBlock;
import static com.facebook.presto.block.ColumnarTestUtils.createTestDictionaryExpectedValues;
import static com.facebook.presto.block.ColumnarTestUtils.createTestRleBlock;
import static com.facebook.presto.block.ColumnarTestUtils.createTestRleExpectedValues;
import static com.facebook.presto.common.block.ColumnarMap.toColumnarMap;
import static com.facebook.presto.common.block.MethodHandleUtil.compose;
import static com.facebook.presto.common.block.MethodHandleUtil.nativeValueGetter;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestColumnarMap
{
    private static final int[] MAP_SIZES = new int[] {16, 0, 13, 1, 2, 11, 4, 7};

    @Test
    public void test()
    {
        Slice[][][] expectedValues = new Slice[MAP_SIZES.length][][];
        for (int mapIndex = 0; mapIndex < MAP_SIZES.length; mapIndex++) {
            expectedValues[mapIndex] = new Slice[MAP_SIZES[mapIndex]][];
            for (int entryIndex = 0; entryIndex < MAP_SIZES[mapIndex]; entryIndex++) {
                Slice[] entry = new Slice[2];
                entry[0] = Slices.utf8Slice(String.format("key.%d.%d", mapIndex, entryIndex));
                if (entryIndex % 3 != 1) {
                    entry[1] = Slices.utf8Slice(String.format("value.%d.%d", mapIndex, entryIndex));
                }
                expectedValues[mapIndex][entryIndex] = entry;
            }
        }
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);
        verifyBlock(blockBuilder, expectedValues);
        verifyBlock(blockBuilder.build(), expectedValues);

        Slice[][][] expectedValuesWithNull = alternatingNullValues(expectedValues);
        BlockBuilder blockBuilderWithNull = createBlockBuilderWithValues(expectedValuesWithNull);
        verifyBlock(blockBuilderWithNull, expectedValuesWithNull);
        verifyBlock(blockBuilderWithNull.build(), expectedValuesWithNull);
    }

    @Test
    public void testMapWithNullKeys()
    {
        // Presto Query Engine does not support Map with Null keys.
        // Presto ORC reader and Writer are used as library in some other
        // projects and it requires null keys to be supported in the Map.
        MapBlockBuilder mapBlockBuilder = createMapBuilder(100);
        mapBlockBuilder.appendNull();
        for (int i = 1; i < 100; i++) {
            Slice valueSlice = Slices.utf8Slice(Integer.toString(i));
            SingleMapBlockWriter blockWriter = mapBlockBuilder.beginBlockEntry();
            blockWriter.appendNull(); // Null Key
            VARCHAR.writeSlice(blockWriter, valueSlice);
            for (int j = 1; j < i; j++) {
                Slice keySlice = Slices.utf8Slice(Integer.toString(j));
                VARCHAR.writeSlice(blockWriter, keySlice);
                VARCHAR.writeSlice(blockWriter, valueSlice);
            }
            mapBlockBuilder.closeEntry();
        }

        ColumnarMap columnarMap = toColumnarMap(mapBlockBuilder.build());
        assertEquals(columnarMap.getPositionCount(), 100);
        assertTrue(columnarMap.isNull(0));
        for (int i = 1; i < 100; i++) {
            assertFalse(columnarMap.isNull(i));
            Slice valueSlice = Slices.utf8Slice(Integer.toString(i));
            int offset = columnarMap.getOffset(i);
            assertTrue(columnarMap.getKeysBlock().isNull(offset));
            verifySlice(columnarMap.getValuesBlock(), offset, valueSlice);
            for (int j = 1; j < i; j++) {
                Slice keySlice = Slices.utf8Slice(Integer.toString(j));
                verifySlice(columnarMap.getKeysBlock(), offset + j, keySlice);
                verifySlice(columnarMap.getValuesBlock(), offset + j, valueSlice);
            }
        }
    }

    private void verifySlice(Block block, int position, Slice expectedSlice)
    {
        int sliceLength = block.getSliceLength(position);
        Slice actualSlice = block.getSlice(position, 0, sliceLength);
        assertEquals(actualSlice, expectedSlice);
    }

    private static void verifyBlock(Block block, Slice[][][] expectedValues)
    {
        assertBlock(block, expectedValues);

        assertColumnarMap(block, expectedValues);
        assertDictionaryBlock(block, expectedValues);
        assertRunLengthEncodedBlock(block, expectedValues);

        int offset = 1;
        int length = expectedValues.length - 2;
        Block blockRegion = block.getRegion(offset, length);
        Slice[][][] expectedValuesRegion = Arrays.copyOfRange(expectedValues, offset, offset + length);

        assertBlock(blockRegion, expectedValuesRegion);

        assertColumnarMap(blockRegion, expectedValuesRegion);
        assertDictionaryBlock(blockRegion, expectedValuesRegion);
        assertRunLengthEncodedBlock(blockRegion, expectedValuesRegion);
    }

    private static void assertDictionaryBlock(Block block, Slice[][][] expectedValues)
    {
        DictionaryBlock dictionaryBlock = createTestDictionaryBlock(block);
        Slice[][][] expectedDictionaryValues = createTestDictionaryExpectedValues(expectedValues);

        assertBlock(dictionaryBlock, expectedDictionaryValues);
        assertColumnarMap(dictionaryBlock, expectedDictionaryValues);
        assertRunLengthEncodedBlock(dictionaryBlock, expectedDictionaryValues);
    }

    private static void assertRunLengthEncodedBlock(Block block, Slice[][][] expectedValues)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            RunLengthEncodedBlock runLengthEncodedBlock = createTestRleBlock(block, position);
            Slice[][][] expectedDictionaryValues = createTestRleExpectedValues(expectedValues, position);

            assertBlock(runLengthEncodedBlock, expectedDictionaryValues);
            assertColumnarMap(runLengthEncodedBlock, expectedDictionaryValues);
        }
    }

    private static void assertColumnarMap(Block block, Slice[][][] expectedValues)
    {
        ColumnarMap columnarMap = toColumnarMap(block);
        assertEquals(columnarMap.getPositionCount(), expectedValues.length);

        Block keysBlock = columnarMap.getKeysBlock();
        Block valuesBlock = columnarMap.getValuesBlock();
        int elementsPosition = 0;

        for (int position = 0; position < expectedValues.length; position++) {
            Slice[][] expectedMap = expectedValues[position];
            assertEquals(columnarMap.isNull(position), expectedMap == null);
            assertEquals(columnarMap.getOffset(position), elementsPosition);

            if (expectedMap == null) {
                assertEquals(columnarMap.getEntryCount(position), 0);
                continue;
            }

            assertEquals(columnarMap.getEntryCount(position), expectedMap.length);
            for (int i = 0; i < columnarMap.getEntryCount(position); i++) {
                Slice[] expectedEntry = expectedMap[i];

                Slice expectedKey = expectedEntry[0];
                assertBlockPosition(keysBlock, elementsPosition, expectedKey);

                Slice expectedValue = expectedEntry[1];
                assertBlockPosition(valuesBlock, elementsPosition, expectedValue);

                elementsPosition++;
            }
        }
    }

    public static BlockBuilder createBlockBuilderWithValues(Slice[][][] expectedValues)
    {
        BlockBuilder blockBuilder = createMapBuilder(100);
        for (Slice[][] expectedMap : expectedValues) {
            if (expectedMap == null) {
                blockBuilder.appendNull();
            }
            else {
                BlockBuilder entryBuilder = blockBuilder.beginBlockEntry();
                VARCHAR.createBlockBuilder(null, expectedMap.length);
                for (Slice[] entry : expectedMap) {
                    Slice key = entry[0];
                    assertNotNull(key);
                    VARCHAR.writeSlice(entryBuilder, key);

                    Slice value = entry[1];
                    if (value == null) {
                        entryBuilder.appendNull();
                    }
                    else {
                        VARCHAR.writeSlice(entryBuilder, value);
                    }
                }
                blockBuilder.closeEntry();
            }
        }
        return blockBuilder;
    }

    private static MapBlockBuilder createMapBuilder(int expectedEntries)
    {
        MethodHandle varcharNativeEquals = MethodHandleUtil.methodHandle(Slice.class, "equals", Object.class).asType(MethodType.methodType(boolean.class, Slice.class, Slice.class));
        MethodHandle varcharBlockEquals = compose(varcharNativeEquals, nativeValueGetter(VARCHAR), nativeValueGetter(VARCHAR));

        return new MapBlockBuilder(
                VARCHAR,
                VARCHAR,
                varcharBlockEquals,
                MethodHandleUtil.methodHandle(TestColumnarMap.class, "blockVarcharHashCode", Block.class, int.class),
                null,
                expectedEntries);
    }

    @SuppressWarnings("unused")
    public static long blockVarcharHashCode(Block block, int position)
    {
        return block.hash(position, 0, block.getSliceLength(position));
    }
}
