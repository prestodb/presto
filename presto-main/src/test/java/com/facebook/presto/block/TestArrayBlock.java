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

import com.facebook.presto.common.block.ArrayBlockBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.ByteArrayBlock;
import com.facebook.presto.common.type.ArrayType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.Random;
import java.util.stream.IntStream;

import static com.facebook.presto.block.BlockAssertions.assertBlockEquals;
import static com.facebook.presto.block.BlockAssertions.createLongDictionaryBlock;
import static com.facebook.presto.block.BlockAssertions.createRLEBlock;
import static com.facebook.presto.block.BlockAssertions.createRandomDictionaryBlock;
import static com.facebook.presto.block.BlockAssertions.createRandomLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createRleBlockWithRandomValue;
import static com.facebook.presto.common.block.ArrayBlock.fromElementBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestArrayBlock
        extends AbstractTestBlock
{
    private static final int[] ARRAY_SIZES = new int[] {16, 0, 13, 1, 2, 11, 4, 7};

    @Test
    public void testWithFixedWidthBlock()
    {
        long[][] expectedValues = new long[ARRAY_SIZES.length][];
        Random rand = new Random(47);
        for (int i = 0; i < ARRAY_SIZES.length; i++) {
            expectedValues[i] = rand.longs(ARRAY_SIZES[i]).toArray();
        }

        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);
        assertBlock(blockBuilder, () -> blockBuilder.newBlockBuilderLike(null), expectedValues);
        assertBlock(blockBuilder.build(), () -> blockBuilder.newBlockBuilderLike(null), expectedValues);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), () -> blockBuilder.newBlockBuilderLike(null), 0, 1, 3, 4, 7);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), () -> blockBuilder.newBlockBuilderLike(null), 2, 3, 5, 6);

        long[][] expectedValuesWithNull = alternatingNullValues(expectedValues);
        BlockBuilder blockBuilderWithNull = createBlockBuilderWithValues(expectedValuesWithNull);
        assertBlock(blockBuilderWithNull, () -> blockBuilder.newBlockBuilderLike(null), expectedValuesWithNull);
        assertBlock(blockBuilderWithNull.build(), () -> blockBuilder.newBlockBuilderLike(null), expectedValuesWithNull);
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull.build(), () -> blockBuilder.newBlockBuilderLike(null), 0, 1, 5, 6, 7, 10, 11, 12, 15);
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull.build(), () -> blockBuilder.newBlockBuilderLike(null), 2, 3, 4, 9, 13, 14);
    }

    @Test
    public void testWithVariableWidthBlock()
    {
        Slice[][] expectedValues = new Slice[ARRAY_SIZES.length][];
        for (int i = 0; i < ARRAY_SIZES.length; i++) {
            expectedValues[i] = new Slice[ARRAY_SIZES[i]];
            for (int j = 0; j < ARRAY_SIZES[i]; j++) {
                expectedValues[i][j] = Slices.utf8Slice(String.format("%d.%d", i, j));
            }
        }

        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);

        assertBlock(blockBuilder, () -> blockBuilder.newBlockBuilderLike(null), expectedValues);
        assertBlock(blockBuilder.build(), () -> blockBuilder.newBlockBuilderLike(null), expectedValues);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), () -> blockBuilder.newBlockBuilderLike(null), 0, 1, 3, 4, 7);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), () -> blockBuilder.newBlockBuilderLike(null), 2, 3, 5, 6);

        Slice[][] expectedValuesWithNull = alternatingNullValues(expectedValues);
        BlockBuilder blockBuilderWithNull = createBlockBuilderWithValues(expectedValuesWithNull);
        assertBlock(blockBuilderWithNull, () -> blockBuilder.newBlockBuilderLike(null), expectedValuesWithNull);
        assertBlock(blockBuilderWithNull.build(), () -> blockBuilder.newBlockBuilderLike(null), expectedValuesWithNull);
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull.build(), () -> blockBuilder.newBlockBuilderLike(null), 0, 1, 5, 6, 7, 10, 11, 12, 15);
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull.build(), () -> blockBuilder.newBlockBuilderLike(null), 2, 3, 4, 9, 13, 14);
    }

    @Test
    public void testWithArrayBlock()
    {
        long[][][] expectedValues = createExpectedValues();

        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);

        assertBlock(blockBuilder, () -> blockBuilder.newBlockBuilderLike(null), expectedValues);
        assertBlock(blockBuilder.build(), () -> blockBuilder.newBlockBuilderLike(null), expectedValues);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), () -> blockBuilder.newBlockBuilderLike(null), 0, 1, 3, 4, 7);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), () -> blockBuilder.newBlockBuilderLike(null), 2, 3, 5, 6);

        long[][][] expectedValuesWithNull = alternatingNullValues(expectedValues);
        BlockBuilder blockBuilderWithNull = createBlockBuilderWithValues(expectedValuesWithNull);
        assertBlock(blockBuilderWithNull, () -> blockBuilder.newBlockBuilderLike(null), expectedValuesWithNull);
        assertBlock(blockBuilderWithNull.build(), () -> blockBuilder.newBlockBuilderLike(null), expectedValuesWithNull);
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull.build(), () -> blockBuilder.newBlockBuilderLike(null), 0, 1, 5, 6, 7, 10, 11, 12, 15);
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull.build(), () -> blockBuilder.newBlockBuilderLike(null), 2, 3, 4, 9, 13, 14);
    }

    @Test
    public void testSingleValueBlock()
    {
        // 1 entry array.
        long[][] values = createTestArray(50);
        BlockBuilder arrayBlockBuilder = createBlockBuilderWithValues(values);
        Block arrayBlock = arrayBlockBuilder.build();

        assertSame(arrayBlock, arrayBlock.getSingleValueBlock(0));
        assertNotSame(arrayBlockBuilder, arrayBlockBuilder.getSingleValueBlock(0));

        // 2 entries array.
        values = createTestArray(50, 50);
        arrayBlockBuilder = createBlockBuilderWithValues(values);
        arrayBlock = arrayBlockBuilder.build();
        Block firstElement = arrayBlock.getRegion(0, 1);
        assertNotSame(firstElement, firstElement.getSingleValueBlock(0));

        Block secondElementCopy = arrayBlock.copyRegion(1, 1);
        assertSame(secondElementCopy, secondElementCopy.getSingleValueBlock(0));

        // Test with null elements.
        values = new long[][] {null};
        arrayBlockBuilder = new ArrayBlockBuilder(BIGINT, null, 1, 100);
        writeValues(values, arrayBlockBuilder);
        arrayBlock = arrayBlockBuilder.build();
        assertSame(arrayBlock, arrayBlock.getSingleValueBlock(0));
        assertNotSame(arrayBlock, arrayBlockBuilder.getSingleValueBlock(0));

        // Test with 2 null elements.
        values = new long[][] {null, null};
        arrayBlockBuilder = createBlockBuilderWithValues(values);
        arrayBlock = arrayBlockBuilder.build();
        assertNotSame(arrayBlock, arrayBlock.getSingleValueBlock(0));
    }

    private static long[][] createTestArray(int... entryCounts)
    {
        long[][] result = new long[entryCounts.length][];
        for (int rowNumber = 0; rowNumber < entryCounts.length; rowNumber++) {
            int entryCount = entryCounts[rowNumber];
            long[] array = new long[entryCount];
            for (int entryNumber = 0; entryNumber < entryCount; entryNumber++) {
                array[entryNumber] = entryNumber;
            }
            result[rowNumber] = array;
        }
        return result;
    }

    private static long[][][] createExpectedValues()
    {
        long[][][] expectedValues = new long[ARRAY_SIZES.length][][];
        for (int i = 0; i < ARRAY_SIZES.length; i++) {
            expectedValues[i] = new long[ARRAY_SIZES[i]][];
            for (int j = 1; j < ARRAY_SIZES[i]; j++) {
                if ((i + j) % 5 == 0) {
                    expectedValues[i][j] = null;
                }
                else {
                    expectedValues[i][j] = new long[] {i, j, i + j};
                }
            }
        }
        return expectedValues;
    }

    @Test
    public void testLazyBlockBuilderInitialization()
    {
        long[][] expectedValues = new long[ARRAY_SIZES.length][];
        Random rand = new Random(47);
        for (int i = 0; i < ARRAY_SIZES.length; i++) {
            expectedValues[i] = rand.longs(ARRAY_SIZES[i]).toArray();
        }
        BlockBuilder emptyBlockBuilder = new ArrayBlockBuilder(BIGINT, null, 0, 0);

        BlockBuilder blockBuilder = new ArrayBlockBuilder(BIGINT, null, 100, 100);
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
        long[][][] expectedValues = alternatingNullValues(createExpectedValues());
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);
        Block block = blockBuilder.build();
        assertEquals(block.getPositionCount(), expectedValues.length);
        for (int i = 0; i < block.getPositionCount(); i++) {
            int expectedSize = getExpectedEstimatedDataSize(expectedValues[i]);
            assertEquals(blockBuilder.getEstimatedDataSizeForStats(i), expectedSize);
            assertEquals(block.getEstimatedDataSizeForStats(i), expectedSize);
        }
    }

    @Test
    public void testLogicalSizeInBytes()
    {
        int positionCount = 100;
        int[] offsets = IntStream.rangeClosed(0, positionCount).toArray();
        boolean[] nulls = new boolean[positionCount];

        // Array(LongArrayBlock)
        Block arrayOfLong = fromElementBlock(positionCount, Optional.of(nulls), offsets, createRandomLongsBlock(positionCount, 0));
        assertEquals(arrayOfLong.getLogicalSizeInBytes(), 1400);

        // Array(RLE(LongArrayBlock))
        Block arrayOfRleOfLong = fromElementBlock(positionCount, Optional.of(nulls), offsets, createRLEBlock(1, 100));
        assertEquals(arrayOfRleOfLong.getLogicalSizeInBytes(), 1400);

        // Array(RLE(Array(LongArrayBlock)))
        Block arrayOfRleOfArrayOfLong = fromElementBlock(positionCount, Optional.of(nulls), offsets, createRleBlockWithRandomValue(arrayOfLong, 100));
        assertEquals(arrayOfRleOfArrayOfLong.getLogicalSizeInBytes(), 1900);

        // Array(Dictionary(LongArrayBlock))
        Block arrayOfDictionaryOfLong = fromElementBlock(positionCount, Optional.of(nulls), offsets, createLongDictionaryBlock(0, 100));
        assertEquals(arrayOfDictionaryOfLong.getLogicalSizeInBytes(), 1400);

        // Array(Array(Dictionary(LongArrayBlock)))
        Block arrayOfArrayOfDictionaryOfLong = fromElementBlock(positionCount, Optional.of(nulls), offsets, arrayOfDictionaryOfLong);
        assertEquals(arrayOfArrayOfDictionaryOfLong.getLogicalSizeInBytes(), 1900);

        // Array(Dictionary(Array(LongArrayBlock)))
        Block arrayOfDictionaryOfArrayOfLong = fromElementBlock(positionCount, Optional.of(nulls), offsets, createRandomDictionaryBlock(arrayOfDictionaryOfLong, 100, true));
        assertEquals(arrayOfDictionaryOfArrayOfLong.getLogicalSizeInBytes(), 1900);
    }

    @Test
    public void testCopyEmptyRawElementPositions()
    {
        int positionCount = 100;
        int[] offsets = new int[positionCount + 1];
        // Only the first array is non-empty
        Arrays.fill(offsets, 1, offsets.length, 1);

        // Array(LongArrayBlock(1)) - single element
        Block elements = fromElementBlock(positionCount, Optional.empty(), offsets, createRandomLongsBlock(1, 0));
        // Shift the array offsets index
        Block offsetsShifted = elements.getRegion(50, 50);
        assertEquals(offsetsShifted.getOffsetBase(), 50);
        // Copy the first, middle, and last elements via copyPositions
        Block copiedArray = offsetsShifted.copyPositions(new int[]{0, 25, 49}, 0, 3);
        assertEquals(copiedArray.getPositionCount(), 3);

        BlockBuilder blockBuilder = new ArrayBlockBuilder(BIGINT, null, 0, 0);
        long[][] expectedValues = new long[3][];
        for (int i = 0; i < expectedValues.length; i++) {
            expectedValues[i] = new long[0]; // empty, but not null
        }
        writeValues(expectedValues, blockBuilder);
        assertBlockEquals(new ArrayType(BIGINT), copiedArray, blockBuilder.build());
    }

    private static int getExpectedEstimatedDataSize(long[][] values)
    {
        if (values == null) {
            return 0;
        }
        int size = 0;
        for (long[] value : values) {
            if (value != null) {
                size += Long.BYTES * value.length;
            }
        }
        return size;
    }

    @Test
    public void testCompactBlock()
    {
        Block emptyValueBlock = new ByteArrayBlock(0, Optional.empty(), new byte[0]);
        Block compactValueBlock = new ByteArrayBlock(16, Optional.empty(), createExpectedValue(16).getBytes());
        Block inCompactValueBlock = new ByteArrayBlock(16, Optional.empty(), createExpectedValue(17).getBytes());
        int[] offsets = {0, 1, 1, 2, 4, 8, 16};
        boolean[] valueIsNull = {false, true, false, false, false, false};

        testCompactBlock(fromElementBlock(0, Optional.empty(), new int[1], emptyValueBlock));
        testCompactBlock(fromElementBlock(valueIsNull.length, Optional.of(valueIsNull), offsets, compactValueBlock));
        testIncompactBlock(fromElementBlock(valueIsNull.length - 1, Optional.of(valueIsNull), offsets, compactValueBlock));
        // underlying value block is not compact
        testIncompactBlock(fromElementBlock(valueIsNull.length, Optional.of(valueIsNull), offsets, inCompactValueBlock));
    }

    private static BlockBuilder createBlockBuilderWithValues(long[][][] expectedValues)
    {
        BlockBuilder blockBuilder = new ArrayBlockBuilder(new ArrayBlockBuilder(BIGINT, null, 100, 100), null, 100);
        for (long[][] expectedValue : expectedValues) {
            if (expectedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                BlockBuilder intermediateBlockBuilder = new ArrayBlockBuilder(BIGINT, null, 100, 100);
                for (int j = 0; j < expectedValue.length; j++) {
                    if (expectedValue[j] == null) {
                        intermediateBlockBuilder.appendNull();
                    }
                    else {
                        BlockBuilder innerMostBlockBuilder = BIGINT.createBlockBuilder(null, expectedValue.length);
                        for (long v : expectedValue[j]) {
                            BIGINT.writeLong(innerMostBlockBuilder, v);
                        }
                        intermediateBlockBuilder.appendStructure(innerMostBlockBuilder.build());
                    }
                }
                blockBuilder.appendStructure(intermediateBlockBuilder.build());
            }
        }
        return blockBuilder;
    }

    private static BlockBuilder createBlockBuilderWithValues(long[][] expectedValues)
    {
        BlockBuilder blockBuilder = new ArrayBlockBuilder(BIGINT, null, expectedValues.length, 100);
        return writeValues(expectedValues, blockBuilder);
    }

    private static BlockBuilder writeValues(long[][] expectedValues, BlockBuilder blockBuilder)
    {
        for (long[] expectedValue : expectedValues) {
            if (expectedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                BlockBuilder elementBlockBuilder = BIGINT.createBlockBuilder(null, expectedValue.length);
                for (long v : expectedValue) {
                    BIGINT.writeLong(elementBlockBuilder, v);
                }
                blockBuilder.appendStructure(elementBlockBuilder);
            }
        }
        return blockBuilder;
    }

    private static BlockBuilder createBlockBuilderWithValues(Slice[][] expectedValues)
    {
        BlockBuilder blockBuilder = new ArrayBlockBuilder(VARCHAR, null, 100, 100);
        for (Slice[] expectedValue : expectedValues) {
            if (expectedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                BlockBuilder elementBlockBuilder = VARCHAR.createBlockBuilder(null, expectedValue.length);
                for (Slice v : expectedValue) {
                    VARCHAR.writeSlice(elementBlockBuilder, v);
                }
                blockBuilder.appendStructure(elementBlockBuilder.build());
            }
        }
        return blockBuilder;
    }
}
