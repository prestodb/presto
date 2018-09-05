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
import com.facebook.presto.spi.block.ByteArrayBlock;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Random;

import static com.facebook.presto.spi.block.ArrayBlock.fromElementBlock;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
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

        long[][] expectedValuesWithNull = (long[][]) alternatingNullValues(expectedValues);
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

        Slice[][] expectedValuesWithNull = (Slice[][]) alternatingNullValues(expectedValues);
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

        long[][][] expectedValuesWithNull = (long[][][]) alternatingNullValues(expectedValues);
        BlockBuilder blockBuilderWithNull = createBlockBuilderWithValues(expectedValuesWithNull);
        assertBlock(blockBuilderWithNull, () -> blockBuilder.newBlockBuilderLike(null), expectedValuesWithNull);
        assertBlock(blockBuilderWithNull.build(), () -> blockBuilder.newBlockBuilderLike(null), expectedValuesWithNull);
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull.build(), () -> blockBuilder.newBlockBuilderLike(null), 0, 1, 5, 6, 7, 10, 11, 12, 15);
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull.build(), () -> blockBuilder.newBlockBuilderLike(null), 2, 3, 4, 9, 13, 14);
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
        long[][][] expectedValues = (long[][][]) alternatingNullValues(createExpectedValues());
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);
        Block block = blockBuilder.build();
        assertEquals(block.getPositionCount(), expectedValues.length);
        for (int i = 0; i < block.getPositionCount(); i++) {
            int expectedSize = getExpectedEstimatedDataSize(expectedValues[i]);
            assertEquals(blockBuilder.getEstimatedDataSizeForStats(i), expectedSize);
            assertEquals(block.getEstimatedDataSizeForStats(i), expectedSize);
        }
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
        BlockBuilder blockBuilder = new ArrayBlockBuilder(BIGINT, null, 100, 100);
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
