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

import com.facebook.presto.spi.block.ArrayBlock;
import com.facebook.presto.spi.block.ArrayBlockBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.ByteArrayBlock;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Random;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
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
        assertBlock(blockBuilder, expectedValues);
        assertBlock(blockBuilder.build(), expectedValues);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), 0, 1, 3, 4, 7);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), 2, 3, 5, 6);
        long[][] expectedValuesWithNull = (long[][]) alternatingNullValues(expectedValues);
        BlockBuilder blockBuilderWithNull = createBlockBuilderWithValues(expectedValuesWithNull);
        assertBlock(blockBuilderWithNull, expectedValuesWithNull);
        assertBlock(blockBuilderWithNull.build(), expectedValuesWithNull);
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull.build(), 0, 1, 5, 6, 7, 10, 11, 12, 15);
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull.build(), 2, 3, 4, 9, 13, 14);
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
        assertBlock(blockBuilder, expectedValues);
        assertBlock(blockBuilder.build(), expectedValues);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), 0, 1, 3, 4, 7);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), 2, 3, 5, 6);
        Slice[][] expectedValuesWithNull = (Slice[][]) alternatingNullValues(expectedValues);
        BlockBuilder blockBuilderWithNull = createBlockBuilderWithValues(expectedValuesWithNull);
        assertBlock(blockBuilderWithNull, expectedValuesWithNull);
        assertBlock(blockBuilderWithNull.build(), expectedValuesWithNull);
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull.build(), 0, 1, 5, 6, 7, 10, 11, 12, 15);
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull.build(), 2, 3, 4, 9, 13, 14);
    }

    @Test
    public void testWithArrayBlock()
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
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);
        assertBlock(blockBuilder, expectedValues);
        assertBlock(blockBuilder.build(), expectedValues);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), 0, 1, 3, 4, 7);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), 2, 3, 5, 6);
        long[][][] expectedValuesWithNull = (long[][][]) alternatingNullValues(expectedValues);
        BlockBuilder blockBuilderWithNull = createBlockBuilderWithValues(expectedValuesWithNull);
        assertBlock(blockBuilderWithNull, expectedValuesWithNull);
        assertBlock(blockBuilderWithNull.build(), expectedValuesWithNull);
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull.build(), 0, 1, 5, 6, 7, 10, 11, 12, 15);
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull.build(), 2, 3, 4, 9, 13, 14);
    }

    @Test
    public void testLazyBlockBuilderInitialization()
            throws Exception
    {
        long[][] expectedValues = new long[ARRAY_SIZES.length][];
        Random rand = new Random(47);
        for (int i = 0; i < ARRAY_SIZES.length; i++) {
            expectedValues[i] = rand.longs(ARRAY_SIZES[i]).toArray();
        }
        BlockBuilder emptyBlockBuilder = new ArrayBlockBuilder(BIGINT, new BlockBuilderStatus(), 0, 0);

        BlockBuilder blockBuilder = new ArrayBlockBuilder(BIGINT, new BlockBuilderStatus(), 100, 100);
        assertEquals(blockBuilder.getSizeInBytes(), emptyBlockBuilder.getSizeInBytes());
        assertEquals(blockBuilder.getRetainedSizeInBytes(), emptyBlockBuilder.getRetainedSizeInBytes());

        writeValues(expectedValues, blockBuilder);
        assertTrue(blockBuilder.getSizeInBytes() > emptyBlockBuilder.getSizeInBytes());
        assertTrue(blockBuilder.getRetainedSizeInBytes() > emptyBlockBuilder.getRetainedSizeInBytes());

        blockBuilder = blockBuilder.newBlockBuilderLike(new BlockBuilderStatus());
        assertEquals(blockBuilder.getSizeInBytes(), emptyBlockBuilder.getSizeInBytes());
        assertEquals(blockBuilder.getRetainedSizeInBytes(), emptyBlockBuilder.getRetainedSizeInBytes());
    }

    public void testCompactBlock()
    {
        // Test Constructor
        Block emptyValueBlock = new ByteArrayBlock(0, new boolean[0], new byte[0]);
        Block valueBlock = new ByteArrayBlock(16, new boolean[16], createExpectedValue(16).getBytes());
        Block largerBlock = new ByteArrayBlock(20, new boolean[20], createExpectedValue(20).getBytes());
        int[] offsets = {0, 1, 1, 2, 4, 8, 16};
        boolean[] valueIsNull = {false, true, false, false, false, false};

        assertCompact(new ArrayBlock(0, new boolean[0], new int[1], emptyValueBlock));
        assertCompact(new ArrayBlock(valueIsNull.length, valueIsNull, offsets, valueBlock));

        assertNotCompact(new ArrayBlock(valueIsNull.length - 1, valueIsNull, offsets, valueBlock));
        assertNotCompact(new ArrayBlock(valueIsNull.length, valueIsNull, offsets, largerBlock));
        assertNotCompact(new ArrayBlock(valueIsNull.length, valueIsNull, offsets, largerBlock.getRegion(0, 16)));

        // Test getRegion and copyRegion
        Block block = new ArrayBlock(valueIsNull.length, valueIsNull, offsets, valueBlock);
        assertGetRegionCompactness(block);
        assertCopyRegionCompactness(block);
        assertCopyRegionCompactness(new ArrayBlock(valueIsNull.length, valueIsNull, offsets, largerBlock));
        assertCopyRegionCompactness(new ArrayBlock(valueIsNull.length, valueIsNull, offsets, largerBlock.getRegion(0, 16)));

        // Test BlockBuilder
        BlockBuilder emptyBlockBuilder = new ArrayBlockBuilder(TINYINT, new BlockBuilderStatus(), 0);
        assertNotCompact(emptyBlockBuilder);
        assertCompact(emptyBlockBuilder.build());

        byte[][] arrays = new byte[17][];
        for (int i = 0; i < 17; i++) {
            arrays[i] = new byte[i];
            for (int j = 0; j < i; j++) {
                arrays[i][j] = (byte) j;
            }
        }

        BlockBuilder nonFullBlockBuilder = createBlockBuilderWithValues(arrays, false);
        assertNotCompact(nonFullBlockBuilder);
        assertNotCompact(nonFullBlockBuilder.build());
        assertCopyRegionCompactness(nonFullBlockBuilder);

        BlockBuilder fullBlockBuilder = createBlockBuilderWithValues(arrays, true);
        assertNotCompact(fullBlockBuilder);
        assertCompact(fullBlockBuilder.build());
        assertCopyRegionCompactness(fullBlockBuilder);

        // NOTE: ArrayBlockBuilder will return itself if getRegion() is called to slice the whole block.
        // assertCompact(fullBlockBuilder.getRegion(0, fullBlockBuilder.getPositionCount()));
        assertNotCompact(fullBlockBuilder.getRegion(0, fullBlockBuilder.getPositionCount() - 1));
        assertNotCompact(fullBlockBuilder.getRegion(1, fullBlockBuilder.getPositionCount() - 1));
    }

    private static BlockBuilder createBlockBuilderWithValues(long[][][] expectedValues)
    {
        BlockBuilder blockBuilder = new ArrayBlockBuilder(new ArrayBlockBuilder(BIGINT, new BlockBuilderStatus(), 100, 100), new BlockBuilderStatus(), 100);
        for (long[][] expectedValue : expectedValues) {
            if (expectedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                BlockBuilder intermediateBlockBuilder = new ArrayBlockBuilder(BIGINT, new BlockBuilderStatus(), 100, 100);
                for (int j = 0; j < expectedValue.length; j++) {
                    if (expectedValue[j] == null) {
                        intermediateBlockBuilder.appendNull();
                    }
                    else {
                        BlockBuilder innerMostBlockBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), expectedValue.length);
                        for (long v : expectedValue[j]) {
                            BIGINT.writeLong(innerMostBlockBuilder, v);
                        }
                        intermediateBlockBuilder.writeObject(innerMostBlockBuilder.build()).closeEntry();
                    }
                }
                blockBuilder.writeObject(intermediateBlockBuilder.build()).closeEntry();
            }
        }
        return blockBuilder;
    }

    private static BlockBuilder createBlockBuilderWithValues(long[][] expectedValues)
    {
        BlockBuilder blockBuilder = new ArrayBlockBuilder(BIGINT, new BlockBuilderStatus(), 100, 100);
        return writeValues(expectedValues, blockBuilder);
    }

    private static BlockBuilder writeValues(long[][] expectedValues, BlockBuilder blockBuilder)
    {
        for (long[] expectedValue : expectedValues) {
            if (expectedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                BlockBuilder elementBlockBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), expectedValue.length);
                for (long v : expectedValue) {
                    BIGINT.writeLong(elementBlockBuilder, v);
                }
                blockBuilder.writeObject(elementBlockBuilder).closeEntry();
            }
        }
        return blockBuilder;
    }

    private static BlockBuilder createBlockBuilderWithValues(Slice[][] expectedValues)
    {
        BlockBuilder blockBuilder = new ArrayBlockBuilder(VARCHAR, new BlockBuilderStatus(), 100, 100);
        for (Slice[] expectedValue : expectedValues) {
            if (expectedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                BlockBuilder elementBlockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), expectedValue.length);
                for (Slice v : expectedValue) {
                    VARCHAR.writeSlice(elementBlockBuilder, v);
                }
                blockBuilder.writeObject(elementBlockBuilder.build()).closeEntry();
            }
        }
        return blockBuilder;
    }

    private static BlockBuilder createBlockBuilderWithValues(byte[][] arrays, boolean useAccurateCapacityEstimation)
    {
        int totalBytes = Arrays.stream(arrays).mapToInt(x -> x.length).sum();
        BlockBuilderStatus blockBuilderStatus = new BlockBuilderStatus();
        BlockBuilder blockBuilder = new ArrayBlockBuilder(
                TINYINT.createBlockBuilder(blockBuilderStatus, useAccurateCapacityEstimation ? totalBytes : totalBytes * 2),
                blockBuilderStatus,
                useAccurateCapacityEstimation ? arrays.length : arrays.length * 2);

        for (byte[] array : arrays) {
            if (array == null) {
                blockBuilder.appendNull();
            }
            else {
                BlockBuilder elementBlockBuilder = TINYINT.createBlockBuilder(new BlockBuilderStatus(), array.length);
                for (byte v : array) {
                    TINYINT.writeLong(elementBlockBuilder, v);
                }
                blockBuilder.writeObject(elementBlockBuilder.build()).closeEntry();
            }
        }
        return blockBuilder;
    }
}
