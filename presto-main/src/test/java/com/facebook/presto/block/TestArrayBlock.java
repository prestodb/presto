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
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.Random;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

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
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), Ints.asList(0, 1, 3, 4, 7));
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), Ints.asList(2, 3, 5, 6));
        long[][] expectedValuesWithNull = (long[][]) alternatingNullValues(expectedValues);
        BlockBuilder blockBuilderWithNull = createBlockBuilderWithValues(expectedValuesWithNull);
        assertBlock(blockBuilderWithNull, expectedValuesWithNull);
        assertBlock(blockBuilderWithNull.build(), expectedValuesWithNull);
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull.build(), Ints.asList(0, 1, 5, 6, 7, 10, 11, 12, 15));
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull.build(), Ints.asList(2, 3, 4, 9, 13, 14));
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
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), Ints.asList(0, 1, 3, 4, 7));
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), Ints.asList(2, 3, 5, 6));
        Slice[][] expectedValuesWithNull = (Slice[][]) alternatingNullValues(expectedValues);
        BlockBuilder blockBuilderWithNull = createBlockBuilderWithValues(expectedValuesWithNull);
        assertBlock(blockBuilderWithNull, expectedValuesWithNull);
        assertBlock(blockBuilderWithNull.build(), expectedValuesWithNull);
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull.build(), Ints.asList(0, 1, 5, 6, 7, 10, 11, 12, 15));
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull.build(), Ints.asList(2, 3, 4, 9, 13, 14));
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
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), Ints.asList(0, 1, 3, 4, 7));
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), Ints.asList(2, 3, 5, 6));
        long[][][] expectedValuesWithNull = (long[][][]) alternatingNullValues(expectedValues);
        BlockBuilder blockBuilderWithNull = createBlockBuilderWithValues(expectedValuesWithNull);
        assertBlock(blockBuilderWithNull, expectedValuesWithNull);
        assertBlock(blockBuilderWithNull.build(), expectedValuesWithNull);
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull.build(), Ints.asList(0, 1, 5, 6, 7, 10, 11, 12, 15));
        assertBlockFilteredPositions(expectedValuesWithNull, blockBuilderWithNull.build(), Ints.asList(2, 3, 4, 9, 13, 14));
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
}
