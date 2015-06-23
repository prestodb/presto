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
        assertArrayValues(expectedValues);
        assertArrayValues((long[][]) alternatingNullValues(expectedValues));
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
        assertArrayValues(expectedValues);
        assertArrayValues((Slice[][]) alternatingNullValues(expectedValues));
    }

    @Test
    public void testWithArrayBlock()
    {
        long[][][] expectedValues = new long[ARRAY_SIZES.length][][];
        Random rand = new Random(47);
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
        assertArrayValues(expectedValues);
        assertArrayValues((long[][][]) alternatingNullValues(expectedValues));
    }

    private static void assertArrayValues(long[][][] expectedValues)
    {
        BlockBuilder blockBuilder = new ArrayBlockBuilder(new ArrayBlockBuilder(BIGINT, new BlockBuilderStatus(), 100, 100), new BlockBuilderStatus(), 100);
        for (int i = 0; i < expectedValues.length; i++) {
            if (expectedValues[i] == null) {
                blockBuilder.appendNull();
            }
            else {
                BlockBuilder intermediateBlockBuilder = new ArrayBlockBuilder(BIGINT, new BlockBuilderStatus(), 100, 100);
                for (int j = 0; j < expectedValues[i].length; j++) {
                    if (expectedValues[i][j] == null) {
                        intermediateBlockBuilder.appendNull();
                    }
                    else {
                        BlockBuilder innerMostBlockBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), expectedValues[i].length);
                        for (long v : expectedValues[i][j]) {
                            BIGINT.writeLong(innerMostBlockBuilder, v);
                        }
                        intermediateBlockBuilder.writeObject(innerMostBlockBuilder.build()).closeEntry();
                    }
                }
                blockBuilder.writeObject(intermediateBlockBuilder.build()).closeEntry();
            }
        }
        assertBlock(blockBuilder, expectedValues);
        assertBlock(blockBuilder.build(), expectedValues);
    }

    private static void assertArrayValues(long[][] expectedValues)
    {
        BlockBuilder blockBuilder = new ArrayBlockBuilder(BIGINT, new BlockBuilderStatus(), 100, 100);
        for (int i = 0; i < expectedValues.length; i++) {
            if (expectedValues[i] == null) {
                blockBuilder.appendNull();
            }
            else {
                BlockBuilder elementBlockBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), expectedValues[i].length);
                for (long v : expectedValues[i]) {
                    BIGINT.writeLong(elementBlockBuilder, v);
                }
                blockBuilder.writeObject(elementBlockBuilder).closeEntry();
            }
        }
        assertBlock(blockBuilder, expectedValues);
        assertBlock(blockBuilder.build(), expectedValues);
    }

    private static void assertArrayValues(Slice[][] expectedValues)
    {
        BlockBuilder blockBuilder = new ArrayBlockBuilder(VARCHAR, new BlockBuilderStatus(), 100, 100);
        for (int i = 0; i < expectedValues.length; i++) {
            if (expectedValues[i] == null) {
                blockBuilder.appendNull();
            }
            else {
                BlockBuilder elementBlockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), expectedValues[i].length);
                for (Slice v : expectedValues[i]) {
                    VARCHAR.writeSlice(elementBlockBuilder, v);
                }
                blockBuilder.writeObject(elementBlockBuilder.build()).closeEntry();
            }
        }
        assertBlock(blockBuilder, expectedValues);
        assertBlock(blockBuilder.build(), expectedValues);
    }
}
