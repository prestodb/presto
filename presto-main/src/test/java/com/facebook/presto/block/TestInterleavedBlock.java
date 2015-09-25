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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.lang.reflect.Array;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestInterleavedBlock
        extends AbstractTestBlock
{
    private static final int[] SIZE_0 = new int[] {16, 0, 13, 1, 2, 11, 4, 7};
    private static final int SIZE_1 = 8;
    private static final int POSITION_COUNT = SIZE_0.length;
    private static final int COLUMN_COUNT = 2;

    @Test
    public void test()
    {
        Slice[] expectedValues = createExpectedValues();
        assertValues(expectedValues);
        assertValues((Slice[]) alternatingNullValues(expectedValues));
    }

    @Test
    public void testCopyPositions()
    {
        Slice[] expectedValues = createExpectedValues();
        InterleavedBlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues, ImmutableList.of(VARCHAR, BIGINT));

        assertBlockFilteredPositions(expectedValues, blockBuilder, Ints.asList(0, 1, 4, 5, 6, 7, 14, 15));
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), Ints.asList(0, 1, 4, 5, 6, 7, 14, 15));
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), Ints.asList(2, 3, 4, 5, 8, 9, 12, 13));
    }

    private static InterleavedBlockBuilder createBlockBuilderWithValues(Slice[] expectedValues, List<Type> types)
    {
        InterleavedBlockBuilder blockBuilder = new InterleavedBlockBuilder(types, new BlockBuilderStatus(), expectedValues.length);

        for (Slice expectedValue : expectedValues) {
            if (expectedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeBytes(expectedValue, 0, expectedValue.length()).closeEntry();
            }
        }

        return blockBuilder;
    }

    private void assertValues(Slice[] expectedValues)
    {
        InterleavedBlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues, ImmutableList.of(VARCHAR, BIGINT));

        assertBlock(blockBuilder, expectedValues);
        assertBlock(blockBuilder.build(), expectedValues);
    }

    private static Slice[] createExpectedValues()
    {
        Slice[] expectedValues = new Slice[POSITION_COUNT * 2];
        for (int position = 0; position < POSITION_COUNT; position++) {
            expectedValues[position * 2] = createExpectedValue(SIZE_0[position]);
            expectedValues[position * 2 + 1] = createExpectedValue(SIZE_1);
        }
        return expectedValues;
    }

    protected static Object[] alternatingNullValues(Object[] objects)
    {
        Object[] objectsWithNulls = (Object[]) Array.newInstance(objects.getClass().getComponentType(), objects.length * 2 + 2);
        for (int i = 0; i < objects.length; i += 2) {
            objectsWithNulls[i * 2] = null;
            objectsWithNulls[i * 2 + 1] = null;
            objectsWithNulls[i * 2 + 2] = objects[i];
            objectsWithNulls[i * 2 + 3] = objects[i + 1];
        }
        objectsWithNulls[objectsWithNulls.length - 2] = null;
        objectsWithNulls[objectsWithNulls.length - 1] = null;
        return objectsWithNulls;
    }

    @Override
    protected <T> void assertBlockPosition(Block block, int position, T expectedValue)
    {
        assertPositionValue(block, position, expectedValue);
        assertPositionValue(block.getSingleValueBlock(position), 0, expectedValue);
        assertPositionValue(block.getRegion(position, 1), 0, expectedValue);
        assertPositionValue(block.getRegion(0, position + 1), position, expectedValue);
        assertPositionValue(block.getRegion(position, block.getPositionCount() - position), 0, expectedValue);
        assertPositionValue(block.copyRegion(position, 1), 0, expectedValue);
        assertPositionValue(block.copyRegion(0, position + 1), position, expectedValue);
        assertPositionValue(block.copyRegion(position, block.getPositionCount() - position), 0, expectedValue);
        int positionFloored = position / COLUMN_COUNT * COLUMN_COUNT;
        assertPositionValue(block.copyPositions(IntStream.range(positionFloored, positionFloored + COLUMN_COUNT).boxed().collect(Collectors.toList())), position % COLUMN_COUNT, expectedValue);
    }
}
