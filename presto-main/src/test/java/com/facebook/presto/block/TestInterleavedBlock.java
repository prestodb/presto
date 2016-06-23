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
import com.google.common.primitives.Longs;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.lang.reflect.Array;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestInterleavedBlock
        extends AbstractTestBlock
{
    private static final int[] SIZE_0 = new int[] {16, 0, 13, 1, 2, 11, 4, 7};
    private static final int SIZE_1 = 8;
    private static final int POSITION_COUNT = SIZE_0.length;
    private static final int COLUMN_COUNT = 2;
    private static final ImmutableList<Type> TYPES = ImmutableList.of(VARCHAR, BIGINT);

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
        InterleavedBlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);

        assertBlockFilteredPositions(expectedValues, blockBuilder, Ints.asList(0, 1, 4, 5, 6, 7, 14, 15));
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), Ints.asList(0, 1, 4, 5, 6, 7, 14, 15));
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), Ints.asList(2, 3, 4, 5, 8, 9, 12, 13));
    }

    private static InterleavedBlockBuilder createBlockBuilderWithValues(Slice[] expectedValues)
    {
        InterleavedBlockBuilder blockBuilder = new InterleavedBlockBuilder(TYPES, new BlockBuilderStatus(), expectedValues.length);

        int valueIndex = 0;
        while (valueIndex < expectedValues.length) {
            for (Type type : TYPES) {
                Class<?> javaType = type.getJavaType();
                Slice expectedValue = expectedValues[valueIndex];

                if (expectedValue == null) {
                    blockBuilder.appendNull();
                }
                else if (javaType == boolean.class) {
                    type.writeBoolean(blockBuilder, expectedValue.getByte(0) != 0);
                }
                else if (javaType == long.class) {
                    type.writeLong(blockBuilder, expectedValue.getLong(0));
                }
                else if (javaType == double.class) {
                    type.writeDouble(blockBuilder, expectedValue.getDouble(0));
                }
                else {
                    blockBuilder.writeBytes(expectedValue, 0, expectedValue.length()).closeEntry();
                }

                valueIndex++;
            }
        }

        return blockBuilder;
    }

    private void assertValues(Slice[] expectedValues)
    {
        InterleavedBlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);

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
        assertInterleavedPosition(TYPES, block, position, expectedValue);

        Type type = TYPES.get(position % TYPES.size());
        assertInterleavedPosition(ImmutableList.of(type), block.getSingleValueBlock(position), 0, expectedValue);

        int alignedPosition = position - position % COLUMN_COUNT;

        assertInterleavedPosition(ImmutableList.of(type), block.getRegion(alignedPosition, COLUMN_COUNT), position - alignedPosition, expectedValue);
        assertInterleavedPosition(TYPES, block.getRegion(0, alignedPosition + COLUMN_COUNT), position, expectedValue);
        assertInterleavedPosition(ImmutableList.of(type), block.getRegion(alignedPosition, block.getPositionCount() - alignedPosition), position - alignedPosition, expectedValue);

        assertInterleavedPosition(ImmutableList.of(type), block.copyRegion(alignedPosition, COLUMN_COUNT), position - alignedPosition, expectedValue);
        assertInterleavedPosition(TYPES, block.copyRegion(0, alignedPosition + COLUMN_COUNT), position, expectedValue);
        assertInterleavedPosition(ImmutableList.of(type), block.copyRegion(alignedPosition, block.getPositionCount() - alignedPosition), position - alignedPosition, expectedValue);

        assertInterleavedPosition(TYPES, block.copyPositions(IntStream.range(alignedPosition, alignedPosition + COLUMN_COUNT).boxed().collect(Collectors.toList())), position % COLUMN_COUNT, expectedValue);
    }

    private <T> void assertInterleavedPosition(List<Type> types, Block block, int position, T expectedValue)
    {
        assertPositionValue(block, position, expectedValue);

        Type type = types.get(position % types.size());
        if (expectedValue == null) {
            assertTrue(block.isNull(position));
        }
        else if (BIGINT.equals(type)) {
            Slice expectedSliceValue = (Slice) expectedValue;
            assertEquals(expectedSliceValue.length(), Longs.BYTES);
            assertEquals(block.getLong(position, 0), expectedSliceValue.getLong(0));
        }
        else if (VARCHAR.equals(type)) {
            Slice expectedSliceValue = (Slice) expectedValue;
            assertSlicePosition(block, position, expectedSliceValue);
        }
        else {
            throw new IllegalArgumentException("Unsupported type " + type);
        }
    }

    @Override
    protected boolean isByteAccessSupported()
    {
        return false;
    }

    @Override
    protected boolean isShortAccessSupported()
    {
        return false;
    }

    @Override
    protected boolean isIntAccessSupported()
    {
        return false;
    }

    @Override
    protected boolean isLongAccessSupported()
    {
        return false;
    }

    @Override
    protected boolean isSliceAccessSupported()
    {
        return false;
    }
}
