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
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockEncoding;
import com.google.common.primitives.Ints;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.lang.reflect.Array;
import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_FLOAT;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Float.floatToIntBits;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test
public abstract class AbstractTestBlock
{
    protected <T> void assertBlock(Block block, T[] expectedValues)
    {
        assertBlockPositions(block, expectedValues);
        assertBlockPositions(copyBlock(block), expectedValues);

        try {
            block.isNull(-1);
            fail("expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected) {
        }
        try {
            block.isNull(block.getPositionCount());
            fail("expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected) {
        }
    }

    protected <T> void assertBlockFilteredPositions(T[] expectedValues, Block block, List<Integer> positions)
    {
        Block filteredBlock = block.copyPositions(positions);
        T[] filteredExpectedValues = filter(expectedValues, positions);
        assertEquals(filteredBlock.getPositionCount(), positions.size());
        assertBlock(filteredBlock, filteredExpectedValues);
    }

    private static <T> T[] filter(T[] expectedValues, List<Integer> positions)
    {
        @SuppressWarnings("unchecked")
        T[] prunedExpectedValues = (T[]) Array.newInstance(expectedValues.getClass().getComponentType(), positions.size());
        for (int i = 0; i < prunedExpectedValues.length; i++) {
            prunedExpectedValues[i] = expectedValues[positions.get(i)];
        }
        return prunedExpectedValues;
    }

    private <T> void assertBlockPositions(Block block, T[] expectedValues)
    {
        assertEquals(block.getPositionCount(), expectedValues.length);
        for (int position = 0; position < block.getPositionCount(); position++) {
            assertBlockPosition(block, position, expectedValues[position]);
        }
    }

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
        assertPositionValue(block.copyPositions(Ints.asList(position)), 0, expectedValue);
    }

    protected static <T> void assertPositionValue(Block block, int position, T expectedValue)
    {
        if (expectedValue == null) {
            assertTrue(block.isNull(position));
            return;
        }

        assertFalse(block.isNull(position));

        if (expectedValue instanceof Slice) {
            Slice expectedSliceValue = (Slice) expectedValue;

            int length = block.getLength(position);
            assertEquals(length, expectedSliceValue.length());

            for (int offset = 0; offset <= length - SIZE_OF_BYTE; offset++) {
                assertEquals(block.getByte(position, offset), expectedSliceValue.getByte(offset));
            }

            for (int offset = 0; offset <= length - SIZE_OF_SHORT; offset++) {
                assertEquals(block.getShort(position, offset), expectedSliceValue.getShort(offset));
            }

            for (int offset = 0; offset <= length - SIZE_OF_INT; offset++) {
                assertEquals(block.getInt(position, offset), expectedSliceValue.getInt(offset));
            }

            for (int offset = 0; offset <= length - SIZE_OF_LONG; offset++) {
                assertEquals(block.getLong(position, offset), expectedSliceValue.getLong(offset));
            }

            for (int offset = 0; offset <= length - SIZE_OF_FLOAT; offset++) {
                assertEquals(floatToIntBits(block.getFloat(position, offset)), floatToIntBits(expectedSliceValue.getFloat(offset)));
            }

            for (int offset = 0; offset <= length - SIZE_OF_DOUBLE; offset++) {
                assertEquals(doubleToLongBits(block.getDouble(position, offset)), doubleToLongBits(expectedSliceValue.getDouble(offset)));
            }

            Block expectedBlock = toSingeValuedBlock(expectedSliceValue);

            for (int offset = 0; offset < length - 3; offset++) {
                assertEquals(block.getSlice(position, offset, 3), expectedSliceValue.slice(offset, 3));
                assertTrue(block.bytesEqual(position, offset, expectedSliceValue, offset, 3));
                // if your tests fail here, please change your test to not use this value
                assertFalse(block.bytesEqual(position, offset, Slices.utf8Slice("XXX"), 0, 3));

                assertEquals(block.bytesCompare(position, offset, 3, expectedSliceValue, offset, 3), 0);
                assertTrue(block.bytesCompare(position, offset, 3, expectedSliceValue, offset, 2) > 0);
                Slice greaterSlice = createGreaterValue(expectedSliceValue, offset, 3);
                assertTrue(block.bytesCompare(position, offset, 3, greaterSlice, 0, greaterSlice.length()) < 0);

                assertTrue(block.equals(position, offset, expectedBlock, 0, offset, 3));
                assertEquals(block.compareTo(position, offset, 3, expectedBlock, 0, offset, 3), 0);

                BlockBuilder blockBuilder = VARBINARY.createBlockBuilder(new BlockBuilderStatus(), 1);
                block.writeBytesTo(position, offset, 3, blockBuilder);
                blockBuilder.closeEntry();
                Block segment = blockBuilder.build();

                assertTrue(block.equals(position, offset, segment, 0, 0, 3));
            }
        }
        else if (expectedValue instanceof long[]) {
            Block actual = block.getObject(position, Block.class);
            long[] expected = (long[]) expectedValue;
            assertEquals(actual.getPositionCount(), expected.length);
            for (int i = 0; i < expected.length; i++) {
                assertEquals(BIGINT.getLong(actual, i), expected[i]);
            }
        }
        else if (expectedValue instanceof Slice[]) {
            Block actual = block.getObject(position, Block.class);
            Slice[] expected = (Slice[]) expectedValue;
            assertEquals(actual.getPositionCount(), expected.length);
            for (int i = 0; i < expected.length; i++) {
                assertEquals(VARCHAR.getSlice(actual, i), expected[i]);
            }
        }
        else if (expectedValue instanceof long[][]) {
            Block actual = block.getObject(position, Block.class);
            long[][] expected = (long[][]) expectedValue;
            assertEquals(actual.getPositionCount(), expected.length);
            for (int i = 0; i < expected.length; i++) {
                assertPositionValue(actual, i, expected[i]);
            }
        }
        else {
            throw new IllegalArgumentException();
        }
    }

    private static Block copyBlock(Block block)
    {
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        BlockEncoding blockEncoding = block.getEncoding();
        blockEncoding.writeBlock(sliceOutput, block);
        return blockEncoding.readBlock(sliceOutput.slice().getInput());
    }

    private static Block toSingeValuedBlock(Slice expectedValue)
    {
        BlockBuilder blockBuilder = VARBINARY.createBlockBuilder(new BlockBuilderStatus(), 1, expectedValue.length());
        VARBINARY.writeSlice(blockBuilder, expectedValue);
        return blockBuilder.build();
    }

    private static Slice createGreaterValue(Slice expectedValue, int offset, int length)
    {
        DynamicSliceOutput greaterOutput = new DynamicSliceOutput(length + 1);
        greaterOutput.writeBytes(expectedValue, offset, length);
        greaterOutput.writeByte('_');
        return greaterOutput.slice();
    }

    protected static Slice[] createExpectedValues(int positionCount)
    {
        Slice[] expectedValues = new Slice[positionCount];
        for (int position = 0; position < positionCount; position++) {
            expectedValues[position] = createExpectedValue(position);
        }
        return expectedValues;
    }

    protected static Slice createExpectedValue(int length)
    {
        DynamicSliceOutput dynamicSliceOutput = new DynamicSliceOutput(16);
        for (int index = 0; index < length; index++) {
            dynamicSliceOutput.writeByte(length * (index + 1));
        }
        return dynamicSliceOutput.slice();
    }

    protected static Object[] alternatingNullValues(Object[] objects)
    {
        Object[] objectsWithNulls = (Object[]) Array.newInstance(objects.getClass().getComponentType(), objects.length * 2 + 1);
        for (int i = 0; i < objects.length; i++) {
            objectsWithNulls[i * 2] = null;
            objectsWithNulls[i * 2 + 1] = objects[i];
        }
        objectsWithNulls[objectsWithNulls.length - 1] = null;
        return objectsWithNulls;
    }
}
