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
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.Arrays;

import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
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
    protected static void assertBlock(Block block, Slice[] expectedValues)
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

    private static void assertBlockPositions(Block block, Slice[] expectedValues)
    {
        assertEquals(block.getPositionCount(), expectedValues.length);
        for (int position = 0; position < block.getPositionCount(); position++) {
            assertBlockPosition(block, position, expectedValues[position]);
        }
    }

    private static void assertBlockPosition(Block block, int position, Slice expectedValue)
    {
        assertPositionValue(block, position, expectedValue);
        assertPositionValue(block.getSingleValueBlock(position), 0, expectedValue);
        assertPositionValue(block.getRegion(position, 1), 0, expectedValue);
        assertPositionValue(block.getRegion(0, position + 1), position, expectedValue);
        assertPositionValue(block.getRegion(position, block.getPositionCount() - position), 0, expectedValue);
        assertPositionValue(block.copyRegion(position, 1), 0, expectedValue);
        assertPositionValue(block.copyRegion(0, position + 1), position, expectedValue);
        assertPositionValue(block.copyRegion(position, block.getPositionCount() - position), 0, expectedValue);
    }

    private static void assertPositionValue(Block block, int position, Slice expectedValue)
    {
        if (expectedValue == null) {
            assertTrue(block.isNull(position));
            return;
        }

        assertFalse(block.isNull(position));

        int length = block.getLength(position);
        assertEquals(length, expectedValue.length());

        for (int offset = 0; offset <= length - SIZE_OF_BYTE; offset++) {
            assertEquals(block.getByte(position, offset), expectedValue.getByte(offset));
        }

        for (int offset = 0; offset <= length - SIZE_OF_SHORT; offset++) {
            assertEquals(block.getShort(position, offset), expectedValue.getShort(offset));
        }

        for (int offset = 0; offset <= length - SIZE_OF_INT; offset++) {
            assertEquals(block.getInt(position, offset), expectedValue.getInt(offset));
        }

        for (int offset = 0; offset <= length - SIZE_OF_LONG; offset++) {
            assertEquals(block.getLong(position, offset), expectedValue.getLong(offset));
        }

        for (int offset = 0; offset <= length - SIZE_OF_FLOAT; offset++) {
            assertEquals(floatToIntBits(block.getFloat(position, offset)), floatToIntBits(expectedValue.getFloat(offset)));
        }

        for (int offset = 0; offset <= length - SIZE_OF_DOUBLE; offset++) {
            assertEquals(doubleToLongBits(block.getDouble(position, offset)), doubleToLongBits(expectedValue.getDouble(offset)));
        }

        Block expectedBlock = toSingeValuedBlock(expectedValue);

        for (int offset = 0; offset < length - 3; offset++) {
            assertEquals(block.getSlice(position, offset, 3), expectedValue.slice(offset, 3));
            assertEquals(block.hash(position, offset, 3), expectedValue.hashCode(offset, 3));
            assertTrue(block.bytesEqual(position, offset, expectedValue, offset, 3));
            // if your tests fail here, please change your test to not use this value
            assertFalse(block.bytesEqual(position, offset, Slices.utf8Slice("XXX"), 0, 3));

            assertEquals(block.bytesCompare(position, offset, 3, expectedValue, offset, 3), 0);
            assertTrue(block.bytesCompare(position, offset, 3, expectedValue, offset, 2) > 0);
            Slice greaterSlice = createGreaterValue(expectedValue, offset, 3);
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

    protected static Slice createExpectedValue(int length)
    {
        DynamicSliceOutput dynamicSliceOutput = new DynamicSliceOutput(16);
        for (int index = 0; index < length; index++) {
            dynamicSliceOutput.writeByte(length * (index + 1));
        }
        return dynamicSliceOutput.slice();
    }

    protected static Object[] alternatingNullValues(Object[] slices)
    {
        Object[] slicesWithNulls = Arrays.copyOf(slices, slices.length * 2);
        for (int i = 0; i < slices.length; i++) {
            slicesWithNulls[i * 2] = slices[i];
            slicesWithNulls[i * 2 + 1] = null;
        }
        return slicesWithNulls;
    }
}
