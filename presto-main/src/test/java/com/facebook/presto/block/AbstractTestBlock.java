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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.Locale;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Collections.unmodifiableSortedMap;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public abstract class AbstractTestBlock
{
    public static final ConnectorSession SESSION = new ConnectorSession("user", "source", "catalog", "schema", UTC_KEY, Locale.ENGLISH, "address", "agent");

    private final SortedMap<Integer, Object> expectedValues = indexValues(createTestBlock());
    private final Type expectedType = createTestBlock().getType();

    protected abstract Block createExpectedValues();

    protected Block createTestBlock()
    {
        return createExpectedValues();
    }

    public final Object getExpectedValue(int position)
    {
        return getExpectedValues().get(position);
    }

    public final SortedMap<Integer, Object> getExpectedValues()
    {
        return expectedValues;
    }

    @Test
    public void testBlock()
    {
        Block block = createTestBlock();
        for (Entry<Integer, Object> entry : expectedValues.entrySet()) {
            assertPositionEquals(block, entry.getKey(), entry.getValue());
        }
    }

    @Test
    public void testBlockEncoding()
    {
        Block block = createTestBlock();

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        BlockEncoding blockEncoding = block.getEncoding();
        blockEncoding.writeBlock(sliceOutput, block);
        Block actualBlock = blockEncoding.readBlock(sliceOutput.slice().getInput());
        BlockAssertions.assertBlockEquals(actualBlock, block);
    }

    protected void assertPositionEquals(Block block, int position, Object value)
    {
        assertPositionValue(block, position, value);
        assertPositionValue(block.getSingleValueBlock(position), 0, value);
        assertPositionValue(block.getRegion(position, 1), 0, value);
        assertPositionValue(block.getRegion(0, position + 1), position, value);
        assertPositionValue(block.getRegion(position, block.getPositionCount() - position), 0, value);

        BlockBuilder blockBuilder = expectedType.createBlockBuilder(new BlockBuilderStatus());
        expectedType.appendTo(block, position, blockBuilder);
        assertPositionValue(blockBuilder.build(), 0, value);
    }

    private void assertPositionValue(Block block, int position, Object expectedValue)
    {
        assertEquals(block.getType(), expectedType);

        Block expectedBlock = createBlock(expectedType, expectedValue);

        assertEquals(block.isNull(position), expectedValue == null);

        int length = block.getLength(position);
        assertEquals(length, expectedBlock.getLength(0));

        if (expectedValue instanceof String) {
            assertTrue(block.compareTo(position, 0, length, expectedBlock, 0, 0, length) == 0);
            if (length > 0) {
                assertTrue(block.compareTo(position, 1, length - 1, expectedBlock, 0, 1, length - 1) == 0);
                assertTrue(block.compareTo(position, 0, length - 1, expectedBlock, 0, 0, length - 1) == 0);
            }

            Slice value = Slices.utf8Slice(expectedValue + "_");
            BlockBuilder blockBuilder = expectedType.createBlockBuilder(new BlockBuilderStatus());
            expectedType.writeSlice(blockBuilder, value, 0, value.length());
            Block greaterValue = blockBuilder.build();

            assertTrue(block.compareTo(position, 0, length, greaterValue, 0, 0, length + 1) < 0);
            if (length > 0) {
                assertTrue(block.compareTo(position, 1, length - 1, greaterValue, 0, 1, length) < 0);
                assertTrue(block.compareTo(position, 0, length - 1, greaterValue, 0, 0, length) < 0);
            }
        }

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

    private static Block createBlock(Type type, Object value)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus());

        Class<?> javaType = type.getJavaType();
        if (value == null) {
            blockBuilder.appendNull();
        }
        else if (javaType == boolean.class) {
            type.writeBoolean(blockBuilder, (Boolean) value);
        }
        else if (javaType == long.class) {
            type.writeLong(blockBuilder, (Long) value);
        }
        else if (javaType == double.class) {
            type.writeDouble(blockBuilder, (Double) value);
        }
        else if (javaType == Slice.class) {
            Slice slice = Slices.utf8Slice((String) value);
            type.writeSlice(blockBuilder, slice, 0, slice.length());
        }
        else {
            throw new UnsupportedOperationException("not yet implemented: " + javaType);
        }
        return blockBuilder.build();
    }

    private static SortedMap<Integer, Object> indexValues(Block block)
    {
        Type type = block.getType();
        SortedMap<Integer, Object> values = new TreeMap<>();
        for (int position = 0; position < block.getPositionCount(); position++) {
            values.put(position, type.getObjectValue(SESSION, block, position));
        }
        return unmodifiableSortedMap(values);
    }
}
