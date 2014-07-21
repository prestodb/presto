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

import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_LAST;
import static com.facebook.presto.spi.block.SortOrder.DESC_NULLS_FIRST;
import static com.facebook.presto.spi.block.SortOrder.DESC_NULLS_LAST;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
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
        int hash = block.hash(position);
        assertPositionValue(block, position, value, hash);
        assertPositionValue(block.getSingleValueBlock(position), 0, value, hash);
        assertPositionValue(block.getRegion(position, 1), 0, value, hash);
        assertPositionValue(block.getRegion(0, position + 1), position, value, hash);
        assertPositionValue(block.getRegion(position, block.getPositionCount() - position), 0, value, hash);

        BlockBuilder blockBuilder = expectedType.createBlockBuilder(new BlockBuilderStatus());
        block.appendTo(position, blockBuilder);
        assertPositionValue(blockBuilder.build(), 0, value, hash);
    }

    private void assertPositionValue(Block block, int position, Object expectedValue, int expectedHash)
    {
        assertEquals(block.getType(), expectedType);
        assertEquals(block.getObjectValue(SESSION, position), expectedValue);
        assertEquals(block.hash(position), expectedHash);

        Block expectedBlock = createBlock(expectedType, expectedValue);
        assertTrue(block.equalTo(position, block, position));
        assertTrue(block.equalTo(position, expectedBlock, 0));
        assertTrue(expectedBlock.equalTo(0, block, position));

        assertEquals(block.isNull(position), expectedValue == null);

        assertTrue((block.compareTo(ASC_NULLS_FIRST, position, expectedBlock, 0) == 0));
        assertTrue((block.compareTo(ASC_NULLS_LAST, position, expectedBlock, 0) == 0));
        assertTrue((block.compareTo(DESC_NULLS_FIRST, position, expectedBlock, 0) == 0));
        assertTrue((block.compareTo(DESC_NULLS_LAST, position, expectedBlock, 0) == 0));

        verifyInvalidPositionHandling(block);

        if (block.isNull(position)) {
            assertTrue((block.compareTo(ASC_NULLS_FIRST, position, getNonNullValue(expectedType), 0) < 0));
            assertTrue((block.compareTo(DESC_NULLS_FIRST, position, getNonNullValue(expectedType), 0) < 0));
            assertTrue((block.compareTo(ASC_NULLS_LAST, position, getNonNullValue(expectedType), 0) > 0));
            assertTrue((block.compareTo(DESC_NULLS_LAST, position, getNonNullValue(expectedType), 0) > 0));
            return;
        }

        if (expectedValue != Boolean.TRUE) {
            assertTrue((block.compareTo(ASC_NULLS_FIRST, position, getGreaterValue(expectedType, expectedValue), 0) < 0));
            assertTrue((block.compareTo(ASC_NULLS_LAST, position, getGreaterValue(expectedType, expectedValue), 0) < 0));
            assertTrue((block.compareTo(DESC_NULLS_FIRST, position, getGreaterValue(expectedType, expectedValue), 0) > 0));
            assertTrue((block.compareTo(DESC_NULLS_LAST, position, getGreaterValue(expectedType, expectedValue), 0) > 0));
        }

        Type type = block.getType();
        if (type.getJavaType() == boolean.class) {
            assertEquals(block.getBoolean(position), expectedValue);
            try {
                block.getLong(position);
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }
            try {
                block.getDouble(position);
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }
        }
        else if (type.getJavaType() == long.class) {
            assertEquals(block.getLong(position), expectedValue);
            try {
                block.getBoolean(position);
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }
            try {
                block.getDouble(position);
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }
        }
        else if (type.getJavaType() == double.class) {
            assertEquals(block.getDouble(position), expectedValue);
            try {
                block.getBoolean(position);
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }
            try {
                block.getLong(position);
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }

        }
        else if (type.getJavaType() == Slice.class) {
            assertEquals(block.getSlice(position).toStringUtf8(), expectedValue);
            try {
                block.getBoolean(position);
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }
            try {
                block.getLong(position);
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }
            try {
                block.getDouble(position);
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }
        }
    }

    private void verifyInvalidPositionHandling(Block block)
    {
        try {
            block.getObjectValue(SESSION, -1);
            fail("expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected) {
        }
        try {
            block.getObjectValue(SESSION, block.getPositionCount());
            fail("expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected) {
        }

        try {
            block.hash(-1);
            fail("expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected) {
        }
        try {
            block.hash(block.getPositionCount());
            fail("expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected) {
        }

        Block other = getNonNullValue(block.getType());
        try {
            block.equalTo(-1, other, 0);
            fail("expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected) {
        }
        try {
            block.equalTo(block.getPositionCount(), other, 0);
            fail("expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected) {
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

        try {
            block.compareTo(ASC_NULLS_FIRST, -1, other, 0);
            fail("expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected) {
        }
        try {
            block.compareTo(ASC_NULLS_FIRST, block.getPositionCount(), other, 0);
            fail("expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected) {
        }

        Type type = block.getType();
        if (type.getJavaType() == boolean.class) {
            try {
                block.getBoolean(-1);
                fail("expected IllegalArgumentException");
            }
            catch (IllegalArgumentException expected) {
            }
            try {
                block.getBoolean(block.getPositionCount());
                fail("expected IllegalArgumentException");
            }
            catch (IllegalArgumentException expected) {
            }
        }
        else if (type.getJavaType() == long.class) {
            try {
                block.getLong(-1);
                fail("expected IllegalArgumentException");
            }
            catch (IllegalArgumentException expected) {
            }
            try {
                block.getLong(block.getPositionCount());
                fail("expected IllegalArgumentException");
            }
            catch (IllegalArgumentException expected) {
            }
        }
        else if (type.getJavaType() == double.class) {
            try {
                block.getDouble(-1);
                fail("expected IllegalArgumentException");
            }
            catch (IllegalArgumentException expected) {
            }
            try {
                block.getDouble(block.getPositionCount());
                fail("expected IllegalArgumentException");
            }
            catch (IllegalArgumentException expected) {
            }
        }
        else if (type.getJavaType() == Slice.class) {
            try {
                block.getSlice(-1);
                fail("expected IllegalArgumentException");
            }
            catch (IllegalArgumentException expected) {
            }
            try {
                block.getSlice(block.getPositionCount());
                fail("expected IllegalArgumentException");
            }
            catch (IllegalArgumentException expected) {
            }
        }
    }

    private static Block createBlock(Type type, Object value)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus());

        if (value == null) {
            blockBuilder.appendNull();
        }
        else if (type.getJavaType() == boolean.class) {
            blockBuilder.appendBoolean((Boolean) value);
        }
        else if (type.getJavaType() == long.class) {
            blockBuilder.appendLong((Long) value);
        }
        else if (type.getJavaType() == double.class) {
            blockBuilder.appendDouble((Double) value);
        }
        else if (type.getJavaType() == Slice.class) {
            blockBuilder.appendSlice(Slices.utf8Slice((String) value));
        }
        return blockBuilder.build();
    }

    private static Block getGreaterValue(Type type, Object value)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus());

        if (type.equals(BOOLEAN)) {
            checkArgument(value != Boolean.TRUE);
            blockBuilder.appendBoolean(true);
        }
        else if (type.equals(BIGINT)) {
            blockBuilder.appendLong(((Long) value) + 1);
        }
        else if (type.equals(DOUBLE)) {
            blockBuilder.appendDouble(((Double) value) + 1);
        }
        else if (type.equals(VARCHAR)) {
            blockBuilder.appendSlice(Slices.utf8Slice(value + "_"));
        }
        return blockBuilder.build();
    }

    private static Block getNonNullValue(Type type)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus());

        if (type.getJavaType() == boolean.class) {
            blockBuilder.appendBoolean(true);
        }
        else if (type.getJavaType() == long.class) {
            blockBuilder.appendLong(1);
        }
        else if (type.getJavaType() == double.class) {
            blockBuilder.appendDouble(1);
        }
        else if (type.getJavaType() == Slice.class) {
            blockBuilder.appendSlice(Slices.utf8Slice("_"));
        }
        return blockBuilder.build();
    }

    private static SortedMap<Integer, Object> indexValues(Block block)
    {
        SortedMap<Integer, Object> values = new TreeMap<>();
        for (int position = 0; position < block.getPositionCount(); position++) {
            values.put(position, block.getObjectValue(SESSION, position));
        }
        return unmodifiableSortedMap(values);
    }
}
