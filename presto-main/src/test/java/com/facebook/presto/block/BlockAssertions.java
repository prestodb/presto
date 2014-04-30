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
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.block.RandomAccessBlock;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Function;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import static com.facebook.presto.block.BlockIterables.createBlockIterable;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public final class BlockAssertions
{
    public static final ConnectorSession SESSION = new ConnectorSession("user", "source", "catalog", "schema", UTC_KEY, Locale.ENGLISH, "address", "agent");

    private BlockAssertions()
    {
    }

    public static Object getOnlyValue(Block block)
    {
        assertEquals(block.getPositionCount(), 1, "Block positions");

        BlockCursor cursor = block.cursor();
        assertTrue(cursor.advanceNextPosition());
        Object value = cursor.getObjectValue(SESSION);
        assertFalse(cursor.advanceNextPosition());

        return value;
    }

    public static void assertBlocksEquals(BlockIterable actual, BlockIterable expected)
    {
        Iterator<Block> expectedIterator = expected.iterator();
        for (Block actualBlock : actual) {
            assertTrue(expectedIterator.hasNext());
            Block expectedBlock = expectedIterator.next();
            assertBlockEquals(actualBlock, expectedBlock);
        }
        assertFalse(expectedIterator.hasNext());
    }

    public static List<Object> toValues(BlockIterable blocks)
    {
        List<Object> values = new ArrayList<>();
        for (Block block : blocks) {
            BlockCursor cursor = block.cursor();
            while (cursor.advanceNextPosition()) {
                values.add(cursor.getObjectValue(SESSION));
            }
        }
        return Collections.unmodifiableList(values);
    }

    public static List<Object> toValues(Block block)
    {
        BlockCursor cursor = block.cursor();
        return toValues(cursor);
    }

    public static List<Object> toValues(BlockCursor cursor)
    {
        List<Object> values = new ArrayList<>();
        while (cursor.advanceNextPosition()) {
            values.add(cursor.getObjectValue(SESSION));
        }
        return Collections.unmodifiableList(values);
    }

    public static void assertBlockEquals(Block actual, Block expected)
    {
        assertEquals(actual.getType(), expected.getType());
        assertCursorsEquals(actual.cursor(), expected.cursor());
    }

    public static void assertCursorsEquals(BlockCursor actualCursor, BlockCursor expectedCursor)
    {
        assertEquals(actualCursor.getType(), expectedCursor.getType());
        while (advanceAllCursorsToNextPosition(actualCursor, expectedCursor)) {
            assertEquals(actualCursor.getObjectValue(SESSION), expectedCursor.getObjectValue(SESSION));
        }
        assertTrue(actualCursor.isFinished());
        assertTrue(expectedCursor.isFinished());
    }

    public static boolean advanceAllCursorsToNextPosition(BlockCursor... cursors)
    {
        boolean allAdvanced = true;
        for (BlockCursor cursor : cursors) {
            allAdvanced = cursor.advanceNextPosition() && allAdvanced;
        }
        return allAdvanced;
    }

    public static Iterable<Long> createLongSequence(long start, long end)
    {
        return ContiguousSet.create(Range.closedOpen(start, end), DiscreteDomain.longs());
    }

    public static Iterable<Double> createDoubleSequence(long start, long end)
    {
        return Iterables.transform(createLongSequence(start, end), new Function<Long, Double>()
        {
            @Override
            public Double apply(Long input)
            {
                return (double) input;
            }
        });
    }

    public static Iterable<String> createStringSequence(long start, long end)
    {
        return Iterables.transform(createLongSequence(start, end), new Function<Long, String>()
        {
            @Override
            public String apply(Long input)
            {
                return String.valueOf(input);
            }
        });
    }

    public static Iterable<Long> createLongNullSequence(int count)
    {
        Long[] values = new Long[count];
        Arrays.fill(values, null);
        return Arrays.asList(values);
    }

    public static Iterable<Double> createDoubleNullSequence(int count)
    {
        Double[] values = new Double[count];
        Arrays.fill(values, null);
        return Arrays.asList(values);
    }

    public static Iterable<String> createStringNullSequence(int count)
    {
        String[] values = new String[count];
        Arrays.fill(values, null);
        return Arrays.asList(values);
    }

    public static Block createStringsBlock(String... values)
    {
        checkNotNull(values, "varargs 'values' is null");

        return createStringsBlock(Arrays.asList(values));
    }

    public static Block createStringsBlock(Iterable<String> values)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(new BlockBuilderStatus());

        for (String value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                builder.appendSlice(Slices.utf8Slice(value));
            }
        }

        return builder.build();
    }

    public static BlockIterable createStringsBlockIterable(@Nullable String... values)
    {
        return createBlockIterable(createStringsBlock(values));
    }

    public static Block createStringSequenceBlock(int start, int end)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(new BlockBuilderStatus());

        for (int i = start; i < end; i++) {
            builder.appendSlice(Slices.utf8Slice(String.valueOf(i)));
        }

        return builder.build();
    }

    public static Block createBooleansBlock(Boolean... values)
    {
        checkNotNull(values, "varargs 'values' is null");

        return createBooleansBlock(Arrays.asList(values));
    }

    public static Block createBooleansBlock(Boolean value, int count)
    {
        return createBooleansBlock(Collections.nCopies(count, value));
    }

    public static Block createBooleansBlock(Iterable<Boolean> values)
    {
        BlockBuilder builder = BOOLEAN.createBlockBuilder(new BlockBuilderStatus());

        for (Boolean value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                builder.appendBoolean(value);
            }
        }

        return builder.build();
    }

    // This method makes it easy to create blocks without having to add an L to every value
    public static RandomAccessBlock createLongsBlock(int... values)
    {
        BlockBuilder builder = BIGINT.createBlockBuilder(new BlockBuilderStatus());

        for (int value : values) {
            builder.appendLong((long) value);
        }

        return builder.build().toRandomAccessBlock();
    }

    public static Block createLongsBlock(Long... values)
    {
        checkNotNull(values, "varargs 'values' is null");

        return createLongsBlock(Arrays.asList(values));
    }

    public static Block createLongsBlock(Iterable<Long> values)
    {
        BlockBuilder builder = BIGINT.createBlockBuilder(new BlockBuilderStatus());

        for (Long value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                builder.appendLong(value);
            }
        }

        return builder.build();
    }

    public static BlockIterable createLongsBlockIterable(int... values)
    {
        return createBlockIterable(createLongsBlock(values));
    }

    public static BlockIterable createLongsBlockIterable(@Nullable Long... values)
    {
        return createBlockIterable(createLongsBlock(values));
    }

    public static Block createLongSequenceBlock(int start, int end)
    {
        BlockBuilder builder = BIGINT.createBlockBuilder(new BlockBuilderStatus());

        for (int i = start; i < end; i++) {
            builder.appendLong(i);
        }

        return builder.build();
    }

    public static Block createBooleanSequenceBlock(int start, int end)
    {
        BlockBuilder builder = BOOLEAN.createBlockBuilder(new BlockBuilderStatus());

        for (int i = start; i < end; i++) {
            builder.appendBoolean(i % 2 == 0);
        }

        return builder.build();
    }

    public static Block createDoublesBlock(Double... values)
    {
        checkNotNull(values, "varargs 'values' is null");

        return createDoublesBlock(Arrays.asList(values));
    }

    public static Block createDoublesBlock(Iterable<Double> values)
    {
        BlockBuilder builder = DOUBLE.createBlockBuilder(new BlockBuilderStatus());

        for (Double value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                builder.appendDouble(value);
            }
        }

        return builder.build();
    }

    public static BlockIterable createDoublesBlockIterable(@Nullable Double... values)
    {
        return createBlockIterable(createDoublesBlock(values));
    }

    public static Block createDoubleSequenceBlock(int start, int end)
    {
        BlockBuilder builder = DOUBLE.createBlockBuilder(new BlockBuilderStatus());

        for (int i = start; i < end; i++) {
            builder.appendDouble((double) i);
        }

        return builder.build();
    }

    public static BlockIterableBuilder blockIterableBuilder(Type type)
    {
        return new BlockIterableBuilder(type);
    }

    public static class BlockIterableBuilder
    {
        private final List<Block> blocks = new ArrayList<>();
        private BlockBuilder blockBuilder;

        private BlockIterableBuilder(Type type)
        {
            blockBuilder = type.createBlockBuilder(new BlockBuilderStatus());
        }

        public BlockIterableBuilder append(Slice value)
        {
            blockBuilder.appendSlice(value);
            return this;
        }

        public BlockIterableBuilder append(double value)
        {
            blockBuilder.appendDouble(value);
            return this;
        }

        public BlockIterableBuilder append(long value)
        {
            blockBuilder.appendLong(value);
            return this;
        }

        public BlockIterableBuilder append(String value)
        {
            blockBuilder.appendSlice(Slices.utf8Slice(value));
            return this;
        }

        public BlockIterableBuilder append(byte[] value)
        {
            blockBuilder.appendSlice(Slices.wrappedBuffer(value));
            return this;
        }

        public BlockIterableBuilder appendNull()
        {
            blockBuilder.appendNull();
            return this;
        }

        public BlockIterableBuilder newBlock()
        {
            if (!blockBuilder.isEmpty()) {
                Block block = blockBuilder.build();
                blocks.add(block);
                blockBuilder = block.getType().createBlockBuilder(new BlockBuilderStatus());
            }
            return this;
        }

        public BlockIterable build()
        {
            newBlock();
            return createBlockIterable(blocks);
        }
    }
}
