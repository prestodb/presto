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

import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.base.Function;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import io.airlift.slice.Slice;
import org.testng.Assert;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.block.BlockIterables.createBlockIterable;
import static com.google.common.base.Charsets.UTF_8;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class BlockAssertions
{
    public static final TupleInfo COMPOSITE_SEQUENCE_TUPLE_INFO = new TupleInfo(Type.BOOLEAN, Type.FIXED_INT_64, Type.DOUBLE, Type.VARIABLE_BINARY);

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

    public static List<List<Object>> toValues(BlockIterable blocks)
    {
        ImmutableList.Builder<List<Object>> values = ImmutableList.builder();
        for (Block block : blocks) {
            BlockCursor cursor = block.cursor();
            while (cursor.advanceNextPosition()) {
                values.add(cursor.getTuple().toValues());
            }
        }
        return values.build();
    }

    public static List<List<Object>> toValues(Block block)
    {
        BlockCursor cursor = block.cursor();
        return toValues(cursor);
    }

    public static List<List<Object>> toValues(BlockCursor cursor)
    {
        ImmutableList.Builder<List<Object>> values = ImmutableList.builder();
        while (cursor.advanceNextPosition()) {
            values.add(cursor.getTuple().toValues());
        }
        return values.build();
    }

    public static void assertBlockEquals(Block actual, Block expected)
    {
        Assert.assertEquals(actual.getTupleInfo(), expected.getTupleInfo());
        assertCursorsEquals(actual.cursor(), expected.cursor());
    }

    public static void assertCursorsEquals(BlockCursor actualCursor, BlockCursor expectedCursor)
    {
        Assert.assertEquals(actualCursor.getTupleInfo(), expectedCursor.getTupleInfo());
        while (advanceAllCursorsToNextPosition(actualCursor, expectedCursor)) {
            assertEquals(actualCursor.getTuple(), expectedCursor.getTuple());
        }
        assertTrue(actualCursor.isFinished());
        assertTrue(expectedCursor.isFinished());
    }

    public static void assertBlockEqualsIgnoreOrder(Block actual, Block expected)
    {
        Assert.assertEquals(actual.getTupleInfo(), expected.getTupleInfo());

        List<Tuple> actualTuples = toTuplesList(actual);
        List<Tuple> expectedTuples = toTuplesList(expected);
        assertEqualsIgnoreOrder(actualTuples, expectedTuples);
    }

    public static List<Tuple> toTuplesList(Block Block)
    {
        ImmutableList.Builder<Tuple> tuples = ImmutableList.builder();
        BlockCursor actualCursor = Block.cursor();
        while (actualCursor.advanceNextPosition()) {
            tuples.add(actualCursor.getTuple());
        }
        return tuples.build();
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

    public static Block createStringsBlock(@Nullable String... values)
    {
        return createStringsBlock(Arrays.asList(values));
    }

    public static Block createStringsBlock(Iterable<String> values)
    {
        BlockBuilder builder = new BlockBuilder(TupleInfo.SINGLE_VARBINARY);

        for (String value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                builder.append(value.getBytes(UTF_8));
            }
        }

        return builder.build();
    }

    public static BlockIterable createStringsBlockIterable(@Nullable String... values)
    {
        return BlockIterables.createBlockIterable(createStringsBlock(values));
    }

    public static Block createStringSequenceBlock(int start, int end)
    {
        BlockBuilder builder = new BlockBuilder(TupleInfo.SINGLE_VARBINARY);

        for (int i = start; i < end; i++) {
            builder.append(String.valueOf(i));
        }

        return builder.build();
    }

    public static Block createBooleansBlock(@Nullable Boolean... values)
    {
        return createBooleansBlock(Arrays.asList(values));
    }

    public static Block createBooleansBlock(Boolean value, int count)
    {
        return createBooleansBlock(Collections.nCopies(count, value));
    }

    public static Block createBooleansBlock(Iterable<Boolean> values)
    {
        BlockBuilder builder = new BlockBuilder(TupleInfo.SINGLE_BOOLEAN);

        for (Boolean value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                builder.append(value);
            }
        }

        return builder.build();
    }

    // This method makes it easy to create blocks without having to add an L to every value
    public static Block createLongsBlock(int... values)
    {
        BlockBuilder builder = new BlockBuilder(TupleInfo.SINGLE_LONG);

        for (int value : values) {
            builder.append((long) value);
        }

        return builder.build();
    }

    public static Block createLongsBlock(@Nullable Long... values)
    {
        return createLongsBlock(Arrays.asList(values));
    }

    public static Block createLongsBlock(Iterable<Long> values)
    {
        BlockBuilder builder = new BlockBuilder(TupleInfo.SINGLE_LONG);

        for (Long value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                builder.append(value);
            }
        }

        return builder.build();
    }

    public static BlockIterable createLongsBlockIterable(int... values)
    {
        return BlockIterables.createBlockIterable(createLongsBlock(values));
    }

    public static BlockIterable createLongsBlockIterable(@Nullable Long... values)
    {
        return BlockIterables.createBlockIterable(createLongsBlock(values));
    }

    public static Block createLongSequenceBlock(int start, int end)
    {
        BlockBuilder builder = new BlockBuilder(TupleInfo.SINGLE_LONG);

        for (int i = start; i < end; i++) {
            builder.append(i);
        }

        return builder.build();
    }

    public static Block createCompositeTupleSequenceBlock(int start, int end)
    {
        BlockBuilder builder = new BlockBuilder(COMPOSITE_SEQUENCE_TUPLE_INFO);

        for (int i = start; i < end; i++) {
            builder.append(i % 2 == 0)
                    .append((long) i)
                    .append((double) i)
                    .append(Long.toString(i));
        }

        return builder.build();
    }

    public static Block createDoublesBlock(@Nullable Double... values)
    {
        return createDoublesBlock(Arrays.asList(values));
    }

    public static Block createDoublesBlock(Iterable<Double> values)
    {
        BlockBuilder builder = new BlockBuilder(TupleInfo.SINGLE_DOUBLE);

        for (Double value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                builder.append(value);
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
        BlockBuilder builder = new BlockBuilder(TupleInfo.SINGLE_DOUBLE);

        for (int i = start; i < end; i++) {
            builder.append((double) i);
        }

        return builder.build();
    }

    public static BlockIterableBuilder blockIterableBuilder(Type... types)
    {
        return new BlockIterableBuilder(new TupleInfo(types));
    }

    public static BlockIterableBuilder blockIterableBuilder(TupleInfo tupleInfo)
    {
        return new BlockIterableBuilder(tupleInfo);
    }

    public static class BlockIterableBuilder
    {
        private final List<Block> blocks = new ArrayList<>();
        private BlockBuilder blockBuilder;

        private BlockIterableBuilder(TupleInfo tupleInfo)
        {
            blockBuilder = new BlockBuilder(tupleInfo);
        }

        public BlockIterableBuilder append(Tuple tuple)
        {
            blockBuilder.append(tuple);
            return this;
        }

        public BlockIterableBuilder append(Slice value)
        {
            blockBuilder.append(value);
            return this;
        }

        public BlockIterableBuilder append(double value)
        {
            blockBuilder.append(value);
            return this;
        }

        public BlockIterableBuilder append(long value)
        {
            blockBuilder.append(value);
            return this;
        }

        public BlockIterableBuilder append(String value)
        {
            blockBuilder.append(value.getBytes(UTF_8));
            return this;
        }

        public BlockIterableBuilder append(byte[] value)
        {
            blockBuilder.append(value);
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
                blockBuilder = new BlockBuilder(block.getTupleInfo());
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
