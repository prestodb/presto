package com.facebook.presto.nblock;

import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.TupleInfo.Type;
import com.facebook.presto.slice.Slice;
import com.google.common.collect.ImmutableList;
import org.testng.Assert;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.nblock.BlockIterables.createBlockIterable;
import static com.google.common.base.Charsets.UTF_8;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class BlockAssertions
{
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
        ImmutableList.Builder<List<Object>> values = ImmutableList.builder();
        BlockCursor cursor = block.cursor();
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

    private static boolean advanceAllCursorsToNextPosition(BlockCursor... cursors)
    {
        boolean allAdvanced = true;
        for (BlockCursor cursor : cursors) {
            allAdvanced = cursor.advanceNextPosition() && allAdvanced;
        }
        return allAdvanced;
    }

    public static Block createStringsBlock(long position, String... values)
    {
        return createStringsBlock(position, ImmutableList.copyOf(values));
    }

    public static Block createStringsBlock(long position, Iterable<String> values)
    {
        BlockBuilder builder = new BlockBuilder(position, TupleInfo.SINGLE_VARBINARY);

        for (String value : values) {
            builder.append(value.getBytes(UTF_8));
        }

        return builder.build();
    }

    public static BlockIterable createStringsBlockIterable(long position, String... values)
    {
        return BlockIterables.createBlockIterable(createStringsBlock(position, values));
    }

    public static Block createStringSequenceBlock(int start, int end)
    {
        BlockBuilder builder = new BlockBuilder(start, TupleInfo.SINGLE_VARBINARY);

        for (int i = start; i < end; i++) {
            builder.append(String.valueOf(i));
        }

        return builder.build();
    }

    public static Block createLongsBlock(long position, long... values)
    {
        BlockBuilder builder = new BlockBuilder(position, TupleInfo.SINGLE_LONG);

        for (long value : values) {
            builder.append(value);
        }

        return builder.build();
    }

    public static BlockIterable createLongsBlockIterable(long position, long... values)
    {
        return BlockIterables.createBlockIterable(createLongsBlock(position, values));
    }

    public static Block createLongSequenceBlock(int start, int end)
    {
        BlockBuilder builder = new BlockBuilder(start, TupleInfo.SINGLE_LONG);

        for (int i = start; i < end; i++) {
            builder.append(i);
        }

        return builder.build();
    }

    public static Block createDoublesBlock(long position, double... values)
    {
        BlockBuilder builder = new BlockBuilder(position, TupleInfo.SINGLE_DOUBLE);

        for (double value : values) {
            builder.append(value);
        }

        return builder.build();
    }

    public static BlockIterable createDoublesBlockIterable(long position, double... values)
    {
        return BlockIterables.createBlockIterable(createDoublesBlock(position, values));
    }

    public static Block createDoubleSequenceBlock(int start, int end)
    {
        BlockBuilder builder = new BlockBuilder(start, TupleInfo.SINGLE_DOUBLE);

        for (int i = start; i < end; i++) {
            builder.append((double) i);
        }

        return builder.build();
    }

    public static BlockIterableBuilder blockIterableBuilder(Type... types)
    {
        return new BlockIterableBuilder(0, new TupleInfo(types));
    }

    public static BlockIterableBuilder blockIterableBuilder(TupleInfo tupleInfo)
    {
        return new BlockIterableBuilder(0, tupleInfo);
    }

    public static BlockIterableBuilder blockIterableBuilder(int startPosition, TupleInfo tupleInfo)
    {
        return new BlockIterableBuilder(startPosition, tupleInfo);
    }

    public static class BlockIterableBuilder
    {
        private final List<Block> blocks = new ArrayList<>();
        private BlockBuilder blockBuilder;

        private BlockIterableBuilder(int startPosition, TupleInfo tupleInfo)
        {
            blockBuilder = new BlockBuilder(startPosition, tupleInfo);
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

        public BlockIterableBuilder newBlock()
        {
            if (!blockBuilder.isEmpty()) {
                Block block = blockBuilder.build();
                blocks.add(block);
                blockBuilder = new BlockBuilder(block.getRange().getEnd() + 1, block.getTupleInfo());
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
