package com.facebook.presto.block;

import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.TupleInfo.Type;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.block.uncompressed.UncompressedBlockStream;
import com.facebook.presto.slice.Slice;
import com.google.common.collect.ImmutableList;
import org.testng.Assert;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.TupleInfo.Type.DOUBLE;
import static com.facebook.presto.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.base.Charsets.UTF_8;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class Blocks
{
    public static void assertBlockStreamEquals(TupleStream actual, TupleStream expected)
    {
        Assert.assertEquals(actual.getTupleInfo(), expected.getTupleInfo());
        Cursor actualCursor = actual.cursor();
        Cursor expectedCursor = expected.cursor();
        while (advanceAllCursorsToNextPosition(actualCursor, expectedCursor)) {
            assertEquals(actualCursor.getTuple(), expectedCursor.getTuple());
        }
        assertTrue(actualCursor.isFinished());
        assertTrue(expectedCursor.isFinished());
    }

    public static void assertBlockStreamEqualsIgnoreOrder(TupleStream actual, TupleStream expected)
    {
        Assert.assertEquals(actual.getTupleInfo(), expected.getTupleInfo());

        List<Tuple> actualTuples = toTuplesList(actual);
        List<Tuple> expectedTuples = toTuplesList(expected);
        assertEqualsIgnoreOrder(actualTuples, expectedTuples);
    }

    public static List<Tuple> toTuplesList(TupleStream tupleStream)
    {
        ImmutableList.Builder<Tuple> tuples = ImmutableList.builder();
        Cursor actualCursor = tupleStream.cursor();
        while(actualCursor.advanceNextPosition()) {
            tuples.add(actualCursor.getTuple());
        }
        return tuples.build();
    }

    private static boolean advanceAllCursorsToNextPosition(Cursor... cursors)
    {
        boolean allAdvanced = true;
        for (Cursor cursor : cursors) {
            allAdvanced = cursor.advanceNextPosition() && allAdvanced;
        }
        return allAdvanced;
    }

    public static UncompressedBlockStream createBlockStream(int position, String... values)
    {
        return new UncompressedBlockStream(new TupleInfo(VARIABLE_BINARY), createBlock(position, values));
    }

    public static UncompressedBlock createBlock(long position, String... values)
    {
        BlockBuilder builder = new BlockBuilder(position, new TupleInfo(VARIABLE_BINARY));

        for (String value : values) {
            builder.append(value.getBytes(UTF_8));
        }

        return builder.build();
    }

    public static UncompressedBlockStream createLongsBlockStream(long position, long... values)
    {
        return new UncompressedBlockStream(new TupleInfo(FIXED_INT_64), createLongsBlock(position, values));
    }

    public static UncompressedBlock createLongsBlock(long position, long... values)
    {
        BlockBuilder builder = new BlockBuilder(position, new TupleInfo(FIXED_INT_64));

        for (long value : values) {
            builder.append(value);
        }

        return builder.build();
    }

    public static UncompressedBlockStream createDoublesBlockStream(long position, double... values)
    {
        return new UncompressedBlockStream(new TupleInfo(DOUBLE), createDoublesBlock(position, values));
    }

    public static UncompressedBlock createDoublesBlock(long position, double... values)
    {
        BlockBuilder builder = new BlockBuilder(position, new TupleInfo(DOUBLE));

        for (double value : values) {
            builder.append(value);
        }

        return builder.build();
    }

    public static BlockStreamBuilder blockStreamBuilder(Type... types) {
        return new BlockStreamBuilder(0, new TupleInfo(types));
    }

    public static BlockStreamBuilder blockStreamBuilder(TupleInfo tupleInfo) {
        return new BlockStreamBuilder(0, tupleInfo);
    }

    public static BlockStreamBuilder blockStreamBuilder(int startPosition, TupleInfo tupleInfo) {
        return new BlockStreamBuilder(startPosition, tupleInfo);
    }

    public static class BlockStreamBuilder
    {
        private final List<UncompressedBlock> blocks = new ArrayList<>();
        private final BlockBuilder blockBuilder;
        private final TupleInfo tupleInfo;

        private BlockStreamBuilder(int startPosition, TupleInfo tupleInfo)
        {
            this.tupleInfo = tupleInfo;
            blockBuilder = new BlockBuilder(startPosition, tupleInfo);
        }

        public BlockStreamBuilder append(Tuple tuple)
        {
            blockBuilder.append(tuple);
            return this;
        }

        public BlockStreamBuilder append(Slice value)
        {
            blockBuilder.append(value);
            return this;
        }

        public BlockStreamBuilder append(double value)
        {
            blockBuilder.append(value);
            return this;
        }

        public BlockStreamBuilder append(long value)
        {
            blockBuilder.append(value);
            return this;
        }

        public BlockStreamBuilder append(String value)
        {
            blockBuilder.append(value.getBytes(UTF_8));
            return this;
        }

        public BlockStreamBuilder append(byte[] value)
        {
            blockBuilder.append(value);
            return this;
        }

        public BlockStreamBuilder newBlock()
        {
            if (!blockBuilder.isEmpty()) {
                blocks.add(blockBuilder.build());
            }
            return this;
        }

        public UncompressedBlockStream build() {
            newBlock();
            return new UncompressedBlockStream(tupleInfo, blocks);
        }
    }
}
