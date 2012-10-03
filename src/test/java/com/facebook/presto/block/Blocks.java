package com.facebook.presto.block;

import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.TupleInfo.Type;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.slice.Slice;
import com.google.common.collect.ImmutableList;
import org.testng.Assert;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Charsets.UTF_8;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class Blocks
{
    public static void assertTupleStreamEquals(TupleStream actual, TupleStream expected)
    {
        Assert.assertEquals(actual.getTupleInfo(), expected.getTupleInfo());
        assertCursorsEquals(actual.cursor(), expected.cursor());
    }
    
    public static void assertCursorsEquals(Cursor actualCursor, Cursor expectedCursor)
    {
        Assert.assertEquals(actualCursor.getTupleInfo(), expectedCursor.getTupleInfo());
        while (advanceAllCursorsToNextPosition(actualCursor, expectedCursor)) {
            assertEquals(actualCursor.getTuple(), expectedCursor.getTuple());
        }
        assertTrue(actualCursor.isFinished());
        assertTrue(expectedCursor.isFinished());
    }

    public static void assertTupleStreamEqualsIgnoreOrder(TupleStream actual, TupleStream expected)
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
        while(Cursors.advanceNextPositionNoYield(actualCursor)) {
            tuples.add(actualCursor.getTuple());
        }
        return tuples.build();
    }

    private static boolean advanceAllCursorsToNextPosition(Cursor... cursors)
    {
        boolean allAdvanced = true;
        for (Cursor cursor : cursors) {
            allAdvanced = Cursors.advanceNextPositionNoYield(cursor) && allAdvanced;
        }
        return allAdvanced;
    }

    public static TupleStream createTupleStream(int position, String... values)
    {
        return new GenericTupleStream<>(TupleInfo.SINGLE_VARBINARY, createBlock(position, values));
    }

    public static UncompressedBlock createBlock(long position, String... values)
    {
        return createStringBlock(position, ImmutableList.copyOf(values));
    }

    public static UncompressedBlock createStringBlock(long position, Iterable<String> values)
    {
        BlockBuilder builder = new BlockBuilder(position, TupleInfo.SINGLE_VARBINARY);

        for (String value : values) {
            builder.append(value.getBytes(UTF_8));
        }

        return builder.build();
    }

    public static TupleStream createLongsTupleStream(long position, long... values)
    {
        return new GenericTupleStream<>(TupleInfo.SINGLE_LONG, createLongsBlock(position, values));
    }

    public static UncompressedBlock createLongsBlock(long position, long... values)
    {
        BlockBuilder builder = new BlockBuilder(position, TupleInfo.SINGLE_LONG);

        for (long value : values) {
            builder.append(value);
        }

        return builder.build();
    }

    public static TupleStream createDoublesTupleStream(long position, double... values)
    {
        return new GenericTupleStream<>(TupleInfo.SINGLE_DOUBLE, createDoublesBlock(position, values));
    }

    public static UncompressedBlock createDoublesBlock(long position, double... values)
    {
        BlockBuilder builder = new BlockBuilder(position, TupleInfo.SINGLE_DOUBLE);

        for (double value : values) {
            builder.append(value);
        }

        return builder.build();
    }

    public static TupleStreamBuilder tupleStreamBuilder(Type... types) {
        return new TupleStreamBuilder(0, new TupleInfo(types));
    }

    public static TupleStreamBuilder tupleStreamBuilder(TupleInfo tupleInfo) {
        return new TupleStreamBuilder(0, tupleInfo);
    }

    public static TupleStreamBuilder tupleStreamBuilder(int startPosition, TupleInfo tupleInfo) {
        return new TupleStreamBuilder(startPosition, tupleInfo);
    }

    public static class TupleStreamBuilder
    {
        private final List<UncompressedBlock> blocks = new ArrayList<>();
        private final BlockBuilder blockBuilder;
        private final TupleInfo tupleInfo;

        private TupleStreamBuilder(int startPosition, TupleInfo tupleInfo)
        {
            this.tupleInfo = tupleInfo;
            blockBuilder = new BlockBuilder(startPosition, tupleInfo);
        }

        public TupleStreamBuilder append(Tuple tuple)
        {
            blockBuilder.append(tuple);
            return this;
        }

        public TupleStreamBuilder append(Slice value)
        {
            blockBuilder.append(value);
            return this;
        }

        public TupleStreamBuilder append(double value)
        {
            blockBuilder.append(value);
            return this;
        }

        public TupleStreamBuilder append(long value)
        {
            blockBuilder.append(value);
            return this;
        }

        public TupleStreamBuilder append(String value)
        {
            blockBuilder.append(value.getBytes(UTF_8));
            return this;
        }

        public TupleStreamBuilder append(byte[] value)
        {
            blockBuilder.append(value);
            return this;
        }

        public TupleStreamBuilder newBlock()
        {
            if (!blockBuilder.isEmpty()) {
                blocks.add(blockBuilder.build());
            }
            return this;
        }

        public TupleStream build() {
            newBlock();
            return new GenericTupleStream<>(tupleInfo, blocks);
        }
    }
}
