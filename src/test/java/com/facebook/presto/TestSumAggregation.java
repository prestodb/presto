package com.facebook.presto;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.PeekingIterator;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.inject.Provider;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.SizeOf.SIZE_OF_BYTE;
import static com.facebook.presto.SizeOf.SIZE_OF_LONG;

public class TestSumAggregation
{
    @Test
    public void testPipelinedAggregation()
    {
        GroupBy groupBy = new GroupBy(newGroupColumn());
        PipelinedAggregation aggregation = new PipelinedAggregation(new TupleInfo(SIZE_OF_BYTE, SIZE_OF_LONG),
                groupBy,
                new ForwardingSeekableIterator<>(newAggregateColumn()),
                new Provider<AggregationFunction>()
                {
                    @Override
                    public AggregationFunction get()
                    {
                        return new SumAggregation();
                    }
                });

        List<Pair> expected = ImmutableList.of(
                new Pair(0, createTuple("a", 10L)),
                new Pair(1, createTuple("b", 17L)),
                new Pair(2, createTuple("c", 15L)),
                new Pair(3, createTuple("d", 6L))
        );

        List<Pair> actual = new ArrayList<>();
        while (aggregation.hasNext()) {
            ValueBlock block = aggregation.next();
            PeekingIterator<Pair> pairs = block.pairIterator();
            while (pairs.hasNext()) {
                Pair pair = pairs.next();
                actual.add(pair);
            }
        }

        Assert.assertEquals(actual, expected);
    }

    private Tuple createTuple(String character, long count)
    {
        Slice slice = Slices.allocate(SIZE_OF_BYTE + SIZE_OF_LONG);
        slice.setByte(0, character.charAt(0));
        slice.setLong(1, count);
        return new Tuple(slice, new TupleInfo(SIZE_OF_BYTE, SIZE_OF_LONG));
    }

    @Test
    public void testHashAggregation()
    {
        GroupBy groupBy = new GroupBy(newGroupColumn());
        HashAggregation aggregation = new HashAggregation(new TupleInfo(SIZE_OF_BYTE, SIZE_OF_LONG),
                groupBy,
                new ForwardingSeekableIterator<>(newAggregateColumn()),
                new Provider<AggregationFunction>()
                {
                    @Override
                    public AggregationFunction get()
                    {
                        return new SumAggregation();
                    }
                });

        Map<Object, Object> expected = ImmutableMap.<Object, Object>of(
                "a", createTuple("a", 10L),
                "b", createTuple("b", 17L),
                "c", createTuple("c", 15L),
                "d", createTuple("d", 6L)
        );

        Map<Object, Object> actual = new HashMap<>();
        while (aggregation.hasNext()) {
            ValueBlock block = aggregation.next();
            PeekingIterator<Pair> pairs = block.pairIterator();
            while (pairs.hasNext()) {
                Pair pair = pairs.next();
                Tuple tuple = pair.getValue();
                actual.put(tuple.getSlice(0).toString(Charsets.UTF_8), tuple);
            }
        }

        Assert.assertEquals(actual, expected);
    }

    public Iterator<ValueBlock> newGroupColumn()
    {
        Iterator<ValueBlock> values = ImmutableList.<ValueBlock>builder()
                .add(new UncompressedValueBlock(0, new TupleInfo(1), Slices.wrappedBuffer(new byte[]{'a', 'a', 'a', 'a', 'b', 'b'})))
                .add(new UncompressedValueBlock(20, new TupleInfo(1), Slices.wrappedBuffer(new byte[]{'b', 'b', 'b', 'c', 'c', 'c'})))
                .add(new UncompressedValueBlock(30, new TupleInfo(1), Slices.wrappedBuffer(new byte[]{'d'})))
                .add(new UncompressedValueBlock(31, new TupleInfo(1), Slices.wrappedBuffer(new byte[]{'d'})))
                .add(new UncompressedValueBlock(32, new TupleInfo(1), Slices.wrappedBuffer(new byte[]{'d'})))
                .build()
                .iterator();

        return values;
    }

    public Iterator<ValueBlock> newAggregateColumn()
    {
        Iterator<ValueBlock> values = ImmutableList.<ValueBlock>builder()
                .add(createBlock(0, 1L, 2L, 3L, 4L, 5L, 6L))
                .add(createBlock(20, 1L, 2L, 3L, 4L, 5L, 6L))
                .add(createBlock(30, 1L))
                .add(createBlock(31, 2L))
                .add(createBlock(32, 3L))
                .build()
                .iterator();

        return values;
    }

    private UncompressedValueBlock createBlock(long position, long... values)
    {
        Slice slice = Slices.allocate(values.length * SIZE_OF_LONG);
        SliceOutput output = slice.output();
        for (long value : values) {
            output.writeLong(value);
        }
        return new UncompressedValueBlock(position, new TupleInfo(SIZE_OF_LONG), slice);
    }
}
