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

import static com.facebook.presto.SizeOf.SIZE_OF_LONG;
import static com.facebook.presto.SizeOf.SIZE_OF_SHORT;
import static com.facebook.presto.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.base.Charsets.UTF_8;

public class TestSumAggregation
{
    @Test
    public void testPipelinedAggregation()
    {
        GroupBy groupBy = new GroupBy(newGroupColumn());
        PipelinedAggregation aggregation = new PipelinedAggregation(new TupleInfo(VARIABLE_BINARY, FIXED_INT_64),
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
                new Pair(0, createTuple("apple", 10L)),
                new Pair(1, createTuple("banana", 17L)),
                new Pair(2, createTuple("cherry", 15L)),
                new Pair(3, createTuple("date", 6L))
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

    private Tuple createTuple(String key, long count)
    {
        byte[] bytes = key.getBytes(Charsets.UTF_8);
        Slice slice = Slices.allocate(SIZE_OF_LONG + SIZE_OF_SHORT + bytes.length);

        slice.output()
                .appendLong(count)
                .appendShort(10 + bytes.length)
                .appendBytes(bytes);

        return new Tuple(slice, new TupleInfo(VARIABLE_BINARY, FIXED_INT_64));
    }

    @Test
    public void testHashAggregation()
    {
        GroupBy groupBy = new GroupBy(newGroupColumn());
        HashAggregation aggregation = new HashAggregation(new TupleInfo(VARIABLE_BINARY, FIXED_INT_64),
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
                "apple", createTuple("apple", 10L),
                "banana", createTuple("banana", 17L),
                "cherry", createTuple("cherry", 15L),
                "date", createTuple("date", 6L)
        );

        Map<Object, Object> actual = new HashMap<>();
        while (aggregation.hasNext()) {
            ValueBlock block = aggregation.next();
            PeekingIterator<Pair> pairs = block.pairIterator();
            while (pairs.hasNext()) {
                Pair pair = pairs.next();
                Tuple tuple = pair.getValue();
                actual.put(tuple.getSlice(0).toString(UTF_8), tuple);
            }
        }

        Assert.assertEquals(actual, expected);
    }

    public Iterator<ValueBlock> newGroupColumn()
    {
        Iterator<ValueBlock> values = ImmutableList.<ValueBlock>builder()
                .add(createBlock(0, "apple", "apple", "apple", "apple", "banana", "banana"))
                .add(createBlock(20, "banana", "banana", "banana", "cherry", "cherry", "cherry"))
                .add(createBlock(30, "date"))
                .add(createBlock(31, "date"))
                .add(createBlock(32, "date"))
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

    private ValueBlock createBlock(int position, String... values)
    {
        BlockBuilder builder = new BlockBuilder(position, new TupleInfo(VARIABLE_BINARY));

        for (String value : values) {
            builder.append(value.getBytes(UTF_8));
        }

        return builder.build();
    }

    private ValueBlock createBlock(long position, long... values)
    {
        BlockBuilder builder = new BlockBuilder(position, new TupleInfo(FIXED_INT_64));

        for (long value : values) {
            builder.append(value);
        }

        return builder.build();
    }
}
