package com.facebook.presto;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.PeekingIterator;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.inject.Provider;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TestSumAggregation
{
    @Test
    public void testPipelinedAggregation()
    {
        GroupBy groupBy = new GroupBy(newGroupColumn());
        PipelinedAggregation aggregation = new PipelinedAggregation(groupBy, new ForwardingSeekableIterator<>(newAggregateColumn()), new Provider<AggregationFunction>()
        {
            @Override
            public AggregationFunction get()
            {
                return new SumAggregation();
            }
        });

        List<Pair> expected = ImmutableList.of(
                new Pair(0, new Tuple("a", 10L)),
                new Pair(4, new Tuple("b", 17L)),
                new Pair(23, new Tuple("c", 15L)),
                new Pair(30, new Tuple("d", 6L))
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

    public Iterator<ValueBlock> newGroupColumn()
    {
        Iterator<ValueBlock> values = ImmutableList.<ValueBlock>builder()
                .add(new UncompressedValueBlock(0, "a", "a", "a", "a", "b", "b"))
                .add(new UncompressedValueBlock(20, "b", "b", "b", "c", "c", "c"))
                .add(new UncompressedValueBlock(30, "d"))
                .add(new UncompressedValueBlock(31, "d"))
                .add(new UncompressedValueBlock(32, "d"))
                .build()
                .iterator();

        return values;
    }

    public Iterator<ValueBlock> newAggregateColumn()
    {
        Iterator<ValueBlock> values = ImmutableList.<ValueBlock>builder()
                .add(new UncompressedValueBlock(0, 1L, 2L, 3L, 4L, 5L, 6L))
                .add(new UncompressedValueBlock(20, 1L, 2L, 3L, 4L, 5L, 6L))
                .add(new UncompressedValueBlock(30, 1L))
                .add(new UncompressedValueBlock(31, 2L))
                .add(new UncompressedValueBlock(32, 3L))
                .build()
                .iterator();

        return values;
    }
}
