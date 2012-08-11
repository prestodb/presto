package com.facebook.presto;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.PeekingIterator;
import org.testng.annotations.Test;

import javax.inject.Provider;
import java.util.Iterator;

public class TestSumAggregation
{
    @Test
    public void test()
    {
        PipelinedAggregation aggregation = new PipelinedAggregation(newGroupColumn(), new ForwardingSeekableIterator<ValueBlock>(newAggregateColumn()), new Provider<AggregationFunction>()
        {
            @Override
            public AggregationFunction get()
            {
                return new SumAggregation();
            }
        });

//        DataScan3 materialize = new DataScan3(newGroupColumn(), )
        while (aggregation.hasNext()) {
            ValueBlock block = aggregation.next();
            PeekingIterator<Pair> pairs = block.pairIterator();
            while (pairs.hasNext()) {
                System.out.println(pairs.next());
            }
            System.out.println();
        }
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
