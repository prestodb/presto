package com.facebook.presto;

import com.facebook.presto.aggregations.SumAggregation;
import com.facebook.presto.operators.GroupByBlockStream;
import com.facebook.presto.operators.HashAggregationBlockStream;
import com.facebook.presto.operators.PipelinedAggregationBlockStream;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.Blocks.createBlock;
import static com.facebook.presto.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.TupleInfo.Type.VARIABLE_BINARY;
import static com.facebook.presto.Tuples.createTuple;
import static com.google.common.base.Charsets.UTF_8;

public class TestAggregations
{
    @Test
    public void testPipelinedAggregation()
    {
        GroupByBlockStream groupBy = new GroupByBlockStream(newGroupColumn());
        PipelinedAggregationBlockStream aggregation = new PipelinedAggregationBlockStream(groupBy,
                newAggregateColumn(),
                SumAggregation.PROVIDER);

        List<Pair> expected = ImmutableList.of(
                new Pair(0, createTuple("apple", 10L)),
                new Pair(1, createTuple("banana", 17L)),
                new Pair(2, createTuple("cherry", 15L)),
                new Pair(3, createTuple("date", 6L))
        );

        List<Pair> actual = new ArrayList<>();
        Cursor cursor = aggregation.cursor();
        while (cursor.hasNextValue()) {
            cursor.advanceNextValue();
            long position = cursor.getPosition();
            Tuple tuple = cursor.getTuple();
            actual.add(new Pair(position, tuple));
        }

        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testHashAggregation()
    {
        GroupByBlockStream groupBy = new GroupByBlockStream(newGroupColumn());
        HashAggregationBlockStream aggregation = new HashAggregationBlockStream(groupBy,
                newAggregateColumn(),
                SumAggregation.PROVIDER);

        Map<Object, Object> expected = ImmutableMap.<Object, Object>of(
                "apple", createTuple("apple", 10L),
                "banana", createTuple("banana", 17L),
                "cherry", createTuple("cherry", 15L),
                "date", createTuple("date", 6L)
        );

        Map<Object, Object> actual = new HashMap<>();
        Cursor cursor = aggregation.cursor();
        while (cursor.hasNextValue()) {
            cursor.advanceNextValue();
            Tuple tuple = cursor.getTuple();
            String key = tuple.getSlice(0).toString(UTF_8);
            actual.put(key, tuple);
        }

        Assert.assertEquals(actual, expected);
    }

    public BlockStream<UncompressedValueBlock> newGroupColumn()
    {
        List<UncompressedValueBlock> values = ImmutableList.<UncompressedValueBlock>builder()
                .add(createBlock(0, "apple", "apple", "apple", "apple", "banana", "banana"))
                .add(createBlock(20, "banana", "banana", "banana", "cherry", "cherry", "cherry"))
                .add(createBlock(30, "date"))
                .add(createBlock(31, "date"))
                .add(createBlock(32, "date"))
                .build();

        return new UncompressedBlockStream(new TupleInfo(VARIABLE_BINARY), values);
    }

    public BlockStream<UncompressedValueBlock> newAggregateColumn()
    {
        List<UncompressedValueBlock> values = ImmutableList.<UncompressedValueBlock>builder()
                .add(createBlock(0, 1L, 2L, 3L, 4L, 5L, 6L))
                .add(createBlock(20, 1L, 2L, 3L, 4L, 5L, 6L))
                .add(createBlock(30, 1L))
                .add(createBlock(31, 2L))
                .add(createBlock(32, 3L))
                .build();

        return new UncompressedBlockStream(new TupleInfo(FIXED_INT_64), values);
    }

}
