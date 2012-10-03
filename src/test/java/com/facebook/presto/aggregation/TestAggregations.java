package com.facebook.presto.aggregation;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.GenericTupleStream;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.Blocks;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.operator.GroupByOperator;
import com.facebook.presto.operator.HashAggregationOperator;
import com.facebook.presto.operator.PipelinedAggregationOperator;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.TupleInfo.Type.VARIABLE_BINARY;
import static com.facebook.presto.block.Blocks.assertTupleStreamEquals;
import static com.facebook.presto.block.Blocks.assertTupleStreamEqualsIgnoreOrder;
import static com.facebook.presto.block.Blocks.tupleStreamBuilder;
import static com.facebook.presto.block.Blocks.createBlock;

public class TestAggregations
{
    @Test
    public void testPipelinedAggregation()
    {
        GroupByOperator groupBy = new GroupByOperator(newGroupColumn());
        PipelinedAggregationOperator aggregation = new PipelinedAggregationOperator(groupBy,
                newAggregateColumn(),
                SumAggregation.PROVIDER);

        assertTupleStreamEquals(aggregation,
                tupleStreamBuilder(VARIABLE_BINARY, FIXED_INT_64)
                        .append("apple").append(10L)
                        .append("banana").append(17L)
                        .append("cherry").append(15L)
                        .append("date").append(6L)
                        .build());
    }

    @Test
    public void testHashAggregation()
    {
        GroupByOperator groupBy = new GroupByOperator(newGroupColumn());
        HashAggregationOperator aggregation = new HashAggregationOperator(groupBy,
                newAggregateColumn(),
                SumAggregation.PROVIDER);

        assertTupleStreamEqualsIgnoreOrder(aggregation,
                tupleStreamBuilder(VARIABLE_BINARY, FIXED_INT_64)
                        .append("apple").append(10L)
                        .append("banana").append(17L)
                        .append("cherry").append(15L)
                        .append("date").append(6L)
                        .build());
    }

    public TupleStream newGroupColumn()
    {
        List<UncompressedBlock> values = ImmutableList.<UncompressedBlock>builder()
                .add(createBlock(0, "apple", "apple", "apple", "apple", "banana", "banana"))
                .add(createBlock(20, "banana", "banana", "banana", "cherry", "cherry", "cherry"))
                .add(createBlock(30, "date"))
                .add(createBlock(31, "date"))
                .add(createBlock(32, "date"))
                .build();

        return new GenericTupleStream<>(TupleInfo.SINGLE_VARBINARY, values);
    }

    public TupleStream newAggregateColumn()
    {
        List<UncompressedBlock> values = ImmutableList.<UncompressedBlock>builder()
                .add(Blocks.createLongsBlock(0L, 1L, 2L, 3L, 4L, 5L, 6L))
                .add(Blocks.createLongsBlock(20, 1L, 2L, 3L, 4L, 5L, 6L))
                .add(Blocks.createLongsBlock(30, 1L))
                .add(Blocks.createLongsBlock(31, 2L))
                .add(Blocks.createLongsBlock(32, 3L))
                .build();

        return new GenericTupleStream<>(TupleInfo.SINGLE_LONG, values);
    }

}
