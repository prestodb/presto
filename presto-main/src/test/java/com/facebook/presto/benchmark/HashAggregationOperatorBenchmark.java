package com.facebook.presto.benchmark;

import com.facebook.presto.tuple.TupleInfo.Type;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.HashAggregationOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.aggregation.DoubleSumAggregation;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.tpch.TpchSchema.Orders;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.operator.ProjectionFunctions.concat;
import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;

public class HashAggregationOperatorBenchmark
        extends AbstractOperatorBenchmark
{
    public HashAggregationOperatorBenchmark()
    {
        super("op_hash_agg", 5, 25);
    }

    @Override
    protected Operator createBenchmarkedOperator(TpchBlocksProvider inputStreamProvider)
    {
        BlockIterable orderStatus = inputStreamProvider.getBlocks(Orders.ORDERSTATUS, BlocksFileEncoding.RAW);
        BlockIterable totalPrice = inputStreamProvider.getBlocks(Orders.TOTALPRICE, BlocksFileEncoding.RAW);

        AlignmentOperator alignmentOperator = new AlignmentOperator(orderStatus, totalPrice);
        return new HashAggregationOperator(alignmentOperator,
                0,
                ImmutableList.of(DoubleSumAggregation.provider(1, 0)),
                ImmutableList.of(concat(singleColumn(Type.VARIABLE_BINARY, 0, 0), singleColumn(Type.DOUBLE, 1, 0))));

    }

    public static void main(String[] args)
    {
        new HashAggregationOperatorBenchmark().runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
