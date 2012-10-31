package com.facebook.presto.benchmark;

import com.facebook.presto.TupleInfo.Type;
import com.facebook.presto.block.TupleStreamSerdes.Encoding;
import com.facebook.presto.nblock.Blocks;
import com.facebook.presto.noperator.AlignmentOperator;
import com.facebook.presto.noperator.HashAggregationOperator;
import com.facebook.presto.noperator.Operator;
import com.facebook.presto.noperator.aggregation.DoubleSumAggregation;
import com.facebook.presto.tpch.TpchSchema.Orders;
import com.facebook.presto.tpch.TpchTupleStreamProvider;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.noperator.ProjectionFunctions.concat;
import static com.facebook.presto.noperator.ProjectionFunctions.singleColumn;

public class HashAggregationOperatorBenchmark
        extends AbstractOperatorBenchmark
{
    public HashAggregationOperatorBenchmark()
    {
        super("op_hash_agg", 5, 25);
    }

    @Override
    protected Operator createBenchmarkedOperator(TpchTupleStreamProvider inputStreamProvider)
    {
        Blocks orderStatus = inputStreamProvider.getBlocks(Orders.ORDERSTATUS, Encoding.RAW);
        Blocks totalPrice = inputStreamProvider.getBlocks(Orders.TOTALPRICE, Encoding.RAW);

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
