package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.InMemoryOrderByOperator;
import com.facebook.presto.operator.LimitOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.facebook.presto.tpch.TpchSchema;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;

public class InMemoryOrderByBenchmark
    extends AbstractOperatorBenchmark
{
    public InMemoryOrderByBenchmark()
    {
        super("in_memory_orderby_100k", 3, 30);
    }

    @Override
    protected Operator createBenchmarkedOperator(TpchBlocksProvider inputStreamProvider)
    {
        BlockIterable totalPrice = inputStreamProvider.getBlocks(TpchSchema.Orders.TOTALPRICE, BlocksFileEncoding.RAW);
        AlignmentOperator alignmentOperator = new AlignmentOperator(totalPrice);
        LimitOperator limitOperator = new LimitOperator(alignmentOperator, 100_000);
        return new InMemoryOrderByOperator(limitOperator, 0, ImmutableList.of(singleColumn(TupleInfo.Type.DOUBLE, 0, 0)));
    }

    public static void main(String[] args)
    {
        new InMemoryOrderByBenchmark().runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
