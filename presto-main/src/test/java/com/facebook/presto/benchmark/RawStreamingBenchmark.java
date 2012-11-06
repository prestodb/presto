package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.tpch.TpchSchema.Orders;
import com.facebook.presto.tpch.TpchBlocksProvider;

public class RawStreamingBenchmark
        extends AbstractOperatorBenchmark
{
    public RawStreamingBenchmark()
    {
        super("raw_stream", 10, 100);
    }

    @Override
    protected Operator createBenchmarkedOperator(TpchBlocksProvider inputStreamProvider)
    {
        BlockIterable totalPrice = inputStreamProvider.getBlocks(Orders.TOTALPRICE, BlocksFileEncoding.RAW);
        return new AlignmentOperator(totalPrice);
    }

    public static void main(String[] args)
    {
        new RawStreamingBenchmark().runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
