package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.serde.FileBlocksSerde.FileEncoding;
import com.facebook.presto.tpch.TpchSchema.Orders;
import com.facebook.presto.tpch.TpchBlocksProvider;

public class RawStreamingOperatorBenchmark
        extends AbstractOperatorBenchmark
{
    public RawStreamingOperatorBenchmark()
    {
        super("op_raw_stream", 10, 100);
    }

    @Override
    protected Operator createBenchmarkedOperator(TpchBlocksProvider inputStreamProvider)
    {
        BlockIterable totalPrice = inputStreamProvider.getBlocks(Orders.TOTALPRICE, FileEncoding.RAW);
        return new AlignmentOperator(totalPrice);
    }

    public static void main(String[] args)
    {
        new RawStreamingOperatorBenchmark().runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
