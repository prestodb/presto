package com.facebook.presto.benchmark;

import com.facebook.presto.nblock.BlockIterable;
import com.facebook.presto.noperator.AlignmentOperator;
import com.facebook.presto.noperator.Operator;
import com.facebook.presto.serde.BlockSerdes.Encoding;
import com.facebook.presto.tpch.TpchSchema.Orders;
import com.facebook.presto.tpch.TpchTupleStreamProvider;

public class RawStreamingOperatorBenchmark
        extends AbstractOperatorBenchmark
{
    public RawStreamingOperatorBenchmark()
    {
        super("op_raw_stream", 10, 100);
    }

    @Override
    protected Operator createBenchmarkedOperator(TpchTupleStreamProvider inputStreamProvider)
    {
        BlockIterable totalPrice = inputStreamProvider.getBlocks(Orders.TOTALPRICE, Encoding.RAW);
        return new AlignmentOperator(totalPrice);
    }

    public static void main(String[] args)
    {
        new RawStreamingOperatorBenchmark().runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
