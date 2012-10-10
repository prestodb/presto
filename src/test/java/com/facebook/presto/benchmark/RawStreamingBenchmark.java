package com.facebook.presto.benchmark;

import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.TupleStreamSerdes;
import com.facebook.presto.tpch.TpchSchema;
import com.facebook.presto.tpch.TpchTupleStreamProvider;

public class RawStreamingBenchmark
        extends AbstractTupleStreamBenchmark
{
    public RawStreamingBenchmark()
    {
        super("raw_stream", 10, 100);
    }

    @Override
    protected TupleStream createBenchmarkedTupleStream(TpchTupleStreamProvider inputStreamProvider)
    {
        return inputStreamProvider.getTupleStream(TpchSchema.Orders.TOTALPRICE, TupleStreamSerdes.Encoding.RAW);
    }

    public static void main(String[] args)
    {
        new RawStreamingBenchmark().runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
