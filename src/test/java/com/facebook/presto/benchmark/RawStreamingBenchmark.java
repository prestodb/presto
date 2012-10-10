package com.facebook.presto.benchmark;

import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.TupleStreamSerdes;
import com.facebook.presto.tpch.TpchSchema;

import java.util.List;

public class RawStreamingBenchmark
        extends AbstractTupleStreamBenchmark
{
    public RawStreamingBenchmark()
    {
        super("raw_stream", 10, 100);
    }

    @Override
    protected void setUp()
    {
        loadColumnFile(TpchSchema.Orders.TOTALPRICE, TupleStreamSerdes.Encoding.RAW);
    }

    @Override
    protected TupleStream createBenchmarkedTupleStream(List<? extends TupleStream> inputTupleStreams)
    {
        return inputTupleStreams.get(0);
    }

    public static void main(String[] args)
    {
        new RawStreamingBenchmark().runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
