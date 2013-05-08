package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.serde.BlocksFileEncoding;

public class RawStreamingBenchmark
        extends AbstractOperatorBenchmark
{
    public RawStreamingBenchmark()
    {
        super("raw_stream", 10, 100);
    }

    @Override
    protected Operator createBenchmarkedOperator()
    {
        BlockIterable blockIterable = getBlockIterable("orders", "totalprice", BlocksFileEncoding.RAW);
        return new AlignmentOperator(blockIterable);
    }

    public static void main(String[] args)
    {
        new RawStreamingBenchmark().runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
