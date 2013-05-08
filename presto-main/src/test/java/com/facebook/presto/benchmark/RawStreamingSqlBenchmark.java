package com.facebook.presto.benchmark;

import com.facebook.presto.tpch.TpchBlocksProvider;

public class RawStreamingSqlBenchmark
        extends AbstractSqlBenchmark
{
    public RawStreamingSqlBenchmark(TpchBlocksProvider tpchBlocksProvider)
    {
        super(tpchBlocksProvider, "sql_raw_stream", 10, 100, "select totalprice from orders");
    }

    public static void main(String[] args)
    {
        new RawStreamingSqlBenchmark(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
