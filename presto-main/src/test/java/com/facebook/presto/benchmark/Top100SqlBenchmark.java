package com.facebook.presto.benchmark;

import com.facebook.presto.tpch.TpchBlocksProvider;

public class Top100SqlBenchmark
        extends AbstractSqlBenchmark
{
    public Top100SqlBenchmark(TpchBlocksProvider tpchBlocksProvider)
    {
        super(tpchBlocksProvider, "sql_top_100", 5, 50, "select totalprice from orders order by totalprice desc limit 100");
    }

    public static void main(String[] args)
    {
        new Top100SqlBenchmark(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
