package com.facebook.presto.benchmark;

import com.facebook.presto.tpch.TpchBlocksProvider;

public class CountWithFilterSqlBenchmark
        extends AbstractSqlBenchmark
{
    public CountWithFilterSqlBenchmark(TpchBlocksProvider tpchBlocksProvider)
    {
        super(tpchBlocksProvider, "sql_count_with_filter", 10, 100, "SELECT count(*) from orders where orderstatus = 'F'");
    }

    public static void main(String[] args)
    {
        new CountWithFilterSqlBenchmark(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
