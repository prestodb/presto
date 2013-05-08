package com.facebook.presto.benchmark;

import com.facebook.presto.tpch.TpchBlocksProvider;

public class CountAggregationSqlBenchmark
        extends AbstractSqlBenchmark
{
    public CountAggregationSqlBenchmark(TpchBlocksProvider tpchBlocksProvider)
    {
        super(tpchBlocksProvider, "sql_count_agg", 10, 100, "select count(*) from orders");
    }

    public static void main(String[] args)
    {
        new CountAggregationSqlBenchmark(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
