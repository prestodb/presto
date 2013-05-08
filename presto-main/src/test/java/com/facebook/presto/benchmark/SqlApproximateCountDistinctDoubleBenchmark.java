package com.facebook.presto.benchmark;

import com.facebook.presto.tpch.TpchBlocksProvider;

public class SqlApproximateCountDistinctDoubleBenchmark
        extends AbstractSqlBenchmark
{
    public SqlApproximateCountDistinctDoubleBenchmark(TpchBlocksProvider tpchBlocksProvider)
    {
        super(tpchBlocksProvider, "sql_approx_count_distinct_double", 10, 50, "select approx_distinct(totalprice) from orders");
    }

    public static void main(String[] args)
    {
        new SqlApproximateCountDistinctDoubleBenchmark(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
