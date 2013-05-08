package com.facebook.presto.benchmark;

import com.facebook.presto.tpch.TpchBlocksProvider;

public class SqlApproximateCountDistinctLongBenchmark
        extends AbstractSqlBenchmark
{
    public SqlApproximateCountDistinctLongBenchmark(TpchBlocksProvider tpchBlocksProvider)
    {
        super(tpchBlocksProvider, "sql_approx_count_distinct_long", 10, 50, "select approx_distinct(custkey) from orders");
    }

    public static void main(String[] args)
    {
        new SqlApproximateCountDistinctLongBenchmark(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
