package com.facebook.presto.benchmark;

import com.facebook.presto.tpch.TpchBlocksProvider;

public class SqlApproximatePercentileBenchmark
        extends AbstractSqlBenchmark
{
    public SqlApproximatePercentileBenchmark(TpchBlocksProvider tpchBlocksProvider)
    {
        super(tpchBlocksProvider, "sql_approx_percentile_long", 10, 30, "select approx_percentile(custkey, 0.9) from orders");
    }

    public static void main(String[] args)
    {
        new SqlApproximatePercentileBenchmark(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
