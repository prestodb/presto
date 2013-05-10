package com.facebook.presto.benchmark;

import com.facebook.presto.tpch.TpchBlocksProvider;

public class SqlApproximateCountDistinctVarBinaryBenchmark
        extends AbstractSqlBenchmark
{
    public SqlApproximateCountDistinctVarBinaryBenchmark(TpchBlocksProvider tpchBlocksProvider)
    {
        super(tpchBlocksProvider, "sql_approx_count_distinct_varbinary", 10, 50, "select approx_distinct(orderdate) from orders");
    }

    public static void main(String[] args)
    {
        new SqlApproximateCountDistinctVarBinaryBenchmark(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
