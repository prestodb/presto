package com.facebook.presto.benchmark;

import com.facebook.presto.tpch.TpchBlocksProvider;

public class SqlDistinctSingleField
        extends AbstractSqlBenchmark
{
    public SqlDistinctSingleField(TpchBlocksProvider tpchBlocksProvider)
    {
        super(tpchBlocksProvider, "sql_distinct_single", 10, 20, "SELECT DISTINCT shippriority FROM orders");
    }

    public static void main(String[] args)
    {
        new SqlDistinctSingleField(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
