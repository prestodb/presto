package com.facebook.presto.benchmark;

import com.facebook.presto.tpch.TpchBlocksProvider;

public class SqlDistinctMultipleFields
        extends AbstractSqlBenchmark
{
    public SqlDistinctMultipleFields(TpchBlocksProvider tpchBlocksProvider)
    {
        super(tpchBlocksProvider, "sql_distinct_multi", 4, 10, "SELECT DISTINCT orderpriority, shippriority FROM orders");
    }

    public static void main(String[] args)
    {
        new SqlDistinctMultipleFields(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
