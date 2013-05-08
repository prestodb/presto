package com.facebook.presto.benchmark;

import com.facebook.presto.tpch.TpchBlocksProvider;

public class SqlLikeBenchmark
        extends AbstractSqlBenchmark
{
    public SqlLikeBenchmark(TpchBlocksProvider tpchBlocksProvider)
    {
        super(tpchBlocksProvider, "sql_like", 4, 5, "SELECT orderkey FROM lineitem WHERE comment LIKE '%ly%ly%'");
    }

    public static void main(String[] args)
    {
        new SqlLikeBenchmark(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
