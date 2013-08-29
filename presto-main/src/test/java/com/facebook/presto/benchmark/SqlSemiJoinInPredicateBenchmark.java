package com.facebook.presto.benchmark;

import com.facebook.presto.tpch.TpchBlocksProvider;

public class SqlSemiJoinInPredicateBenchmark
        extends AbstractSqlBenchmark
{
    public SqlSemiJoinInPredicateBenchmark(TpchBlocksProvider tpchBlocksProvider)
    {
        super(tpchBlocksProvider, "sql_semijoin_in", 2, 4, "SELECT orderkey FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderkey % 2 = 0)");
    }

    public static void main(String[] args)
    {
        new SqlSemiJoinInPredicateBenchmark(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
