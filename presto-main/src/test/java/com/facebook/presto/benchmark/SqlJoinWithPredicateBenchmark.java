package com.facebook.presto.benchmark;

import com.facebook.presto.tpch.TpchBlocksProvider;

public class SqlJoinWithPredicateBenchmark
        extends AbstractSqlBenchmark
{
    public SqlJoinWithPredicateBenchmark(TpchBlocksProvider tpchBlocksProvider)
    {
        super(tpchBlocksProvider, "sql_join_with_predicate", 1, 5,
                "select count(*) from lineitem l join orders o on l.orderkey = o.orderkey and l.partkey % 2 = 0 and o.orderkey % 2 = 0\n");
    }

    public static void main(String[] args)
    {
        new SqlJoinWithPredicateBenchmark(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
