package com.facebook.presto.benchmark;

import com.facebook.presto.tpch.TpchBlocksProvider;

public class GroupBySumWithArithmeticSqlBenchmark
        extends AbstractSqlBenchmark
{
    public GroupBySumWithArithmeticSqlBenchmark(TpchBlocksProvider tpchBlocksProvider)
    {
        super(tpchBlocksProvider, "sql_groupby_agg_with_arithmetic", 1, 4, "select linestatus, sum(orderkey - partkey) from lineitem group by linestatus");
    }

    public static void main(String[] args)
    {
        new GroupBySumWithArithmeticSqlBenchmark(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
