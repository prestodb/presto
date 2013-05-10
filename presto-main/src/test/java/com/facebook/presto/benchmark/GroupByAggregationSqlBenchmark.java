package com.facebook.presto.benchmark;

import com.facebook.presto.tpch.TpchBlocksProvider;

public class GroupByAggregationSqlBenchmark
        extends AbstractSqlBenchmark
{
    public GroupByAggregationSqlBenchmark(TpchBlocksProvider tpchBlocksProvider)
    {
        super(tpchBlocksProvider, "sql_groupby_agg", 5, 25, "select orderstatus, sum(totalprice) from orders group by orderstatus");
    }

    public static void main(String[] args)
    {
        new GroupByAggregationSqlBenchmark(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
