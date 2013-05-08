package com.facebook.presto.benchmark;

import com.facebook.presto.tpch.TpchBlocksProvider;

public class SqlDoubleSumAggregationBenchmark
        extends AbstractSqlBenchmark
{
    public SqlDoubleSumAggregationBenchmark(TpchBlocksProvider tpchBlocksProvider)
    {
        super(tpchBlocksProvider, "sql_double_sum_agg", 10, 100, "select sum(totalprice) from orders");
    }

    public static void main(String[] args)
    {
        new SqlDoubleSumAggregationBenchmark(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
