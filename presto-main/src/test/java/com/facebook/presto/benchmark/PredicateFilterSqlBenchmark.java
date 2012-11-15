package com.facebook.presto.benchmark;

import com.facebook.presto.tpch.TpchBlocksProvider;

public class PredicateFilterSqlBenchmark
        extends AbstractSqlBenchmark
{
    public PredicateFilterSqlBenchmark(TpchBlocksProvider tpchBlocksProvider)
    {
        super(tpchBlocksProvider, "sql_predicate_filter", 5, 50, "select totalprice + 10 from orders where totalprice > 50000");
    }

    public static void main(String[] args)
    {
        new PredicateFilterSqlBenchmark(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
