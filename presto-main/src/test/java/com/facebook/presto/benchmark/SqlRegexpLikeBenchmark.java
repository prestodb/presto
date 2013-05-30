package com.facebook.presto.benchmark;

import com.facebook.presto.tpch.TpchBlocksProvider;

public class SqlRegexpLikeBenchmark
        extends AbstractSqlBenchmark
{
    public SqlRegexpLikeBenchmark(TpchBlocksProvider tpchBlocksProvider)
    {
        super(tpchBlocksProvider, "sql_regexp_like", 4, 5, "SELECT count(*) FROM orders WHERE regexp_like(comment, '\\b[a-z]{5}ly\\b')");
    }

    public static void main(String[] args)
    {
        new SqlRegexpLikeBenchmark(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
