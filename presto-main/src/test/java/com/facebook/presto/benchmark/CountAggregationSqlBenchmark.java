package com.facebook.presto.benchmark;

public class CountAggregationSqlBenchmark
        extends AbstractSqlBenchmark
{
    public CountAggregationSqlBenchmark()
    {
        super("sql_count_agg", 10, 100, "select count(*) from orders");
    }

    public static void main(String[] args)
    {
        new CountAggregationSqlBenchmark().runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
