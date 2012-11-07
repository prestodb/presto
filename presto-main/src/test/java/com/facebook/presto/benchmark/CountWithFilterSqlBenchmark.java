package com.facebook.presto.benchmark;

public class CountWithFilterSqlBenchmark
        extends AbstractSqlBenchmark
{
    public CountWithFilterSqlBenchmark()
    {
        super("sql_count_with_filter", 10, 100, "SELECT count(*) from orders where orderstatus = 'F'");
    }

    public static void main(String[] args)
    {
        new CountWithFilterSqlBenchmark().runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
