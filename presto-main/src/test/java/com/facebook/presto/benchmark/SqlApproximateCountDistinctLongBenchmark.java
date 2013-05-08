package com.facebook.presto.benchmark;

public class SqlApproximateCountDistinctLongBenchmark
        extends AbstractSqlBenchmark
{
    public SqlApproximateCountDistinctLongBenchmark()
    {
        super("sql_approx_count_distinct_long", 10, 50, "select approx_distinct(custkey) from orders");
    }

    public static void main(String[] args)
    {
        new SqlApproximateCountDistinctLongBenchmark().runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
