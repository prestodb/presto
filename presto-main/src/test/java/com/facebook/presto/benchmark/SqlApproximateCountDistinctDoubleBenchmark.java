package com.facebook.presto.benchmark;

public class SqlApproximateCountDistinctDoubleBenchmark
        extends AbstractSqlBenchmark
{
    public SqlApproximateCountDistinctDoubleBenchmark()
    {
        super("sql_approx_count_distinct_double", 10, 50, "select approx_distinct(totalprice) from orders");
    }

    public static void main(String[] args)
    {
        new SqlApproximateCountDistinctDoubleBenchmark().runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
