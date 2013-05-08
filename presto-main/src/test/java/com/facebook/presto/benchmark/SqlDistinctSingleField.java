package com.facebook.presto.benchmark;

public class SqlDistinctSingleField
        extends AbstractSqlBenchmark
{
    public SqlDistinctSingleField()
    {
        super("sql_distinct_single", 10, 20, "SELECT DISTINCT shippriority FROM orders");
    }

    public static void main(String[] args)
    {
        new SqlDistinctSingleField().runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
