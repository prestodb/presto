package com.facebook.presto.benchmark;

public class SqlDistinctMultipleFields
        extends AbstractSqlBenchmark
{
    public SqlDistinctMultipleFields()
    {
        super("sql_distinct_multi", 4, 10, "SELECT DISTINCT orderpriority, shippriority FROM orders");
    }

    public static void main(String[] args)
    {
        new SqlDistinctMultipleFields().runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
