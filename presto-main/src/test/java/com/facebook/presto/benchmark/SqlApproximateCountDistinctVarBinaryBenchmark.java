package com.facebook.presto.benchmark;

public class SqlApproximateCountDistinctVarBinaryBenchmark
        extends AbstractSqlBenchmark
{
    public SqlApproximateCountDistinctVarBinaryBenchmark()
    {
        super("sql_approx_count_distinct_varbinary", 10, 50, "select approx_distinct(orderdate) from orders");
    }

    public static void main(String[] args)
    {
        new SqlApproximateCountDistinctVarBinaryBenchmark().runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
