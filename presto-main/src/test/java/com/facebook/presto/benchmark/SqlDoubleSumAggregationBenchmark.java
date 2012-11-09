package com.facebook.presto.benchmark;

public class SqlDoubleSumAggregationBenchmark
        extends AbstractSqlBenchmark
{
    public SqlDoubleSumAggregationBenchmark()
    {
        super("sql_double_sum_agg", 10, 100, "select sum(totalprice) from orders");
    }

    public static void main(String[] args)
    {
        new SqlDoubleSumAggregationBenchmark().runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
