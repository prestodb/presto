package com.facebook.presto.benchmark;

public class GroupByAggregationSqlBenchmark
        extends AbstractSqlBenchmark
{
    public GroupByAggregationSqlBenchmark()
    {
        super("sql_groupby_agg", 5, 25, "select orderstatus, sum(totalprice) from orders group by orderstatus");
    }

    public static void main(String[] args)
    {
        new GroupByAggregationSqlBenchmark().runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
