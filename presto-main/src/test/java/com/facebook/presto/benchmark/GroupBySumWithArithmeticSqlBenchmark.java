package com.facebook.presto.benchmark;

public class GroupBySumWithArithmeticSqlBenchmark
        extends AbstractSqlBenchmark
{
    public GroupBySumWithArithmeticSqlBenchmark()
    {
        super("sql_groupby_agg_with_arithmetic", 1, 4, "select linestatus, sum(orderkey - partkey) from lineitem group by linestatus");
    }

    public static void main(String[] args)
    {
        new GroupBySumWithArithmeticSqlBenchmark().runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
