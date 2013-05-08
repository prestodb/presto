package com.facebook.presto.benchmark;

public class SqlLikeBenchmark
        extends AbstractSqlBenchmark
{
    public SqlLikeBenchmark()
    {
        super("sql_like", 4, 5, "SELECT orderkey FROM lineitem WHERE comment LIKE '%ly%ly%'");
    }

    public static void main(String[] args)
    {
        new SqlLikeBenchmark().runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
