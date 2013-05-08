package com.facebook.presto.benchmark;

public class Top100SqlBenchmark
        extends AbstractSqlBenchmark
{
    public Top100SqlBenchmark()
    {
        super("sql_top_100", 5, 50, "select totalprice from orders order by totalprice desc limit 100");
    }

    public static void main(String[] args)
    {
        new Top100SqlBenchmark().runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
