package com.facebook.presto.benchmark;

public class RawStreamingSqlBenchmark
        extends AbstractSqlBenchmark
{
    public RawStreamingSqlBenchmark()
    {
        super("sql_raw_stream", 10, 100, "select totalprice from orders");
    }

    public static void main(String[] args)
    {
        new RawStreamingSqlBenchmark().runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
