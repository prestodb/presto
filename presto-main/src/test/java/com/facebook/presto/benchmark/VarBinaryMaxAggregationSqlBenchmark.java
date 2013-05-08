package com.facebook.presto.benchmark;

public class VarBinaryMaxAggregationSqlBenchmark
    extends AbstractSqlBenchmark
{
    public VarBinaryMaxAggregationSqlBenchmark()
    {
        super("sql_varbinary_max", 4, 20, "select max(shipinstruct) from lineitem");
    }

    public static void main(String[] args)
    {
        new VarBinaryMaxAggregationSqlBenchmark().runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
