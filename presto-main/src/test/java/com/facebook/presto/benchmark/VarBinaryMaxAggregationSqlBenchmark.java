package com.facebook.presto.benchmark;

import com.facebook.presto.tpch.TpchBlocksProvider;

public class VarBinaryMaxAggregationSqlBenchmark
    extends AbstractSqlBenchmark
{
    public VarBinaryMaxAggregationSqlBenchmark(TpchBlocksProvider tpchBlocksProvider)
    {
        super(tpchBlocksProvider, "sql_varbinary_max", 4, 20, "select max(shipinstruct) from lineitem");
    }

    public static void main(String[] args)
    {
        new VarBinaryMaxAggregationSqlBenchmark(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
