package com.facebook.presto.benchmark;

import com.facebook.presto.tpch.TpchBlocksProvider;

public class SqlHashJoinBenchmark
        extends AbstractSqlBenchmark
{
    public SqlHashJoinBenchmark(TpchBlocksProvider tpchBlocksProvider)
    {
        super(tpchBlocksProvider, "sql_hash_join", 4, 5,
                "select lineitem.orderkey, lineitem.quantity, orders.totalprice, orders.orderkey from lineitem join orders using (orderkey)" );
    }

    public static void main(String[] args)
    {
        new SqlHashJoinBenchmark(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
