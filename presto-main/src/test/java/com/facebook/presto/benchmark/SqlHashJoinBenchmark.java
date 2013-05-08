package com.facebook.presto.benchmark;

public class SqlHashJoinBenchmark
        extends AbstractSqlBenchmark
{
    public SqlHashJoinBenchmark()
    {
        super("sql_hash_join", 4, 5, "select lineitem.orderkey, lineitem.quantity, orders.totalprice, orders.orderkey from lineitem join orders using (orderkey)");
    }

    public static void main(String[] args)
    {
        new SqlHashJoinBenchmark().runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
