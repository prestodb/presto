package com.facebook.presto.benchmark;

import com.facebook.presto.tpch.TpchBlocksProvider;

import java.util.concurrent.ExecutorService;

import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class SqlTpchQuery1
        extends AbstractSqlBenchmark
{
    public SqlTpchQuery1(ExecutorService executor, TpchBlocksProvider tpchBlocksProvider)
    {
        super(executor, tpchBlocksProvider, "sql_tpch_query_1", 1, 5, "" +
                "select\n" +
                "    returnflag,\n" +
                "    linestatus,\n" +
                "    sum(quantity) as sum_qty,\n" +
                "    sum(extendedprice) as sum_base_price,\n" +
                "    sum(extendedprice * (1 - discount)) as sum_disc_price,\n" +
                "    sum(extendedprice * (1 - discount) * (1 + tax)) as sum_charge,\n" +
                "    avg(quantity) as avg_qty,\n" +
                "    avg(extendedprice) as avg_price,\n" +
                "    avg(discount) as avg_disc,\n" +
                "    count(*) as count_order\n" +
                "from\n" +
                "    lineitem\n" +
                "where\n" +
                "    shipdate <= '1998-09-02'\n" +
                "group by\n" +
                "    returnflag,\n" +
                "    linestatus\n" +
                "order by\n" +
                "    returnflag,\n" +
                "    linestatus");
    }

    public static void main(String[] args)
    {
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test"));
        new SqlTpchQuery1(executor, DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
