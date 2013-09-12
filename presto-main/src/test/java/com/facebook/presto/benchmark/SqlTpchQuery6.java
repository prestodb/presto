package com.facebook.presto.benchmark;

import com.facebook.presto.tpch.TpchBlocksProvider;

import java.util.concurrent.ExecutorService;

import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class SqlTpchQuery6
        extends AbstractSqlBenchmark
{
    public SqlTpchQuery6(ExecutorService executor, TpchBlocksProvider tpchBlocksProvider)
    {
        super(executor, tpchBlocksProvider, "sql_tpch_query_6", 4, 20, "" +
                "select sum(extendedprice * discount) as revenue \n" +
                "from lineitem \n" +
                "where shipdate >= '1994-01-01' \n" +
                "   and shipdate < '1995-01-01' \n" +
                "   and discount >= 0.05 \n" +
                "   and discount <= 0.07 \n" +
                "   and quantity < 24");
    }

    public static void main(String[] args)
    {
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test"));
        new SqlTpchQuery6(executor, DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
