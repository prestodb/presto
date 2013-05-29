package com.facebook.presto.benchmark;

import com.facebook.presto.tpch.TpchBlocksProvider;

public class SqlTpchQuery6
        extends AbstractSqlBenchmark
{
    public SqlTpchQuery6(TpchBlocksProvider tpchBlocksProvider)
    {
        super(tpchBlocksProvider, "sql_tpch_query_6", 2, 10, "" +
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
        new SqlTpchQuery6(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
