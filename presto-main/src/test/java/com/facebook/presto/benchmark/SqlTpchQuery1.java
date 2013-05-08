package com.facebook.presto.benchmark;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;

public class SqlTpchQuery1
        extends AbstractSqlBenchmark
{
    public SqlTpchQuery1()
    {
        super("sql_tpch_query_1", 1, 5, "" +
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

    @Override
    protected long[] execute(OperatorStats operatorStats)
    {
        Operator operator = createBenchmarkedOperator();

        long outputRows = 0;
        long outputBytes = 0;
        PageIterator iterator = operator.iterator(operatorStats);
        while (iterator.hasNext()) {
            Page page = iterator.next();
            BlockCursor cursor = page.getBlock(0).cursor();
            while (cursor.advanceNextPosition()) {
                outputRows++;
            }

            for (Block block : page.getBlocks()) {
                outputBytes += block.getDataSize().toBytes();
            }
        }
        return new long[] {outputRows, outputBytes};
    }

    public static void main(String[] args)
    {
        new SqlTpchQuery1().runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
