package com.facebook.presto.benchmark;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;

public class SqlHashJoinBenchmark
        extends AbstractSqlBenchmark
{
    public SqlHashJoinBenchmark()
    {
        super("sql_hash_join", 4, 5, "select lineitem.orderkey, lineitem.quantity, orders.totalprice, orders.orderkey from lineitem join orders using (orderkey)");
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
        new SqlHashJoinBenchmark().runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
