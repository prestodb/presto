package com.facebook.presto.benchmark;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.tpch.TpchBlocksProvider;

public class SqlApproximateCountDistinctDoubleBenchmark
        extends AbstractSqlBenchmark
{
    public SqlApproximateCountDistinctDoubleBenchmark()
    {
        super("sql_approx_count_distinct_double", 10, 50, "select approx_distinct(totalprice) from orders");
    }

    @Override
    protected long[] execute(TpchBlocksProvider blocksProvider)
    {
        Operator operator = createBenchmarkedOperator(blocksProvider);

        long outputRows = 0;
        long outputBytes = 0;
        PageIterator iterator = operator.iterator(new OperatorStats());
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
        new SqlApproximateCountDistinctDoubleBenchmark().runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
