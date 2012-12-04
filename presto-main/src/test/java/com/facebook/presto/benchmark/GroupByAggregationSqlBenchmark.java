package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.Page;
import com.facebook.presto.tpch.TpchBlocksProvider;

public class GroupByAggregationSqlBenchmark
        extends AbstractSqlBenchmark
{
    public GroupByAggregationSqlBenchmark()
    {
        super("sql_groupby_agg", 5, 25, "select orderstatus, sum(totalprice) from orders group by orderstatus");
    }

    @Override
    protected long execute(TpchBlocksProvider blocksProvider)
    {
        Operator operator = createBenchmarkedOperator(blocksProvider);

        long outputRows = 0;
        for (Page page : operator) {
            BlockCursor cursor = page.getBlock(0).cursor();
            while (cursor.advanceNextPosition()) {
                outputRows++;
            }
        }
        return outputRows;
    }

    public static void main(String[] args)
    {
        new GroupByAggregationSqlBenchmark().runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
