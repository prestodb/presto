package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.Page;
import com.facebook.presto.tpch.TpchBlocksProvider;

public class GroupBySumWithArithmeticSqlBenchmark
        extends AbstractSqlBenchmark
{
    public GroupBySumWithArithmeticSqlBenchmark()
    {
        super("sql_groupby_agg_with_arithmetic", 1, 4, "select linestatus, sum(orderkey - partkey) from lineitem group by linestatus");
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
        new GroupBySumWithArithmeticSqlBenchmark().runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
