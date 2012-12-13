package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.operator.AggregationOperator;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;
import static com.facebook.presto.operator.aggregation.AggregationFunctions.singleNodeAggregation;
import static com.facebook.presto.operator.aggregation.CountAggregation.countAggregation;
import static com.facebook.presto.tpch.TpchSchema.columnHandle;
import static com.facebook.presto.tpch.TpchSchema.tableHandle;

public class CountAggregationBenchmark
        extends AbstractOperatorBenchmark
{
    public CountAggregationBenchmark()
    {
        super("count_agg", 10, 100);
    }

    @Override
    protected Operator createBenchmarkedOperator(TpchBlocksProvider blocksProvider)
    {
        TpchTableHandle orders = tableHandle("orders");
        TpchColumnHandle orderkey = columnHandle(orders, "orderkey");
        BlockIterable blockIterable = blocksProvider.getBlocks(orders, orderkey, BlocksFileEncoding.RAW);
        AlignmentOperator alignmentOperator = new AlignmentOperator(blockIterable);
        return new AggregationOperator(alignmentOperator, ImmutableList.of(singleNodeAggregation(countAggregation(0, 0))), ImmutableList.of(singleColumn(Type.FIXED_INT_64, 0, 0)));
    }

    @Override
    protected long execute(TpchBlocksProvider blocksProvider)
    {
        Operator operator = createBenchmarkedOperator(blocksProvider);

        long outputRows = 0;
        PageIterator iterator = operator.iterator(new OperatorStats());
        while (iterator.hasNext()) {
            Page page = iterator.next();
            BlockCursor cursor = page.getBlock(0).cursor();
            while (cursor.advanceNextPosition()) {
                outputRows++;
            }
        }
        return outputRows;
    }

    public static void main(String[] args)
    {
        new CountAggregationBenchmark().runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
