package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.HashAggregationOperator;
import com.facebook.presto.operator.Input;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import static com.facebook.presto.operator.AggregationFunctionDefinition.aggregation;
import static com.facebook.presto.operator.aggregation.DoubleSumAggregation.DOUBLE_SUM;
import static com.facebook.presto.tpch.TpchSchema.columnHandle;
import static com.facebook.presto.tpch.TpchSchema.tableHandle;

public class HashAggregationBenchmark
        extends AbstractOperatorBenchmark
{
    public HashAggregationBenchmark()
    {
        super("hash_agg", 5, 25);
    }

    @Override
    protected Operator createBenchmarkedOperator(TpchBlocksProvider blocksProvider)
    {
        TpchTableHandle orders = tableHandle("orders");
        TpchColumnHandle orderStatus = columnHandle(orders, "orderstatus");
        TpchColumnHandle totalPrice = columnHandle(orders, "totalprice");
        BlockIterable orderStatusBlockIterable = blocksProvider.getBlocks(orders, orderStatus, BlocksFileEncoding.RAW);
        BlockIterable totalPriceBlockIterable = blocksProvider.getBlocks(orders, totalPrice, BlocksFileEncoding.RAW);

        AlignmentOperator alignmentOperator = new AlignmentOperator(orderStatusBlockIterable, totalPriceBlockIterable);
        return new HashAggregationOperator(alignmentOperator,
                0,
                Step.SINGLE,
                ImmutableList.of(aggregation(DOUBLE_SUM, new Input(1, 0))),
                100_000,
                new DataSize(100, Unit.MEGABYTE));

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
        new HashAggregationBenchmark().runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
