package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.operator.AggregationOperator;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;
import static com.facebook.presto.operator.aggregation.AggregationFunctions.singleNodeAggregation;
import static com.facebook.presto.operator.aggregation.DoubleSumAggregation.doubleSumAggregation;
import static com.facebook.presto.tpch.TpchSchema.columnHandle;
import static com.facebook.presto.tpch.TpchSchema.tableHandle;

public class DoubleSumAggregationBenchmark
        extends AbstractOperatorBenchmark
{
    public DoubleSumAggregationBenchmark()
    {
        super("double_sum_agg", 10, 100);
    }

    @Override
    protected Operator createBenchmarkedOperator(TpchBlocksProvider blocksProvider)
    {
        TpchTableHandle orders = tableHandle("orders");
        TpchColumnHandle totalprice = columnHandle(orders, "totalprice");
        BlockIterable blockIterable = blocksProvider.getBlocks(orders, totalprice, BlocksFileEncoding.RAW);
        AlignmentOperator alignmentOperator = new AlignmentOperator(blockIterable);
        return new AggregationOperator(alignmentOperator,
                ImmutableList.of(singleNodeAggregation(doubleSumAggregation(0, 0))),
                ImmutableList.of(singleColumn(Type.DOUBLE, 0, 0)));
    }

    public static void main(String[] args)
    {
        new DoubleSumAggregationBenchmark().runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
