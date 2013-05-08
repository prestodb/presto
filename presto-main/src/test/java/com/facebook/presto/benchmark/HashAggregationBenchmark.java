package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.HashAggregationOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import static com.facebook.presto.operator.AggregationFunctionDefinition.aggregation;
import static com.facebook.presto.operator.aggregation.DoubleSumAggregation.DOUBLE_SUM;

public class HashAggregationBenchmark
        extends AbstractOperatorBenchmark
{
    public HashAggregationBenchmark(TpchBlocksProvider tpchBlocksProvider)
    {
        super(tpchBlocksProvider, "hash_agg", 5, 25);
    }

    @Override
    protected Operator createBenchmarkedOperator()
    {
        BlockIterable orderStatusBlockIterable = getBlockIterable("orders", "orderstatus", BlocksFileEncoding.RAW);
        BlockIterable totalPriceBlockIterable = getBlockIterable("orders", "totalprice", BlocksFileEncoding.RAW);

        AlignmentOperator alignmentOperator = new AlignmentOperator(orderStatusBlockIterable, totalPriceBlockIterable);
        return new HashAggregationOperator(alignmentOperator,
                0,
                Step.SINGLE,
                ImmutableList.of(aggregation(DOUBLE_SUM, new Input(1, 0))),
                100_000,
                new DataSize(100, Unit.MEGABYTE));

    }

    public static void main(String[] args)
    {
        new HashAggregationBenchmark(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
