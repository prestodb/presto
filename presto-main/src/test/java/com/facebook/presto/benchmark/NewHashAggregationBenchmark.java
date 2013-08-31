package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.noperator.NewAlignmentOperator.NewAlignmentOperatorFactory;
import com.facebook.presto.noperator.NewHashAggregationOperator.NewHashAggregationOperatorFactory;
import com.facebook.presto.noperator.NewOperatorFactory;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.operator.AggregationFunctionDefinition.aggregation;
import static com.facebook.presto.operator.aggregation.DoubleSumAggregation.DOUBLE_SUM;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class NewHashAggregationBenchmark
        extends AbstractSimpleOperatorBenchmark
{
    public NewHashAggregationBenchmark(ExecutorService executor, TpchBlocksProvider tpchBlocksProvider)
    {
        super(executor, tpchBlocksProvider, "hash_agg", 5, 25);
    }

    @Override
    protected List<? extends NewOperatorFactory> createOperatorFactories()
    {
        BlockIterable orderStatusBlockIterable = getBlockIterable("orders", "orderstatus", BlocksFileEncoding.RAW);
        BlockIterable totalPriceBlockIterable = getBlockIterable("orders", "totalprice", BlocksFileEncoding.RAW);

        NewAlignmentOperatorFactory alignmentOperator = new NewAlignmentOperatorFactory(0, orderStatusBlockIterable, totalPriceBlockIterable);
        NewHashAggregationOperatorFactory aggregationOperator = new NewHashAggregationOperatorFactory(1,
                alignmentOperator.getTupleInfos().get(0),
                0,
                Step.SINGLE,
                ImmutableList.of(aggregation(DOUBLE_SUM, new Input(1, 0))),
                100_000);
        return ImmutableList.of(alignmentOperator, aggregationOperator);
    }

    public static void main(String[] args)
    {
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test"));
        new NewHashAggregationBenchmark(executor, DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
