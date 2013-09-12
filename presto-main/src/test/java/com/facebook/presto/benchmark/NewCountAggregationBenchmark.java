package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.noperator.NewAggregationOperator.NewAggregationOperatorFactory;
import com.facebook.presto.noperator.NewAlignmentOperator.NewAlignmentOperatorFactory;
import com.facebook.presto.noperator.NewOperatorFactory;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.operator.AggregationFunctionDefinition.aggregation;
import static com.facebook.presto.operator.aggregation.CountAggregation.COUNT;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class NewCountAggregationBenchmark
        extends AbstractSimpleOperatorBenchmark
{
    public NewCountAggregationBenchmark(ExecutorService executor, TpchBlocksProvider tpchBlocksProvider)
    {
        super(executor, tpchBlocksProvider, "count_agg", 10, 100);
    }

    @Override
    protected List<? extends NewOperatorFactory> createOperatorFactories()
    {
        BlockIterable blockIterable = getBlockIterable("orders", "orderkey", BlocksFileEncoding.RAW);
        NewAlignmentOperatorFactory alignmentOperator = new NewAlignmentOperatorFactory(0, blockIterable);
        NewAggregationOperatorFactory aggregationOperator = new NewAggregationOperatorFactory(1, Step.SINGLE, ImmutableList.of(aggregation(COUNT, new Input(0, 0))));
        return ImmutableList.of(alignmentOperator, aggregationOperator);
    }

    public static void main(String[] args)
    {
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test"));
        new NewCountAggregationBenchmark(executor, DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
