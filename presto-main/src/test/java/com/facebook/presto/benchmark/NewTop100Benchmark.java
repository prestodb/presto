package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.operator.NewAlignmentOperator.NewAlignmentOperatorFactory;
import com.facebook.presto.operator.NewOperatorFactory;
import com.facebook.presto.operator.NewTopNOperator.NewTopNOperatorFactory;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.facebook.presto.tuple.FieldOrderedTupleComparator;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class NewTop100Benchmark
        extends AbstractSimpleOperatorBenchmark
{
    public NewTop100Benchmark(ExecutorService executor, TpchBlocksProvider tpchBlocksProvider)
    {
        super(executor, tpchBlocksProvider, "top100", 5, 50);
    }

    @Override
    protected List<? extends NewOperatorFactory> createOperatorFactories()
    {
        BlockIterable blockIterable = getBlockIterable("orders", "totalprice", BlocksFileEncoding.RAW);
        NewAlignmentOperatorFactory alignmentOperator = new NewAlignmentOperatorFactory(0, blockIterable);
        NewTopNOperatorFactory topNOperator = new NewTopNOperatorFactory(
                1,
                100,
                0,
                ImmutableList.of(singleColumn(Type.DOUBLE, 0, 0)),
                Ordering.from(new FieldOrderedTupleComparator(ImmutableList.of(0), ImmutableList.of(SortItem.Ordering.DESCENDING))),
                false);
        return ImmutableList.of(alignmentOperator, topNOperator);
    }

    public static void main(String[] args)
    {
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test"));
        new NewTop100Benchmark(executor, DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
