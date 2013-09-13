package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.operator.AlignmentOperator.AlignmentOperatorFactory;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.TopNOperator.TopNOperatorFactory;
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

public class Top100Benchmark
        extends AbstractSimpleOperatorBenchmark
{
    public Top100Benchmark(ExecutorService executor, TpchBlocksProvider tpchBlocksProvider)
    {
        super(executor, tpchBlocksProvider, "top100", 5, 50);
    }

    @Override
    protected List<? extends OperatorFactory> createOperatorFactories()
    {
        BlockIterable blockIterable = getBlockIterable("orders", "totalprice", BlocksFileEncoding.RAW);
        AlignmentOperatorFactory alignmentOperator = new AlignmentOperatorFactory(0, blockIterable);
        TopNOperatorFactory topNOperator = new TopNOperatorFactory(
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
        new Top100Benchmark(executor, DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
