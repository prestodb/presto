package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.noperator.DriverOperator;
import com.facebook.presto.noperator.NewAlignmentOperator.NewAlignmentOperatorFactory;
import com.facebook.presto.noperator.NewInMemoryOrderByOperator.NewInMemoryOrderByOperatorFactory;
import com.facebook.presto.noperator.NewLimitOperator.NewLimitOperatorFactory;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.tpch.TpchBlocksProvider;

import java.util.concurrent.ExecutorService;

import static com.facebook.presto.block.BlockIterables.concat;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.Collections.nCopies;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class NewInMemoryOrderByBenchmark
        extends AbstractOperatorBenchmark
{
    private static final int ROWS = 1_500_000;

    public NewInMemoryOrderByBenchmark(ExecutorService executor, TpchBlocksProvider tpchBlocksProvider)
    {
        super(executor, tpchBlocksProvider, "in_memory_orderby_1.5M", 5, 10);
    }

    @Override
    protected Operator createBenchmarkedOperator()
    {
        BlockIterable totalPrice = getBlockIterable("orders", "totalprice", BlocksFileEncoding.RAW);
        BlockIterable clerk = getBlockIterable("orders", "clerk", BlocksFileEncoding.RAW);

        NewAlignmentOperatorFactory alignmentOperator = new NewAlignmentOperatorFactory(0, concat(nCopies(100, totalPrice)), concat(nCopies(100, clerk)));

        NewLimitOperatorFactory limitOperator = new NewLimitOperatorFactory(1, alignmentOperator.getTupleInfos(), ROWS);

        NewInMemoryOrderByOperatorFactory orderByOperator = new NewInMemoryOrderByOperatorFactory(
                2,
                limitOperator.getTupleInfos(),
                0,
                new int[]{1},
                ROWS);

        return new DriverOperator(alignmentOperator, limitOperator, orderByOperator);
    }

    public static void main(String[] args)
    {
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test"));
        new NewInMemoryOrderByBenchmark(executor, DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
