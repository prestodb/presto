package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.noperator.DriverOperator;
import com.facebook.presto.noperator.NewAlignmentOperator.NewAlignmentOperatorFactory;
import com.facebook.presto.noperator.NewHashBuilderOperator.NewHashBuilderOperatorFactory;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.tpch.TpchBlocksProvider;

import java.util.concurrent.ExecutorService;

import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class NewHashBuildBenchmark
        extends AbstractOperatorBenchmark
{
    public NewHashBuildBenchmark(ExecutorService executor, TpchBlocksProvider tpchBlocksProvider)
    {
        super(executor, tpchBlocksProvider, "hash_build", 4, 5);
    }

    @Override
    protected Operator createBenchmarkedOperator()
    {
        BlockIterable orderOrderKey = getBlockIterable("orders", "orderkey", BlocksFileEncoding.RAW);
        BlockIterable totalPrice = getBlockIterable("orders", "totalprice", BlocksFileEncoding.RAW);

        NewAlignmentOperatorFactory ordersTableScan = new NewAlignmentOperatorFactory(0, orderOrderKey, totalPrice);
        NewHashBuilderOperatorFactory hashBuilder = new NewHashBuilderOperatorFactory(1, ordersTableScan.getTupleInfos(), 0, 1_500_000);

        return new DriverOperator(ordersTableScan, hashBuilder);
    }

    public static void main(String[] args)
    {
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test"));
        new NewHashBuildBenchmark(executor, DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
