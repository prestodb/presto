package com.facebook.presto.benchmark;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.noperator.Driver;
import com.facebook.presto.noperator.DriverFactory;
import com.facebook.presto.noperator.DriverOperator;
import com.facebook.presto.noperator.NewAlignmentOperator.NewAlignmentOperatorFactory;
import com.facebook.presto.noperator.NewHashBuilderOperator.NewHashBuilderOperatorFactory;
import com.facebook.presto.noperator.NewHashJoinOperator;
import com.facebook.presto.noperator.NewHashJoinOperator.NewHashJoinOperatorFactory;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.tpch.TpchBlocksProvider;

import java.util.concurrent.ExecutorService;

import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class NewHashBuildAndJoinBenchmark
        extends AbstractOperatorBenchmark
{
    public NewHashBuildAndJoinBenchmark(ExecutorService executor, TpchBlocksProvider tpchBlocksProvider)
    {
        super(executor, tpchBlocksProvider, "hash_build_and_join", 4, 5);
    }

    /*
    select orderkey, quantity, totalprice
    from lineitem join orders using (orderkey)
     */
    @Override
    protected Operator createBenchmarkedOperator()
    {
        BlockIterable orderOrderKey = getBlockIterable("orders", "orderkey", BlocksFileEncoding.RAW);
        BlockIterable totalPrice = getBlockIterable("orders", "totalprice", BlocksFileEncoding.RAW);

        NewAlignmentOperatorFactory ordersTableScan = new NewAlignmentOperatorFactory(0, orderOrderKey, totalPrice);
        NewHashBuilderOperatorFactory hashBuilder = new NewHashBuilderOperatorFactory(1, ordersTableScan.getTupleInfos(), 0, 1_500_000);

        Driver driver = new DriverFactory(ordersTableScan, hashBuilder).createDriver(taskContext.addPipelineContext().addDriverContext());
        while (!driver.isFinished()) {
            driver.process();
        }

        BlockIterable lineItemOrderKey = getBlockIterable("lineitem", "orderkey", BlocksFileEncoding.RAW);
        BlockIterable lineNumber = getBlockIterable("lineitem", "quantity", BlocksFileEncoding.RAW);
        NewAlignmentOperatorFactory lineItemTableScan = new NewAlignmentOperatorFactory(0, lineItemOrderKey, lineNumber);

        NewHashJoinOperatorFactory joinOperator = NewHashJoinOperator.innerJoin(1, hashBuilder.getHashSupplier(), lineItemTableScan.getTupleInfos(), 0);

        return new DriverOperator(lineItemTableScan, joinOperator);
    }

    @Override
    protected long[] execute(OperatorStats operatorStats)
    {
        Operator operator = createBenchmarkedOperator();

        long outputRows = 0;
        long outputBytes = 0;
        PageIterator iterator = operator.iterator(operatorStats);
        while (iterator.hasNext()) {
            Page page = iterator.next();
            BlockCursor cursor = page.getBlock(0).cursor();
            while (cursor.advanceNextPosition()) {
                outputRows++;
            }

            for (Block block : page.getBlocks()) {
                outputBytes += block.getDataSize().toBytes();
            }
        }
        return new long[]{outputRows, outputBytes};
    }

    public static void main(String[] args)
    {
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test"));
        new NewHashBuildAndJoinBenchmark(executor, DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
