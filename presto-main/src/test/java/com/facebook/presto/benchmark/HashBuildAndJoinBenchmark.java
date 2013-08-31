package com.facebook.presto.benchmark;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.HashJoinOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.operator.SourceHashSupplier;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.tpch.TpchBlocksProvider;
import io.airlift.units.DataSize;

import java.util.concurrent.ExecutorService;

import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class HashBuildAndJoinBenchmark
        extends AbstractOperatorBenchmark
{
    public HashBuildAndJoinBenchmark(ExecutorService executor, TpchBlocksProvider tpchBlocksProvider)
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
        AlignmentOperator ordersTableScan = new AlignmentOperator(orderOrderKey, totalPrice);
//        AlignmentOperator ordersTableScan = new AlignmentOperator(concat(nCopies(100, orderOrderKey)), concat(nCopies(100, totalPrice)));
//        LimitOperator ordersLimit = new LimitOperator(ordersTableScan, 1_500_000);
        SourceHashSupplier sourceHashSupplier = new SourceHashSupplier(ordersTableScan, 0, 1_500_000, new TaskMemoryManager(new DataSize(100, MEGABYTE)), new OperatorStats());

        BlockIterable lineItemOrderKey = getBlockIterable("lineitem", "orderkey", BlocksFileEncoding.RAW);
        BlockIterable lineNumber = getBlockIterable("lineitem", "quantity", BlocksFileEncoding.RAW);
        AlignmentOperator lineItemTableScan = new AlignmentOperator(lineItemOrderKey, lineNumber);
//        AlignmentOperator lineItemTableScan = new AlignmentOperator(concat(nCopies(100, lineItemOrderKey)), concat(nCopies(100, lineNumber)));
//        LimitOperator lineItemLimit = new LimitOperator(lineItemTableScan, 10_000_000);

        return HashJoinOperator.innerJoin(sourceHashSupplier, lineItemTableScan, 0);
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
        return new long[] {outputRows, outputBytes};
    }

    public static void main(String[] args)
    {
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test"));
        new HashBuildAndJoinBenchmark(executor, DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
