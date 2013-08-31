package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.noperator.Driver;
import com.facebook.presto.noperator.DriverContext;
import com.facebook.presto.noperator.DriverFactory;
import com.facebook.presto.noperator.NewAlignmentOperator.NewAlignmentOperatorFactory;
import com.facebook.presto.noperator.NewHashBuilderOperator.NewHashBuilderOperatorFactory;
import com.facebook.presto.noperator.NewHashBuilderOperator.NewHashSupplier;
import com.facebook.presto.noperator.NewHashJoinOperator;
import com.facebook.presto.noperator.NewHashJoinOperator.NewHashJoinOperatorFactory;
import com.facebook.presto.noperator.NullOutputOperator.NullOutputOperatorFactory;
import com.facebook.presto.noperator.TaskContext;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class NewHashJoinBenchmark
        extends AbstractNewOperatorBenchmark
{
    private NewHashSupplier hashSupplier;

    public NewHashJoinBenchmark(ExecutorService executor, TpchBlocksProvider tpchBlocksProvider)
    {
        super(executor, tpchBlocksProvider, "hash_join", 4, 5);
    }

    /*
    select orderkey, quantity, totalprice
    from lineitem join orders using (orderkey)
     */

    @Override
    protected List<Driver> createDrivers(TaskContext taskContext)
    {
        if (hashSupplier == null) {
            BlockIterable orderOrderKey = getBlockIterable("orders", "orderkey", BlocksFileEncoding.RAW);
            BlockIterable totalPrice = getBlockIterable("orders", "totalprice", BlocksFileEncoding.RAW);

            NewAlignmentOperatorFactory ordersTableScan = new NewAlignmentOperatorFactory(0, orderOrderKey, totalPrice);
            NewHashBuilderOperatorFactory hashBuilder = new NewHashBuilderOperatorFactory(1, ordersTableScan.getTupleInfos(), 0, 1_500_000);

            DriverContext driverContext = taskContext.addPipelineContext(false, false).addDriverContext();
            Driver driver = new DriverFactory(false, false, ordersTableScan, hashBuilder).createDriver(driverContext);
            while (!driver.isFinished()) {
                driver.process();
            }
            hashSupplier = hashBuilder.getHashSupplier();
        }

        BlockIterable lineItemOrderKey = getBlockIterable("lineitem", "orderkey", BlocksFileEncoding.RAW);
        BlockIterable lineNumber = getBlockIterable("lineitem", "quantity", BlocksFileEncoding.RAW);
        NewAlignmentOperatorFactory lineItemTableScan = new NewAlignmentOperatorFactory(0, lineItemOrderKey, lineNumber);

        NewHashJoinOperatorFactory joinOperator = NewHashJoinOperator.innerJoin(1, hashSupplier, lineItemTableScan.getTupleInfos(), 0);

        NullOutputOperatorFactory output = new NullOutputOperatorFactory(2, joinOperator.getTupleInfos());

        DriverFactory driverFactory = new DriverFactory(true, true, lineItemTableScan, joinOperator, output);
        DriverContext driverContext = taskContext.addPipelineContext(true, true).addDriverContext();
        Driver driver = driverFactory.createDriver(driverContext);
        return ImmutableList.of(driver);
    }

    public static void main(String[] args)
    {
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test"));
        new NewHashJoinBenchmark(executor, DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
