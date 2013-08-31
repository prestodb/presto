package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.noperator.Driver;
import com.facebook.presto.noperator.DriverFactory;
import com.facebook.presto.noperator.DriverOperator;
import com.facebook.presto.noperator.NewAlignmentOperator.NewAlignmentOperatorFactory;
import com.facebook.presto.noperator.NewHashBuilderOperator.NewHashBuilderOperatorFactory;
import com.facebook.presto.noperator.NewHashBuilderOperator.NewHashSupplier;
import com.facebook.presto.noperator.NewHashJoinOperator;
import com.facebook.presto.noperator.NewHashJoinOperator.NewHashJoinOperatorFactory;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.tpch.TpchBlocksProvider;
import io.airlift.units.DataSize;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class NewHashJoinBenchmark
        extends AbstractOperatorBenchmark
{
    private NewHashSupplier hashSupplier;

    public NewHashJoinBenchmark(TpchBlocksProvider tpchBlocksProvider)
    {
        super(tpchBlocksProvider, "hash_join", 4, 5);
    }

    /*
    select orderkey, quantity, totalprice
    from lineitem join orders using (orderkey)
     */
    @Override
    protected Operator createBenchmarkedOperator()
    {
        if (hashSupplier == null) {
            BlockIterable orderOrderKey = getBlockIterable("orders", "orderkey", BlocksFileEncoding.RAW);
            BlockIterable totalPrice = getBlockIterable("orders", "totalprice", BlocksFileEncoding.RAW);

            NewAlignmentOperatorFactory ordersTableScan = new NewAlignmentOperatorFactory(orderOrderKey, totalPrice);
            NewHashBuilderOperatorFactory hashBuilder = new NewHashBuilderOperatorFactory(ordersTableScan.getTupleInfos(), 0, 1_500_000);

            Driver driver = new DriverFactory(ordersTableScan, hashBuilder).createDriver(new OperatorStats(), new TaskMemoryManager(new DataSize(100, MEGABYTE)));
            while (!driver.isFinished()) {
                driver.process();
            }
            hashSupplier = hashBuilder.getHashSupplier();
        }

        BlockIterable lineItemOrderKey = getBlockIterable("lineitem", "orderkey", BlocksFileEncoding.RAW);
        BlockIterable lineNumber = getBlockIterable("lineitem", "quantity", BlocksFileEncoding.RAW);
        NewAlignmentOperatorFactory lineItemTableScan = new NewAlignmentOperatorFactory(lineItemOrderKey, lineNumber);

        NewHashJoinOperatorFactory joinOperator = NewHashJoinOperator.innerJoin(hashSupplier, lineItemTableScan.getTupleInfos(), 0);

        return new DriverOperator(lineItemTableScan, joinOperator);
    }

    public static void main(String[] args)
    {
        new NewHashJoinBenchmark(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
