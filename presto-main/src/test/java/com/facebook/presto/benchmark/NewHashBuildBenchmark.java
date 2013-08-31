package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.noperator.DriverOperator;
import com.facebook.presto.noperator.NewAlignmentOperator.NewAlignmentOperatorFactory;
import com.facebook.presto.noperator.NewHashBuilderOperator.NewHashBuilderOperatorFactory;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.tpch.TpchBlocksProvider;

public class NewHashBuildBenchmark
        extends AbstractOperatorBenchmark
{
    public NewHashBuildBenchmark(TpchBlocksProvider tpchBlocksProvider)
    {
        super(tpchBlocksProvider, "hash_build", 4, 5);
    }

    @Override
    protected Operator createBenchmarkedOperator()
    {
        BlockIterable orderOrderKey = getBlockIterable("orders", "orderkey", BlocksFileEncoding.RAW);
        BlockIterable totalPrice = getBlockIterable("orders", "totalprice", BlocksFileEncoding.RAW);

        NewAlignmentOperatorFactory ordersTableScan = new NewAlignmentOperatorFactory(orderOrderKey, totalPrice);
        NewHashBuilderOperatorFactory hashBuilder = new NewHashBuilderOperatorFactory(ordersTableScan.getTupleInfos(), 0, 1_500_000);

        return new DriverOperator(ordersTableScan, hashBuilder);
    }

    public static void main(String[] args)
    {
        new NewHashBuildBenchmark(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
