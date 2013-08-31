package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.noperator.DriverOperator;
import com.facebook.presto.noperator.NewAlignmentOperator.NewAlignmentOperatorFactory;
import com.facebook.presto.noperator.NewTopNOperator.NewTopNOperatorFactory;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.facebook.presto.tuple.FieldOrderedTupleComparator;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;

public class NewTop100Benchmark
        extends AbstractOperatorBenchmark
{
    public NewTop100Benchmark(TpchBlocksProvider tpchBlocksProvider)
    {
        super(tpchBlocksProvider, "top100", 5, 50);
    }

    @Override
    protected Operator createBenchmarkedOperator()
    {
        BlockIterable blockIterable = getBlockIterable("orders", "totalprice", BlocksFileEncoding.RAW);
        NewAlignmentOperatorFactory alignmentOperator = new NewAlignmentOperatorFactory(blockIterable);
        NewTopNOperatorFactory topNOperator = new NewTopNOperatorFactory(
                100,
                0,
                ImmutableList.of(singleColumn(Type.DOUBLE, 0, 0)),
                Ordering.from(new FieldOrderedTupleComparator(ImmutableList.of(0), ImmutableList.of(SortItem.Ordering.DESCENDING))),
                false);

        return new DriverOperator(alignmentOperator, topNOperator);
    }

    public static void main(String[] args)
    {
        new NewTop100Benchmark(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
