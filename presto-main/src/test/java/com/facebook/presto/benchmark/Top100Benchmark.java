package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.TopNOperator;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;

public class Top100Benchmark
        extends AbstractOperatorBenchmark
{
    public Top100Benchmark(TpchBlocksProvider tpchBlocksProvider)
    {
        super(tpchBlocksProvider, "top100", 5, 50);
    }

    @Override
    protected Operator createBenchmarkedOperator()
    {
        BlockIterable blockIterable = getBlockIterable("orders", "totalprice", BlocksFileEncoding.RAW);
        AlignmentOperator alignmentOperator = new AlignmentOperator(blockIterable);
        return new TopNOperator(alignmentOperator, 100, 0, ImmutableList.of(singleColumn(TupleInfo.Type.DOUBLE, 0, 0)));
    }

    public static void main(String[] args)
    {
        new Top100Benchmark(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
