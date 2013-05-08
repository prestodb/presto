package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.FilterAndProjectOperator;
import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.facebook.presto.tuple.TupleReadable;

import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;

public class PredicateFilterBenchmark
        extends AbstractOperatorBenchmark
{
    public PredicateFilterBenchmark(TpchBlocksProvider tpchBlocksProvider)
    {
        super(tpchBlocksProvider, "predicate_filter", 5, 50);
    }

    @Override
    protected Operator createBenchmarkedOperator()
    {
        BlockIterable blockIterable = getBlockIterable("orders", "totalprice", BlocksFileEncoding.RAW);
        AlignmentOperator alignmentOperator = new AlignmentOperator(blockIterable);
        return new FilterAndProjectOperator(alignmentOperator, new DoubleFilter(50000.00), singleColumn(Type.DOUBLE, 0, 0) );
    }

    public static class DoubleFilter implements FilterFunction {

        private final double minValue;

        public DoubleFilter(double minValue)
        {
            this.minValue = minValue;
        }

        @Override
        public boolean filter(TupleReadable... cursors)
        {
            return cursors[0].getDouble(0) >= minValue;
        }
    }

    public static void main(String[] args)
    {
        new PredicateFilterBenchmark(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
