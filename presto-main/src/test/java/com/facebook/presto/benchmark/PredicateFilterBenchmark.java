package com.facebook.presto.benchmark;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.FilterAndProjectOperator;
import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.facebook.presto.tuple.TupleReadable;

import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;

public class PredicateFilterBenchmark
        extends AbstractOperatorBenchmark
{
    public PredicateFilterBenchmark()
    {
        super("predicate_filter", 5, 50);
    }

    @Override
    protected Operator createBenchmarkedOperator()
    {
        BlockIterable blockIterable = getBlockIterable("orders", "totalprice", BlocksFileEncoding.RAW);
        AlignmentOperator alignmentOperator = new AlignmentOperator(blockIterable);
        return new FilterAndProjectOperator(alignmentOperator, new DoubleFilter(50000.00), singleColumn(Type.DOUBLE, 0, 0) );
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
        new PredicateFilterBenchmark().runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
