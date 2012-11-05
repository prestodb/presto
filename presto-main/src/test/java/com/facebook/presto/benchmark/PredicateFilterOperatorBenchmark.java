package com.facebook.presto.benchmark;

import com.facebook.presto.TupleInfo.Type;
import com.facebook.presto.nblock.BlockCursor;
import com.facebook.presto.nblock.BlockIterable;
import com.facebook.presto.noperator.AlignmentOperator;
import com.facebook.presto.noperator.FilterAndProjectOperator;
import com.facebook.presto.noperator.FilterFunction;
import com.facebook.presto.noperator.Operator;
import com.facebook.presto.serde.FileBlocksSerde.FileEncoding;
import com.facebook.presto.tpch.TpchSchema.Orders;
import com.facebook.presto.tpch.TpchBlocksProvider;

import static com.facebook.presto.noperator.ProjectionFunctions.singleColumn;

public class PredicateFilterOperatorBenchmark
        extends AbstractOperatorBenchmark
{
    public PredicateFilterOperatorBenchmark()
    {
        super("op_predicate_filter", 5, 50);
    }

    @Override
    protected Operator createBenchmarkedOperator(TpchBlocksProvider inputStreamProvider)
    {
        BlockIterable totalPrice = inputStreamProvider.getBlocks(Orders.TOTALPRICE, FileEncoding.RAW);
        AlignmentOperator alignmentOperator = new AlignmentOperator(totalPrice);
        return new FilterAndProjectOperator(alignmentOperator, new DoubleFilter(50000.00), singleColumn(Type.DOUBLE, 0, 0) );
    }

    public static class DoubleFilter implements FilterFunction {

        private final double minValue;

        public DoubleFilter(double minValue)
        {
            this.minValue = minValue;
        }

        @Override
        public boolean filter(BlockCursor[] cursors)
        {
            return cursors[0].getDouble(0) >= minValue;
        }
    }

    public static void main(String[] args)
    {
        new PredicateFilterOperatorBenchmark().runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
