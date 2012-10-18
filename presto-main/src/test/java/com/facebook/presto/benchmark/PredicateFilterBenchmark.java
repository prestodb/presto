package com.facebook.presto.benchmark;

import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.TupleStreamSerdes;
import com.facebook.presto.operator.ApplyPredicateOperator;
import com.facebook.presto.tpch.TpchSchema;
import com.facebook.presto.tpch.TpchTupleStreamProvider;
import com.google.common.base.Predicate;

public class PredicateFilterBenchmark
        extends AbstractTupleStreamBenchmark
{
    public PredicateFilterBenchmark()
    {
        super("predicate_filter", 10, 100);
    }

    @Override
    protected TupleStream createBenchmarkedTupleStream(TpchTupleStreamProvider inputStreamProvider)
    {
        return new ApplyPredicateOperator(
                inputStreamProvider.getTupleStream(TpchSchema.Orders.TOTALPRICE, TupleStreamSerdes.Encoding.RAW),
                new DoubleFilter(50000.00)
        );
    }

    public static class DoubleFilter implements Predicate<Cursor> {

        private final double minValue;

        public DoubleFilter(double minValue)
        {
            this.minValue = minValue;
        }

        @Override
        public boolean apply(Cursor input)
        {
            return input.getDouble(0) >= minValue;
        }
    }

    public static void main(String[] args)
    {
        new PredicateFilterBenchmark().runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
