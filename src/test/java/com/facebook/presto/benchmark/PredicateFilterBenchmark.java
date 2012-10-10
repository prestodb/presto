package com.facebook.presto.benchmark;

import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.operator.ApplyPredicateOperator;
import com.facebook.presto.tpch.TpchSchema;
import com.google.common.base.Predicate;

import java.util.List;

import static com.facebook.presto.block.TupleStreamSerdes.Encoding;

public class PredicateFilterBenchmark
        extends AbstractTupleStreamBenchmark
{
    public PredicateFilterBenchmark()
    {
        super("predicate_filter", 10, 100);
    }

    @Override
    protected void setUp()
    {
        loadColumnFile(TpchSchema.Orders.TOTALPRICE, Encoding.RAW);
    }

    @Override
    protected TupleStream createBenchmarkedTupleStream(List<? extends TupleStream> inputTupleStreams)
    {
        return new ApplyPredicateOperator(inputTupleStreams.get(0), new DoubleFilter(50000.00));
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
