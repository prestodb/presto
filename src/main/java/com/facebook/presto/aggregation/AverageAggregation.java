package com.facebook.presto.aggregation;

import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;

import javax.inject.Provider;

public class AverageAggregation
        implements AggregationFunction
{
    public static final Provider<AggregationFunction> PROVIDER = new Provider<AggregationFunction>()
    {
        @Override
        public AverageAggregation get()
        {
            return new AverageAggregation();
        }
    };

    private static final TupleInfo TUPLE_INFO = new TupleInfo(TupleInfo.Type.DOUBLE);

    private double sum;
    private long count;

    @Override
    public TupleInfo getTupleInfo()
    {
        return TUPLE_INFO;
    }

    @Override
    public void add(Cursor cursor, long endPosition)
    {
        if (cursor.getPosition() > endPosition) {
            return;
        }

        do {
            // TODO: operate on longs. Coercions?
            sum += cursor.getDouble(0);
            ++count;
        } while (cursor.advanceNextPosition() && cursor.getPosition() <= endPosition);
    }

    @Override
    public Tuple evaluate()
    {
        return getTupleInfo().builder()
                .append(sum / count)
                .build();
    }
}
