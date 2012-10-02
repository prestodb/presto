package com.facebook.presto.aggregation;

import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;

import javax.inject.Provider;

public class AverageAggregation
        implements AggregationFunction
{
    public static final Provider<AggregationFunction> PROVIDER = provider(0);

    public static Provider<AggregationFunction> provider(final int field)
    {
        return new Provider<AggregationFunction>()
        {
            @Override
            public AverageAggregation get()
            {
                return new AverageAggregation(field);
            }
        };
    }


    private final int field;
    private double sum;
    private long count;

    public AverageAggregation(int field)
    {
        this.field = field;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return TupleInfo.SINGLE_DOUBLE;
    }

    @Override
    public void add(Cursor cursor, long endPosition)
    {
        if (cursor.getPosition() <= endPosition) {
            do {
                // TODO: operate on longs. Coercions?
                sum += cursor.getDouble(0);
                ++count;
            }
            while (cursor.getPosition() < endPosition && cursor.advanceNextPosition());
        }
    }

    @Override
    public void addCurrentPosition(Cursor cursor)
    {
        sum += cursor.getDouble(field);
        ++count;
    }

    @Override
    public Tuple evaluate()
    {
        return getTupleInfo().builder()
                .append(sum / count)
                .build();
    }
}
