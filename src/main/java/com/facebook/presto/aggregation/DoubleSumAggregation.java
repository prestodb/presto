package com.facebook.presto.aggregation;

import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;

import javax.inject.Provider;

public class DoubleSumAggregation
        implements AggregationFunction
{
    public static final Provider<AggregationFunction> PROVIDER = provider(0);

    public static Provider<AggregationFunction> provider(final int field)
    {
        return new Provider<AggregationFunction>()
        {
            @Override
            public DoubleSumAggregation get()
            {
                return new DoubleSumAggregation(field);
            }
        };
    }

    private final int field;
    private double sum;

    public DoubleSumAggregation(int field)
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
        if (cursor.getPosition() > endPosition) {
            return;
        }

        do {
            sum += cursor.getDouble(field);
        }
        while (cursor.advanceNextPosition() && cursor.getPosition() <= endPosition);
    }

    @Override
    public void addCurrentPosition(Cursor cursor)
    {
        sum += cursor.getDouble(field);
    }

    @Override
    public Tuple evaluate()
    {
        return getTupleInfo().builder()
                .append(sum)
                .build();
    }
}
