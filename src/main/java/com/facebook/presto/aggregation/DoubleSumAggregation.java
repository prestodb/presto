package com.facebook.presto.aggregation;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;

import javax.inject.Provider;

public class DoubleSumAggregation
        implements AggregationFunction
{
    public static final Provider<AggregationFunction> PROVIDER = new Provider<AggregationFunction>()
    {
        @Override
        public DoubleSumAggregation get()
        {
            return new DoubleSumAggregation();
        }
    };

    private double sum;

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
            sum += cursor.getDouble(0);
        }
        while (cursor.advanceNextPosition() && cursor.getPosition() <= endPosition);
    }

    @Override
    public Tuple evaluate()
    {
        return getTupleInfo().builder()
                .append(sum)
                .build();
    }
}
