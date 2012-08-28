package com.facebook.presto.aggregations;

import com.facebook.presto.Cursor;
import com.facebook.presto.PositionBlock;
import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.ValueBlock;
import com.google.common.base.Optional;

import javax.inject.Provider;

public class SumAggregation
        implements AggregationFunction
{
    public static Provider<AggregationFunction> PROVIDER = new Provider<AggregationFunction>() {
        @Override
        public SumAggregation get()
        {
            return new SumAggregation();
        }
    };

    private long sum;

    @Override
    public TupleInfo getTupleInfo()
    {
        return new TupleInfo(TupleInfo.Type.FIXED_INT_64);
    }

    @Override
    public void add(ValueBlock values, PositionBlock relevantPositions)
    {
        Optional<ValueBlock> filtered = values.filter(relevantPositions);

        if (filtered.isPresent()) {
            for (Tuple value : filtered.get()) {
                sum += value.getLong(0);
            }
        }
    }

    @Override
    public void add(Cursor cursor, Range relevantRange)
    {
        // todo if cursor is not "valid", advance to first position

        // advance to start of range
        // todo add seek method to cursor
        while (cursor.getPosition() < relevantRange.getStart()) {
            cursor.advanceNextPosition();
        }

        while (relevantRange.contains(cursor.getPosition())) {
            sum += cursor.getLong(0);
            cursor.advanceNextPosition();
        }
    }

    @Override
    public Tuple evaluate()
    {
        return getTupleInfo().builder()
                .append(sum)
                .build();
    }
}
