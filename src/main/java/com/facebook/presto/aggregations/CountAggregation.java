package com.facebook.presto.aggregations;

import com.facebook.presto.Cursor;
import com.facebook.presto.PositionBlock;
import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.ValueBlock;
import com.google.common.base.Optional;

import javax.inject.Provider;

public class CountAggregation
        implements AggregationFunction
{
    public static final Provider<AggregationFunction> PROVIDER = new Provider<AggregationFunction>() {
        @Override
        public CountAggregation get()
        {
            return new CountAggregation();
        }
    };

    private long count;

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
            count += filtered.get().getCount();
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
            count++;
            cursor.advanceNextPosition();
        }
    }

    @Override
    public Tuple evaluate()
    {
        return getTupleInfo().builder()
                .append(count)
                .build();
    }
}
