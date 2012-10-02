package com.facebook.presto.aggregation;

import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;

import javax.inject.Provider;

public class SumAggregation
        implements AggregationFunction
{
    public static final Provider<AggregationFunction> PROVIDER = provider(0);

    public static Provider<AggregationFunction> provider(final int field)
    {
        return new Provider<AggregationFunction>()
        {
            @Override
            public SumAggregation get()
            {
                return new SumAggregation(field);
            }
        };
    }

    private final int field;
    private long sum;

    public SumAggregation(int field)
    {
        this.field = field;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return TupleInfo.SINGLE_LONG;
    }

    @Override
    public void add(Cursor cursor, long endPosition)
    {
//  todo: rle code
//        cursor.advanceToPosition(relevantRange.getStart());
//        do {
//            long endPosition = Math.min(cursor.getCurrentValueEndPosition(), relevantRange.getEnd());
//            long size = endPosition - cursor.getPosition() + 1;
//            sum += (cursor.getLong(0) * size);
//            if (!cursor.hasNextPosition()) {
//                break;
//            }
//            cursor.advanceNextPosition();
//        }  while (relevantRange.contains(cursor.getPosition()));

        if (cursor.getPosition() <= endPosition) {
            do {
                sum += cursor.getLong(0);
            }
            while (cursor.getPosition() < endPosition && cursor.advanceNextPosition());
        }
    }

    @Override
    public void addCurrentPosition(Cursor cursor)
    {
        sum += cursor.getLong(field);
    }

    @Override
    public Tuple evaluate()
    {
//        System.out.println(count + ", " + sum);
        return getTupleInfo().builder()
                .append(sum)
                .build();
    }
}
