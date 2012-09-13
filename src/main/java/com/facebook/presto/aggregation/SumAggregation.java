package com.facebook.presto.aggregation;

import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;

import javax.inject.Provider;

public class SumAggregation
        implements AggregationFunction
{
    public static final Provider<AggregationFunction> PROVIDER = new Provider<AggregationFunction>()
    {
        @Override
        public SumAggregation get()
        {
            return new SumAggregation();
        }
    };
    private static final TupleInfo TUPLE_INFO = new TupleInfo(TupleInfo.Type.FIXED_INT_64);

    private long sum;

    @Override
    public TupleInfo getTupleInfo()
    {
        return TUPLE_INFO;
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

        if (cursor.getPosition() > endPosition) {
            return;
        }

        do {
            sum += cursor.getLong(0);
        } while (cursor.advanceNextPosition() && cursor.getPosition() <= endPosition);
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
