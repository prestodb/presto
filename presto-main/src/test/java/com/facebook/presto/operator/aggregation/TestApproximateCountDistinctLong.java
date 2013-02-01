package com.facebook.presto.operator.aggregation;

import com.facebook.presto.tuple.TupleInfo;

import java.util.concurrent.ThreadLocalRandom;

public class TestApproximateCountDistinctLong
        extends AbstractTestApproximateCountDistinct
{
    @Override
    public ApproximateCountDistinctAggregation getAggregationFunction()
    {
        return ApproximateCountDistinctAggregation.LONG_INSTANCE;
    }

    @Override
    public TupleInfo.Type getValueType()
    {
        return TupleInfo.Type.FIXED_INT_64;
    }

    @Override
    public Object randomValue()
    {
        return ThreadLocalRandom.current().nextLong();
    }
}
