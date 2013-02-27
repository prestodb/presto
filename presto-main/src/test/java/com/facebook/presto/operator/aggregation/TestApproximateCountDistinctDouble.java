package com.facebook.presto.operator.aggregation;

import com.facebook.presto.tuple.TupleInfo;

import java.util.concurrent.ThreadLocalRandom;

public class TestApproximateCountDistinctDouble
        extends AbstractTestApproximateCountDistinct
{
    @Override
    public ApproximateCountDistinctAggregation getAggregationFunction()
    {
        return ApproximateCountDistinctAggregation.DOUBLE_INSTANCE;
    }

    @Override
    public TupleInfo.Type getValueType()
    {
        return TupleInfo.Type.DOUBLE;
    }

    @Override
    public Object randomValue()
    {
        return ThreadLocalRandom.current().nextDouble();
    }
}
