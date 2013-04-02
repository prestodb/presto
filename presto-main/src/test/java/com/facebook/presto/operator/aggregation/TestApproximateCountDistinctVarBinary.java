package com.facebook.presto.operator.aggregation;

import com.facebook.presto.tuple.TupleInfo;
import io.airlift.slice.Slices;

import java.util.concurrent.ThreadLocalRandom;

public class TestApproximateCountDistinctVarBinary
        extends AbstractTestApproximateCountDistinct
{
    @Override
    public ApproximateCountDistinctAggregation getAggregationFunction()
    {
        return ApproximateCountDistinctAggregation.VARBINARY_INSTANCE;
    }

    @Override
    public TupleInfo.Type getValueType()
    {
        return TupleInfo.Type.VARIABLE_BINARY;
    }

    @Override
    public Object randomValue()
    {
        int length = ThreadLocalRandom.current().nextInt(100);
        byte[] bytes = new byte[length];
        ThreadLocalRandom.current().nextBytes(bytes);

        return Slices.wrappedBuffer(bytes);
    }
}
