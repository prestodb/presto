package com.facebook.presto.operator.aggregation;

import com.facebook.presto.slice.Slices;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.primitives.Longs;

import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Charsets.*;
import static com.google.common.base.Charsets.UTF_8;

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
