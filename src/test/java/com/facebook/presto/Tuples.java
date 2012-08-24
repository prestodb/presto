package com.facebook.presto;

import static com.facebook.presto.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.base.Charsets.UTF_8;

public class Tuples
{
    public static Tuple createTuple(String key, long count)
    {
        TupleInfo tupleInfo = new TupleInfo(VARIABLE_BINARY, FIXED_INT_64);
        Tuple tuple = tupleInfo.builder()
                .append(Slices.wrappedBuffer(key.getBytes(UTF_8)))
                .append(count)
                .build();

        return tuple;
    }

    public static Tuple createTuple(long value)
    {
        TupleInfo tupleInfo = new TupleInfo(FIXED_INT_64);
        Tuple tuple = tupleInfo.builder()
                .append(value)
                .build();

        return tuple;
    }

    public static Tuple createTuple(String value)
    {
        TupleInfo tupleInfo = new TupleInfo(VARIABLE_BINARY);
        Tuple tuple = tupleInfo.builder()
                .append(Slices.wrappedBuffer(value.getBytes(UTF_8)))
                .build();

        return tuple;
    }
}
