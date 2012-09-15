package com.facebook.presto;

import com.facebook.presto.slice.Slices;

import static com.facebook.presto.TupleInfo.Type.DOUBLE;
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
        Tuple tuple = TupleInfo.SINGLE_LONG.builder()
                .append(value)
                .build();

        return tuple;
    }

    public static Tuple createTuple(double value)
    {
        Tuple tuple = TupleInfo.SINGLE_DOUBLE.builder()
                .append(value)
                .build();

        return tuple;
    }

    public static Tuple createTuple(String value)
    {
        Tuple tuple = TupleInfo.SINGLE_VARBINARY.builder()
                .append(Slices.wrappedBuffer(value.getBytes(UTF_8)))
                .build();

        return tuple;
    }
}
