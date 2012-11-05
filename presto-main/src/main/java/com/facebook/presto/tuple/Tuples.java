package com.facebook.presto.tuple;

import com.facebook.presto.slice.Slices;

import static com.google.common.base.Charsets.UTF_8;

public class Tuples
{
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
