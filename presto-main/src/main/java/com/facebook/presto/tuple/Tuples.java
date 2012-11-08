package com.facebook.presto.tuple;

import com.facebook.presto.slice.Slices;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.google.common.base.Charsets.UTF_8;

public class Tuples
{
    public static final Tuple NULL_STRING_TUPLE = SINGLE_VARBINARY.builder()
            .appendNull()
            .build();

    public static final Tuple NULL_LONG_TUPLE = SINGLE_LONG.builder()
            .appendNull()
            .build();

    public static final Tuple NULL_DOUBLE_TUPLE = SINGLE_DOUBLE.builder()
            .appendNull()
            .build();

    public static Tuple createTuple(long value)
    {
        Tuple tuple = SINGLE_LONG.builder()
                .append(value)
                .build();

        return tuple;
    }

    public static Tuple createTuple(double value)
    {
        Tuple tuple = SINGLE_DOUBLE.builder()
                .append(value)
                .build();

        return tuple;
    }

    public static Tuple createTuple(String value)
    {
        Tuple tuple = SINGLE_VARBINARY.builder()
                .append(Slices.wrappedBuffer(value.getBytes(UTF_8)))
                .build();

        return tuple;
    }
}
