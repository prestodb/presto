package com.facebook.presto.tuple;

import com.facebook.presto.slice.Slices;
import com.facebook.presto.tuple.TupleInfo.Builder;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.google.common.base.Charsets.UTF_8;

public class Tuples
{
    public static final Tuple NULL_STRING_TUPLE = nullTuple(SINGLE_VARBINARY);
    public static final Tuple NULL_LONG_TUPLE = nullTuple(SINGLE_LONG);
    public static final Tuple NULL_DOUBLE_TUPLE = nullTuple(SINGLE_DOUBLE);

    public static Tuple nullTuple(TupleInfo tupleInfo)
    {
        Builder builder = tupleInfo.builder();
        for (int i = 0; i < tupleInfo.getFieldCount(); i++) {
            builder.appendNull();
        }
        return builder.build();
    }

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
