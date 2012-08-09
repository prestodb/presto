package com.facebook.presto;

import com.google.common.base.Function;

import javax.annotation.Nullable;

public class Pair
{
    private final long position;
    private final Object value;

    public Pair(long position, Object value)
    {
        this.position = position;
        this.value = value;
    }

    public long getPosition()
    {
        return position;
    }

    public Object getValue()
    {
        return value;
    }

    public static Function<Pair, Long> positionGetter()
    {
        return new Function<Pair, Long>()
        {
            @Override
            public Long apply(Pair input)
            {
                return input.getPosition();
            }
        };
    }

    public static Function<Pair, Object> valueGetter()
    {
        return new Function<Pair, Object>()
        {
            @Override
            public Object apply(Pair input)
            {
                return input.getValue();
            }
        };
    }


    @Override
    public String toString()
    {
        return String.format("[%s, %s]", position, value);
    }
}
