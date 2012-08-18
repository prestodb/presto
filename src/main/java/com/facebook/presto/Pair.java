package com.facebook.presto;

import com.google.common.base.Function;

public class Pair
{
    private final long position;
    private final Tuple value;

    public Pair(long position, Tuple value)
    {
        this.position = position;
        this.value = value;
    }

    public long getPosition()
    {
        return position;
    }

    public Tuple getValue()
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

    public static Function<Pair, Tuple> valueGetter()
    {
        return new Function<Pair, Tuple>()
        {
            @Override
            public Tuple apply(Pair input)
            {
                return input.getValue();
            }
        };
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Pair pair = (Pair) o;

        if (position != pair.position) {
            return false;
        }
        if (value != null ? !value.equals(pair.value) : pair.value != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (position ^ (position >>> 32));
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return String.format("[%s, %s]", position, value);
    }
}
