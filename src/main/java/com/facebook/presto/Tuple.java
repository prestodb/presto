package com.facebook.presto;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class Tuple
{
    private final List<Object> values;

    public Tuple(Object... values) {
        this(ImmutableList.copyOf(values));
    }

    public Tuple(List<Object> values)
    {
        Preconditions.checkNotNull(values, "values is null");
        this.values = ImmutableList.copyOf(values);
    }

    public List<Object> getValues()
    {
        return values;
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

        Tuple tuple = (Tuple) o;

        if (!values.equals(tuple.values)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return values.hashCode();
    }

    @Override
    public String toString()
    {
        return values.toString();
    }
}
