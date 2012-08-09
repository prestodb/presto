package com.facebook.presto;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class Tuple
{
    private final List<Object> values;

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
    public String toString()
    {
        return values.toString();
    }
}
