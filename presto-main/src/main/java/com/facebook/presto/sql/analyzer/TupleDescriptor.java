package com.facebook.presto.sql.analyzer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class TupleDescriptor
{
    private final List<Field> fields;

    public TupleDescriptor(List<Field> fields)
    {
        Preconditions.checkNotNull(fields, "fields is null");
        this.fields = ImmutableList.copyOf(fields);
    }

    public List<Field> getFields()
    {
        return fields;
    }

    @Override
    public String toString()
    {
        return fields.toString();
    }
}
