package com.facebook.presto.sql.compiler;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class Schema
{
    private final List<Field> fields;

    public Schema(List<Field> fields)
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
        return "{" + Joiner.on(", ").join(fields) + "}";
    }
}
