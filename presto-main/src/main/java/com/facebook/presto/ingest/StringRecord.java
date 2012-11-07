/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class StringRecord implements Record
{
    private final List<String> columns;

    public StringRecord(String... columns)
    {
        Preconditions.checkNotNull(columns, "columns is null");
        this.columns = ImmutableList.copyOf(columns);
    }

    public StringRecord(Iterable<String> columns)
    {
        Preconditions.checkNotNull(columns, "columns is null");
        this.columns = ImmutableList.copyOf(columns);
    }

    @Override
    public int getFieldCount()
    {
        return columns.size();
    }

    @Override
    public Long getLong(int field)
    {
        return Long.parseLong(getString(field));
    }

    @Override
    public Double getDouble(int field)
    {
        return Double.parseDouble(getString(field));
    }

    @Override
    public String getString(int field)
    {
        return columns.get(field);
    }
}
