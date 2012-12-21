/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Charsets.UTF_8;

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
    public long getLong(int field)
    {
        return Long.parseLong(columns.get(field));
    }

    @Override
    public double getDouble(int field)
    {
        return Double.parseDouble(columns.get(field));
    }

    @Override
    public byte[] getString(int field)
    {
        return columns.get(field).getBytes(UTF_8);
    }

    @Override
    public boolean isNull(int field)
    {
        return columns.get(field).isEmpty();
    }
}
