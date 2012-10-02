package com.facebook.presto.metadata;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class TableMetadata
{
    private final String name;
    private final List<ColumnMetadata> columns;

    public TableMetadata(String name, List<ColumnMetadata> columns)
    {
        this.name = name;
        this.columns = ImmutableList.copyOf(columns);
    }

    public String getName()
    {
        return name;
    }

    public List<ColumnMetadata> getColumns()
    {
        return columns;
    }
}
