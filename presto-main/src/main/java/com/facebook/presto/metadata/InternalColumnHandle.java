package com.facebook.presto.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class InternalColumnHandle
        implements ColumnHandle
{
    private final int columnIndex;

    @JsonCreator
    public InternalColumnHandle(@JsonProperty("columnIndex") int columnIndex)
    {
        this.columnIndex = columnIndex;
    }

    @JsonProperty
    public int getColumnIndex()
    {
        return columnIndex;
    }

    @Override
    public DataSourceType getDataSourceType()
    {
        return DataSourceType.INTERNAL;
    }
}
