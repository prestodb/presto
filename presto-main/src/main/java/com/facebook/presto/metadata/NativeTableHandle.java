package com.facebook.presto.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Preconditions.checkArgument;

public class NativeTableHandle
        implements TableHandle
{
    private final long tableId;

    @JsonCreator
    public NativeTableHandle(@JsonProperty("tableId") long tableId)
    {
        checkArgument(tableId > 0, "tableId must be greater than zero");
        this.tableId = tableId;
    }

    @Override
    public DataSourceType getDataSourceType()
    {
        return DataSourceType.NATIVE;
    }

    @JsonProperty
    public long getTableId()
    {
        return tableId;
    }

    @Override
    public String toString()
    {
        return "native:" + tableId;
    }
}
