package com.facebook.presto.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Preconditions.checkArgument;

public class NativeColumnHandle
        implements ColumnHandle
{
    private final long columnId;

    @JsonCreator
    public NativeColumnHandle(@JsonProperty("columnId") long columnId)
    {
        checkArgument(columnId > 0, "columnId must be greater than zero");
        this.columnId = columnId;
    }

    @Override
    public DataSourceType getDataSourceType()
    {
        return DataSourceType.NATIVE;
    }

    @JsonProperty
    public long getColumnId()
    {
        return columnId;
    }

    @Override
    public String toString()
    {
        return "native:" + columnId;
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

        NativeColumnHandle that = (NativeColumnHandle) o;

        if (columnId != that.columnId) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return (int) (columnId ^ (columnId >>> 32));
    }
}
