package com.facebook.presto.connector.system;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.TableMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class SystemColumnHandle
        implements ColumnHandle
{
    private final String columnName;

    @JsonCreator
    public SystemColumnHandle(@JsonProperty("columnName") String columnName)
    {
        this.columnName = columnName;
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(columnName);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final SystemColumnHandle other = (SystemColumnHandle) obj;
        return Objects.equal(this.columnName, other.columnName);
    }

    @Override
    public String toString()
    {
        return "system:" + columnName;
    }

    public static Map<String, ColumnHandle> toSystemColumnHandles(TableMetadata tableMetadata)
    {
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            columnHandles.put(columnMetadata.getName(), new SystemColumnHandle(columnMetadata.getName()));
        }

        return columnHandles.build();
    }
}
