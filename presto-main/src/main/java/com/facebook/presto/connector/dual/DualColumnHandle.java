package com.facebook.presto.connector.dual;

import com.facebook.presto.spi.ColumnHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

public class DualColumnHandle
        implements ColumnHandle
{
    private final String columnName;

    @JsonCreator
    public DualColumnHandle(@JsonProperty("columnName") String columnName)
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
        final DualColumnHandle other = (DualColumnHandle) obj;
        return Objects.equal(this.columnName, other.columnName);
    }

    @Override
    public String toString()
    {
        return "dual:" + columnName;
    }
}
