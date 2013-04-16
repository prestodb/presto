package com.facebook.presto.hive;

import com.facebook.presto.spi.ColumnHandle;

import java.util.Objects;

public class HiveColumnHandle
        implements ColumnHandle
{
    private final String columnName;

    public HiveColumnHandle(String columnName)
    {
        this.columnName = columnName;
    }

    public String getColumnName()
    {
        return columnName;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName);
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
        final HiveColumnHandle other = (HiveColumnHandle) obj;
        return Objects.equals(this.columnName, other.columnName);
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("HiveColumnHandle{");
        sb.append("columnName='").append(columnName).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
