package com.facebook.presto.hive;

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;

import java.util.Objects;

public class HiveTableHandle
        implements TableHandle
{
    private final SchemaTableName tableName;

    public HiveTableHandle(SchemaTableName tableName)
    {
        if (tableName == null) {
            throw new NullPointerException("tableName is null");
        }
        this.tableName = tableName;
    }

    public SchemaTableName getTableName()
    {
        return tableName;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName);
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
        final HiveTableHandle other = (HiveTableHandle) obj;
        return Objects.equals(this.tableName, other.tableName);
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("HiveTableHandle{");
        sb.append("tableName=").append(tableName);
        sb.append('}');
        return sb.toString();
    }
}
