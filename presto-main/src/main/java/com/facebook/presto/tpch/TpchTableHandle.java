package com.facebook.presto.tpch;

import com.facebook.presto.metadata.DataSourceType;
import com.facebook.presto.metadata.TableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

public class TpchTableHandle
    implements TableHandle
{
    private final String tableName;

    @JsonCreator
    public TpchTableHandle(@JsonProperty("tableName") String tableName)
    {
        this.tableName = Preconditions.checkNotNull(tableName, "tableName is null");
    }

    @Override
    public DataSourceType getDataSourceType()
    {
        throw new UnsupportedOperationException();
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @Override
    public String toString()
    {
        return "tpch:" + tableName;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TpchTableHandle)) {
            return false;
        }

        TpchTableHandle that = (TpchTableHandle) o;

        if (!tableName.equals(that.tableName)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return tableName.hashCode();
    }
}
