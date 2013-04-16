package com.facebook.presto.tpch;

import com.facebook.presto.spi.TableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import static com.facebook.presto.metadata.MetadataUtil.checkTableName;

public class TpchTableHandle
        implements TableHandle
{
    private final String tableName;

    @JsonCreator
    public TpchTableHandle(@JsonProperty("tableName") String tableName)
    {
        this.tableName = checkTableName(tableName);
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
    public int hashCode()
    {
        return Objects.hashCode(tableName);
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
        final TpchTableHandle other = (TpchTableHandle) obj;
        return Objects.equal(this.tableName, other.tableName);
    }
}
