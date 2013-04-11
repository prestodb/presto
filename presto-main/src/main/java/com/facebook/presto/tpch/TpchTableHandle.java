package com.facebook.presto.tpch;

import com.facebook.presto.metadata.DataSourceType;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import static com.facebook.presto.metadata.MetadataUtil.checkTable;

public class TpchTableHandle
        implements TableHandle
{
    private final QualifiedTableName tableName;

    @JsonCreator
    public TpchTableHandle(@JsonProperty("tableName") QualifiedTableName tableName)
    {
        this.tableName = checkTable(tableName);
    }

    @Override
    public DataSourceType getDataSourceType()
    {
        return DataSourceType.TPCH;
    }

    @JsonProperty
    public QualifiedTableName getTableName()
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
