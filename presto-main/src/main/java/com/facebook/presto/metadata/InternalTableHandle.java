package com.facebook.presto.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import static com.facebook.presto.metadata.MetadataUtil.checkTable;

public class InternalTableHandle
        implements TableHandle
{
    private final QualifiedTableName tableName;

    @JsonCreator
    public InternalTableHandle(@JsonProperty("tableName") QualifiedTableName table)
    {
        this.tableName = checkTable(table);
    }

    @Override
    public DataSourceType getDataSourceType()
    {
        return DataSourceType.INTERNAL;
    }

    @JsonProperty
    public QualifiedTableName getTableName()
    {
        return tableName;
    }

    @Override
    public String toString()
    {
        return "internal:" + tableName;
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
        final InternalTableHandle other = (InternalTableHandle) obj;
        return Objects.equal(this.tableName, other.tableName);
    }
}
