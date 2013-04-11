package com.facebook.presto.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import static com.facebook.presto.metadata.MetadataUtil.checkTable;

public class ImportTableHandle
        implements TableHandle
{
    private final QualifiedTableName tableName;

    @JsonCreator
    public ImportTableHandle(@JsonProperty("tableName") QualifiedTableName tableName)
    {
        this.tableName = checkTable(tableName);
    }

    @Override
    public DataSourceType getDataSourceType()
    {
        return DataSourceType.IMPORT;
    }

    @JsonProperty
    public QualifiedTableName getTableName()
    {
        return tableName;
    }

    @Override
    public String toString()
    {
        return "import:" + tableName;
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
        final ImportTableHandle other = (ImportTableHandle) obj;
        return Objects.equal(this.tableName, other.tableName);
    }
}
