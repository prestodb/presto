package com.facebook.presto.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;

import static com.facebook.presto.metadata.MetadataUtil.checkTable;
import static com.google.common.base.Preconditions.checkArgument;

public class NativeTableHandle
        implements TableHandle
{
    private final QualifiedTableName tableName;
    private final long tableId;

    @JsonCreator
    public NativeTableHandle(@JsonProperty("tableName") QualifiedTableName tableName, @JsonProperty("tableId") long tableId)
    {
        this.tableName = checkTable(tableName);
        checkArgument(tableId > 0, "tableId must be greater than zero");
        this.tableId = tableId;
    }

    @Override
    public DataSourceType getDataSourceType()
    {
        return DataSourceType.NATIVE;
    }

    @JsonProperty
    @VisibleForTesting
    public QualifiedTableName getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public long getTableId()
    {
        return tableId;
    }

    @Override
    public String toString()
    {
        return "native:" + tableName+ ":" + tableId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(tableName, tableId);
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
        final NativeTableHandle other = (NativeTableHandle) obj;
        return Objects.equal(this.tableName, other.tableName) &&
                Objects.equal(this.tableId, other.tableId);
    }
}
