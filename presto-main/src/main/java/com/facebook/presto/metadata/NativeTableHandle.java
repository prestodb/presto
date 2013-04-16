package com.facebook.presto.metadata;

import com.facebook.presto.spi.TableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import static com.facebook.presto.metadata.MetadataUtil.checkSchemaName;
import static com.facebook.presto.metadata.MetadataUtil.checkTableName;
import static com.google.common.base.Preconditions.checkArgument;

public class NativeTableHandle
        implements TableHandle
{
    private final String schemaName;
    private final String tableName;
    private final long tableId;

    @JsonCreator
    public NativeTableHandle(@JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tableId") long tableId)
    {
        this.schemaName = checkSchemaName(schemaName);
        this.tableName = checkTableName(tableName);

        checkArgument(tableId > 0, "tableId must be greater than zero");
        this.tableId = tableId;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
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
        return "native:" + schemaName + "." + tableName + ":" + tableId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(schemaName, tableName, tableId);
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
        return Objects.equal(this.schemaName, other.schemaName) &&
                Objects.equal(this.tableName, other.tableName) &&
                Objects.equal(this.tableId, other.tableId);
    }
}
