package com.facebook.presto.connector.system;

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import static com.facebook.presto.metadata.MetadataUtil.checkSchemaName;
import static com.facebook.presto.metadata.MetadataUtil.checkTableName;
import static com.google.common.base.Preconditions.checkNotNull;

public class SystemTableHandle
        implements TableHandle
{
    private final String schemaName;
    private final String tableName;

    @JsonCreator
    public SystemTableHandle(@JsonProperty("schemaName") String schemaName, @JsonProperty("tableName") String tableName)
    {
        this.schemaName = checkSchemaName(schemaName);
        this.tableName = checkTableName(tableName);
    }

    public SystemTableHandle(SchemaTableName tableName)
    {
        checkNotNull(tableName, "tableName is null");
        this.schemaName = tableName.getSchemaName();
        this.tableName = tableName.getTableName();
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

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public String toString()
    {
        return "system:" + schemaName + "." + tableName;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(schemaName, tableName);
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
        final SystemTableHandle other = (SystemTableHandle) obj;
        return Objects.equal(this.schemaName, other.schemaName) &&
                Objects.equal(this.tableName, other.tableName);
    }
}
