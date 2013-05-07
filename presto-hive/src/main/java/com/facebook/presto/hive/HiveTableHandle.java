package com.facebook.presto.hive;

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class HiveTableHandle
        implements TableHandle
{
    private final String clientId;
    private final String schemaName;
    private final String tableName;

    @JsonCreator
    public HiveTableHandle(@JsonProperty("clientId") String clientId, @JsonProperty("schemaName") String schemaName, @JsonProperty("tableName") String tableName)
    {
        this.clientId = checkNotNull(clientId, "clientId is null");
        this.schemaName = checkNotNull(schemaName, "schemaName is null");
        this.tableName = checkNotNull(tableName, "tableName is null");
    }

    @JsonProperty
    public String getClientId()
    {
        return clientId;
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
    public int hashCode()
    {
        return Objects.hashCode(clientId, schemaName, tableName);
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
        return Objects.equal(this.clientId, other.clientId) &&
                Objects.equal(this.schemaName, other.schemaName) &&
                Objects.equal(this.tableName, other.tableName);
    }

    @Override
    public String toString()
    {
        return clientId + ":" + schemaName + ":" + tableName;
    }
}
