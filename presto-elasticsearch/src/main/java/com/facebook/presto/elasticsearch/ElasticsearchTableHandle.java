
package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public final class ElasticsearchTableHandle
        implements ConnectorTableHandle
{
    private final String connectorId;
    private final String schemaName;
    private final String tableName;

    @JsonCreator
    public ElasticsearchTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.schemaName = checkNotNull(schemaName, "schemaName is null");
        this.tableName = checkNotNull(tableName, "tableName is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
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

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, schemaName, tableName);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        ElasticsearchTableHandle other = (ElasticsearchTableHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId) &&
                Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName);
    }

    @Override
    public String toString()
    {
        return Joiner.on(":").join(connectorId, schemaName, tableName);
    }
}
