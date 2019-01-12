/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.connector.system;

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.facebook.presto.metadata.MetadataUtil.checkSchemaName;
import static com.facebook.presto.metadata.MetadataUtil.checkTableName;
import static java.util.Objects.requireNonNull;

public class SystemTableHandle
        implements ConnectorTableHandle
{
    private final ConnectorId connectorId;
    private final String schemaName;
    private final String tableName;

    @JsonCreator
    public SystemTableHandle(
            @JsonProperty("connectorId") ConnectorId connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schemaName = checkSchemaName(schemaName);
        this.tableName = checkTableName(tableName);
    }

    public static SystemTableHandle fromSchemaTableName(ConnectorId connectorId, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        return new SystemTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName());
    }

    @JsonProperty
    public ConnectorId getConnectorId()
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

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public String toString()
    {
        return connectorId + ":" + schemaName + "." + tableName;
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
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final SystemTableHandle other = (SystemTableHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId) &&
                Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName);
    }
}
