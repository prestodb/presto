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
package com.facebook.presto.kafkastream;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

public final class KafkaTableHandle
        implements ConnectorTableHandle
{
    private final String connectorId;
    private final String schemaName;
    private final String tableName;

    @JsonCreator
    public KafkaTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
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

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((connectorId == null) ? 0 : connectorId.hashCode());
        result = prime * result + ((schemaName == null) ? 0 : schemaName.hashCode());
        result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        KafkaTableHandle other = (KafkaTableHandle) obj;
        if (connectorId == null) {
            if (other.connectorId != null) {
                return false;
            }
        }
        else if (!connectorId.equals(other.connectorId)) {
            return false;
        }
        if (schemaName == null) {
            if (other.schemaName != null) {
                return false;
            }
        }
        else if (!schemaName.equals(other.schemaName)) {
            return false;
        }
        if (tableName == null) {
            if (other.tableName != null) {
                return false;
            }
        }
        else if (!tableName.equals(other.tableName)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString()
    {
        return "KafkaTableHandle [connectorId=" + connectorId + ", schemaName=" + schemaName + ", tableName="
                + tableName + "]";
    }
}
