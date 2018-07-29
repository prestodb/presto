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
package com.facebook.presto.pulsar;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class PulsarTableHandle implements ConnectorTableHandle {

    /**
     * connector id
     */
    private final String connectorId;

    /**
     * The schema name for this table.
     */
    private final String schemaName;

    /**
     * The table name used by presto.
     */
    private final String tableName;

    /**
     * The topic name that is read from Pulsar.
     */
    private final String topicName;

    @JsonCreator
    public PulsarTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("topicName") String topicName
    ) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.topicName = requireNonNull(topicName, "topicName is null");
    }

    @JsonProperty
    public String getConnectorId() {
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public String getTopicName() {
        return topicName;
    }

    public SchemaTableName toSchemaTableName() {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectorId, schemaName, tableName, topicName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        PulsarTableHandle other = (PulsarTableHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId)
                && Objects.equals(this.schemaName, other.schemaName)
                && Objects.equals(this.tableName, other.tableName)
                && Objects.equals(this.topicName, other.topicName);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .add("topicName", topicName)
                .toString();
    }
}
