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
package com.facebook.presto.kafka;

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Kafka specific {@link ConnectorTableHandle}.
 */
public final class KafkaTableHandle
        implements ConnectorTableHandle, ConnectorInsertTableHandle
{
    /**
     * connector id
     */
    private final String connectorId;

    /**
     * The schema name for this table. Is set through configuration and read
     * using {@link KafkaConnectorConfig#getDefaultSchema()}. Usually 'default'.
     */
    private final String schemaName;

    /**
     * The table name used by presto.
     */
    private final String tableName;

    /**
     * The topic name that is read from Kafka.
     */
    private final String topicName;

    private final String keyDataFormat;
    private final String messageDataFormat;
    private final Optional<String> keyDataSchemaLocation;
    private final Optional<String> messageDataSchemaLocation;
    private final List<KafkaColumnHandle> columns;

    @JsonCreator
    public KafkaTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("topicName") String topicName,
            @JsonProperty("keyDataFormat") String keyDataFormat,
            @JsonProperty("messageDataFormat") String messageDataFormat,
            @JsonProperty("keyDataSchemaLocation") Optional<String> keyDataSchemaLocation,
            @JsonProperty("messageDataSchemaLocation") Optional<String> messageDataSchemaLocation,
            @JsonProperty("columns") List<KafkaColumnHandle> columns)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.topicName = requireNonNull(topicName, "topicName is null");
        this.keyDataFormat = requireNonNull(keyDataFormat, "keyDataFormat is null");
        this.messageDataFormat = requireNonNull(messageDataFormat, "messageDataFormat is null");
        this.keyDataSchemaLocation = requireNonNull(keyDataSchemaLocation, "keyDataSchemaLocation is null");
        this.messageDataSchemaLocation = requireNonNull(messageDataSchemaLocation, "messageDataSchemaLocation is null");
        this.columns = requireNonNull(ImmutableList.copyOf(columns), "columns is null");
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

    @JsonProperty
    public String getTopicName()
    {
        return topicName;
    }

    @JsonProperty
    public String getKeyDataFormat()
    {
        return keyDataFormat;
    }

    @JsonProperty
    public String getMessageDataFormat()
    {
        return messageDataFormat;
    }

    @JsonProperty
    public Optional<String> getMessageDataSchemaLocation()
    {
        return messageDataSchemaLocation;
    }

    @JsonProperty
    public Optional<String> getKeyDataSchemaLocation()
    {
        return keyDataSchemaLocation;
    }

    @JsonProperty
    public List<KafkaColumnHandle> getColumns()
    {
        return columns;
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, schemaName, tableName, topicName, keyDataFormat, messageDataFormat, keyDataSchemaLocation, messageDataSchemaLocation, columns);
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

        KafkaTableHandle other = (KafkaTableHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId)
                && Objects.equals(this.schemaName, other.schemaName)
                && Objects.equals(this.tableName, other.tableName)
                && Objects.equals(this.topicName, other.topicName)
                && Objects.equals(this.keyDataFormat, other.keyDataFormat)
                && Objects.equals(this.messageDataFormat, other.messageDataFormat)
                && Objects.equals(this.keyDataSchemaLocation, other.keyDataSchemaLocation)
                && Objects.equals(this.messageDataSchemaLocation, other.messageDataSchemaLocation)
                && Objects.equals(this.columns, other.columns);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .add("topicName", topicName)
                .add("keyDataFormat", keyDataFormat)
                .add("messageDataFormat", messageDataFormat)
                .add("keyDataSchemaLocation", keyDataSchemaLocation)
                .add("messageDataSchemaLocation", messageDataSchemaLocation)
                .add("columns", columns)
                .toString();
    }
}
