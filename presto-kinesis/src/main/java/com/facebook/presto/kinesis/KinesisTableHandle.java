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
package com.facebook.presto.kinesis;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

/**
 *
 * Class maintains all the properties of Presto Table
 *
 */
public class KinesisTableHandle
        implements ConnectorTableHandle
{
    /**
     * connector id
     */
    private final String connectorId;

    /**
     * The schema name for this table. Is set through configuration and read
     * using {@link KinesisConnectorConfig#getDefaultSchema()}. Usually 'default'.
     */
    private final String schemaName;

    /**
     * The table name used by presto.
     */
    private final String tableName;

    /**
     * The stream name that is read from Kinesis
     */
    private final String streamName;

    private final String messageDataFormat;

    @JsonCreator
    public KinesisTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("streamName") String streamName,
            @JsonProperty("messageDataFormat") String messageDataFormat)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.schemaName = checkNotNull(schemaName, "schemaName is null");
        this.tableName = checkNotNull(tableName, "tableName is null");
        this.streamName = checkNotNull(streamName, "topicName is null");
        this.messageDataFormat = checkNotNull(messageDataFormat, "messageDataFormat is null");
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
    public String getStreamName()
    {
        return streamName;
    }

    @JsonProperty
    public String getMessageDataFormat()
    {
        return messageDataFormat;
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(connectorId, schemaName, tableName, streamName, messageDataFormat);
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

        KinesisTableHandle other = (KinesisTableHandle) obj;
        return Objects.equal(this.connectorId, other.connectorId)
                && Objects.equal(this.schemaName, other.schemaName)
                && Objects.equal(this.tableName, other.tableName)
                && Objects.equal(this.streamName, other.streamName)
                && Objects.equal(this.messageDataFormat, other.messageDataFormat);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .add("streamName", streamName)
                .add("messageDataFormat", messageDataFormat)
                .toString();
    }
}
