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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

public final class KafkaColumnHandle
        implements ColumnHandle
{
    private final String connectorId;
    private final String columnName;
    private final String jsonPath;
    private final Type columnType;
    private final int ordinalPosition;

    @JsonCreator
    public KafkaColumnHandle(@JsonProperty("connectorId") String connectorId,
            @JsonProperty("columnName") String columnName, @JsonProperty("columnType") Type columnType,
            @JsonProperty("jsonPath") String jsonPath, @JsonProperty("ordinalPosition") int ordinalPosition)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.jsonPath = jsonPath;
        this.ordinalPosition = ordinalPosition;
    }
    public ColumnMetadata getColumnMetadata()
    {
        return new KafkaColumnMetadata(columnName, columnType, jsonPath);
    }
    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }
    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }
    @JsonProperty
    public String getJsonPath()
    {
        return jsonPath;
    }
    @JsonProperty
    public Type getColumnType()
    {
        return columnType;
    }

    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    @Override
    public String toString()
    {
        return "KafkaColumnHandle [connectorId=" + connectorId + ", columnName=" + columnName + ", jsonPath=" + jsonPath
                + ", columnType=" + columnType + ", ordinalPosition=" + ordinalPosition + "]";
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((columnName == null) ? 0 : columnName.hashCode());
        result = prime * result + ((columnType == null) ? 0 : columnType.hashCode());
        result = prime * result + ((connectorId == null) ? 0 : connectorId.hashCode());
        result = prime * result + ((jsonPath == null) ? 0 : jsonPath.hashCode());
        result = prime * result + ordinalPosition;
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
        KafkaColumnHandle other = (KafkaColumnHandle) obj;
        if (columnName == null) {
            if (other.columnName != null) {
                return false;
            }
        }
        else if (!columnName.equals(other.columnName)) {
            return false;
        }
        if (columnType == null) {
            if (other.columnType != null) {
                return false;
            }
        }
        else if (!columnType.equals(other.columnType)) {
            return false;
        }
        if (connectorId == null) {
            if (other.connectorId != null) {
                return false;
            }
        }
        else if (!connectorId.equals(other.connectorId)) {
            return false;
        }
        if (jsonPath == null) {
            if (other.jsonPath != null) {
                return false;
            }
        }
        else if (!jsonPath.equals(other.jsonPath)) {
            return false;
        }
        if (ordinalPosition != other.ordinalPosition) {
            return false;
        }
        return true;
    }
}
