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
package com.facebook.presto.raptor;

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class RaptorColumnHandle
        implements ConnectorColumnHandle
{
    // This is intentionally not named "$sampleWeight" because column names are lowercase and case insensitive
    public static final String SAMPLE_WEIGHT_COLUMN_NAME = "$sample_weight";

    private final String connectorId;
    private final String columnName;
    private final long columnId;
    private final Type columnType;

    @JsonCreator
    public RaptorColumnHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnId") long columnId,
            @JsonProperty("columnType") Type columnType)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.columnName = checkNotNull(columnName, "columnName is null");
        checkArgument(columnId > 0, "columnId must be greater than zero");
        this.columnId = columnId;
        this.columnType = checkNotNull(columnType, "columnType is null");
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
    public long getColumnId()
    {
        return columnId;
    }

    @JsonProperty
    public Type getColumnType()
    {
        return columnType;
    }

    @Override
    public String toString()
    {
        return connectorId + ":" + columnName + ":" + columnId + ":" + columnType;
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
        RaptorColumnHandle other = (RaptorColumnHandle) obj;
        return Objects.equals(this.columnId, other.columnId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnId);
    }
}
