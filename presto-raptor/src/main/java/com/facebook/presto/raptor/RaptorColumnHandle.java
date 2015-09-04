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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public final class RaptorColumnHandle
        implements ColumnHandle
{
    // This is intentionally not named "$sampleWeight" because column names are lowercase and case insensitive
    public static final String SAMPLE_WEIGHT_COLUMN_NAME = "$sample_weight";

    // Generated rowId column for updates
    private static final long SHARD_ROW_ID_COLUMN_ID = -1;
    private static final String SHARD_ROW_ID_COLUMN_NAME = "$shard_row_id";

    public static final long SHARD_UUID_COLUMN_ID = -2;
    public static final String SHARD_UUID_COLUMN_NAME = "$shard_uuid";

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
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnId = columnId;
        this.columnType = requireNonNull(columnType, "columnType is null");
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

    public boolean isShardRowId()
    {
        return isShardRowIdColumn(columnId);
    }

    public boolean isShardUuid()
    {
        return isShardUuidColumn(columnId);
    }

    public static boolean isShardRowIdColumn(long columnId)
    {
        return columnId == SHARD_ROW_ID_COLUMN_ID;
    }

    public static RaptorColumnHandle shardRowIdHandle(String connectorId)
    {
        return new RaptorColumnHandle(connectorId, SHARD_ROW_ID_COLUMN_NAME, SHARD_ROW_ID_COLUMN_ID, BIGINT);
    }

    public static boolean isShardUuidColumn(long columnId)
    {
        return columnId == SHARD_UUID_COLUMN_ID;
    }

    public static RaptorColumnHandle shardUuidColumnHandle(String connectorId)
    {
        return new RaptorColumnHandle(connectorId, SHARD_UUID_COLUMN_NAME, SHARD_UUID_COLUMN_ID, VARCHAR);
    }
}
