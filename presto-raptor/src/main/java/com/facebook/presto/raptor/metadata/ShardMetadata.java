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
package com.facebook.presto.raptor.metadata;

import io.airlift.units.DataSize;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.UUID;

import static com.facebook.presto.raptor.util.UuidUtil.uuidFromBytes;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.units.DataSize.Unit.BYTE;

public class ShardMetadata
{
    private final long shardId;
    private final UUID shardUuid;
    private final long rowCount;
    private final long dataSize;

    public ShardMetadata(long shardId, UUID shardUuid, long rowCount, long dataSize)
    {
        checkArgument(shardId > 0, "shardId must be > 0");
        checkArgument(rowCount >= 0, "rowCount must be >= 0");
        checkArgument(dataSize >= 0, "dataSize must be >= 0");

        this.shardId = shardId;
        this.shardUuid = checkNotNull(shardUuid, "shardUuid is null");
        this.rowCount = rowCount;
        this.dataSize = dataSize;
    }

    public UUID getShardUuid()
    {
        return shardUuid;
    }

    public long getShardId()
    {
        return shardId;
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public long getDataSize()
    {
        return dataSize;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("shardId", shardId)
                .add("shardUuid", shardUuid)
                .add("rowCount", rowCount)
                .add("dataSize", new DataSize(dataSize, BYTE).convertToMostSuccinctDataSize())
                .toString();
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        ShardMetadata that = (ShardMetadata) other;

        return Objects.equals(this.shardId, that.shardId) &&
                Objects.equals(this.shardUuid, that.shardUuid) &&
                Objects.equals(this.rowCount, that.rowCount) &&
                Objects.equals(this.dataSize, that.dataSize);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(shardId, shardUuid, rowCount, dataSize);
    }

    public static class Mapper
            implements ResultSetMapper<ShardMetadata>
    {
        @Override
        public ShardMetadata map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return new ShardMetadata(
                    r.getLong("shard_id"),
                    uuidFromBytes(r.getBytes("shard_uuid")),
                    r.getLong("row_count"),
                    r.getLong("data_size"));
        }
    }
}
