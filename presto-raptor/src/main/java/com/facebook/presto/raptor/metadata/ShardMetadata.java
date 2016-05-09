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
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.UUID;

import static com.facebook.presto.raptor.util.DatabaseUtil.getOptionalInt;
import static com.facebook.presto.raptor.util.UuidUtil.uuidFromBytes;
import static com.google.common.base.MoreObjects.ToStringHelper;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ShardMetadata
{
    private final long tableId;
    private final long shardId;
    private final UUID shardUuid;
    private final OptionalInt bucketNumber;
    private final long rowCount;
    private final long compressedSize;
    private final long uncompressedSize;
    private final OptionalLong rangeStart;
    private final OptionalLong rangeEnd;

    public ShardMetadata(
            long tableId,
            long shardId,
            UUID shardUuid,
            OptionalInt bucketNumber,
            long rowCount,
            long compressedSize,
            long uncompressedSize,
            OptionalLong rangeStart,
            OptionalLong rangeEnd)
    {
        checkArgument(tableId > 0, "tableId must be > 0");
        checkArgument(shardId > 0, "shardId must be > 0");
        checkArgument(rowCount >= 0, "rowCount must be >= 0");
        checkArgument(compressedSize >= 0, "compressedSize must be >= 0");
        checkArgument(uncompressedSize >= 0, "uncompressedSize must be >= 0");

        this.tableId = tableId;
        this.shardId = shardId;
        this.shardUuid = requireNonNull(shardUuid, "shardUuid is null");
        this.bucketNumber = requireNonNull(bucketNumber, "bucketNumber is null");
        this.rowCount = rowCount;
        this.compressedSize = compressedSize;
        this.uncompressedSize = uncompressedSize;
        this.rangeStart = requireNonNull(rangeStart, "rangeStart is null");
        this.rangeEnd = requireNonNull(rangeEnd, "rangeEnd is null");
    }

    public long getTableId()
    {
        return tableId;
    }

    public UUID getShardUuid()
    {
        return shardUuid;
    }

    public long getShardId()
    {
        return shardId;
    }

    public OptionalInt getBucketNumber()
    {
        return bucketNumber;
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public long getCompressedSize()
    {
        return compressedSize;
    }

    public long getUncompressedSize()
    {
        return uncompressedSize;
    }

    public OptionalLong getRangeStart()
    {
        return rangeStart;
    }

    public OptionalLong getRangeEnd()
    {
        return rangeEnd;
    }

    public ShardMetadata withTimeRange(long rangeStart, long rangeEnd)
    {
        return new ShardMetadata(
                tableId,
                shardId,
                shardUuid,
                bucketNumber,
                rowCount,
                compressedSize,
                uncompressedSize,
                OptionalLong.of(rangeStart),
                OptionalLong.of(rangeEnd));
    }
    @Override
    public String toString()
    {
        ToStringHelper stringHelper = toStringHelper(this)
                .add("tableId", tableId)
                .add("shardId", shardId)
                .add("shardUuid", shardUuid)
                .add("rowCount", rowCount)
                .add("compressedSize", DataSize.succinctBytes(compressedSize))
                .add("uncompressedSize", DataSize.succinctBytes(uncompressedSize));

        if (bucketNumber.isPresent()) {
            stringHelper.add("bucketNumber", bucketNumber.getAsInt());
        }
        if (rangeStart.isPresent()) {
            stringHelper.add("rangeStart", rangeStart.getAsLong());
        }
        if (rangeEnd.isPresent()) {
            stringHelper.add("rangeEnd", rangeEnd.getAsLong());
        }
        return stringHelper.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ShardMetadata that = (ShardMetadata) o;
        return Objects.equals(tableId, that.tableId) &&
                Objects.equals(shardId, that.shardId) &&
                Objects.equals(bucketNumber, that.bucketNumber) &&
                Objects.equals(rowCount, that.rowCount) &&
                Objects.equals(compressedSize, that.compressedSize) &&
                Objects.equals(uncompressedSize, that.uncompressedSize) &&
                Objects.equals(shardUuid, that.shardUuid) &&
                Objects.equals(rangeStart, that.rangeStart) &&
                Objects.equals(rangeEnd, that.rangeEnd);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                tableId,
                shardId,
                shardUuid,
                bucketNumber,
                rowCount,
                compressedSize,
                uncompressedSize,
                rangeStart,
                rangeEnd);
    }

    public static class Mapper
            implements ResultSetMapper<ShardMetadata>
    {
        @Override
        public ShardMetadata map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return new ShardMetadata(
                    r.getLong("table_id"),
                    r.getLong("shard_id"),
                    uuidFromBytes(r.getBytes("shard_uuid")),
                    getOptionalInt(r, "bucket_number"),
                    r.getLong("row_count"),
                    r.getLong("compressed_size"),
                    r.getLong("uncompressed_size"),
                    OptionalLong.empty(),
                    OptionalLong.empty());
        }
    }
}
