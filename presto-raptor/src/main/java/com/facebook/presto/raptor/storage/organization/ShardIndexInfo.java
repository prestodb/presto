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
package com.facebook.presto.raptor.storage.organization;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ShardIndexInfo
{
    private final long tableId;
    private final OptionalInt bucketNumber;
    private final UUID shardUuid;
    private final long rowCount;
    private final long uncompressedSize;
    private final Optional<ShardRange> sortRange;
    private final Optional<ShardRange> temporalRange;

    public ShardIndexInfo(
            long tableId,
            OptionalInt bucketNumber,
            UUID shardUuid,
            long rowCount,
            long uncompressedSize,
            Optional<ShardRange> sortRange,
            Optional<ShardRange> temporalRange)
    {
        this.tableId = tableId;
        this.bucketNumber = requireNonNull(bucketNumber, "bucketNumber is null");
        this.shardUuid = requireNonNull(shardUuid, "shardUuid is null");
        this.rowCount = rowCount;
        this.uncompressedSize = uncompressedSize;
        this.sortRange = requireNonNull(sortRange, "sortRange is null");
        this.temporalRange = requireNonNull(temporalRange, "temporalRange is null");
    }

    public long getTableId()
    {
        return tableId;
    }

    public OptionalInt getBucketNumber()
    {
        return bucketNumber;
    }

    public UUID getShardUuid()
    {
        return shardUuid;
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public long getUncompressedSize()
    {
        return uncompressedSize;
    }

    public Optional<ShardRange> getSortRange()
    {
        return sortRange;
    }

    public Optional<ShardRange> getTemporalRange()
    {
        return temporalRange;
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
        ShardIndexInfo that = (ShardIndexInfo) o;
        return tableId == that.tableId &&
                rowCount == that.rowCount &&
                uncompressedSize == that.uncompressedSize &&
                Objects.equals(bucketNumber, that.bucketNumber) &&
                Objects.equals(shardUuid, that.shardUuid) &&
                Objects.equals(sortRange, that.sortRange) &&
                Objects.equals(temporalRange, that.temporalRange);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableId, bucketNumber, shardUuid, rowCount, uncompressedSize, sortRange, temporalRange);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableId", tableId)
                .add("bucketNumber", bucketNumber.isPresent() ? bucketNumber.getAsInt() : null)
                .add("shardUuid", shardUuid)
                .add("rowCount", rowCount)
                .add("uncompressedSize", uncompressedSize)
                .add("sortRange", sortRange.orElse(null))
                .add("temporalRange", temporalRange.orElse(null))
                .omitNullValues()
                .toString();
    }
}
