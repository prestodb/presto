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
package com.facebook.presto.raptorx.storage.organization;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ChunkIndexInfo
{
    private final long tableId;
    private final int bucketNumber;
    private final long chunkId;
    private final long rowCount;
    private final long uncompressedSize;
    private final Optional<ChunkRange> sortRange;
    private final Optional<ChunkRange> temporalRange;

    public ChunkIndexInfo(
            long tableId,
            int bucketNumber,
            long chunkId,
            long rowCount,
            long uncompressedSize,
            Optional<ChunkRange> sortRange,
            Optional<ChunkRange> temporalRange)
    {
        this.tableId = tableId;
        this.bucketNumber = bucketNumber;
        this.chunkId = chunkId;
        this.rowCount = rowCount;
        this.uncompressedSize = uncompressedSize;
        this.sortRange = requireNonNull(sortRange, "sortRange is null");
        this.temporalRange = requireNonNull(temporalRange, "temporalRange is null");
    }

    public long getTableId()
    {
        return tableId;
    }

    public int getBucketNumber()
    {
        return bucketNumber;
    }

    public long getChunkId()
    {
        return chunkId;
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public long getUncompressedSize()
    {
        return uncompressedSize;
    }

    public Optional<ChunkRange> getSortRange()
    {
        return sortRange;
    }

    public Optional<ChunkRange> getTemporalRange()
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
        ChunkIndexInfo that = (ChunkIndexInfo) o;
        return tableId == that.tableId &&
                rowCount == that.rowCount &&
                uncompressedSize == that.uncompressedSize &&
                Objects.equals(bucketNumber, that.bucketNumber) &&
                Objects.equals(chunkId, that.chunkId) &&
                Objects.equals(sortRange, that.sortRange) &&
                Objects.equals(temporalRange, that.temporalRange);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableId, bucketNumber, chunkId, rowCount, uncompressedSize, sortRange, temporalRange);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableId", tableId)
                .add("bucketNumber", bucketNumber)
                .add("chunkId", chunkId)
                .add("rowCount", rowCount)
                .add("uncompressedSize", uncompressedSize)
                .add("sortRange", sortRange.orElse(null))
                .add("temporalRange", temporalRange.orElse(null))
                .omitNullValues()
                .toString();
    }
}
