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
package com.facebook.presto.raptorx.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ChunkInfo
{
    private final long chunkId;
    private final int bucketNumber;
    private final List<ColumnStats> columnStats;
    private final long rowCount;
    private final long compressedSize;
    private final long uncompressedSize;
    private final long xxhash64;

    @JsonCreator
    public ChunkInfo(
            @JsonProperty("chunkId") long chunkId,
            @JsonProperty("bucketNumber") int bucketNumber,
            @JsonProperty("columnStats") List<ColumnStats> columnStats,
            @JsonProperty("rowCount") long rowCount,
            @JsonProperty("compressedSize") long compressedSize,
            @JsonProperty("uncompressedSize") long uncompressedSize,
            @JsonProperty("xxhash64") long xxhash64)
    {
        this.chunkId = chunkId;
        this.bucketNumber = bucketNumber;
        this.columnStats = ImmutableList.copyOf(requireNonNull(columnStats, "columnStats is null"));

        checkArgument(rowCount >= 0, "rowCount must be positive");
        checkArgument(compressedSize >= 0, "compressedSize must be positive");
        checkArgument(uncompressedSize >= 0, "uncompressedSize must be positive");
        this.rowCount = rowCount;
        this.compressedSize = compressedSize;
        this.uncompressedSize = uncompressedSize;

        this.xxhash64 = xxhash64;
    }

    @JsonProperty
    public long getChunkId()
    {
        return chunkId;
    }

    @JsonProperty
    public int getBucketNumber()
    {
        return bucketNumber;
    }

    @JsonProperty
    public List<ColumnStats> getColumnStats()
    {
        return columnStats;
    }

    @JsonProperty
    public long getRowCount()
    {
        return rowCount;
    }

    @JsonProperty
    public long getCompressedSize()
    {
        return compressedSize;
    }

    @JsonProperty
    public long getUncompressedSize()
    {
        return uncompressedSize;
    }

    @JsonProperty
    public long getXxhash64()
    {
        return xxhash64;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("chunkId", chunkId)
                .add("bucketNumber", bucketNumber)
                .add("columnStats", columnStats)
                .add("rowCount", rowCount)
                .add("compressedSize", compressedSize)
                .add("uncompressedSize", uncompressedSize)
                .add("xxhash64", format("%016x", xxhash64))
                .omitNullValues()
                .toString();
    }
}
