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
package io.prestosql.plugin.raptor.legacy.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ShardInfo
{
    private final UUID shardUuid;
    private final OptionalInt bucketNumber;
    private final Set<String> nodeIdentifiers;
    private final List<ColumnStats> columnStats;
    private final long rowCount;
    private final long compressedSize;
    private final long uncompressedSize;
    private final long xxhash64;

    @JsonCreator
    public ShardInfo(
            @JsonProperty("shardUuid") UUID shardUuid,
            @JsonProperty("bucketNumber") OptionalInt bucketNumber,
            @JsonProperty("nodeIdentifiers") Set<String> nodeIdentifiers,
            @JsonProperty("columnStats") List<ColumnStats> columnStats,
            @JsonProperty("rowCount") long rowCount,
            @JsonProperty("compressedSize") long compressedSize,
            @JsonProperty("uncompressedSize") long uncompressedSize,
            @JsonProperty("xxhash64") long xxhash64)
    {
        this.shardUuid = requireNonNull(shardUuid, "shardUuid is null");
        this.bucketNumber = requireNonNull(bucketNumber, "bucketNumber is null");
        this.nodeIdentifiers = ImmutableSet.copyOf(requireNonNull(nodeIdentifiers, "nodeIdentifiers is null"));
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
    public UUID getShardUuid()
    {
        return shardUuid;
    }

    @JsonProperty
    public OptionalInt getBucketNumber()
    {
        return bucketNumber;
    }

    @JsonProperty
    public Set<String> getNodeIdentifiers()
    {
        return nodeIdentifiers;
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
                .add("shardUuid", shardUuid)
                .add("bucketNumber", bucketNumber.isPresent() ? bucketNumber.getAsInt() : null)
                .add("nodeIdentifiers", nodeIdentifiers)
                .add("columnStats", columnStats)
                .add("rowCount", rowCount)
                .add("compressedSize", compressedSize)
                .add("uncompressedSize", uncompressedSize)
                .add("xxhash64", format("%016x", xxhash64))
                .omitNullValues()
                .toString();
    }
}
