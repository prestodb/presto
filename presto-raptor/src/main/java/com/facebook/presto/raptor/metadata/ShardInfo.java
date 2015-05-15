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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ShardInfo
{
    private final UUID shardUuid;
    private final Set<String> nodeIdentifiers;
    private final List<ColumnStats> columnStats;
    private final long rowCount;
    private final long compressedSize;
    private final long uncompressedSize;

    @JsonCreator
    public ShardInfo(
            @JsonProperty("shardUuid") UUID shardUuid,
            @JsonProperty("nodeIdentifiers") Set<String> nodeIdentifiers,
            @JsonProperty("columnStats") List<ColumnStats> columnStats,
            @JsonProperty("rowCount") long rowCount,
            @JsonProperty("compressedSize") long compressedSize,
            @JsonProperty("uncompressedSize") long uncompressedSize)
    {
        this.shardUuid = checkNotNull(shardUuid, "shardUuid is null");
        this.nodeIdentifiers = ImmutableSet.copyOf(checkNotNull(nodeIdentifiers, "nodeIdentifiers is null"));
        this.columnStats = ImmutableList.copyOf(checkNotNull(columnStats, "columnStats is null"));

        checkArgument(rowCount >= 0, "rowCount must be positive");
        checkArgument(compressedSize >= 0, "compressedSize must be positive");
        checkArgument(uncompressedSize >= 0, "uncompressedSize must be positive");
        this.rowCount = rowCount;
        this.compressedSize = compressedSize;
        this.uncompressedSize = uncompressedSize;
    }

    @JsonProperty
    public UUID getShardUuid()
    {
        return shardUuid;
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

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("shardUuid", shardUuid)
                .add("nodeIdentifiers", nodeIdentifiers)
                .add("columnStats", columnStats)
                .add("rowCount", rowCount)
                .add("compressedSize", compressedSize)
                .add("uncompressedSize", uncompressedSize)
                .toString();
    }
}
