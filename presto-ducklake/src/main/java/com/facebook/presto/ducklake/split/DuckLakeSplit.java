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
package com.facebook.presto.ducklake.split;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.SplitWeight;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * A subset of {@code IcebergSplit}'s shape (same JSON property names for the fields the two
 * connectors share) so that a native translation to a Velox Hive split with Iceberg-style delete
 * files needs no new fields. Two shapes, distinguished by {@link #kind}: a Parquet data file
 * (built by {@link #parquetSplit}), and an inlined table read entirely over JDBC (built by {@link
 * #inlinedSplit}). {@code partitionKeys} is keyed by partition key index (matching {@code
 * DuckLakePartitionField#getPartitionKeyIndex()}), not column id, because a single column can be
 * partitioned by more than one transform (for example {@code month(ts)} and {@code year(ts)} on
 * the same column), and column id would collide. {@code snapshotId} is the scan snapshot, needed
 * by both kinds. Node selection is always {@link NodeSelectionStrategy#NO_PREFERENCE}: DuckLake
 * data lives on a distributed file system, not on worker-local disk.
 */
public class DuckLakeSplit
        implements ConnectorSplit
{
    private final DuckLakeSplitKind kind;
    private final String path;
    private final long start;
    private final long length;
    private final String fileFormat;
    private final long fileSize;
    private final Map<Integer, Optional<String>> partitionKeys;
    private final List<DeleteFile> deletes;
    private final OptionalLong rowIdStart;
    private final OptionalLong partialMax;
    private final long snapshotId;
    private final Optional<String> inlinedTableName;
    private final NodeSelectionStrategy nodeSelectionStrategy;
    private final SplitWeight splitWeight;

    @JsonCreator
    public DuckLakeSplit(
            @JsonProperty("kind") DuckLakeSplitKind kind,
            @JsonProperty("path") String path,
            @JsonProperty("start") long start,
            @JsonProperty("length") long length,
            @JsonProperty("fileFormat") String fileFormat,
            @JsonProperty("fileSize") long fileSize,
            @JsonProperty("partitionKeys") Map<Integer, Optional<String>> partitionKeys,
            @JsonProperty("deletes") List<DeleteFile> deletes,
            @JsonProperty("rowIdStart") OptionalLong rowIdStart,
            @JsonProperty("partialMax") OptionalLong partialMax,
            @JsonProperty("snapshotId") long snapshotId,
            @JsonProperty("inlinedTableName") Optional<String> inlinedTableName,
            @JsonProperty("nodeSelectionStrategy") NodeSelectionStrategy nodeSelectionStrategy,
            @JsonProperty("splitWeight") SplitWeight splitWeight)
    {
        this.kind = requireNonNull(kind, "kind is null");
        this.path = requireNonNull(path, "path is null");
        this.start = start;
        this.length = length;
        this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
        this.fileSize = fileSize;
        this.partitionKeys = ImmutableMap.copyOf(requireNonNull(partitionKeys, "partitionKeys is null"));
        this.deletes = ImmutableList.copyOf(requireNonNull(deletes, "deletes is null"));
        this.rowIdStart = requireNonNull(rowIdStart, "rowIdStart is null");
        this.partialMax = requireNonNull(partialMax, "partialMax is null");
        this.snapshotId = snapshotId;
        this.inlinedTableName = requireNonNull(inlinedTableName, "inlinedTableName is null");
        this.nodeSelectionStrategy = requireNonNull(nodeSelectionStrategy, "nodeSelectionStrategy is null");
        this.splitWeight = requireNonNull(splitWeight, "splitWeight is null");
    }

    public static DuckLakeSplit parquetSplit(
            String path,
            long start,
            long length,
            String fileFormat,
            long fileSize,
            Map<Integer, Optional<String>> partitionKeys,
            List<DeleteFile> deletes,
            OptionalLong rowIdStart,
            OptionalLong partialMax,
            long snapshotId,
            SplitWeight splitWeight)
    {
        return new DuckLakeSplit(
                DuckLakeSplitKind.PARQUET,
                path,
                start,
                length,
                fileFormat,
                fileSize,
                partitionKeys,
                deletes,
                rowIdStart,
                partialMax,
                snapshotId,
                Optional.empty(),
                NO_PREFERENCE,
                splitWeight);
    }

    public static DuckLakeSplit inlinedSplit(String inlinedTableName, long snapshotId, SplitWeight splitWeight)
    {
        return new DuckLakeSplit(
                DuckLakeSplitKind.INLINED,
                "",
                0,
                0,
                "",
                0,
                ImmutableMap.of(),
                ImmutableList.of(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                snapshotId,
                Optional.of(requireNonNull(inlinedTableName, "inlinedTableName is null")),
                NO_PREFERENCE,
                splitWeight);
    }

    @JsonProperty
    public DuckLakeSplitKind getKind()
    {
        return kind;
    }

    @JsonProperty
    public String getPath()
    {
        return path;
    }

    @JsonProperty
    public long getStart()
    {
        return start;
    }

    @JsonProperty
    public long getLength()
    {
        return length;
    }

    @JsonProperty
    public String getFileFormat()
    {
        return fileFormat;
    }

    @JsonProperty
    public long getFileSize()
    {
        return fileSize;
    }

    @JsonProperty
    public Map<Integer, Optional<String>> getPartitionKeys()
    {
        return partitionKeys;
    }

    @JsonProperty
    public List<DeleteFile> getDeletes()
    {
        return deletes;
    }

    @JsonProperty
    public OptionalLong getRowIdStart()
    {
        return rowIdStart;
    }

    @JsonProperty
    public OptionalLong getPartialMax()
    {
        return partialMax;
    }

    @JsonProperty
    public long getSnapshotId()
    {
        return snapshotId;
    }

    @JsonProperty
    public Optional<String> getInlinedTableName()
    {
        return inlinedTableName;
    }

    @JsonProperty
    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return nodeSelectionStrategy;
    }

    @JsonProperty
    @Override
    public SplitWeight getSplitWeight()
    {
        return splitWeight;
    }

    @Override
    public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
    {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo()
    {
        ImmutableMap.Builder<String, Object> info = ImmutableMap.builder();
        info.put("kind", kind);
        info.put("path", path);
        info.put("start", start);
        info.put("length", length);
        info.put("snapshotId", snapshotId);
        inlinedTableName.ifPresent(name -> info.put("inlinedTableName", name));
        return info.build();
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
        DuckLakeSplit that = (DuckLakeSplit) o;
        return start == that.start &&
                length == that.length &&
                fileSize == that.fileSize &&
                snapshotId == that.snapshotId &&
                kind == that.kind &&
                path.equals(that.path) &&
                fileFormat.equals(that.fileFormat) &&
                partitionKeys.equals(that.partitionKeys) &&
                deletes.equals(that.deletes) &&
                rowIdStart.equals(that.rowIdStart) &&
                partialMax.equals(that.partialMax) &&
                inlinedTableName.equals(that.inlinedTableName) &&
                nodeSelectionStrategy == that.nodeSelectionStrategy &&
                splitWeight.equals(that.splitWeight);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                kind,
                path,
                start,
                length,
                fileFormat,
                fileSize,
                partitionKeys,
                deletes,
                rowIdStart,
                partialMax,
                snapshotId,
                inlinedTableName,
                nodeSelectionStrategy,
                splitWeight);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(kind)
                .addValue(path)
                .addValue(start)
                .addValue(length)
                .toString();
    }
}
