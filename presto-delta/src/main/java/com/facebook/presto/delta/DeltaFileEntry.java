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
package com.facebook.presto.delta;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Immutable value object for a single active file in a Delta snapshot, with per-file statistics
 * parsed from {@code add.stats}. Used by {@link DeltaTableStatisticsProvider}.
 */
public class DeltaFileEntry
{
    private final String path;
    private final long size;
    private final long modificationTime;
    private final Map<String, String> partitionValues;
    private final Optional<DeltaJsonFileStatistics> stats;
    private final long deletedRowCount;

    public DeltaFileEntry(
            String path,
            long size,
            long modificationTime,
            Map<String, String> partitionValues,
            Optional<DeltaJsonFileStatistics> stats)
    {
        this(path, size, modificationTime, partitionValues, stats, 0L);
    }

    public DeltaFileEntry(
            String path,
            long size,
            long modificationTime,
            Map<String, String> partitionValues,
            Optional<DeltaJsonFileStatistics> stats,
            long deletedRowCount)
    {
        this.path = requireNonNull(path, "path is null");
        this.size = size;
        this.modificationTime = modificationTime;
        requireNonNull(partitionValues, "partitionValues is null");
        ImmutableMap.Builder<String, String> pvBuilder = ImmutableMap.builder();
        for (Map.Entry<String, String> entry : partitionValues.entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                pvBuilder.put(entry.getKey(), entry.getValue());
            }
        }
        this.partitionValues = pvBuilder.build();
        this.stats = requireNonNull(stats, "stats is null");
        this.deletedRowCount = deletedRowCount;
    }

    public String getPath()
    {
        return path;
    }

    public long getSize()
    {
        return size;
    }

    public long getModificationTime()
    {
        return modificationTime;
    }

    public Map<String, String> getPartitionValues()
    {
        return partitionValues;
    }

    public Optional<DeltaJsonFileStatistics> getStats()
    {
        return stats;
    }

    public long getDeletedRowCount()
    {
        return deletedRowCount;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("path", path)
                .add("size", size)
                .add("modificationTime", modificationTime)
                .add("partitionValues", partitionValues)
                .add("stats", stats)
                .add("deletedRowCount", deletedRowCount)
                .toString();
    }
}
