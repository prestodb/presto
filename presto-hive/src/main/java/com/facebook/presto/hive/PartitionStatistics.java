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

package com.facebook.presto.hive;

import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public class PartitionStatistics
{
    public static final PartitionStatistics EMPTY_STATISTICS = new PartitionStatistics(
            false,
            OptionalLong.empty(),
            OptionalLong.empty(),
            OptionalLong.empty(),
            OptionalLong.empty(),
            ImmutableMap.of());

    private final boolean columnStatsAcurate;
    private final OptionalLong fileCount;
    private final OptionalLong rowCount;
    private final OptionalLong rawDataSize;
    private final OptionalLong totalSize;
    private final Map<String, HiveColumnStatistics> columnStatistics;

    public PartitionStatistics(
            boolean columnStatsAcurate,
            OptionalLong fileCount,
            OptionalLong rowCount,
            OptionalLong rawDataSize,
            OptionalLong totalSize,
            Map<String, HiveColumnStatistics> columnStatistics)
    {
        this.columnStatsAcurate = columnStatsAcurate;
        this.fileCount = fileCount;
        this.rowCount = rowCount;
        this.rawDataSize = rawDataSize;
        this.totalSize = totalSize;
        this.columnStatistics = ImmutableMap.copyOf(requireNonNull(columnStatistics, "columnStatistics can not be null"));
    }

    public boolean isColumnStatsAcurate()
    {
        return columnStatsAcurate;
    }

    public OptionalLong getFileCount()
    {
        return fileCount;
    }

    public OptionalLong getRowCount()
    {
        return rowCount;
    }

    public OptionalLong getRawDataSize()
    {
        return rawDataSize;
    }

    public OptionalLong getTotalSize()
    {
        return totalSize;
    }

    public Map<String, HiveColumnStatistics> getColumnStatistics()
    {
        return columnStatistics;
    }
}
