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

import java.util.Optional;

public class PartitionStatistics
{
    public static final PartitionStatistics EMPTY_STATISTICS = new PartitionStatistics(
            false,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    private final boolean columnStatsAcurate;
    private final Optional<Long> numFiles;
    private final Optional<Long> numRows;
    private final Optional<Long> rawDataSize;
    private final Optional<Long> totalSize;

    public PartitionStatistics(boolean columnStatsAcurate,
            Optional<Long> numFiles,
            Optional<Long> numRows,
            Optional<Long> rawDataSize,
            Optional<Long> totalSize)
    {
        this.columnStatsAcurate = columnStatsAcurate;
        this.numFiles = numFiles;
        this.numRows = numRows;
        this.rawDataSize = rawDataSize;
        this.totalSize = totalSize;
    }

    public boolean isColumnStatsAcurate()
    {
        return columnStatsAcurate;
    }

    public Optional<Long> getNumFiles()
    {
        return numFiles;
    }

    public Optional<Long> getNumRows()
    {
        return numRows;
    }

    public Optional<Long> getRawDataSize()
    {
        return rawDataSize;
    }

    public Optional<Long> getTotalSize()
    {
        return totalSize;
    }
}
