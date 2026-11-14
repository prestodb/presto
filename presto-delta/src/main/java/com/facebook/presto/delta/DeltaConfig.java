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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.units.Duration;
import com.facebook.airlift.units.MinDuration;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class DeltaConfig
{
    private int maxSplitsBatchSize = 200;
    private boolean parquetDereferencePushdownEnabled = true;
    private boolean caseSensitivePartitionsEnabled = true;
    private boolean tableStatisticsEnabled;
    private int tableStatisticsCacheMaxEntries = 1_000_000;
    private Duration tableStatisticsCacheTtl = new Duration(1, TimeUnit.HOURS);

    @NotNull
    public boolean isParquetDereferencePushdownEnabled()
    {
        return parquetDereferencePushdownEnabled;
    }

    @Config("delta.parquet-dereference-pushdown-enabled")
    public DeltaConfig setParquetDereferencePushdownEnabled(boolean parquetDereferencePushdownEnabled)
    {
        this.parquetDereferencePushdownEnabled = parquetDereferencePushdownEnabled;
        return this;
    }

    public int getMaxSplitsBatchSize()
    {
        return maxSplitsBatchSize;
    }

    @Config("delta.max-splits-batch-size")
    public DeltaConfig setMaxSplitsBatchSize(int maxSplitsBatchSize)
    {
        this.maxSplitsBatchSize = maxSplitsBatchSize;
        return this;
    }

    public boolean isCaseSensitivePartitionsEnabled()
    {
        return this.caseSensitivePartitionsEnabled;
    }

    @Config("delta.case-sensitive-partitions-enabled")
    public DeltaConfig setCaseSensitivePartitionsEnabled(boolean caseSensitivePartitionsEnabled)
    {
        this.caseSensitivePartitionsEnabled = caseSensitivePartitionsEnabled;
        return this;
    }

    public boolean isTableStatisticsEnabled()
    {
        return tableStatisticsEnabled;
    }

    @Config("delta.table-statistics-enabled")
    @ConfigDescription("Enable table statistics derived from the Delta transaction log")
    public DeltaConfig setTableStatisticsEnabled(boolean tableStatisticsEnabled)
    {
        this.tableStatisticsEnabled = tableStatisticsEnabled;
        return this;
    }

    @Min(0)
    public int getTableStatisticsCacheMaxEntries()
    {
        return tableStatisticsCacheMaxEntries;
    }

    @Config("delta.table-statistics-cache-max-entries")
    @ConfigDescription("Maximum number of cached table statistics results (keyed by table snapshot and partition predicate)")
    public DeltaConfig setTableStatisticsCacheMaxEntries(int tableStatisticsCacheMaxEntries)
    {
        this.tableStatisticsCacheMaxEntries = tableStatisticsCacheMaxEntries;
        return this;
    }

    @NotNull
    @MinDuration("1s")
    public Duration getTableStatisticsCacheTtl()
    {
        return tableStatisticsCacheTtl;
    }

    @Config("delta.table-statistics-cache-ttl")
    @ConfigDescription("Time after last access when a cached table statistics entry is evicted")
    public DeltaConfig setTableStatisticsCacheTtl(Duration tableStatisticsCacheTtl)
    {
        this.tableStatisticsCacheTtl = tableStatisticsCacheTtl;
        return this;
    }
}
