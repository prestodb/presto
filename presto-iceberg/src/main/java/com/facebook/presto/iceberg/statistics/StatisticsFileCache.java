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
package com.facebook.presto.iceberg.statistics;

import com.facebook.airlift.stats.DistributionStat;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.google.common.cache.Cache;
import com.google.common.cache.ForwardingCache.SimpleForwardingCache;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class StatisticsFileCache
        extends SimpleForwardingCache<StatisticsFileCacheKey, ColumnStatistics>
{
    private final DistributionStat fileSizes = new DistributionStat();
    private final DistributionStat columnCounts = new DistributionStat();

    public StatisticsFileCache(Cache<StatisticsFileCacheKey, ColumnStatistics> delegate)
    {
        super(delegate);
    }

    @Managed
    @Nested
    public DistributionStat getFileSizeDistribution()
    {
        return fileSizes;
    }

    public void recordFileSize(long size)
    {
        fileSizes.add(size);
    }

    @Managed
    @Nested
    public DistributionStat getColumnCountDistribution()
    {
        return columnCounts;
    }

    public void recordColumnCount(long count)
    {
        columnCounts.add(count);
    }
}
