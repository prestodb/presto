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
package com.facebook.presto.plugin.jdbc;

import com.google.common.cache.LoadingCache;
import org.weakref.jmx.Managed;

public class JdbcMetadataCacheStats
{
    private LoadingCache<?, ?> tableHandleCache;
    private LoadingCache<?, ?> columnHandlesCache;
    private volatile LoadingCache<?, ?> tableStatisticsCache;

    public void setTableHandleCache(LoadingCache<?, ?> tableHandleCache)
    {
        this.tableHandleCache = tableHandleCache;
    }

    public void setColumnHandlesCache(LoadingCache<?, ?> columnHandlesCache)
    {
        this.columnHandlesCache = columnHandlesCache;
    }

    public void setTableStatisticsCache(LoadingCache<?, ?> tableStatisticsCache)
    {
        this.tableStatisticsCache = tableStatisticsCache;
    }

    @Managed
    public long getTableHandleCacheHit()
    {
        return tableHandleCache.stats().hitCount();
    }

    @Managed
    public long getTableHandleCacheMiss()
    {
        return tableHandleCache.stats().missCount();
    }

    @Managed
    public long getTableHandleCacheEviction()
    {
        return tableHandleCache.stats().evictionCount();
    }

    @Managed
    public long getTableHandleCacheSize()
    {
        return tableHandleCache.size();
    }

    @Managed
    public long getColumnHandlesCacheHit()
    {
        return columnHandlesCache.stats().hitCount();
    }

    @Managed
    public long getColumnHandlesCacheMiss()
    {
        return columnHandlesCache.stats().missCount();
    }

    @Managed
    public long getColumnHandlesCacheEviction()
    {
        return columnHandlesCache.stats().evictionCount();
    }

    @Managed
    public long getColumnHandlesCacheSize()
    {
        return columnHandlesCache.size();
    }

    @Managed
    public long getTableStatisticsCacheHit()
    {
        return tableStatisticsCache == null ? 0 : tableStatisticsCache.stats().hitCount();
    }

    @Managed
    public long getTableStatisticsCacheMiss()
    {
        return tableStatisticsCache == null ? 0 : tableStatisticsCache.stats().missCount();
    }

    @Managed
    public long getTableStatisticsCacheEviction()
    {
        return tableStatisticsCache == null ? 0 : tableStatisticsCache.stats().evictionCount();
    }

    @Managed
    public long getTableStatisticsCacheSize()
    {
        return tableStatisticsCache == null ? 0 : tableStatisticsCache.size();
    }

    @Managed
    public long getTableStatisticsCacheLoadSuccessCount()
    {
        return tableStatisticsCache == null ? 0 : tableStatisticsCache.stats().loadSuccessCount();
    }

    @Managed
    public long getTableStatisticsCacheLoadExceptionCount()
    {
        return tableStatisticsCache == null ? 0 : tableStatisticsCache.stats().loadExceptionCount();
    }

    @Managed
    public double getTableStatisticsCacheAverageLoadPenalty()
    {
        return tableStatisticsCache == null ? 0 : tableStatisticsCache.stats().averageLoadPenalty();
    }
}
