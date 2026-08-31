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
package com.facebook.presto.hive.metastore;

import com.facebook.airlift.stats.CounterStat;
import com.google.common.cache.LoadingCache;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class HiveMetastoreCacheStats
        implements MetastoreCacheStats
{
    private final CounterStat partitionsWithColumnCountGreaterThanThreshold = new CounterStat();
    private LoadingCache<?, ?> databaseCache;
    private LoadingCache<?, ?> databaseNamesCache;
    private LoadingCache<?, ?> tableCache;
    private LoadingCache<?, ?> tableNamesCache;
    private LoadingCache<?, ?> tableStatisticsCache;
    private LoadingCache<?, ?> tableConstraintsCache;
    private LoadingCache<?, ?> partitionStatisticsCache;
    private LoadingCache<?, ?> viewNamesCache;
    private LoadingCache<?, ?> partitionCache;
    private LoadingCache<?, ?> partitionFilterCache;
    private LoadingCache<?, ?> partitionNamesCache;
    private LoadingCache<?, ?> tablePrivilegesCache;
    private LoadingCache<?, ?> rolesCache;
    private LoadingCache<?, ?> roleGrantsCache;

    @Override
    public void setDatabaseCache(LoadingCache<?, ?> databaseCache)
    {
        this.databaseCache = databaseCache;
    }

    @Override
    public void setDatabaseNamesCache(LoadingCache<?, ?> databaseNamesCache)
    {
        this.databaseNamesCache = databaseNamesCache;
    }

    @Override
    public void setTableCache(LoadingCache<?, ?> tableCache)
    {
        this.tableCache = tableCache;
    }

    @Override
    public void setTableNamesCache(LoadingCache<?, ?> tableNamesCache)
    {
        this.tableNamesCache = tableNamesCache;
    }

    @Override
    public void setTableStatisticsCache(LoadingCache<?, ?> tableStatisticsCache)
    {
        this.tableStatisticsCache = tableStatisticsCache;
    }

    @Override
    public void setTableConstraintsCache(LoadingCache<?, ?> tableConstraintsCache)
    {
        this.tableConstraintsCache = tableConstraintsCache;
    }

    @Override
    public void setPartitionStatisticsCache(LoadingCache<?, ?> partitionStatisticsCache)
    {
        this.partitionStatisticsCache = partitionStatisticsCache;
    }

    @Override
    public void setViewNamesCache(LoadingCache<?, ?> viewNamesCache)
    {
        this.viewNamesCache = viewNamesCache;
    }

    @Override
    public void setPartitionCache(LoadingCache<?, ?> partitionCache)
    {
        this.partitionCache = partitionCache;
    }

    @Override
    public void setPartitionFilterCache(LoadingCache<?, ?> partitionFilterCache)
    {
        this.partitionFilterCache = partitionFilterCache;
    }

    @Override
    public void setPartitionNamesCache(LoadingCache<?, ?> partitionNamesCache)
    {
        this.partitionNamesCache = partitionNamesCache;
    }

    @Override
    public void setTablePrivilegesCache(LoadingCache<?, ?> tablePrivilegesCache)
    {
        this.tablePrivilegesCache = tablePrivilegesCache;
    }

    @Override
    public void setRolesCache(LoadingCache<?, ?> rolesCache)
    {
        this.rolesCache = rolesCache;
    }

    @Override
    public void setRoleGrantsCache(LoadingCache<?, ?> roleGrantsCache)
    {
        this.roleGrantsCache = roleGrantsCache;
    }

    @Override
    public void incrementPartitionsWithColumnCountGreaterThanThreshold()
    {
        partitionsWithColumnCountGreaterThanThreshold.update(1);
    }

    @Managed
    @Override
    public long getDatabaseCacheHit()
    {
        return databaseCache.stats().hitCount();
    }

    @Managed
    @Override
    public long getDatabaseCacheMiss()
    {
        return databaseCache.stats().missCount();
    }

    @Managed
    @Override
    public long getDatabaseCacheEviction()
    {
        return databaseCache.stats().evictionCount();
    }

    @Managed
    @Override
    public long getDatabaseCacheSize()
    {
        return databaseCache.size();
    }

    @Managed
    @Override
    public long getDatabaseNamesCacheHit()
    {
        return databaseNamesCache.stats().hitCount();
    }

    @Managed
    @Override
    public long getDatabaseNamesCacheMiss()
    {
        return databaseNamesCache.stats().missCount();
    }

    @Managed
    @Override
    public long getDatabaseNamesCacheEviction()
    {
        return databaseNamesCache.stats().evictionCount();
    }

    @Managed
    @Override
    public long getDatabaseNamesCacheSize()
    {
        return databaseNamesCache.size();
    }

    @Managed
    @Override
    public long getTableCacheHit()
    {
        return tableCache.stats().hitCount();
    }

    @Managed
    @Override
    public long getTableCacheMiss()
    {
        return tableCache.stats().missCount();
    }

    @Managed
    @Override
    public long getTableCacheEviction()
    {
        return tableCache.stats().evictionCount();
    }

    @Managed
    @Override
    public long getTableCacheSize()
    {
        return tableCache.size();
    }

    @Managed
    @Override
    public long getTableNamesCacheHit()
    {
        return tableNamesCache.stats().hitCount();
    }

    @Managed
    @Override
    public long getTableNamesCacheMiss()
    {
        return tableNamesCache.stats().missCount();
    }

    @Managed
    @Override
    public long getTableNamesCacheEviction()
    {
        return tableNamesCache.stats().evictionCount();
    }

    @Managed
    @Override
    public long getTableNamesCacheSize()
    {
        return tableNamesCache.size();
    }

    @Managed
    @Override
    public long getTableStatisticsCacheHit()
    {
        return tableStatisticsCache.stats().hitCount();
    }

    @Managed
    @Override
    public long getTableStatisticsCacheMiss()
    {
        return tableStatisticsCache.stats().missCount();
    }

    @Managed
    @Override
    public long getTableStatisticsCacheEviction()
    {
        return tableStatisticsCache.stats().evictionCount();
    }

    @Managed
    @Override
    public long getTableStatisticsCacheSize()
    {
        return tableStatisticsCache.size();
    }

    @Managed
    @Override
    public long getTableConstraintsCacheHit()
    {
        return tableConstraintsCache.stats().hitCount();
    }

    @Managed
    @Override
    public long getTableConstraintsCacheMiss()
    {
        return tableConstraintsCache.stats().missCount();
    }

    @Managed
    @Override
    public long getTableConstraintsCacheEviction()
    {
        return tableConstraintsCache.stats().evictionCount();
    }

    @Managed
    @Override
    public long getTableConstraintsCacheSize()
    {
        return tableConstraintsCache.size();
    }

    @Managed
    @Override
    public long getPartitionStatisticsCacheHit()
    {
        return partitionStatisticsCache.stats().hitCount();
    }

    @Managed
    @Override
    public long getPartitionStatisticsCacheMiss()
    {
        return partitionStatisticsCache.stats().missCount();
    }

    @Managed
    @Override
    public long getPartitionStatisticsCacheEviction()
    {
        return partitionStatisticsCache.stats().evictionCount();
    }

    @Managed
    @Override
    public long getPartitionStatisticsCacheSize()
    {
        return partitionStatisticsCache.size();
    }

    @Managed
    @Override
    public long getViewNamesCacheHit()
    {
        return viewNamesCache.stats().hitCount();
    }

    @Managed
    @Override
    public long getViewNamesCacheMiss()
    {
        return viewNamesCache.stats().missCount();
    }

    @Managed
    @Override
    public long getViewNamesCacheEviction()
    {
        return viewNamesCache.stats().evictionCount();
    }

    @Managed
    @Override
    public long getViewNamesCacheSize()
    {
        return viewNamesCache.size();
    }

    @Managed
    @Override
    public long getPartitionCacheHit()
    {
        return partitionCache.stats().hitCount();
    }

    @Managed
    @Override
    public long getPartitionCacheMiss()
    {
        return partitionCache.stats().missCount();
    }

    @Managed
    @Override
    public long getPartitionCacheEviction()
    {
        return partitionCache.stats().evictionCount();
    }

    @Managed
    @Override
    public long getPartitionCacheSize()
    {
        return partitionCache.size();
    }

    @Managed
    @Override
    public long getPartitionFilterCacheHit()
    {
        return partitionFilterCache.stats().hitCount();
    }

    @Managed
    @Override
    public long getPartitionFilterCacheMiss()
    {
        return partitionFilterCache.stats().missCount();
    }

    @Managed
    @Override
    public long getPartitionFilterCacheEviction()
    {
        return partitionFilterCache.stats().evictionCount();
    }

    @Managed
    @Override
    public long getPartitionFilterCacheSize()
    {
        return partitionFilterCache.size();
    }

    @Managed
    @Override
    public long getPartitionNamesCacheHit()
    {
        return partitionNamesCache.stats().hitCount();
    }

    @Managed
    @Override
    public long getPartitionNamesCacheMiss()
    {
        return partitionNamesCache.stats().missCount();
    }

    @Managed
    @Override
    public long getPartitionNamesCacheEviction()
    {
        return partitionNamesCache.stats().evictionCount();
    }

    @Managed
    @Override
    public long getPartitionNamesCacheSize()
    {
        return partitionNamesCache.size();
    }

    @Managed
    @Override
    public long getTablePrivilegesCacheHit()
    {
        return tablePrivilegesCache.stats().hitCount();
    }

    @Managed
    @Override
    public long getTablePrivilegesCacheMiss()
    {
        return tablePrivilegesCache.stats().missCount();
    }

    @Managed
    @Override
    public long getTablePrivilegesCacheEviction()
    {
        return tablePrivilegesCache.stats().evictionCount();
    }

    @Managed
    @Override
    public long getTablePrivilegesCacheSize()
    {
        return tablePrivilegesCache.size();
    }

    @Managed
    @Override
    public long getRolesCacheHit()
    {
        return rolesCache.stats().hitCount();
    }

    @Managed
    @Override
    public long getRolesCacheMiss()
    {
        return rolesCache.stats().missCount();
    }

    @Managed
    @Override
    public long getRolesCacheEviction()
    {
        return rolesCache.stats().evictionCount();
    }

    @Managed
    @Override
    public long getRolesCacheSize()
    {
        return rolesCache.size();
    }

    @Managed
    @Override
    public long getRoleGrantsCacheHit()
    {
        return roleGrantsCache.stats().hitCount();
    }

    @Managed
    @Override
    public long getRoleGrantsCacheMiss()
    {
        return roleGrantsCache.stats().missCount();
    }

    @Managed
    @Override
    public long getRoleGrantsCacheEviction()
    {
        return roleGrantsCache.stats().evictionCount();
    }

    @Managed
    @Override
    public long getRoleGrantsCacheSize()
    {
        return roleGrantsCache.size();
    }

    @Managed
    @Nested
    @Override
    public CounterStat getPartitionsWithColumnCountGreaterThanThreshold()
    {
        return partitionsWithColumnCountGreaterThanThreshold;
    }
}
