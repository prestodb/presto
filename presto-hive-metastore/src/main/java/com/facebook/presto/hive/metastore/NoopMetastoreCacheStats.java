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

public class NoopMetastoreCacheStats
        implements MetastoreCacheStats
{
    public static final NoopMetastoreCacheStats NOOP_METASTORE_CACHE_STATS = new NoopMetastoreCacheStats();

    @Override
    public void setDatabaseCache(LoadingCache<?, ?> databaseCache)
    {
    }

    @Override
    public void setDatabaseNamesCache(LoadingCache<?, ?> databaseNamesCache)
    {
    }

    @Override
    public void setTableCache(LoadingCache<?, ?> tableCache)
    {
    }

    @Override
    public void setTableNamesCache(LoadingCache<?, ?> tableNamesCache)
    {
    }

    @Override
    public void setTableStatisticsCache(LoadingCache<?, ?> tableStatisticsCache)
    {
    }

    @Override
    public void setTableConstraintsCache(LoadingCache<?, ?> tableConstraintsCache)
    {
    }

    @Override
    public void setPartitionStatisticsCache(LoadingCache<?, ?> partitionStatisticsCache)
    {
    }

    @Override
    public void setViewNamesCache(LoadingCache<?, ?> viewNamesCache)
    {
    }

    @Override
    public void setPartitionCache(LoadingCache<?, ?> partitionCache)
    {
    }

    @Override
    public void setPartitionFilterCache(LoadingCache<?, ?> partitionFilterCache)
    {
    }

    @Override
    public void setPartitionNamesCache(LoadingCache<?, ?> partitionNamesCache)
    {
    }

    @Override
    public void setTablePrivilegesCache(LoadingCache<?, ?> tablePrivilegesCache)
    {
    }

    @Override
    public void setRolesCache(LoadingCache<?, ?> rolesCache)
    {
    }

    @Override
    public void setRoleGrantsCache(LoadingCache<?, ?> roleGrantsCache)
    {
    }

    @Override
    public void incrementPartitionsWithColumnCountGreaterThanThreshold()
    {
    }

    @Override
    public long getDatabaseCacheHit()
    {
        return 0;
    }

    @Override
    public long getDatabaseCacheMiss()
    {
        return 0;
    }

    @Override
    public long getDatabaseCacheEviction()
    {
        return 0;
    }

    @Override
    public long getDatabaseCacheSize()
    {
        return 0;
    }

    @Override
    public long getDatabaseNamesCacheHit()
    {
        return 0;
    }

    @Override
    public long getDatabaseNamesCacheMiss()
    {
        return 0;
    }

    @Override
    public long getDatabaseNamesCacheEviction()
    {
        return 0;
    }

    @Override
    public long getDatabaseNamesCacheSize()
    {
        return 0;
    }

    @Override
    public long getTableCacheHit()
    {
        return 0;
    }

    @Override
    public long getTableCacheMiss()
    {
        return 0;
    }

    @Override
    public long getTableCacheEviction()
    {
        return 0;
    }

    @Override
    public long getTableCacheSize()
    {
        return 0;
    }

    @Override
    public long getTableNamesCacheHit()
    {
        return 0;
    }

    @Override
    public long getTableNamesCacheMiss()
    {
        return 0;
    }

    @Override
    public long getTableNamesCacheEviction()
    {
        return 0;
    }

    @Override
    public long getTableNamesCacheSize()
    {
        return 0;
    }

    @Override
    public long getTableStatisticsCacheHit()
    {
        return 0;
    }

    @Override
    public long getTableStatisticsCacheMiss()
    {
        return 0;
    }

    @Override
    public long getTableStatisticsCacheEviction()
    {
        return 0;
    }

    @Override
    public long getTableStatisticsCacheSize()
    {
        return 0;
    }

    @Override
    public long getTableConstraintsCacheHit()
    {
        return 0;
    }

    @Override
    public long getTableConstraintsCacheMiss()
    {
        return 0;
    }

    @Override
    public long getTableConstraintsCacheEviction()
    {
        return 0;
    }

    @Override
    public long getTableConstraintsCacheSize()
    {
        return 0;
    }

    @Override
    public long getPartitionStatisticsCacheHit()
    {
        return 0;
    }

    @Override
    public long getPartitionStatisticsCacheMiss()
    {
        return 0;
    }

    @Override
    public long getPartitionStatisticsCacheEviction()
    {
        return 0;
    }

    @Override
    public long getPartitionStatisticsCacheSize()
    {
        return 0;
    }

    @Override
    public long getViewNamesCacheHit()
    {
        return 0;
    }

    @Override
    public long getViewNamesCacheMiss()
    {
        return 0;
    }

    @Override
    public long getViewNamesCacheEviction()
    {
        return 0;
    }

    @Override
    public long getViewNamesCacheSize()
    {
        return 0;
    }

    @Override
    public long getPartitionCacheHit()
    {
        return 0;
    }

    @Override
    public long getPartitionCacheMiss()
    {
        return 0;
    }

    @Override
    public long getPartitionCacheEviction()
    {
        return 0;
    }

    @Override
    public long getPartitionCacheSize()
    {
        return 0;
    }

    @Override
    public long getPartitionFilterCacheHit()
    {
        return 0;
    }

    @Override
    public long getPartitionFilterCacheMiss()
    {
        return 0;
    }

    @Override
    public long getPartitionFilterCacheEviction()
    {
        return 0;
    }

    @Override
    public long getPartitionFilterCacheSize()
    {
        return 0;
    }

    @Override
    public long getPartitionNamesCacheHit()
    {
        return 0;
    }

    @Override
    public long getPartitionNamesCacheMiss()
    {
        return 0;
    }

    @Override
    public long getPartitionNamesCacheEviction()
    {
        return 0;
    }

    @Override
    public long getPartitionNamesCacheSize()
    {
        return 0;
    }

    @Override
    public long getTablePrivilegesCacheHit()
    {
        return 0;
    }

    @Override
    public long getTablePrivilegesCacheMiss()
    {
        return 0;
    }

    @Override
    public long getTablePrivilegesCacheEviction()
    {
        return 0;
    }

    @Override
    public long getTablePrivilegesCacheSize()
    {
        return 0;
    }

    @Override
    public long getRolesCacheHit()
    {
        return 0;
    }

    @Override
    public long getRolesCacheMiss()
    {
        return 0;
    }

    @Override
    public long getRolesCacheEviction()
    {
        return 0;
    }

    @Override
    public long getRolesCacheSize()
    {
        return 0;
    }

    @Override
    public long getRoleGrantsCacheHit()
    {
        return 0;
    }

    @Override
    public long getRoleGrantsCacheMiss()
    {
        return 0;
    }

    @Override
    public long getRoleGrantsCacheEviction()
    {
        return 0;
    }

    @Override
    public long getRoleGrantsCacheSize()
    {
        return 0;
    }

    @Override
    public CounterStat getPartitionsWithColumnCountGreaterThanThreshold()
    {
        return null;
    }
}
