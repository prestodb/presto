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

public interface MetastoreCacheStats
{
    void setDatabaseCache(LoadingCache<?, ?> databaseCache);

    void setDatabaseNamesCache(LoadingCache<?, ?> databaseNamesCache);

    void setTableCache(LoadingCache<?, ?> tableCache);

    void setTableNamesCache(LoadingCache<?, ?> tableNamesCache);

    void setTableStatisticsCache(LoadingCache<?, ?> tableStatisticsCache);

    void setTableConstraintsCache(LoadingCache<?, ?> tableConstraintsCache);

    void setPartitionStatisticsCache(LoadingCache<?, ?> partitionStatisticsCache);

    void setViewNamesCache(LoadingCache<?, ?> viewNamesCache);

    void setPartitionCache(LoadingCache<?, ?> partitionCache);

    void setPartitionFilterCache(LoadingCache<?, ?> partitionFilterCache);

    void setPartitionNamesCache(LoadingCache<?, ?> partitionNamesCache);

    void setTablePrivilegesCache(LoadingCache<?, ?> tablePrivilegesCache);

    void setRolesCache(LoadingCache<?, ?> rolesCache);

    void setRoleGrantsCache(LoadingCache<?, ?> roleGrantsCache);

    void incrementPartitionsWithColumnCountGreaterThanThreshold();

    long getDatabaseCacheHit();

    long getDatabaseCacheMiss();

    long getDatabaseCacheEviction();

    long getDatabaseCacheSize();

    long getDatabaseNamesCacheHit();

    long getDatabaseNamesCacheMiss();

    long getDatabaseNamesCacheEviction();

    long getDatabaseNamesCacheSize();

    long getTableCacheHit();

    long getTableCacheMiss();

    long getTableCacheEviction();

    long getTableCacheSize();

    long getTableNamesCacheHit();

    long getTableNamesCacheMiss();

    long getTableNamesCacheEviction();

    long getTableNamesCacheSize();

    long getTableStatisticsCacheHit();

    long getTableStatisticsCacheMiss();

    long getTableStatisticsCacheEviction();

    long getTableStatisticsCacheSize();

    long getTableConstraintsCacheHit();

    long getTableConstraintsCacheMiss();

    long getTableConstraintsCacheEviction();

    long getTableConstraintsCacheSize();

    long getPartitionStatisticsCacheHit();

    long getPartitionStatisticsCacheMiss();

    long getPartitionStatisticsCacheEviction();

    long getPartitionStatisticsCacheSize();

    long getViewNamesCacheHit();

    long getViewNamesCacheMiss();

    long getViewNamesCacheEviction();

    long getViewNamesCacheSize();

    long getPartitionCacheHit();

    long getPartitionCacheMiss();

    long getPartitionCacheEviction();

    long getPartitionCacheSize();

    long getPartitionFilterCacheHit();

    long getPartitionFilterCacheMiss();

    long getPartitionFilterCacheEviction();

    long getPartitionFilterCacheSize();

    long getPartitionNamesCacheHit();

    long getPartitionNamesCacheMiss();

    long getPartitionNamesCacheEviction();

    long getPartitionNamesCacheSize();

    long getTablePrivilegesCacheHit();

    long getTablePrivilegesCacheMiss();

    long getTablePrivilegesCacheEviction();

    long getTablePrivilegesCacheSize();

    long getRolesCacheHit();

    long getRolesCacheMiss();

    long getRolesCacheEviction();

    long getRolesCacheSize();

    long getRoleGrantsCacheHit();

    long getRoleGrantsCacheMiss();

    long getRoleGrantsCacheEviction();

    long getRoleGrantsCacheSize();

    CounterStat getPartitionsWithColumnCountGreaterThanThreshold();
}
