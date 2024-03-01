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

import com.facebook.presto.hive.HiveTableHandle;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.RoleGrant;
import com.google.common.cache.LoadingCache;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Guava Loading Cache implementation of Metastore Cache
 */
public class InMemoryMetastoreCache
        implements MetastoreCache
{
    private final LoadingCache<CachingHiveMetastore.KeyAndContext<String>, Optional<Database>> databaseCache = null;
    private final LoadingCache<CachingHiveMetastore.KeyAndContext<String>, List<String>> databaseNamesCache = null;
    private final LoadingCache<CachingHiveMetastore.KeyAndContext<HiveTableHandle>, Optional<Table>> tableCache = null;
    private final LoadingCache<CachingHiveMetastore.KeyAndContext<String>, Optional<List<String>>> tableNamesCache = null;
    private final LoadingCache<CachingHiveMetastore.KeyAndContext<HiveTableName>, PartitionStatistics> tableStatisticsCache = null;
    private final LoadingCache<CachingHiveMetastore.KeyAndContext<HiveTableName>, List<TableConstraint<String>>> tableConstraintsCache = null;
    private final LoadingCache<CachingHiveMetastore.KeyAndContext<HivePartitionName>, PartitionStatistics> partitionStatisticsCache = null;
    private final LoadingCache<CachingHiveMetastore.KeyAndContext<String>, Optional<List<String>>> viewNamesCache = null;
    private final LoadingCache<CachingHiveMetastore.KeyAndContext<HivePartitionName>, Optional<Partition>> partitionCache = null;
    private final LoadingCache<CachingHiveMetastore.KeyAndContext<PartitionFilter>, List<String>> partitionFilterCache = null;
    private final LoadingCache<CachingHiveMetastore.KeyAndContext<HiveTableName>, Optional<List<String>>> partitionNamesCache = null;
    private final LoadingCache<CachingHiveMetastore.KeyAndContext<UserTableKey>, Set<HivePrivilegeInfo>> tablePrivilegesCache = null;
    private final LoadingCache<CachingHiveMetastore.KeyAndContext<String>, Set<String>> rolesCache = null;
    private final LoadingCache<CachingHiveMetastore.KeyAndContext<PrestoPrincipal>, Set<RoleGrant>> roleGrantsCache = null;
}
