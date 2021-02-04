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

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.ForCachingHiveMetastore;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_PARTITION_DROPPED_DURING_QUERY;
import static com.facebook.presto.hive.metastore.CachingHiveMetastore.MetastoreCacheScope.ALL;
import static com.facebook.presto.hive.metastore.HivePartitionName.hivePartitionName;
import static com.facebook.presto.hive.metastore.HiveTableName.hiveTableName;
import static com.facebook.presto.hive.metastore.PartitionFilter.partitionFilter;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.ImmutableSetMultimap.toImmutableSetMultimap;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Streams.stream;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Hive Metastore Cache
 */
@ThreadSafe
public class CachingHiveMetastore
        implements ExtendedHiveMetastore
{
    public enum MetastoreCacheScope
    {
        ALL, PARTITION
    }

    protected final ExtendedHiveMetastore delegate;
    private final LoadingCache<String, Optional<Database>> databaseCache;
    private final LoadingCache<String, List<String>> databaseNamesCache;
    private final LoadingCache<HiveTableName, Optional<Table>> tableCache;
    private final LoadingCache<String, Optional<List<String>>> tableNamesCache;
    private final LoadingCache<HiveTableName, PartitionStatistics> tableStatisticsCache;
    private final LoadingCache<HivePartitionName, PartitionStatistics> partitionStatisticsCache;
    private final LoadingCache<String, Optional<List<String>>> viewNamesCache;
    private final LoadingCache<HivePartitionName, Optional<Partition>> partitionCache;
    private final LoadingCache<PartitionFilter, List<String>> partitionFilterCache;
    private final LoadingCache<HiveTableName, Optional<List<String>>> partitionNamesCache;
    private final LoadingCache<UserTableKey, Set<HivePrivilegeInfo>> tablePrivilegesCache;
    private final LoadingCache<String, Set<String>> rolesCache;
    private final LoadingCache<PrestoPrincipal, Set<RoleGrant>> roleGrantsCache;

    private final boolean partitionVersioningEnabled;

    @Inject
    public CachingHiveMetastore(
            @ForCachingHiveMetastore ExtendedHiveMetastore delegate,
            @ForCachingHiveMetastore ExecutorService executor,
            MetastoreClientConfig metastoreClientConfig)
    {
        this(
                delegate,
                executor,
                metastoreClientConfig.getMetastoreCacheTtl(),
                metastoreClientConfig.getMetastoreRefreshInterval(),
                metastoreClientConfig.getMetastoreCacheMaximumSize(),
                metastoreClientConfig.isPartitionVersioningEnabled(),
                metastoreClientConfig.getMetastoreCacheScope());
    }

    public CachingHiveMetastore(
            ExtendedHiveMetastore delegate,
            ExecutorService executor,
            Duration cacheTtl,
            Duration refreshInterval,
            long maximumSize,
            boolean partitionVersioningEnabled,
            MetastoreCacheScope metastoreCacheScope)
    {
        this(
                delegate,
                executor,
                OptionalLong.of(cacheTtl.toMillis()),
                refreshInterval.toMillis() >= cacheTtl.toMillis() ? OptionalLong.empty() : OptionalLong.of(refreshInterval.toMillis()),
                maximumSize,
                partitionVersioningEnabled,
                metastoreCacheScope);
    }

    public static CachingHiveMetastore memoizeMetastore(ExtendedHiveMetastore delegate, long maximumSize)
    {
        return new CachingHiveMetastore(
                delegate,
                newDirectExecutorService(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                maximumSize,
                false,
                ALL);
    }

    private CachingHiveMetastore(
            ExtendedHiveMetastore delegate,
            ExecutorService executor,
            OptionalLong expiresAfterWriteMillis,
            OptionalLong refreshMills,
            long maximumSize,
            boolean partitionVersioningEnabled,
            MetastoreCacheScope metastoreCacheScope)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        requireNonNull(executor, "executor is null");
        this.partitionVersioningEnabled = partitionVersioningEnabled;

        OptionalLong cacheExpiresAfterWriteMillis;
        OptionalLong cacheRefreshMills;
        long cacheMaxSize;

        OptionalLong partitionCacheExpiresAfterWriteMillis;
        OptionalLong partitionCacheRefreshMills;
        long partitionCacheMaxSize;

        switch (metastoreCacheScope) {
            case PARTITION:
                partitionCacheExpiresAfterWriteMillis = expiresAfterWriteMillis;
                partitionCacheRefreshMills = refreshMills;
                partitionCacheMaxSize = maximumSize;
                cacheExpiresAfterWriteMillis = OptionalLong.of(0);
                cacheRefreshMills = OptionalLong.of(0);
                cacheMaxSize = 0;
                break;

            case ALL:
                partitionCacheExpiresAfterWriteMillis = expiresAfterWriteMillis;
                partitionCacheRefreshMills = refreshMills;
                partitionCacheMaxSize = maximumSize;
                cacheExpiresAfterWriteMillis = expiresAfterWriteMillis;
                cacheRefreshMills = refreshMills;
                cacheMaxSize = maximumSize;
                break;

            default:
                throw new IllegalArgumentException("Unknown metastore-cache-scope: " + metastoreCacheScope);
        }

        databaseNamesCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheRefreshMills, cacheMaxSize)
                .build(asyncReloading(CacheLoader.from(this::loadAllDatabases), executor));

        databaseCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheRefreshMills, cacheMaxSize)
                .build(asyncReloading(CacheLoader.from(this::loadDatabase), executor));

        tableNamesCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheRefreshMills, cacheMaxSize)
                .build(asyncReloading(CacheLoader.from(this::loadAllTables), executor));

        tableStatisticsCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheRefreshMills, cacheMaxSize)
                .build(asyncReloading(new CacheLoader<HiveTableName, PartitionStatistics>()
                {
                    @Override
                    public PartitionStatistics load(HiveTableName key)
                    {
                        return loadTableColumnStatistics(key);
                    }
                }, executor));

        partitionStatisticsCache = newCacheBuilder(partitionCacheExpiresAfterWriteMillis, partitionCacheRefreshMills, partitionCacheMaxSize)
                .build(asyncReloading(new CacheLoader<HivePartitionName, PartitionStatistics>()
                {
                    @Override
                    public PartitionStatistics load(HivePartitionName key)
                    {
                        return loadPartitionColumnStatistics(key);
                    }

                    @Override
                    public Map<HivePartitionName, PartitionStatistics> loadAll(Iterable<? extends HivePartitionName> keys)
                    {
                        return loadPartitionColumnStatistics(keys);
                    }
                }, executor));

        tableCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheRefreshMills, cacheMaxSize)
                .build(asyncReloading(CacheLoader.from(this::loadTable), executor));

        viewNamesCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheRefreshMills, cacheMaxSize)
                .build(asyncReloading(CacheLoader.from(this::loadAllViews), executor));

        partitionNamesCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheRefreshMills, cacheMaxSize)
                .build(asyncReloading(CacheLoader.from(this::loadPartitionNames), executor));

        partitionFilterCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheRefreshMills, cacheMaxSize)
                .build(asyncReloading(CacheLoader.from(this::loadPartitionNamesByFilter), executor));

        partitionCache = newCacheBuilder(partitionCacheExpiresAfterWriteMillis, partitionCacheRefreshMills, partitionCacheMaxSize)
                .build(asyncReloading(new CacheLoader<HivePartitionName, Optional<Partition>>()
                {
                    @Override
                    public Optional<Partition> load(HivePartitionName partitionName)
                    {
                        return loadPartitionByName(partitionName);
                    }

                    @Override
                    public Map<HivePartitionName, Optional<Partition>> loadAll(Iterable<? extends HivePartitionName> partitionNames)
                    {
                        return loadPartitionsByNames(partitionNames);
                    }
                }, executor));

        tablePrivilegesCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheRefreshMills, cacheMaxSize)
                .build(asyncReloading(CacheLoader.from(key -> loadTablePrivileges(key.getDatabase(), key.getTable(), key.getPrincipal())), executor));

        rolesCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheRefreshMills, cacheMaxSize)
                .build(asyncReloading(CacheLoader.from(() -> loadRoles()), executor));

        roleGrantsCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheRefreshMills, cacheMaxSize)
                .build(asyncReloading(CacheLoader.from(this::loadRoleGrants), executor));
    }

    @Managed
    public void flushCache()
    {
        databaseNamesCache.invalidateAll();
        tableNamesCache.invalidateAll();
        viewNamesCache.invalidateAll();
        partitionNamesCache.invalidateAll();
        databaseCache.invalidateAll();
        tableCache.invalidateAll();
        partitionCache.invalidateAll();
        partitionFilterCache.invalidateAll();
        tablePrivilegesCache.invalidateAll();
        tableStatisticsCache.invalidateAll();
        partitionStatisticsCache.invalidateAll();
        rolesCache.invalidateAll();
    }

    private static <K, V> V get(LoadingCache<K, V> cache, K key)
    {
        try {
            return cache.getUnchecked(key);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw e;
        }
    }

    private static <K, V> Map<K, V> getAll(LoadingCache<K, V> cache, Iterable<K> keys)
    {
        try {
            return cache.getAll(keys);
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throwIfUnchecked(e);
            throw new UncheckedExecutionException(e);
        }
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        return get(databaseCache, databaseName);
    }

    private Optional<Database> loadDatabase(String databaseName)
    {
        return delegate.getDatabase(databaseName);
    }

    @Override
    public List<String> getAllDatabases()
    {
        return get(databaseNamesCache, "");
    }

    private List<String> loadAllDatabases()
    {
        return delegate.getAllDatabases();
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        return get(tableCache, hiveTableName(databaseName, tableName));
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return delegate.getSupportedColumnStatistics(type);
    }

    private Optional<Table> loadTable(HiveTableName hiveTableName)
    {
        return delegate.getTable(hiveTableName.getDatabaseName(), hiveTableName.getTableName());
    }

    @Override
    public PartitionStatistics getTableStatistics(String databaseName, String tableName)
    {
        return get(tableStatisticsCache, hiveTableName(databaseName, tableName));
    }

    private PartitionStatistics loadTableColumnStatistics(HiveTableName hiveTableName)
    {
        return delegate.getTableStatistics(hiveTableName.getDatabaseName(), hiveTableName.getTableName());
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(String databaseName, String tableName, Set<String> partitionNames)
    {
        List<HivePartitionName> partitions = partitionNames.stream()
                .map(partitionName -> HivePartitionName.hivePartitionName(databaseName, tableName, partitionName))
                .collect(toImmutableList());
        Map<HivePartitionName, PartitionStatistics> statistics = getAll(partitionStatisticsCache, partitions);
        return statistics.entrySet()
                .stream()
                .collect(toImmutableMap(entry -> entry.getKey().getPartitionName().get(), Entry::getValue));
    }

    private PartitionStatistics loadPartitionColumnStatistics(HivePartitionName partition)
    {
        String partitionName = partition.getPartitionName().get();
        Map<String, PartitionStatistics> partitionStatistics = delegate.getPartitionStatistics(
                partition.getHiveTableName().getDatabaseName(),
                partition.getHiveTableName().getTableName(),
                ImmutableSet.of(partitionName));
        if (!partitionStatistics.containsKey(partitionName)) {
            throw new PrestoException(HIVE_PARTITION_DROPPED_DURING_QUERY, "Statistics result does not contain entry for partition: " + partition.getPartitionName());
        }
        return partitionStatistics.get(partitionName);
    }

    private Map<HivePartitionName, PartitionStatistics> loadPartitionColumnStatistics(Iterable<? extends HivePartitionName> keys)
    {
        SetMultimap<HiveTableName, HivePartitionName> tablePartitions = stream(keys)
                .collect(toImmutableSetMultimap(HivePartitionName::getHiveTableName, key -> key));
        ImmutableMap.Builder<HivePartitionName, PartitionStatistics> result = ImmutableMap.builder();
        tablePartitions.keySet().forEach(table -> {
            Set<String> partitionNames = tablePartitions.get(table).stream()
                    .map(partitionName -> partitionName.getPartitionName().get())
                    .collect(toImmutableSet());
            Map<String, PartitionStatistics> partitionStatistics = delegate.getPartitionStatistics(table.getDatabaseName(), table.getTableName(), partitionNames);
            for (String partitionName : partitionNames) {
                if (!partitionStatistics.containsKey(partitionName)) {
                    throw new PrestoException(HIVE_PARTITION_DROPPED_DURING_QUERY, "Statistics result does not contain entry for partition: " + partitionName);
                }
                result.put(HivePartitionName.hivePartitionName(table, partitionName), partitionStatistics.get(partitionName));
            }
        });
        return result.build();
    }

    @Override
    public void updateTableStatistics(String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        try {
            delegate.updateTableStatistics(databaseName, tableName, update);
        }
        finally {
            tableStatisticsCache.invalidate(hiveTableName(databaseName, tableName));
        }
    }

    @Override
    public void updatePartitionStatistics(String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        try {
            delegate.updatePartitionStatistics(databaseName, tableName, partitionName, update);
        }
        finally {
            partitionStatisticsCache.invalidate(HivePartitionName.hivePartitionName(databaseName, tableName, partitionName));
        }
    }

    @Override
    public Optional<List<String>> getAllTables(String databaseName)
    {
        return get(tableNamesCache, databaseName);
    }

    private Optional<List<String>> loadAllTables(String databaseName)
    {
        return delegate.getAllTables(databaseName);
    }

    @Override
    public Optional<List<String>> getAllViews(String databaseName)
    {
        return get(viewNamesCache, databaseName);
    }

    private Optional<List<String>> loadAllViews(String databaseName)
    {
        return delegate.getAllViews(databaseName);
    }

    @Override
    public void createDatabase(Database database)
    {
        try {
            delegate.createDatabase(database);
        }
        finally {
            invalidateDatabase(database.getDatabaseName());
        }
    }

    @Override
    public void dropDatabase(String databaseName)
    {
        try {
            delegate.dropDatabase(databaseName);
        }
        finally {
            invalidateDatabase(databaseName);
        }
    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName)
    {
        try {
            delegate.renameDatabase(databaseName, newDatabaseName);
        }
        finally {
            invalidateDatabase(databaseName);
            invalidateDatabase(newDatabaseName);
        }
    }

    protected void invalidateDatabase(String databaseName)
    {
        databaseCache.invalidate(databaseName);
        databaseNamesCache.invalidateAll();
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        try {
            delegate.createTable(table, principalPrivileges);
        }
        finally {
            invalidateTable(table.getDatabaseName(), table.getTableName());
        }
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        try {
            delegate.dropTable(databaseName, tableName, deleteData);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    @Override
    public void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        try {
            delegate.replaceTable(databaseName, tableName, newTable, principalPrivileges);
        }
        finally {
            invalidateTable(databaseName, tableName);
            invalidateTable(newTable.getDatabaseName(), newTable.getTableName());
        }
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        try {
            delegate.renameTable(databaseName, tableName, newDatabaseName, newTableName);
        }
        finally {
            invalidateTable(databaseName, tableName);
            invalidateTable(newDatabaseName, newTableName);
        }
    }

    @Override
    public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        try {
            delegate.addColumn(databaseName, tableName, columnName, columnType, columnComment);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        try {
            delegate.renameColumn(databaseName, tableName, oldColumnName, newColumnName);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName)
    {
        try {
            delegate.dropColumn(databaseName, tableName, columnName);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    protected void invalidateTable(String databaseName, String tableName)
    {
        tableCache.invalidate(hiveTableName(databaseName, tableName));
        tableNamesCache.invalidate(databaseName);
        viewNamesCache.invalidate(databaseName);
        tablePrivilegesCache.asMap().keySet().stream()
                .filter(userTableKey -> userTableKey.matches(databaseName, tableName))
                .forEach(tablePrivilegesCache::invalidate);
        tableStatisticsCache.invalidate(hiveTableName(databaseName, tableName));
        invalidatePartitionCache(databaseName, tableName);
    }

    @Override
    public Optional<Partition> getPartition(String databaseName, String tableName, List<String> partitionValues)
    {
        HivePartitionName name = hivePartitionName(databaseName, tableName, partitionValues);
        return get(partitionCache, name);
    }

    @Override
    public Optional<List<String>> getPartitionNames(String databaseName, String tableName)
    {
        return get(partitionNamesCache, hiveTableName(databaseName, tableName));
    }

    private Optional<List<String>> loadPartitionNames(HiveTableName hiveTableName)
    {
        return delegate.getPartitionNames(hiveTableName.getDatabaseName(), hiveTableName.getTableName());
    }

    @Override
    public List<String> getPartitionNamesByFilter(
            String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates)
    {
        if (partitionVersioningEnabled) {
            List<PartitionNameWithVersion> partitionNamesWithVersion = getPartitionNamesWithVersionByFilter(databaseName, tableName, partitionPredicates);
            List<String> result = partitionNamesWithVersion.stream().map(PartitionNameWithVersion::getPartitionName).collect(toImmutableList());
            partitionNamesWithVersion.forEach(partitionNameWithVersion -> invalidateStalePartition(partitionNameWithVersion, databaseName, tableName));
            return result;
        }
        return get(
                partitionFilterCache,
                partitionFilter(databaseName, tableName, partitionPredicates));
    }

    @Override
    public List<PartitionNameWithVersion> getPartitionNamesWithVersionByFilter(
            String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates)
    {
        return delegate.getPartitionNamesWithVersionByFilter(databaseName, tableName, partitionPredicates);
    }

    private void invalidateStalePartition(PartitionNameWithVersion partitionNameWithVersion, String databaseName, String tableName)
    {
        HivePartitionName hivePartitionName = hivePartitionName(databaseName, tableName, partitionNameWithVersion.getPartitionName());
        Optional<Partition> partition = partitionCache.getIfPresent(hivePartitionName);
        if (partition != null && partition.isPresent()) {
            Optional<Long> partitionVersion = partition.get().getPartitionVersion();
            if (!partitionVersion.isPresent() || partitionVersion.get() != partitionNameWithVersion.getPartitionVersion()) {
                partitionCache.invalidate(hivePartitionName);
                partitionStatisticsCache.invalidate(hivePartitionName);
            }
        }
    }

    private List<String> loadPartitionNamesByFilter(PartitionFilter partitionFilter)
    {
        return delegate.getPartitionNamesByFilter(
                partitionFilter.getHiveTableName().getDatabaseName(),
                partitionFilter.getHiveTableName().getTableName(),
                partitionFilter.getPartitionPredicates());
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
    {
        Iterable<HivePartitionName> names = transform(partitionNames, name -> HivePartitionName.hivePartitionName(databaseName, tableName, name));

        Map<HivePartitionName, Optional<Partition>> all = getAll(partitionCache, names);
        ImmutableMap.Builder<String, Optional<Partition>> partitionsByName = ImmutableMap.builder();
        for (Entry<HivePartitionName, Optional<Partition>> entry : all.entrySet()) {
            partitionsByName.put(entry.getKey().getPartitionName().get(), entry.getValue());
        }
        return partitionsByName.build();
    }

    private Optional<Partition> loadPartitionByName(HivePartitionName partitionName)
    {
        return delegate.getPartition(
                partitionName.getHiveTableName().getDatabaseName(),
                partitionName.getHiveTableName().getTableName(),
                partitionName.getPartitionValues());
    }

    private Map<HivePartitionName, Optional<Partition>> loadPartitionsByNames(Iterable<? extends HivePartitionName> partitionNames)
    {
        requireNonNull(partitionNames, "partitionNames is null");
        checkArgument(!Iterables.isEmpty(partitionNames), "partitionNames is empty");

        HivePartitionName firstPartition = Iterables.get(partitionNames, 0);

        HiveTableName hiveTableName = firstPartition.getHiveTableName();
        String databaseName = hiveTableName.getDatabaseName();
        String tableName = hiveTableName.getTableName();

        List<String> partitionsToFetch = new ArrayList<>();
        for (HivePartitionName partitionName : partitionNames) {
            checkArgument(partitionName.getHiveTableName().equals(hiveTableName), "Expected table name %s but got %s", hiveTableName, partitionName.getHiveTableName());
            partitionsToFetch.add(partitionName.getPartitionName().get());
        }

        ImmutableMap.Builder<HivePartitionName, Optional<Partition>> partitions = ImmutableMap.builder();
        Map<String, Optional<Partition>> partitionsByNames = delegate.getPartitionsByNames(databaseName, tableName, partitionsToFetch);
        for (Entry<String, Optional<Partition>> entry : partitionsByNames.entrySet()) {
            partitions.put(HivePartitionName.hivePartitionName(hiveTableName, entry.getKey()), entry.getValue());
        }
        return partitions.build();
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        try {
            delegate.addPartitions(databaseName, tableName, partitions);
        }
        finally {
            // todo do we need to invalidate all partitions?
            invalidatePartitionCache(databaseName, tableName);
        }
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        try {
            delegate.dropPartition(databaseName, tableName, parts, deleteData);
        }
        finally {
            invalidatePartitionCache(databaseName, tableName);
        }
    }

    @Override
    public void alterPartition(String databaseName, String tableName, PartitionWithStatistics partition)
    {
        try {
            delegate.alterPartition(databaseName, tableName, partition);
        }
        finally {
            invalidatePartitionCache(databaseName, tableName);
        }
    }

    @Override
    public void createRole(String role, String grantor)
    {
        try {
            delegate.createRole(role, grantor);
        }
        finally {
            rolesCache.invalidateAll();
        }
    }

    @Override
    public void dropRole(String role)
    {
        try {
            delegate.dropRole(role);
        }
        finally {
            rolesCache.invalidateAll();
            roleGrantsCache.invalidateAll();
        }
    }

    @Override
    public Set<String> listRoles()
    {
        return get(rolesCache, "");
    }

    private Set<String> loadRoles()
    {
        return delegate.listRoles();
    }

    @Override
    public void grantRoles(Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, PrestoPrincipal grantor)
    {
        try {
            delegate.grantRoles(roles, grantees, withAdminOption, grantor);
        }
        finally {
            roleGrantsCache.invalidateAll();
        }
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, PrestoPrincipal grantor)
    {
        try {
            delegate.revokeRoles(roles, grantees, adminOptionFor, grantor);
        }
        finally {
            roleGrantsCache.invalidateAll();
        }
    }

    @Override
    public Set<RoleGrant> listRoleGrants(PrestoPrincipal principal)
    {
        return get(roleGrantsCache, principal);
    }

    private Set<RoleGrant> loadRoleGrants(PrestoPrincipal principal)
    {
        return delegate.listRoleGrants(principal);
    }

    private void invalidatePartitionCache(String databaseName, String tableName)
    {
        HiveTableName hiveTableName = hiveTableName(databaseName, tableName);
        partitionNamesCache.invalidate(hiveTableName);
        partitionCache.asMap().keySet().stream()
                .filter(partitionName -> partitionName.getHiveTableName().equals(hiveTableName))
                .forEach(partitionCache::invalidate);
        partitionFilterCache.asMap().keySet().stream()
                .filter(partitionFilter -> partitionFilter.getHiveTableName().equals(hiveTableName))
                .forEach(partitionFilterCache::invalidate);
        partitionStatisticsCache.asMap().keySet().stream()
                .filter(partitionFilter -> partitionFilter.getHiveTableName().equals(hiveTableName))
                .forEach(partitionStatisticsCache::invalidate);
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        try {
            delegate.grantTablePrivileges(databaseName, tableName, grantee, privileges);
        }
        finally {
            tablePrivilegesCache.invalidate(new UserTableKey(grantee, databaseName, tableName));
        }
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        try {
            delegate.revokeTablePrivileges(databaseName, tableName, grantee, privileges);
        }
        finally {
            tablePrivilegesCache.invalidate(new UserTableKey(grantee, databaseName, tableName));
        }
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, PrestoPrincipal principal)
    {
        return get(tablePrivilegesCache, new UserTableKey(principal, databaseName, tableName));
    }

    public Set<HivePrivilegeInfo> loadTablePrivileges(String databaseName, String tableName, PrestoPrincipal principal)
    {
        return delegate.listTablePrivileges(databaseName, tableName, principal);
    }

    private static CacheBuilder<Object, Object> newCacheBuilder(OptionalLong expiresAfterWriteMillis, OptionalLong refreshMillis, long maximumSize)
    {
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        if (expiresAfterWriteMillis.isPresent()) {
            cacheBuilder = cacheBuilder.expireAfterWrite(expiresAfterWriteMillis.getAsLong(), MILLISECONDS);
        }
        if (refreshMillis.isPresent() && (!expiresAfterWriteMillis.isPresent() || expiresAfterWriteMillis.getAsLong() > refreshMillis.getAsLong())) {
            cacheBuilder = cacheBuilder.refreshAfterWrite(refreshMillis.getAsLong(), MILLISECONDS);
        }
        cacheBuilder = cacheBuilder.maximumSize(maximumSize);
        return cacheBuilder;
    }
}
