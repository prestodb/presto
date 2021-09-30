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
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_CORRUPTED_PARTITION_CACHE;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PARTITION_DROPPED_DURING_QUERY;
import static com.facebook.presto.hive.metastore.CachingHiveMetastore.MetastoreCacheScope.ALL;
import static com.facebook.presto.hive.metastore.HivePartitionName.hivePartitionName;
import static com.facebook.presto.hive.metastore.HiveTableName.hiveTableName;
import static com.facebook.presto.hive.metastore.PartitionFilter.partitionFilter;
import static com.google.common.base.MoreObjects.toStringHelper;
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
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Hive Metastore Cache
 */
@ThreadSafe
public class CachingHiveMetastore
        implements ExtendedHiveMetastore
{
    private static final String NO_IMPERSONATION_USER = "no-impersonation-caching-user";

    public enum MetastoreCacheScope
    {
        ALL, PARTITION
    }

    protected final ExtendedHiveMetastore delegate;
    private final LoadingCache<KeyAndContext<String>, Optional<Database>> databaseCache;
    private final LoadingCache<KeyAndContext<String>, List<String>> databaseNamesCache;
    private final LoadingCache<KeyAndContext<HiveTableName>, Optional<Table>> tableCache;
    private final LoadingCache<KeyAndContext<String>, Optional<List<String>>> tableNamesCache;
    private final LoadingCache<KeyAndContext<HiveTableName>, PartitionStatistics> tableStatisticsCache;
    private final LoadingCache<KeyAndContext<HivePartitionName>, PartitionStatistics> partitionStatisticsCache;
    private final LoadingCache<KeyAndContext<String>, Optional<List<String>>> viewNamesCache;
    private final LoadingCache<KeyAndContext<HivePartitionName>, Optional<Partition>> partitionCache;
    private final LoadingCache<KeyAndContext<PartitionFilter>, List<String>> partitionFilterCache;
    private final LoadingCache<KeyAndContext<HiveTableName>, Optional<List<String>>> partitionNamesCache;
    private final LoadingCache<KeyAndContext<UserTableKey>, Set<HivePrivilegeInfo>> tablePrivilegesCache;
    private final LoadingCache<KeyAndContext<String>, Set<String>> rolesCache;
    private final LoadingCache<KeyAndContext<PrestoPrincipal>, Set<RoleGrant>> roleGrantsCache;

    private final boolean metastoreImpersonationEnabled;
    private final boolean partitionVersioningEnabled;
    private final double partitionCacheValidationPercentage;

    @Inject
    public CachingHiveMetastore(
            @ForCachingHiveMetastore ExtendedHiveMetastore delegate,
            @ForCachingHiveMetastore ExecutorService executor,
            MetastoreClientConfig metastoreClientConfig)
    {
        this(
                delegate,
                executor,
                metastoreClientConfig.isMetastoreImpersonationEnabled(),
                metastoreClientConfig.getMetastoreCacheTtl(),
                metastoreClientConfig.getMetastoreRefreshInterval(),
                metastoreClientConfig.getMetastoreCacheMaximumSize(),
                metastoreClientConfig.isPartitionVersioningEnabled(),
                metastoreClientConfig.getMetastoreCacheScope(),
                metastoreClientConfig.getPartitionCacheValidationPercentage());
    }

    public CachingHiveMetastore(
            ExtendedHiveMetastore delegate,
            ExecutorService executor,
            boolean metastoreImpersonationEnabled,
            Duration cacheTtl,
            Duration refreshInterval,
            long maximumSize,
            boolean partitionVersioningEnabled,
            MetastoreCacheScope metastoreCacheScope,
            double partitionCacheValidationPercentage)
    {
        this(
                delegate,
                executor,
                metastoreImpersonationEnabled,
                OptionalLong.of(cacheTtl.toMillis()),
                refreshInterval.toMillis() >= cacheTtl.toMillis() ? OptionalLong.empty() : OptionalLong.of(refreshInterval.toMillis()),
                maximumSize,
                partitionVersioningEnabled,
                metastoreCacheScope,
                partitionCacheValidationPercentage);
    }

    public static CachingHiveMetastore memoizeMetastore(ExtendedHiveMetastore delegate, boolean isMetastoreImpersonationEnabled, long maximumSize)
    {
        return new CachingHiveMetastore(
                delegate,
                newDirectExecutorService(),
                isMetastoreImpersonationEnabled,
                OptionalLong.empty(),
                OptionalLong.empty(),
                maximumSize,
                false,
                ALL,
                0.0);
    }

    private CachingHiveMetastore(
            ExtendedHiveMetastore delegate,
            ExecutorService executor,
            boolean metastoreImpersonationEnabled,
            OptionalLong expiresAfterWriteMillis,
            OptionalLong refreshMills,
            long maximumSize,
            boolean partitionVersioningEnabled,
            MetastoreCacheScope metastoreCacheScope,
            double partitionCacheValidationPercentage)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        requireNonNull(executor, "executor is null");
        this.metastoreImpersonationEnabled = metastoreImpersonationEnabled;
        this.partitionVersioningEnabled = partitionVersioningEnabled;
        this.partitionCacheValidationPercentage = partitionCacheValidationPercentage;

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
                .build(asyncReloading(new CacheLoader<KeyAndContext<HiveTableName>, PartitionStatistics>()
                {
                    @Override
                    public PartitionStatistics load(KeyAndContext<HiveTableName> key)
                    {
                        return loadTableColumnStatistics(key);
                    }
                }, executor));

        partitionStatisticsCache = newCacheBuilder(partitionCacheExpiresAfterWriteMillis, partitionCacheRefreshMills, partitionCacheMaxSize)
                .build(asyncReloading(new CacheLoader<KeyAndContext<HivePartitionName>, PartitionStatistics>()
                {
                    @Override
                    public PartitionStatistics load(KeyAndContext<HivePartitionName> key)
                    {
                        return loadPartitionColumnStatistics(key);
                    }

                    @Override
                    public Map<KeyAndContext<HivePartitionName>, PartitionStatistics> loadAll(Iterable<? extends KeyAndContext<HivePartitionName>> keys)
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
                .build(asyncReloading(new CacheLoader<KeyAndContext<HivePartitionName>, Optional<Partition>>()
                {
                    @Override
                    public Optional<Partition> load(KeyAndContext<HivePartitionName> partitionName)
                    {
                        return loadPartitionByName(partitionName);
                    }

                    @Override
                    public Map<KeyAndContext<HivePartitionName>, Optional<Partition>> loadAll(Iterable<? extends KeyAndContext<HivePartitionName>> partitionNames)
                    {
                        return loadPartitionsByNames(partitionNames);
                    }
                }, executor));

        tablePrivilegesCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheRefreshMills, cacheMaxSize)
                .build(asyncReloading(CacheLoader.from(this::loadTablePrivileges), executor));

        rolesCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheRefreshMills, cacheMaxSize)
                .build(asyncReloading(CacheLoader.from(this::loadAllRoles), executor));

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
    public Optional<Database> getDatabase(MetastoreContext metastoreContext, String databaseName)
    {
        return get(databaseCache, getCachingKey(metastoreContext, databaseName));
    }

    private Optional<Database> loadDatabase(KeyAndContext<String> databaseName)
    {
        return delegate.getDatabase(databaseName.getContext(), databaseName.getKey());
    }

    @Override
    public List<String> getAllDatabases(MetastoreContext metastoreContext)
    {
        return get(databaseNamesCache, getCachingKey(metastoreContext, ""));
    }

    private List<String> loadAllDatabases(KeyAndContext<String> key)
    {
        return delegate.getAllDatabases(key.getContext());
    }

    @Override
    public Optional<Table> getTable(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        return get(tableCache, getCachingKey(metastoreContext, hiveTableName(databaseName, tableName)));
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(MetastoreContext metastoreContext, Type type)
    {
        return delegate.getSupportedColumnStatistics(metastoreContext, type);
    }

    private Optional<Table> loadTable(KeyAndContext<HiveTableName> hiveTableName)
    {
        return delegate.getTable(hiveTableName.getContext(), hiveTableName.getKey().getDatabaseName(), hiveTableName.getKey().getTableName());
    }

    @Override
    public PartitionStatistics getTableStatistics(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        return get(tableStatisticsCache, getCachingKey(metastoreContext, hiveTableName(databaseName, tableName)));
    }

    private PartitionStatistics loadTableColumnStatistics(KeyAndContext<HiveTableName> hiveTableName)
    {
        return delegate.getTableStatistics(hiveTableName.getContext(), hiveTableName.getKey().getDatabaseName(), hiveTableName.getKey().getTableName());
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Set<String> partitionNames)
    {
        List<KeyAndContext<HivePartitionName>> partitions = partitionNames.stream()
                .map(partitionName -> getCachingKey(metastoreContext, HivePartitionName.hivePartitionName(databaseName, tableName, partitionName)))
                .collect(toImmutableList());
        Map<KeyAndContext<HivePartitionName>, PartitionStatistics> statistics = getAll(partitionStatisticsCache, partitions);
        return statistics.entrySet()
                .stream()
                .collect(toImmutableMap(entry -> entry.getKey().getKey().getPartitionName().get(), Entry::getValue));
    }

    private PartitionStatistics loadPartitionColumnStatistics(KeyAndContext<HivePartitionName> partition)
    {
        String partitionName = partition.getKey().getPartitionName().get();
        Map<String, PartitionStatistics> partitionStatistics = delegate.getPartitionStatistics(
                partition.getContext(),
                partition.getKey().getHiveTableName().getDatabaseName(),
                partition.getKey().getHiveTableName().getTableName(),
                ImmutableSet.of(partitionName));
        if (!partitionStatistics.containsKey(partitionName)) {
            throw new PrestoException(HIVE_PARTITION_DROPPED_DURING_QUERY, "Statistics result does not contain entry for partition: " + partition.getKey().getPartitionName());
        }
        return partitionStatistics.get(partitionName);
    }

    private Map<KeyAndContext<HivePartitionName>, PartitionStatistics> loadPartitionColumnStatistics(Iterable<? extends KeyAndContext<HivePartitionName>> keys)
    {
        SetMultimap<KeyAndContext<HiveTableName>, KeyAndContext<HivePartitionName>> tablePartitions = stream(keys)
                .collect(toImmutableSetMultimap(nameKey -> getCachingKey(nameKey.getContext(), nameKey.getKey().getHiveTableName()), nameKey -> nameKey));
        ImmutableMap.Builder<KeyAndContext<HivePartitionName>, PartitionStatistics> result = ImmutableMap.builder();
        tablePartitions.keySet().forEach(table -> {
            Set<String> partitionNames = tablePartitions.get(table).stream()
                    .map(partitionName -> partitionName.getKey().getPartitionName().get())
                    .collect(toImmutableSet());
            Map<String, PartitionStatistics> partitionStatistics = delegate.getPartitionStatistics(table.getContext(), table.getKey().getDatabaseName(), table.getKey().getTableName(), partitionNames);
            for (String partitionName : partitionNames) {
                if (!partitionStatistics.containsKey(partitionName)) {
                    throw new PrestoException(HIVE_PARTITION_DROPPED_DURING_QUERY, "Statistics result does not contain entry for partition: " + partitionName);
                }
                result.put(getCachingKey(table.getContext(), HivePartitionName.hivePartitionName(table.getKey(), partitionName)), partitionStatistics.get(partitionName));
            }
        });
        return result.build();
    }

    @Override
    public void updateTableStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        try {
            delegate.updateTableStatistics(metastoreContext, databaseName, tableName, update);
        }
        finally {
            tableStatisticsCache.asMap().keySet().stream()
                    .filter(hiveTableNameKey -> hiveTableNameKey.getKey().equals(hiveTableName(databaseName, tableName)))
                    .forEach(tableStatisticsCache::invalidate);
        }
    }

    @Override
    public void updatePartitionStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        try {
            delegate.updatePartitionStatistics(metastoreContext, databaseName, tableName, partitionName, update);
        }
        finally {
            partitionStatisticsCache.asMap().keySet().stream()
                    .filter(partitionFilterKey -> partitionFilterKey.getKey().equals(hivePartitionName(databaseName, tableName, partitionName)))
                    .forEach(partitionStatisticsCache::invalidate);
        }
    }

    @Override
    public Optional<List<String>> getAllTables(MetastoreContext metastoreContext, String databaseName)
    {
        return get(tableNamesCache, getCachingKey(metastoreContext, databaseName));
    }

    private Optional<List<String>> loadAllTables(KeyAndContext<String> databaseNameKey)
    {
        return delegate.getAllTables(databaseNameKey.getContext(), databaseNameKey.getKey());
    }

    @Override
    public Optional<List<String>> getAllViews(MetastoreContext metastoreContext, String databaseName)
    {
        return get(viewNamesCache, getCachingKey(metastoreContext, databaseName));
    }

    private Optional<List<String>> loadAllViews(KeyAndContext<String> databaseNameKey)
    {
        return delegate.getAllViews(databaseNameKey.getContext(), databaseNameKey.getKey());
    }

    @Override
    public void createDatabase(MetastoreContext metastoreContext, Database database)
    {
        try {
            delegate.createDatabase(metastoreContext, database);
        }
        finally {
            invalidateDatabase(database.getDatabaseName());
        }
    }

    @Override
    public void dropDatabase(MetastoreContext metastoreContext, String databaseName)
    {
        try {
            delegate.dropDatabase(metastoreContext, databaseName);
        }
        finally {
            invalidateDatabase(databaseName);
        }
    }

    @Override
    public void renameDatabase(MetastoreContext metastoreContext, String databaseName, String newDatabaseName)
    {
        try {
            delegate.renameDatabase(metastoreContext, databaseName, newDatabaseName);
        }
        finally {
            invalidateDatabase(databaseName);
            invalidateDatabase(newDatabaseName);
        }
    }

    protected void invalidateDatabase(String databaseName)
    {
        databaseCache.asMap().keySet().stream()
                .filter(databaseKey -> databaseKey.getKey().equals(databaseName))
                .forEach(databaseCache::invalidate);
        databaseNamesCache.invalidateAll();
    }

    @Override
    public void createTable(MetastoreContext metastoreContext, Table table, PrincipalPrivileges principalPrivileges)
    {
        try {
            delegate.createTable(metastoreContext, table, principalPrivileges);
        }
        finally {
            invalidateTable(table.getDatabaseName(), table.getTableName());
        }
    }

    @Override
    public void dropTable(MetastoreContext metastoreContext, String databaseName, String tableName, boolean deleteData)
    {
        try {
            delegate.dropTable(metastoreContext, databaseName, tableName, deleteData);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    @Override
    public void replaceTable(MetastoreContext metastoreContext, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        try {
            delegate.replaceTable(metastoreContext, databaseName, tableName, newTable, principalPrivileges);
        }
        finally {
            invalidateTable(databaseName, tableName);
            invalidateTable(newTable.getDatabaseName(), newTable.getTableName());
        }
    }

    @Override
    public void renameTable(MetastoreContext metastoreContext, String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        try {
            delegate.renameTable(metastoreContext, databaseName, tableName, newDatabaseName, newTableName);
        }
        finally {
            invalidateTable(databaseName, tableName);
            invalidateTable(newDatabaseName, newTableName);
        }
    }

    @Override
    public void addColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        try {
            delegate.addColumn(metastoreContext, databaseName, tableName, columnName, columnType, columnComment);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    @Override
    public void renameColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        try {
            delegate.renameColumn(metastoreContext, databaseName, tableName, oldColumnName, newColumnName);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    @Override
    public void dropColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String columnName)
    {
        try {
            delegate.dropColumn(metastoreContext, databaseName, tableName, columnName);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    protected void invalidateTable(String databaseName, String tableName)
    {
        HiveTableName hiveTableName = hiveTableName(databaseName, tableName);

        tableCache.asMap().keySet().stream()
                .filter(hiveTableNameKey -> hiveTableNameKey.getKey().equals(hiveTableName))
                .forEach(tableCache::invalidate);

        tableNamesCache.asMap().keySet().stream()
                .filter(tableNameKey -> tableNameKey.getKey().equals(databaseName))
                .forEach(tableNamesCache::invalidate);

        viewNamesCache.asMap().keySet().stream()
                .filter(viewNameKey -> viewNameKey.getKey().equals(databaseName))
                .forEach(viewNamesCache::invalidate);

        tablePrivilegesCache.asMap().keySet().stream()
                .filter(userTableKey -> userTableKey.getKey().matches(databaseName, tableName))
                .forEach(tablePrivilegesCache::invalidate);

        tableStatisticsCache.asMap().keySet().stream()
                .filter(hiveTableNameKey -> hiveTableNameKey.getKey().equals(hiveTableName))
                .forEach(tableStatisticsCache::invalidate);

        invalidatePartitionCache(databaseName, tableName);
    }

    @Override
    public Optional<Partition> getPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionValues)
    {
        KeyAndContext<HivePartitionName> key = getCachingKey(metastoreContext, hivePartitionName(databaseName, tableName, partitionValues));
        Optional<Partition> result = get(partitionCache, key);
        if (isPartitionCacheValidationEnabled()) {
            validatePartitionCache(key, result);
        }
        return result;
    }

    @Override
    public Optional<List<String>> getPartitionNames(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        return get(partitionNamesCache, getCachingKey(metastoreContext, hiveTableName(databaseName, tableName)));
    }

    private Optional<List<String>> loadPartitionNames(KeyAndContext<HiveTableName> hiveTableNameKey)
    {
        return delegate.getPartitionNames(hiveTableNameKey.getContext(), hiveTableNameKey.getKey().getDatabaseName(), hiveTableNameKey.getKey().getTableName());
    }

    @Override
    public List<String> getPartitionNamesByFilter(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates)
    {
        if (partitionVersioningEnabled) {
            List<PartitionNameWithVersion> partitionNamesWithVersion = getPartitionNamesWithVersionByFilter(metastoreContext, databaseName, tableName, partitionPredicates);
            List<String> result = partitionNamesWithVersion.stream().map(PartitionNameWithVersion::getPartitionName).collect(toImmutableList());
            invalidateStalePartitions(partitionNamesWithVersion, databaseName, tableName, metastoreContext);
            return result;
        }
        return get(partitionFilterCache, getCachingKey(metastoreContext, partitionFilter(databaseName, tableName, partitionPredicates)));
    }

    @Override
    public List<PartitionNameWithVersion> getPartitionNamesWithVersionByFilter(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates)
    {
        return delegate.getPartitionNamesWithVersionByFilter(metastoreContext, databaseName, tableName, partitionPredicates);
    }

    private void invalidateStalePartitions(
            List<PartitionNameWithVersion> partitionNamesWithVersion,
            String databaseName,
            String tableName,
            MetastoreContext metastoreContext)
    {
        for (PartitionNameWithVersion partitionNameWithVersion : partitionNamesWithVersion) {
            HivePartitionName hivePartitionName = hivePartitionName(databaseName, tableName, partitionNameWithVersion.getPartitionName());
            KeyAndContext<HivePartitionName> partitionNameKey = getCachingKey(metastoreContext, hivePartitionName);
            Optional<Partition> partition = partitionCache.getIfPresent(partitionNameKey);
            if (partition == null || !partition.isPresent()) {
                partitionCache.invalidate(partitionNameKey);
                partitionStatisticsCache.invalidate(partitionNameKey);
            }
            else {
                Optional<Long> partitionVersion = partition.get().getPartitionVersion();
                if (!partitionVersion.isPresent() || !partitionVersion.equals(partitionNameWithVersion.getPartitionVersion())) {
                    partitionCache.invalidate(partitionNameKey);
                    partitionStatisticsCache.invalidate(partitionNameKey);
                }
            }
        }
    }

    private boolean isPartitionCacheValidationEnabled()
    {
        return partitionCacheValidationPercentage > 0 &&
                ThreadLocalRandom.current().nextDouble(100) < partitionCacheValidationPercentage;
    }

    private void validatePartitionCache(KeyAndContext<HivePartitionName> partitionName, Optional<Partition> partitionFromCache)
    {
        Optional<Partition> partitionFromMetastore = loadPartitionByName(partitionName);
        if (!partitionFromCache.equals(partitionFromMetastore)) {
            String errorMessage = format("Partition returned from cache is different from partition from Metastore.\nPartition name = %s.\nPartition from cache = %s\n Partition from Metastore = %s",
                    partitionName,
                    partitionFromCache,
                    partitionFromMetastore);
            throw new PrestoException(HIVE_CORRUPTED_PARTITION_CACHE, errorMessage);
        }
    }

    private void validatePartitionCache(Map<KeyAndContext<HivePartitionName>, Optional<Partition>> actualResult)
    {
        Map<KeyAndContext<HivePartitionName>, Optional<Partition>> expectedResult = loadPartitionsByNames(actualResult.keySet());

        for (Entry<KeyAndContext<HivePartitionName>, Optional<Partition>> entry : expectedResult.entrySet()) {
            HivePartitionName partitionName = entry.getKey().getKey();
            Optional<Partition> partitionFromCache = actualResult.get(entry.getKey());
            Optional<Partition> partitionFromMetastore = entry.getValue();

            if (!partitionFromCache.equals(partitionFromMetastore)) {
                String errorMessage = format("Partition returned from cache is different from partition from Metastore.\nPartition name = %s.\nPartition from cache = %s\n Partition from Metastore = %s",
                        partitionName,
                        partitionFromCache,
                        partitionFromMetastore);
                throw new PrestoException(HIVE_CORRUPTED_PARTITION_CACHE, errorMessage);
            }
        }
    }

    private List<String> loadPartitionNamesByFilter(KeyAndContext<PartitionFilter> partitionFilterKey)
    {
        return delegate.getPartitionNamesByFilter(
                partitionFilterKey.getContext(),
                partitionFilterKey.getKey().getHiveTableName().getDatabaseName(),
                partitionFilterKey.getKey().getHiveTableName().getTableName(),
                partitionFilterKey.getKey().getPartitionPredicates());
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionNames)
    {
        Iterable<KeyAndContext<HivePartitionName>> names = transform(partitionNames, name -> getCachingKey(metastoreContext, HivePartitionName.hivePartitionName(databaseName, tableName, name)));

        Map<KeyAndContext<HivePartitionName>, Optional<Partition>> all = getAll(partitionCache, names);
        if (isPartitionCacheValidationEnabled()) {
            validatePartitionCache(all);
        }
        ImmutableMap.Builder<String, Optional<Partition>> partitionsByName = ImmutableMap.builder();
        for (Entry<KeyAndContext<HivePartitionName>, Optional<Partition>> entry : all.entrySet()) {
            partitionsByName.put(entry.getKey().getKey().getPartitionName().get(), entry.getValue());
        }
        return partitionsByName.build();
    }

    private Optional<Partition> loadPartitionByName(KeyAndContext<HivePartitionName> partitionName)
    {
        return delegate.getPartition(
                partitionName.getContext(),
                partitionName.getKey().getHiveTableName().getDatabaseName(),
                partitionName.getKey().getHiveTableName().getTableName(),
                partitionName.getKey().getPartitionValues());
    }

    private Map<KeyAndContext<HivePartitionName>, Optional<Partition>> loadPartitionsByNames(Iterable<? extends KeyAndContext<HivePartitionName>> partitionNamesKey)
    {
        requireNonNull(partitionNamesKey, "partitionNames is null");
        checkArgument(!Iterables.isEmpty(partitionNamesKey), "partitionNames is empty");

        KeyAndContext<HivePartitionName> firstPartitionKey = Iterables.get(partitionNamesKey, 0);

        HiveTableName hiveTableName = firstPartitionKey.getKey().getHiveTableName();
        String databaseName = hiveTableName.getDatabaseName();
        String tableName = hiveTableName.getTableName();

        List<String> partitionsToFetch = new ArrayList<>();
        for (KeyAndContext<HivePartitionName> partitionNameKey : partitionNamesKey) {
            checkArgument(partitionNameKey.getKey().getHiveTableName().equals(hiveTableName), "Expected table name %s but got %s", hiveTableName, partitionNameKey.getKey().getHiveTableName());
            checkArgument(partitionNameKey.getContext().equals(firstPartitionKey.getContext()), "Expected context %s but got %s", firstPartitionKey.getContext(), partitionNameKey.getContext());
            partitionsToFetch.add(partitionNameKey.getKey().getPartitionName().get());
        }

        ImmutableMap.Builder<KeyAndContext<HivePartitionName>, Optional<Partition>> partitions = ImmutableMap.builder();
        Map<String, Optional<Partition>> partitionsByNames = delegate.getPartitionsByNames(firstPartitionKey.getContext(), databaseName, tableName, partitionsToFetch);
        for (Entry<String, Optional<Partition>> entry : partitionsByNames.entrySet()) {
            partitions.put(getCachingKey(firstPartitionKey.getContext(), HivePartitionName.hivePartitionName(hiveTableName, entry.getKey())), entry.getValue());
        }
        return partitions.build();
    }

    @Override
    public void addPartitions(MetastoreContext metastoreContext, String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        try {
            delegate.addPartitions(metastoreContext, databaseName, tableName, partitions);
        }
        finally {
            // todo do we need to invalidate all partitions?
            invalidatePartitionCache(databaseName, tableName);
        }
    }

    @Override
    public void dropPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        try {
            delegate.dropPartition(metastoreContext, databaseName, tableName, parts, deleteData);
        }
        finally {
            invalidatePartitionCache(databaseName, tableName);
        }
    }

    @Override
    public void alterPartition(MetastoreContext metastoreContext, String databaseName, String tableName, PartitionWithStatistics partition)
    {
        try {
            delegate.alterPartition(metastoreContext, databaseName, tableName, partition);
        }
        finally {
            invalidatePartitionCache(databaseName, tableName);
        }
    }

    @Override
    public void createRole(MetastoreContext metastoreContext, String role, String grantor)
    {
        try {
            delegate.createRole(metastoreContext, role, grantor);
        }
        finally {
            rolesCache.invalidateAll();
        }
    }

    @Override
    public void dropRole(MetastoreContext metastoreContext, String role)
    {
        try {
            delegate.dropRole(metastoreContext, role);
        }
        finally {
            rolesCache.invalidateAll();
            roleGrantsCache.invalidateAll();
        }
    }

    @Override
    public Set<String> listRoles(MetastoreContext metastoreContext)
    {
        return get(rolesCache, getCachingKey(metastoreContext, ""));
    }

    private Set<String> loadAllRoles(KeyAndContext<String> rolesKey)
    {
        return delegate.listRoles(rolesKey.getContext());
    }

    @Override
    public void grantRoles(MetastoreContext metastoreContext, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, PrestoPrincipal grantor)
    {
        try {
            delegate.grantRoles(metastoreContext, roles, grantees, withAdminOption, grantor);
        }
        finally {
            roleGrantsCache.invalidateAll();
        }
    }

    @Override
    public void revokeRoles(MetastoreContext metastoreContext, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, PrestoPrincipal grantor)
    {
        try {
            delegate.revokeRoles(metastoreContext, roles, grantees, adminOptionFor, grantor);
        }
        finally {
            roleGrantsCache.invalidateAll();
        }
    }

    @Override
    public Set<RoleGrant> listRoleGrants(MetastoreContext metastoreContext, PrestoPrincipal principal)
    {
        return get(roleGrantsCache, getCachingKey(metastoreContext, principal));
    }

    private Set<RoleGrant> loadRoleGrants(KeyAndContext<PrestoPrincipal> principalKey)
    {
        return delegate.listRoleGrants(principalKey.getContext(), principalKey.getKey());
    }

    private void invalidatePartitionCache(String databaseName, String tableName)
    {
        HiveTableName hiveTableName = hiveTableName(databaseName, tableName);
        partitionNamesCache.asMap().keySet().stream()
                .filter(hiveTableNameKey -> hiveTableNameKey.getKey().equals(hiveTableName))
                .forEach(partitionNamesCache::invalidate);
        partitionCache.asMap().keySet().stream()
                .filter(partitionNameKey -> partitionNameKey.getKey().getHiveTableName().equals(hiveTableName))
                .forEach(partitionCache::invalidate);
        partitionFilterCache.asMap().keySet().stream()
                .filter(partitionFilterKey -> partitionFilterKey.getKey().getHiveTableName().equals(hiveTableName))
                .forEach(partitionFilterCache::invalidate);
        partitionStatisticsCache.asMap().keySet().stream()
                .filter(partitionFilterKey -> partitionFilterKey.getKey().getHiveTableName().equals(hiveTableName))
                .forEach(partitionStatisticsCache::invalidate);
    }

    private void invalidateTablePrivilegesCache(PrestoPrincipal grantee, String databaseName, String tableName)
    {
        UserTableKey userTableKey = new UserTableKey(grantee, databaseName, tableName);
        tablePrivilegesCache.asMap().keySet().stream()
                .filter(tablePrivilegesCacheKey -> tablePrivilegesCacheKey.getKey().equals(userTableKey))
                .forEach(tablePrivilegesCache::invalidate);
    }

    @Override
    public void grantTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        try {
            delegate.grantTablePrivileges(metastoreContext, databaseName, tableName, grantee, privileges);
        }
        finally {
            invalidateTablePrivilegesCache(grantee, databaseName, tableName);
        }
    }

    @Override
    public void revokeTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        try {
            delegate.revokeTablePrivileges(metastoreContext, databaseName, tableName, grantee, privileges);
        }
        finally {
            invalidateTablePrivilegesCache(grantee, databaseName, tableName);
        }
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal principal)
    {
        return get(tablePrivilegesCache, getCachingKey(metastoreContext, new UserTableKey(principal, databaseName, tableName)));
    }

    @Override
    public void setPartitionLeases(MetastoreContext metastoreContext, String databaseName, String tableName, Map<String, String> partitionNameToLocation, Duration leaseDuration)
    {
        delegate.setPartitionLeases(metastoreContext, databaseName, tableName, partitionNameToLocation, leaseDuration);
    }

    public Set<HivePrivilegeInfo> loadTablePrivileges(KeyAndContext<UserTableKey> loadTablePrivilegesKey)
    {
        return delegate.listTablePrivileges(loadTablePrivilegesKey.getContext(), loadTablePrivilegesKey.getKey().getDatabase(), loadTablePrivilegesKey.getKey().getTable(), loadTablePrivilegesKey.getKey().getPrincipal());
    }

    private static class KeyAndContext<T>
    {
        private final MetastoreContext context;
        private final T key;

        public KeyAndContext(MetastoreContext context, T key)
        {
            this.context = requireNonNull(context, "context is null");
            this.key = requireNonNull(key, "key is null");
        }

        public MetastoreContext getContext()
        {
            return context;
        }

        public T getKey()
        {
            return key;
        }

        // QueryId changes for every query. For caching to be effective across multiple queries, we should NOT include queryId,
        // other fields of MetastoreContext in equals() and hashCode() methods below.
        // But we should include username because we want the cache to be effective at per-user level when impersonation is enabled.
        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            KeyAndContext<?> other = (KeyAndContext<?>) o;
            if (context.isImpersonationEnabled()) {
                return Objects.equals(context.getUsername(), other.context.getUsername()) &&
                        Objects.equals(key, other.key);
            }
            return Objects.equals(key, other.key);
        }

        @Override
        public int hashCode()
        {
            if (context.isImpersonationEnabled()) {
                return Objects.hash(context.getUsername(), key);
            }
            return Objects.hash(key);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("context", context)
                    .add("key", key)
                    .toString();
        }
    }

    private <T> KeyAndContext<T> getCachingKey(MetastoreContext context, T key)
    {
        MetastoreContext metastoreContext = metastoreImpersonationEnabled ? new MetastoreContext(context.getUsername(), context.getQueryId(), context.getClientInfo(), context.getSource(), true, context.getMetastoreHeaders()) : context;
        return new KeyAndContext<>(metastoreContext, key);
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
