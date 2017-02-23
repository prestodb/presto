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

import com.facebook.presto.hive.ForCachingHiveMetastore;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.RoleGrant;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ExecutionError;
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

import static com.facebook.presto.hive.HiveUtil.toPartitionValues;
import static com.facebook.presto.spi.security.PrincipalType.USER;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static com.google.common.collect.Iterables.transform;
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
    protected final ExtendedHiveMetastore delegate;
    private final LoadingCache<String, Optional<Database>> databaseCache;
    private final LoadingCache<String, List<String>> databaseNamesCache;
    private final LoadingCache<HiveTableName, Optional<Table>> tableCache;
    private final LoadingCache<String, Optional<List<String>>> tableNamesCache;
    private final LoadingCache<String, Optional<List<String>>> viewNamesCache;
    private final LoadingCache<HivePartitionName, Optional<Partition>> partitionCache;
    private final LoadingCache<PartitionFilter, Optional<List<String>>> partitionFilterCache;
    private final LoadingCache<HiveTableName, Optional<List<String>>> partitionNamesCache;
    private final LoadingCache<UserTableKey, Set<HivePrivilegeInfo>> tablePrivilegesCache;
    private final LoadingCache<String, Set<String>> rolesCache;
    private final LoadingCache<PrestoPrincipal, Set<RoleGrant>> roleGrantsCache;

    @Inject
    public CachingHiveMetastore(@ForCachingHiveMetastore ExtendedHiveMetastore delegate, @ForCachingHiveMetastore ExecutorService executor, HiveClientConfig hiveClientConfig)
    {
        this(requireNonNull(delegate, "delegate is null"),
                requireNonNull(executor, "executor is null"),
                requireNonNull(hiveClientConfig, "hiveClientConfig is null").getMetastoreCacheTtl(),
                hiveClientConfig.getMetastoreRefreshInterval(),
                hiveClientConfig.getMetastoreCacheMaximumSize());
    }

    public CachingHiveMetastore(ExtendedHiveMetastore delegate, ExecutorService executor, Duration cacheTtl, Duration refreshInterval, long maximumSize)
    {
        this(requireNonNull(delegate, "delegate is null"),
                requireNonNull(executor, "executor is null"),
                OptionalLong.of(requireNonNull(cacheTtl, "cacheTtl is null").toMillis()),
                OptionalLong.of(requireNonNull(refreshInterval, "refreshInterval is null").toMillis()),
                maximumSize);
    }

    public static CachingHiveMetastore memoizeMetastore(ExtendedHiveMetastore delegate, long maximumSize)
    {
        return new CachingHiveMetastore(requireNonNull(delegate, "delegate is null"),
                newDirectExecutorService(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                maximumSize);
    }

    private CachingHiveMetastore(ExtendedHiveMetastore delegate, ExecutorService executor, OptionalLong expiresAfterWriteMillis, OptionalLong refreshMills, long maximumSize)
    {
        this.delegate = delegate;

        databaseNamesCache = newCacheBuilder(expiresAfterWriteMillis, refreshMills, maximumSize)
                .build(asyncReloading(new CacheLoader<String, List<String>>()
                {
                    @Override
                    public List<String> load(String key)
                            throws Exception
                    {
                        return loadAllDatabases();
                    }
                }, executor));

        databaseCache = newCacheBuilder(expiresAfterWriteMillis, refreshMills, maximumSize)
                .build(asyncReloading(new CacheLoader<String, Optional<Database>>()
                {
                    @Override
                    public Optional<Database> load(String databaseName)
                            throws Exception
                    {
                        return loadDatabase(databaseName);
                    }
                }, executor));

        tableNamesCache = newCacheBuilder(expiresAfterWriteMillis, refreshMills, maximumSize)
                .build(asyncReloading(new CacheLoader<String, Optional<List<String>>>()
                {
                    @Override
                    public Optional<List<String>> load(String databaseName)
                            throws Exception
                    {
                        return loadAllTables(databaseName);
                    }
                }, executor));

        tableCache = newCacheBuilder(expiresAfterWriteMillis, refreshMills, maximumSize)
                .build(asyncReloading(new CacheLoader<HiveTableName, Optional<Table>>()
                {
                    @Override
                    public Optional<Table> load(HiveTableName hiveTableName)
                            throws Exception
                    {
                        return loadTable(hiveTableName);
                    }
                }, executor));

        viewNamesCache = newCacheBuilder(expiresAfterWriteMillis, refreshMills, maximumSize)
                .build(asyncReloading(new CacheLoader<String, Optional<List<String>>>()
                {
                    @Override
                    public Optional<List<String>> load(String databaseName)
                            throws Exception
                    {
                        return loadAllViews(databaseName);
                    }
                }, executor));

        partitionNamesCache = newCacheBuilder(expiresAfterWriteMillis, refreshMills, maximumSize)
                .build(asyncReloading(new CacheLoader<HiveTableName, Optional<List<String>>>()
                {
                    @Override
                    public Optional<List<String>> load(HiveTableName hiveTableName)
                            throws Exception
                    {
                        return loadPartitionNames(hiveTableName);
                    }
                }, executor));

        partitionFilterCache = newCacheBuilder(expiresAfterWriteMillis, refreshMills, maximumSize)
                .build(asyncReloading(new CacheLoader<PartitionFilter, Optional<List<String>>>()
                {
                    @Override
                    public Optional<List<String>> load(PartitionFilter partitionFilter)
                            throws Exception
                    {
                        return loadPartitionNamesByParts(partitionFilter);
                    }
                }, executor));

        partitionCache = newCacheBuilder(expiresAfterWriteMillis, refreshMills, maximumSize)
                .build(asyncReloading(new CacheLoader<HivePartitionName, Optional<Partition>>()
                {
                    @Override
                    public Optional<Partition> load(HivePartitionName partitionName)
                            throws Exception
                    {
                        return loadPartitionByName(partitionName);
                    }

                    @Override
                    public Map<HivePartitionName, Optional<Partition>> loadAll(Iterable<? extends HivePartitionName> partitionNames)
                            throws Exception
                    {
                        return loadPartitionsByNames(partitionNames);
                    }
                }, executor));

        tablePrivilegesCache = newCacheBuilder(expiresAfterWriteMillis, refreshMills, maximumSize)
                .build(asyncReloading(new CacheLoader<UserTableKey, Set<HivePrivilegeInfo>>()
                {
                    @Override
                    public Set<HivePrivilegeInfo> load(UserTableKey key)
                            throws Exception
                    {
                        return loadTablePrivileges(key.getDatabase(), key.getTable(), key.getPrincipal());
                    }
                }, executor));

        rolesCache = newCacheBuilder(expiresAfterWriteMillis, refreshMills, maximumSize)
                .build(asyncReloading(new CacheLoader<String, Set<String>>()
                {
                    @Override
                    public Set<String> load(String key)
                            throws Exception
                    {
                        return loadRoles();
                    }
                }, executor));

        roleGrantsCache = newCacheBuilder(expiresAfterWriteMillis, refreshMills, maximumSize)
                .build(asyncReloading(new CacheLoader<PrestoPrincipal, Set<RoleGrant>>()
                {
                    @Override
                    public Set<RoleGrant> load(PrestoPrincipal key)
                            throws Exception
                    {
                        return loadRoleGrants(key);
                    }
                }, executor));
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
        rolesCache.invalidateAll();
    }

    private static <K, V> V get(LoadingCache<K, V> cache, K key)
    {
        try {
            return cache.get(key);
        }
        catch (ExecutionException | UncheckedExecutionException | ExecutionError e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    private static <K, V> Map<K, V> getAll(LoadingCache<K, V> cache, Iterable<K> keys)
    {
        try {
            return cache.getAll(keys);
        }
        catch (ExecutionException | UncheckedExecutionException | ExecutionError e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        return get(databaseCache, databaseName);
    }

    private Optional<Database> loadDatabase(String databaseName)
            throws Exception
    {
        return delegate.getDatabase(databaseName);
    }

    @Override
    public List<String> getAllDatabases()
    {
        return get(databaseNamesCache, "");
    }

    private List<String> loadAllDatabases()
            throws Exception
    {
        return delegate.getAllDatabases();
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        return get(tableCache, HiveTableName.table(databaseName, tableName));
    }

    private Optional<Table> loadTable(HiveTableName hiveTableName)
            throws Exception
    {
        return delegate.getTable(hiveTableName.getDatabaseName(), hiveTableName.getTableName());
    }

    @Override
    public Optional<List<String>> getAllTables(String databaseName)
    {
        return get(tableNamesCache, databaseName);
    }

    private Optional<List<String>> loadAllTables(String databaseName)
            throws Exception
    {
        return delegate.getAllTables(databaseName);
    }

    @Override
    public Optional<List<String>> getAllViews(String databaseName)
    {
        return get(viewNamesCache, databaseName);
    }

    private Optional<List<String>> loadAllViews(String databaseName)
            throws Exception
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

    protected void invalidateTable(String databaseName, String tableName)
    {
        tableCache.invalidate(new HiveTableName(databaseName, tableName));
        tableNamesCache.invalidate(databaseName);
        viewNamesCache.invalidate(databaseName);
        tablePrivilegesCache.asMap().keySet().stream()
                .filter(userTableKey -> userTableKey.matches(databaseName, tableName))
                .forEach(tablePrivilegesCache::invalidate);
        invalidatePartitionCache(databaseName, tableName);
    }

    @Override
    public Optional<Partition> getPartition(String databaseName, String tableName, List<String> partitionValues)
    {
        HivePartitionName name = HivePartitionName.partition(databaseName, tableName, partitionValues);
        return get(partitionCache, name);
    }

    @Override
    public Optional<List<String>> getPartitionNames(String databaseName, String tableName)
    {
        return get(partitionNamesCache, HiveTableName.table(databaseName, tableName));
    }

    private Optional<List<String>> loadPartitionNames(HiveTableName hiveTableName)
            throws Exception
    {
        return delegate.getPartitionNames(hiveTableName.getDatabaseName(), hiveTableName.getTableName());
    }

    @Override
    public Optional<List<String>> getPartitionNamesByParts(String databaseName, String tableName, List<String> parts)
    {
        return get(partitionFilterCache, PartitionFilter.partitionFilter(databaseName, tableName, parts));
    }

    private Optional<List<String>> loadPartitionNamesByParts(PartitionFilter partitionFilter)
            throws Exception
    {
        return delegate.getPartitionNamesByParts(
                partitionFilter.getHiveTableName().getDatabaseName(),
                partitionFilter.getHiveTableName().getTableName(),
                partitionFilter.getParts());
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
    {
        Iterable<HivePartitionName> names = transform(partitionNames, name -> HivePartitionName.partition(databaseName, tableName, name));

        Map<HivePartitionName, Optional<Partition>> all = getAll(partitionCache, names);
        ImmutableMap.Builder<String, Optional<Partition>> partitionsByName = ImmutableMap.builder();
        for (Entry<HivePartitionName, Optional<Partition>> entry : all.entrySet()) {
            partitionsByName.put(entry.getKey().getPartitionName(), entry.getValue());
        }
        return partitionsByName.build();
    }

    private Optional<Partition> loadPartitionByName(HivePartitionName partitionName)
            throws Exception
    {
        return delegate.getPartition(
                partitionName.getHiveTableName().getDatabaseName(),
                partitionName.getHiveTableName().getTableName(),
                partitionName.getPartitionValues());
    }

    private Map<HivePartitionName, Optional<Partition>> loadPartitionsByNames(Iterable<? extends HivePartitionName> partitionNames)
            throws Exception
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
            partitionsToFetch.add(partitionName.getPartitionName());
        }

        ImmutableMap.Builder<HivePartitionName, Optional<Partition>> partitions = ImmutableMap.builder();
        Map<String, Optional<Partition>> partitionsByNames = delegate.getPartitionsByNames(databaseName, tableName, partitionsToFetch);
        for (Entry<String, Optional<Partition>> entry : partitionsByNames.entrySet()) {
            partitions.put(HivePartitionName.partition(hiveTableName, entry.getKey()), entry.getValue());
        }
        return partitions.build();
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<Partition> partitions)
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
    public void alterPartition(String databaseName, String tableName, Partition partition)
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

    public Set<RoleGrant> loadRoleGrants(PrestoPrincipal principal)
    {
        return delegate.listRoleGrants(principal);
    }

    private void invalidatePartitionCache(String databaseName, String tableName)
    {
        HiveTableName hiveTableName = HiveTableName.table(databaseName, tableName);
        partitionNamesCache.invalidate(hiveTableName);
        partitionCache.asMap().keySet().stream()
                .filter(partitionName -> partitionName.getHiveTableName().equals(hiveTableName))
                .forEach(partitionCache::invalidate);
        partitionFilterCache.asMap().keySet().stream()
                .filter(partitionFilter -> partitionFilter.getHiveTableName().equals(hiveTableName))
                .forEach(partitionFilterCache::invalidate);
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String grantee, Set<HivePrivilegeInfo> privileges)
    {
        try {
            delegate.grantTablePrivileges(databaseName, tableName, grantee, privileges);
        }
        finally {
            tablePrivilegesCache.invalidate(new UserTableKey(new PrestoPrincipal(USER, grantee), databaseName, tableName));
        }
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String grantee, Set<HivePrivilegeInfo> privileges)
    {
        try {
            delegate.revokeTablePrivileges(databaseName, tableName, grantee, privileges);
        }
        finally {
            tablePrivilegesCache.invalidate(new UserTableKey(new PrestoPrincipal(USER, grantee), databaseName, tableName));
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
        if (refreshMillis.isPresent()) {
            cacheBuilder = cacheBuilder.refreshAfterWrite(refreshMillis.getAsLong(), MILLISECONDS);
        }
        cacheBuilder = cacheBuilder.maximumSize(maximumSize);
        return cacheBuilder;
    }

    private static class HiveTableName
    {
        private final String databaseName;
        private final String tableName;

        private HiveTableName(String databaseName, String tableName)
        {
            this.databaseName = databaseName;
            this.tableName = tableName;
        }

        public static HiveTableName table(String databaseName, String tableName)
        {
            return new HiveTableName(databaseName, tableName);
        }

        public String getDatabaseName()
        {
            return databaseName;
        }

        public String getTableName()
        {
            return tableName;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("databaseName", databaseName)
                    .add("tableName", tableName)
                    .toString();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            HiveTableName other = (HiveTableName) o;
            return Objects.equals(databaseName, other.databaseName) &&
                    Objects.equals(tableName, other.tableName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(databaseName, tableName);
        }
    }

    private static class HivePartitionName
    {
        private final HiveTableName hiveTableName;
        private final List<String> partitionValues;
        private final String partitionName; // does not participate in hashCode/equals

        private HivePartitionName(HiveTableName hiveTableName, List<String> partitionValues, String partitionName)
        {
            this.hiveTableName = requireNonNull(hiveTableName, "hiveTableName is null");
            this.partitionValues = requireNonNull(partitionValues, "partitionValues is null");
            this.partitionName = partitionName;
        }

        public static HivePartitionName partition(HiveTableName hiveTableName, String partitionName)
        {
            return new HivePartitionName(hiveTableName, toPartitionValues(partitionName), partitionName);
        }

        public static HivePartitionName partition(String databaseName, String tableName, String partitionName)
        {
            return partition(HiveTableName.table(databaseName, tableName), partitionName);
        }

        public static HivePartitionName partition(String databaseName, String tableName, List<String> partitionValues)
        {
            return new HivePartitionName(HiveTableName.table(databaseName, tableName), partitionValues, null);
        }

        public HiveTableName getHiveTableName()
        {
            return hiveTableName;
        }

        public List<String> getPartitionValues()
        {
            return partitionValues;
        }

        public String getPartitionName()
        {
            return requireNonNull(partitionName, "partitionName is null");
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("hiveTableName", hiveTableName)
                    .add("partitionValues", partitionValues)
                    .add("partitionName", partitionName)
                    .toString();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            HivePartitionName other = (HivePartitionName) o;
            return Objects.equals(hiveTableName, other.hiveTableName) &&
                    Objects.equals(partitionValues, other.partitionValues);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(hiveTableName, partitionValues);
        }
    }

    private static class PartitionFilter
    {
        private final HiveTableName hiveTableName;
        private final List<String> parts;

        private PartitionFilter(HiveTableName hiveTableName, List<String> parts)
        {
            this.hiveTableName = hiveTableName;
            this.parts = ImmutableList.copyOf(parts);
        }

        public static PartitionFilter partitionFilter(String databaseName, String tableName, List<String> parts)
        {
            return new PartitionFilter(HiveTableName.table(databaseName, tableName), parts);
        }

        public HiveTableName getHiveTableName()
        {
            return hiveTableName;
        }

        public List<String> getParts()
        {
            return parts;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("hiveTableName", hiveTableName)
                    .add("parts", parts)
                    .toString();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            PartitionFilter other = (PartitionFilter) o;
            return Objects.equals(hiveTableName, other.hiveTableName) &&
                    Objects.equals(parts, other.parts);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(hiveTableName, parts);
        }
    }

    private static class UserTableKey
    {
        private final PrestoPrincipal principal;
        private final String database;
        private final String table;

        public UserTableKey(PrestoPrincipal principal, String database, String table)
        {
            this.principal = requireNonNull(principal, "principal is null");
            this.table = requireNonNull(table, "table is null");
            this.database = requireNonNull(database, "database is null");
        }

        public PrestoPrincipal getPrincipal()
        {
            return principal;
        }

        public String getDatabase()
        {
            return database;
        }

        public String getTable()
        {
            return table;
        }

        public boolean matches(String databaseName, String tableName)
        {
            return this.database.equals(databaseName) && this.table.equals(tableName);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            UserTableKey that = (UserTableKey) o;
            return Objects.equals(principal, that.principal) &&
                    Objects.equals(table, that.table) &&
                    Objects.equals(database, that.database);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(principal, table, database);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("principal", principal)
                    .add("table", table)
                    .add("database", database)
                    .toString();
        }
    }
}
