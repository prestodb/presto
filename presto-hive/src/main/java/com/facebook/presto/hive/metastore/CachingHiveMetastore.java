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

import com.facebook.presto.hive.ForHiveMetastore;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveCluster;
import com.facebook.presto.hive.HiveViewNotSupportedException;
import com.facebook.presto.hive.RetryDriver;
import com.facebook.presto.hive.TableAlreadyExistsException;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.units.Duration;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.thrift.TException;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static com.facebook.presto.hive.HiveUtil.PRESTO_VIEW_FLAG;
import static com.facebook.presto.hive.HiveUtil.isPrestoView;
import static com.facebook.presto.hive.metastore.HivePrivilege.OWNERSHIP;
import static com.facebook.presto.hive.metastore.HivePrivilege.parsePrivilege;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static com.google.common.collect.Iterables.transform;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.StreamSupport.stream;
import static org.apache.hadoop.hive.metastore.api.PrincipalType.ROLE;
import static org.apache.hadoop.hive.metastore.api.PrincipalType.USER;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS;

/**
 * Hive Metastore Cache
 */
@ThreadSafe
public class CachingHiveMetastore
        implements HiveMetastore
{
    private final CachingHiveMetastoreStats stats = new CachingHiveMetastoreStats();
    protected final HiveCluster clientProvider;
    private final LoadingCache<String, List<String>> databaseNamesCache;
    private final LoadingCache<String, Optional<Database>> databaseCache;
    private final LoadingCache<String, Optional<List<String>>> tableNamesCache;
    private final LoadingCache<String, Optional<List<String>>> viewNamesCache;
    private final LoadingCache<HiveTableName, Optional<List<String>>> partitionNamesCache;
    private final LoadingCache<HiveTableName, Optional<Table>> tableCache;
    private final LoadingCache<HivePartitionName, Optional<Partition>> partitionCache;
    private final LoadingCache<PartitionFilter, Optional<List<String>>> partitionFilterCache;
    private final LoadingCache<String, Set<String>> userRolesCache;
    private final LoadingCache<UserTableKey, Set<HivePrivilege>> userTablePrivileges;

    @Inject
    public CachingHiveMetastore(HiveCluster hiveCluster, @ForHiveMetastore ExecutorService executor, HiveClientConfig hiveClientConfig)
    {
        this(requireNonNull(hiveCluster, "hiveCluster is null"),
                requireNonNull(executor, "executor is null"),
                requireNonNull(hiveClientConfig, "hiveClientConfig is null").getMetastoreCacheTtl(),
                hiveClientConfig.getMetastoreRefreshInterval());
    }

    public CachingHiveMetastore(HiveCluster hiveCluster, ExecutorService executor, Duration cacheTtl, Duration refreshInterval)
    {
        this.clientProvider = requireNonNull(hiveCluster, "hiveCluster is null");

        long expiresAfterWriteMillis = requireNonNull(cacheTtl, "cacheTtl is null").toMillis();
        long refreshMills = requireNonNull(refreshInterval, "refreshInterval is null").toMillis();

        databaseNamesCache = CacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .refreshAfterWrite(refreshMills, MILLISECONDS)
                .build(asyncReloading(new CacheLoader<String, List<String>>()
                {
                    @Override
                    public List<String> load(String key)
                            throws Exception
                    {
                        return loadAllDatabases();
                    }
                }, executor));

        databaseCache = CacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .refreshAfterWrite(refreshMills, MILLISECONDS)
                .build(asyncReloading(new CacheLoader<String, Optional<Database>>()
                {
                    @Override
                    public Optional<Database> load(String databaseName)
                            throws Exception
                    {
                        return loadDatabase(databaseName);
                    }
                }, executor));

        tableNamesCache = CacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .refreshAfterWrite(refreshMills, MILLISECONDS)
                .build(asyncReloading(new CacheLoader<String, Optional<List<String>>>()
                {
                    @Override
                    public Optional<List<String>> load(String databaseName)
                            throws Exception
                    {
                        return loadAllTables(databaseName);
                    }
                }, executor));

        tableCache = CacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .refreshAfterWrite(refreshMills, MILLISECONDS)
                .build(asyncReloading(new CacheLoader<HiveTableName, Optional<Table>>()
                {
                    @Override
                    public Optional<Table> load(HiveTableName hiveTableName)
                            throws Exception
                    {
                        return loadTable(hiveTableName);
                    }
                }, executor));

        viewNamesCache = CacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .refreshAfterWrite(refreshMills, MILLISECONDS)
                .build(asyncReloading(new CacheLoader<String, Optional<List<String>>>()
                {
                    @Override
                    public Optional<List<String>> load(String databaseName)
                            throws Exception
                    {
                        return loadAllViews(databaseName);
                    }
                }, executor));

        partitionNamesCache = CacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .refreshAfterWrite(refreshMills, MILLISECONDS)
                .build(asyncReloading(new CacheLoader<HiveTableName, Optional<List<String>>>()
                {
                    @Override
                    public Optional<List<String>> load(HiveTableName hiveTableName)
                            throws Exception
                    {
                        return loadPartitionNames(hiveTableName);
                    }
                }, executor));

        partitionFilterCache = CacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .refreshAfterWrite(refreshMills, MILLISECONDS)
                .build(asyncReloading(new CacheLoader<PartitionFilter, Optional<List<String>>>()
                {
                    @Override
                    public Optional<List<String>> load(PartitionFilter partitionFilter)
                            throws Exception
                    {
                        return loadPartitionNamesByParts(partitionFilter);
                    }
                }, executor));

        partitionCache = CacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .refreshAfterWrite(refreshMills, MILLISECONDS)
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

        userRolesCache = CacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .refreshAfterWrite(refreshMills, MILLISECONDS)
                .build(asyncReloading(new CacheLoader<String, Set<String>>()
                {
                    @Override
                    public Set<String> load(String user)
                            throws Exception
                    {
                        return loadRoles(user);
                    }
                }, executor));

        userTablePrivileges = CacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .refreshAfterWrite(refreshMills, MILLISECONDS)
                .build(asyncReloading(new CacheLoader<UserTableKey, Set<HivePrivilege>>()
                {
                    @Override
                    public Set<HivePrivilege> load(UserTableKey key)
                            throws Exception
                    {
                        return loadTablePrivileges(key.getUser(), key.getDatabase(), key.getTable());
                    }
                }, executor));
    }

    @Managed
    @Flatten
    public CachingHiveMetastoreStats getStats()
    {
        return stats;
    }

    @Override
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
        userTablePrivileges.invalidateAll();
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
    public List<String> getAllDatabases()
    {
        return get(databaseNamesCache, "");
    }

    private List<String> loadAllDatabases()
            throws Exception
    {
        try {
            return retry()
                    .stopOnIllegalExceptions()
                    .run("getAllDatabases", stats.getGetAllDatabases().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            return client.getAllDatabases();
                        }
                    }));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
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
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getDatabase", stats.getGetDatabase().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            return Optional.of(client.getDatabase(databaseName));
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<List<String>> getAllTables(String databaseName)
    {
        return get(tableNamesCache, databaseName);
    }

    private Optional<List<String>> loadAllTables(String databaseName)
            throws Exception
    {
        Callable<List<String>> getAllTables = stats.getGetAllTables().wrap(() -> {
            try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                return client.getAllTables(databaseName);
            }
        });

        Callable<Void> getDatabase = stats.getGetDatabase().wrap(() -> {
            try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                client.getDatabase(databaseName);
                return null;
            }
        });

        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getAllTables", () -> {
                        List<String> tables = getAllTables.call();
                        if (tables.isEmpty()) {
                            // Check to see if the database exists
                            getDatabase.call();
                        }
                        return Optional.of(tables);
                    });
        }
        catch (NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        return get(tableCache, HiveTableName.table(databaseName, tableName));
    }

    @Override
    public Optional<List<String>> getAllViews(String databaseName)
    {
        return get(viewNamesCache, databaseName);
    }

    private Optional<List<String>> loadAllViews(String databaseName)
            throws Exception
    {
        try {
            return retry()
                    .stopOn(UnknownDBException.class)
                    .stopOnIllegalExceptions()
                    .run("getAllViews", stats.getAllViews().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            String filter = HIVE_FILTER_FIELD_PARAMS + PRESTO_VIEW_FLAG + " = \"true\"";
                            return Optional.of(client.getTableNamesByFilter(databaseName, filter));
                        }
                    }));
        }
        catch (UnknownDBException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void createTable(Table table)
    {
        try {
            retry()
                    .stopOn(AlreadyExistsException.class, InvalidObjectException.class, MetaException.class, NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("createTable", stats.getCreateTable().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            client.createTable(table);
                        }
                        return null;
                    }));
        }
        catch (AlreadyExistsException e) {
            throw new TableAlreadyExistsException(new SchemaTableName(table.getDbName(), table.getTableName()));
        }
        catch (NoSuchObjectException e) {
            throw new SchemaNotFoundException(table.getDbName());
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw Throwables.propagate(e);
        }
        finally {
            invalidateTable(table.getDbName(), table.getTableName());
        }
    }

    @Override
    public void dropTable(String databaseName, String tableName)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("dropTable", stats.getDropTable().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            client.dropTable(databaseName, tableName, true);
                        }
                        return null;
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw Throwables.propagate(e);
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
        invalidatePartitionCache(databaseName, tableName);
    }

    @Override
    public void alterTable(String databaseName, String tableName, Table table)
    {
        try {
            retry()
                    .stopOn(InvalidOperationException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("alterTable", stats.getAlterTable().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            Optional<Table> source = loadTable(new HiveTableName(databaseName, tableName));
                            if (!source.isPresent()) {
                                throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
                            }
                            client.alterTable(databaseName, tableName, table);
                        }
                        return null;
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (InvalidOperationException | MetaException e) {
            throw Throwables.propagate(e);
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw Throwables.propagate(e);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    private Optional<Table> loadTable(HiveTableName hiveTableName)
            throws Exception
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class, HiveViewNotSupportedException.class)
                    .stopOnIllegalExceptions()
                    .run("getTable", stats.getGetTable().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            Table table = client.getTable(hiveTableName.getDatabaseName(), hiveTableName.getTableName());
                            if (table.getTableType().equals(TableType.VIRTUAL_VIEW.name()) && (!isPrestoView(table))) {
                                throw new HiveViewNotSupportedException(new SchemaTableName(hiveTableName.getDatabaseName(), hiveTableName.getTableName()));
                            }
                            return Optional.of(table);
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<List<String>> getPartitionNames(String databaseName, String tableName)
    {
        return get(partitionNamesCache, HiveTableName.table(databaseName, tableName));
    }

    private Optional<List<String>> loadPartitionNames(HiveTableName hiveTableName)
            throws Exception
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartitionNames", stats.getGetPartitionNames().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            return Optional.of(client.getPartitionNames(hiveTableName.getDatabaseName(), hiveTableName.getTableName()));
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<List<String>> getPartitionNamesByParts(String databaseName, String tableName, List<String> parts)
    {
        return get(partitionFilterCache, PartitionFilter.partitionFilter(databaseName, tableName, parts));
    }

    private Optional<List<String>> loadPartitionNamesByParts(PartitionFilter partitionFilter)
            throws Exception
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartitionNamesByParts", stats.getGetPartitionNamesPs().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            return Optional.of(client.getPartitionNamesFiltered(
                                    partitionFilter.getHiveTableName().getDatabaseName(),
                                    partitionFilter.getHiveTableName().getTableName(),
                                    partitionFilter.getParts()));
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<Partition> partitions)
    {
        if (partitions.isEmpty()) {
            return;
        }
        try {
            retry()
                    .stopOn(AlreadyExistsException.class, InvalidObjectException.class, MetaException.class, NoSuchObjectException.class, PrestoException.class)
                    .stopOnIllegalExceptions()
                    .run("addPartitions", stats.getAddPartitions().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            int partitionsAdded = client.addPartitions(partitions);
                            if (partitionsAdded != partitions.size()) {
                                throw new PrestoException(HIVE_METASTORE_ERROR,
                                        format("Hive metastore only added %s of %s partitions", partitionsAdded, partitions.size()));
                            }
                            return null;
                        }
                    }));
        }
        catch (AlreadyExistsException e) {
            // todo partition already exists exception
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw Throwables.propagate(e);
        }
        finally {
            // todo do we need to invalidate all partitions?
            invalidatePartitionCache(databaseName, tableName);
        }
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("dropPartition", stats.getDropPartition().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            client.dropPartition(databaseName, tableName, parts, true);
                        }
                        return null;
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw Throwables.propagate(e);
        }
        finally {
            invalidatePartitionCache(databaseName, tableName);
        }
    }

    @Override
    public void dropPartitionByName(String databaseName, String tableName, String partitionName)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("dropPartitionByName", stats.getDropPartitionByName().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            // It is observed that: (examples below assumes a table with one partition column `ds`)
                            //  * When a partition doesn't exist (e.g. ds=2015-09-99), this thrift call is a no-op. It doesn't throw any exception.
                            //  * When a typo exists in partition column name (e.g. dxs=2015-09-01), this thrift call will delete ds=2015-09-01.
                            client.dropPartitionByName(databaseName, tableName, partitionName, true);
                        }
                        return null;
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw Throwables.propagate(e);
        }
        finally {
            invalidatePartitionCache(databaseName, tableName);
        }
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
    public Optional<Map<String, Partition>> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
    {
        Iterable<HivePartitionName> names = transform(partitionNames, name -> HivePartitionName.partition(databaseName, tableName, name));

        ImmutableMap.Builder<String, Partition> partitionsByName = ImmutableMap.builder();
        Map<HivePartitionName, Optional<Partition>> all = getAll(partitionCache, names);
        for (Entry<HivePartitionName, Optional<Partition>> entry : all.entrySet()) {
            if (!entry.getValue().isPresent()) {
                return Optional.empty();
            }
            partitionsByName.put(entry.getKey().getPartitionName(), entry.getValue().get());
        }
        return Optional.of(partitionsByName.build());
    }

    @Override
    public Optional<Partition> getPartition(String databaseName, String tableName, String partitionName)
    {
        HivePartitionName name = HivePartitionName.partition(databaseName, tableName, partitionName);
        return get(partitionCache, name);
    }

    private Optional<Partition> loadPartitionByName(HivePartitionName partitionName)
            throws Exception
    {
        requireNonNull(partitionName, "partitionName is null");
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartitionsByNames", stats.getGetPartitionByName().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            return Optional.of(client.getPartitionByName(
                                    partitionName.getHiveTableName().getDatabaseName(),
                                    partitionName.getHiveTableName().getTableName(),
                                    partitionName.getPartitionName()));
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
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

        List<String> partitionColumnNames = ImmutableList.copyOf(Warehouse.makeSpecFromName(firstPartition.getPartitionName()).keySet());

        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartitionsByNames", stats.getGetPartitionsByNames().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            ImmutableMap.Builder<HivePartitionName, Optional<Partition>> partitions = ImmutableMap.builder();
                            for (Partition partition : client.getPartitionsByNames(databaseName, tableName, partitionsToFetch)) {
                                String partitionId = FileUtils.makePartName(partitionColumnNames, partition.getValues(), null);
                                partitions.put(HivePartitionName.partition(databaseName, tableName, partitionId), Optional.of(partition));
                            }
                            return partitions.build();
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            // assume none of the partitions in the batch are available
            return stream(partitionNames.spliterator(), false)
                    .collect(toMap(identity(), (name) -> Optional.empty()));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Set<String> getRoles(String user)
    {
        return get(userRolesCache, user);
    }

    private Set<String> loadRoles(String user)
    {
        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
            List<Role> roles = client.listRoles(user, USER);
            if (roles == null) {
                return ImmutableSet.of();
            }
            return ImmutableSet.copyOf(roles.stream()
                    .map(Role::getRoleName)
                    .collect(toSet()));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Set<HivePrivilege> getDatabasePrivileges(String user, String databaseName)
    {
        ImmutableSet.Builder<HivePrivilege> privileges = ImmutableSet.builder();

        if (isDatabaseOwner(user, databaseName)) {
            privileges.add(OWNERSHIP);
        }
        privileges.addAll(getPrivileges(user, new HiveObjectRef(HiveObjectType.DATABASE, databaseName, null, null, null)));

        return privileges.build();
    }

    @Override
    public Set<HivePrivilege> getTablePrivileges(String user, String databaseName, String tableName)
    {
        return get(userTablePrivileges, new UserTableKey(user, tableName, databaseName));
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String grantee, Set<PrivilegeGrantInfo> privilegeGrantInfoSet)
    {
        try {
            retry()
                    .stopOnIllegalExceptions()
                    .run("grantTablePrivileges", stats.getGrantTablePrivileges().wrap(() -> {
                        try (HiveMetastoreClient metastoreClient = clientProvider.createMetastoreClient()) {
                            PrincipalType principalType;

                            if (metastoreClient.getRoleNames().contains(grantee)) {
                                principalType = ROLE;
                            }
                            else {
                                principalType = USER;
                            }

                            ImmutableList.Builder<HiveObjectPrivilege> privilegeBagBuilder = ImmutableList.builder();
                            for (PrivilegeGrantInfo privilegeGrantInfo : privilegeGrantInfoSet) {
                                privilegeBagBuilder.add(
                                        new HiveObjectPrivilege(new HiveObjectRef(HiveObjectType.TABLE, databaseName, tableName, null, null),
                                        grantee,
                                        principalType,
                                        privilegeGrantInfo));
                            }
                            // TODO: Check whether the user/role exists in the hive metastore.
                            // TODO: Check whether the user already has the given privilege.
                            metastoreClient.grantPrivileges(new PrivilegeBag(privilegeBagBuilder.build()));
                        }
                        return null;
                    }));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw Throwables.propagate(e);
        }
        finally {
            userTablePrivileges.invalidate(new UserTableKey(grantee, tableName, databaseName));
        }
    }

    @Override
    public boolean hasPrivilegeWithGrantOptionOnTable(String user, String databaseName, String tableName, HivePrivilege hivePrivilege)
    {
        try (HiveMetastoreClient metastoreClient = clientProvider.createMetastoreClient()) {
            PrincipalPrivilegeSet principalPrivilegeSet = metastoreClient.getPrivilegeSet(new HiveObjectRef(HiveObjectType.TABLE, databaseName, tableName, null, null), user, null);

            for (PrivilegeGrantInfo privilegeGrantInfo : principalPrivilegeSet.getUserPrivileges().get(user)) {
                if (privilegeGrantInfo.getPrivilege().equalsIgnoreCase(hivePrivilege.name()) && privilegeGrantInfo.isGrantOption()) {
                    return true;
                }
            }
            return false;
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    private Set<HivePrivilege> loadTablePrivileges(String user, String databaseName, String tableName)
    {
        ImmutableSet.Builder<HivePrivilege> privileges = ImmutableSet.builder();

        if (isTableOwner(user, databaseName, tableName)) {
            privileges.add(OWNERSHIP);
        }
        privileges.addAll(getPrivileges(user, new HiveObjectRef(HiveObjectType.TABLE, databaseName, tableName, null, null)));

        return privileges.build();
    }

    private Set<HivePrivilege> getPrivileges(String user, HiveObjectRef objectReference)
    {
        ImmutableSet.Builder<HivePrivilege> privileges = ImmutableSet.builder();
        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
            PrincipalPrivilegeSet privilegeSet = client.getPrivilegeSet(objectReference, user, null);

            if (privilegeSet != null) {
                Map<String, List<PrivilegeGrantInfo>> userPrivileges = privilegeSet.getUserPrivileges();
                if (userPrivileges != null) {
                    privileges.addAll(toGrants(userPrivileges.get(user)));
                }
                for (List<PrivilegeGrantInfo> rolePrivileges : privilegeSet.getRolePrivileges().values()) {
                    privileges.addAll(toGrants(rolePrivileges));
                }
                // We do not add the group permissions as Hive does not seem to process these
            }
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }

        return privileges.build();
    }

    private static Set<HivePrivilege> toGrants(List<PrivilegeGrantInfo> userGrants)
    {
        if (userGrants == null) {
            return ImmutableSet.of();
        }

        ImmutableSet.Builder<HivePrivilege> privileges = ImmutableSet.builder();
        for (PrivilegeGrantInfo userGrant : userGrants) {
            privileges.addAll(parsePrivilege(userGrant));
            if (userGrant.isGrantOption()) {
                privileges.add(HivePrivilege.GRANT);
            }
        }
        return privileges.build();
    }

    private RetryDriver retry()
    {
        return RetryDriver.retry()
                .exceptionMapper(getExceptionMapper())
                .stopOn(PrestoException.class);
    }

    protected Function<Exception, Exception> getExceptionMapper()
    {
        return identity();
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
        private final String partitionName;

        private HivePartitionName(HiveTableName hiveTableName, String partitionName)
        {
            this.hiveTableName = hiveTableName;
            this.partitionName = partitionName;
        }

        public static HivePartitionName partition(String databaseName, String tableName, String partitionName)
        {
            return new HivePartitionName(HiveTableName.table(databaseName, tableName), partitionName);
        }

        public HiveTableName getHiveTableName()
        {
            return hiveTableName;
        }

        public String getPartitionName()
        {
            return partitionName;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("hiveTableName", hiveTableName)
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
                    Objects.equals(partitionName, other.partitionName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(hiveTableName, partitionName);
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
        private final String user;
        private final String database;
        private final String table;

        public UserTableKey(String user, String table, String database)
        {
            this.user = requireNonNull(user, "principalName is null");
            this.table = requireNonNull(table, "table is null");
            this.database = requireNonNull(database, "database is null");
        }

        public String getUser()
        {
            return user;
        }

        public String getDatabase()
        {
            return database;
        }

        public String getTable()
        {
            return table;
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
            return Objects.equals(user, that.user) &&
                    Objects.equals(table, that.table) &&
                    Objects.equals(database, that.database);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(user, table, database);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("principalName", user)
                    .add("table", table)
                    .add("database", database)
                    .toString();
        }
    }
}
