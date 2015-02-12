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
import com.facebook.presto.hive.HiveMetastoreClient;
import com.facebook.presto.hive.HiveViewNotSupportedException;
import com.facebook.presto.hive.TableAlreadyExistsException;
import com.facebook.presto.hive.util.BackgroundCacheLoader;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.units.Duration;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static com.facebook.presto.hive.HiveUtil.PRESTO_VIEW_FLAG;
import static com.facebook.presto.hive.HiveUtil.isPrestoView;
import static com.facebook.presto.hive.RetryDriver.retry;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
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
    private final LoadingCache<String, Database> databaseCache;
    private final LoadingCache<String, List<String>> tableNamesCache;
    private final LoadingCache<String, List<String>> viewNamesCache;
    private final LoadingCache<HiveTableName, List<String>> partitionNamesCache;
    private final LoadingCache<HiveTableName, Table> tableCache;
    private final LoadingCache<HivePartitionName, Partition> partitionCache;
    private final LoadingCache<PartitionFilter, List<String>> partitionFilterCache;

    @Inject
    public CachingHiveMetastore(HiveCluster hiveCluster, @ForHiveMetastore ExecutorService executor, HiveClientConfig hiveClientConfig)
    {
        this(checkNotNull(hiveCluster, "hiveCluster is null"),
                checkNotNull(executor, "executor is null"),
                checkNotNull(hiveClientConfig, "hiveClientConfig is null").getMetastoreCacheTtl(),
                hiveClientConfig.getMetastoreRefreshInterval());
    }

    public CachingHiveMetastore(HiveCluster hiveCluster, ExecutorService executor, Duration cacheTtl, Duration refreshInterval)
    {
        this.clientProvider = checkNotNull(hiveCluster, "hiveCluster is null");

        long expiresAfterWriteMillis = checkNotNull(cacheTtl, "cacheTtl is null").toMillis();
        long refreshMills = checkNotNull(refreshInterval, "refreshInterval is null").toMillis();

        ListeningExecutorService listeningExecutor = MoreExecutors.listeningDecorator(executor);

        databaseNamesCache = CacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .refreshAfterWrite(refreshMills, MILLISECONDS)
                .build(new BackgroundCacheLoader<String, List<String>>(listeningExecutor)
                {
                    @Override
                    public List<String> load(String key)
                            throws Exception
                    {
                        return loadAllDatabases();
                    }
                });

        databaseCache = CacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .refreshAfterWrite(refreshMills, MILLISECONDS)
                .build(new BackgroundCacheLoader<String, Database>(listeningExecutor)
                {
                    @Override
                    public Database load(String databaseName)
                            throws Exception
                    {
                        return loadDatabase(databaseName);
                    }
                });

        tableNamesCache = CacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .refreshAfterWrite(refreshMills, MILLISECONDS)
                .build(new BackgroundCacheLoader<String, List<String>>(listeningExecutor)
                {
                    @Override
                    public List<String> load(String databaseName)
                            throws Exception
                    {
                        return loadAllTables(databaseName);
                    }
                });

        tableCache = CacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .refreshAfterWrite(refreshMills, MILLISECONDS)
                .build(new BackgroundCacheLoader<HiveTableName, Table>(listeningExecutor)
                {
                    @Override
                    public Table load(HiveTableName hiveTableName)
                            throws Exception
                    {
                        return loadTable(hiveTableName);
                    }
                });

        viewNamesCache = CacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .refreshAfterWrite(refreshMills, MILLISECONDS)
                .build(new BackgroundCacheLoader<String, List<String>>(listeningExecutor)
                {
                    @Override
                    public List<String> load(String databaseName)
                            throws Exception
                    {
                        return loadAllViews(databaseName);
                    }
                });

        partitionNamesCache = CacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .refreshAfterWrite(refreshMills, MILLISECONDS)
                .build(new BackgroundCacheLoader<HiveTableName, List<String>>(listeningExecutor)
                {
                    @Override
                    public List<String> load(HiveTableName hiveTableName)
                            throws Exception
                    {
                        return loadPartitionNames(hiveTableName);
                    }
                });

        partitionFilterCache = CacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .refreshAfterWrite(refreshMills, MILLISECONDS)
                .build(new BackgroundCacheLoader<PartitionFilter, List<String>>(listeningExecutor)
                {
                    @Override
                    public List<String> load(PartitionFilter partitionFilter)
                            throws Exception
                    {
                        return loadPartitionNamesByParts(partitionFilter);
                    }
                });

        partitionCache = CacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .refreshAfterWrite(refreshMills, MILLISECONDS)
                .build(new BackgroundCacheLoader<HivePartitionName, Partition>(listeningExecutor)
                {
                    @Override
                    public Partition load(HivePartitionName partitionName)
                            throws Exception
                    {
                        return loadPartitionByName(partitionName);
                    }

                    @Override
                    public Map<HivePartitionName, Partition> loadAll(Iterable<? extends HivePartitionName> partitionNames)
                            throws Exception
                    {
                        return loadPartitionsByNames(partitionNames);
                    }
                });
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
    }

    private static <K, V, E extends Exception> V get(LoadingCache<K, V> cache, K key, Class<E> exceptionClass)
            throws E
    {
        try {
            return cache.get(key);
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            Throwable t = e.getCause();
            Throwables.propagateIfInstanceOf(t, exceptionClass);
            throw Throwables.propagate(t);
        }
    }

    private static <K, V, E extends Exception> Map<K, V> getAll(LoadingCache<K, V> cache, Iterable<K> keys, Class<E> exceptionClass)
            throws E
    {
        try {
            return cache.getAll(keys);
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            Throwable t = e.getCause();
            Throwables.propagateIfInstanceOf(t, exceptionClass);
            throw Throwables.propagate(t);
        }
    }

    @Override
    public List<String> getAllDatabases()
    {
        return get(databaseNamesCache, "", RuntimeException.class);
    }

    private List<String> loadAllDatabases()
            throws Exception
    {
        try {
            return retry()
                    .stopOnIllegalExceptions()
                    .run("getAllDatabases", stats.getGetAllDatabases().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            return client.get_all_databases();
                        }
                    }));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Database getDatabase(String databaseName)
            throws NoSuchObjectException
    {
        return get(databaseCache, databaseName, NoSuchObjectException.class);
    }

    private Database loadDatabase(final String databaseName)
            throws Exception
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getDatabase", stats.getGetDatabase().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            return client.get_database(databaseName);
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            throw e;
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public List<String> getAllTables(String databaseName)
            throws NoSuchObjectException
    {
        return get(tableNamesCache, databaseName, NoSuchObjectException.class);
    }

    private List<String> loadAllTables(final String databaseName)
            throws Exception
    {
        final Callable<List<String>> getAllTables = stats.getGetAllTables().wrap(() -> {
            try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                return client.get_all_tables(databaseName);
            }
        });

        final Callable<Void> getDatabase = stats.getGetDatabase().wrap(() -> {
            try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                client.get_database(databaseName);
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
                        return tables;
                    });
        }
        catch (NoSuchObjectException e) {
            throw e;
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Table getTable(String databaseName, String tableName)
            throws NoSuchObjectException
    {
        return get(tableCache, HiveTableName.table(databaseName, tableName), NoSuchObjectException.class);
    }

    @Override
    public List<String> getAllViews(String databaseName)
            throws NoSuchObjectException
    {
        return get(viewNamesCache, databaseName, NoSuchObjectException.class);
    }

    private List<String> loadAllViews(final String databaseName)
            throws Exception
    {
        try {
            return retry()
                    .stopOn(UnknownDBException.class)
                    .stopOnIllegalExceptions()
                    .run("getAllViews", stats.getAllViews().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            String filter = HIVE_FILTER_FIELD_PARAMS + PRESTO_VIEW_FLAG + " = \"true\"";
                            return client.get_table_names_by_filter(databaseName, filter, (short) -1);
                        }
                    }));
        }
        catch (UnknownDBException e) {
            throw new NoSuchObjectException(e.getMessage());
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void createTable(final Table table)
    {
        try {
            retry()
                    .stopOn(AlreadyExistsException.class, InvalidObjectException.class, MetaException.class, NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("createTable", stats.getCreateTable().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            client.create_table(table);
                        }
                        tableNamesCache.invalidate(table.getDbName());
                        viewNamesCache.invalidate(table.getDbName());
                        return null;
                    }));
        }
        catch (AlreadyExistsException e) {
            throw new TableAlreadyExistsException(new SchemaTableName(table.getDbName(), table.getTableName()));
        }
        catch (InvalidObjectException | NoSuchObjectException | MetaException e) {
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
    }

    @Override
    public void dropTable(final String databaseName, final String tableName)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("dropTable", stats.getDropTable().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            client.drop_table(databaseName, tableName, true);
                        }
                        tableCache.invalidate(new HiveTableName(databaseName, tableName));
                        tableNamesCache.invalidate(databaseName);
                        viewNamesCache.invalidate(databaseName);
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
    }

    @Override
    public void renameTable(final String databaseName, final String tableName, final String newDatabaseName, final String newTableName)
    {
        try {
            retry()
                    .stopOn(InvalidOperationException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("renameTable", stats.getRenameTable().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            Table table = new Table(loadTable(new HiveTableName(databaseName, tableName)));
                            table.setDbName(newDatabaseName);
                            table.setTableName(newTableName);
                            client.alter_table(databaseName, tableName, table);
                        }
                        tableCache.invalidate(new HiveTableName(databaseName, tableName));
                        tableNamesCache.invalidate(databaseName);
                        viewNamesCache.invalidate(databaseName);
                        return null;
                    }));
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
    }

    private Table loadTable(final HiveTableName hiveTableName)
            throws Exception
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class, HiveViewNotSupportedException.class)
                    .stopOnIllegalExceptions()
                    .run("getTable", stats.getGetTable().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            Table table = client.get_table(hiveTableName.getDatabaseName(), hiveTableName.getTableName());
                            if (table.getTableType().equals(TableType.VIRTUAL_VIEW.name()) && (!isPrestoView(table))) {
                                throw new HiveViewNotSupportedException(new SchemaTableName(hiveTableName.getDatabaseName(), hiveTableName.getTableName()));
                            }
                            return table;
                        }
                    }));
        }
        catch (NoSuchObjectException | HiveViewNotSupportedException e) {
            throw e;
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public List<String> getPartitionNames(String databaseName, String tableName)
            throws NoSuchObjectException
    {
        return get(partitionNamesCache, HiveTableName.table(databaseName, tableName), NoSuchObjectException.class);
    }

    private List<String> loadPartitionNames(final HiveTableName hiveTableName)
            throws Exception
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartitionNames", stats.getGetPartitionNames().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            return client.get_partition_names(hiveTableName.getDatabaseName(), hiveTableName.getTableName(), (short) 0);
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            throw e;
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public List<String> getPartitionNamesByParts(String databaseName, String tableName, List<String> parts)
            throws NoSuchObjectException
    {
        return get(partitionFilterCache, PartitionFilter.partitionFilter(databaseName, tableName, parts), NoSuchObjectException.class);
    }

    private List<String> loadPartitionNamesByParts(final PartitionFilter partitionFilter)
            throws Exception
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartitionNamesByParts", stats.getGetPartitionNamesPs().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            return client.get_partition_names_ps(partitionFilter.getHiveTableName().getDatabaseName(),
                                    partitionFilter.getHiveTableName().getTableName(),
                                    partitionFilter.getParts(),
                                    (short) -1);
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            throw e;
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    public Map<String, Partition> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
            throws NoSuchObjectException
    {
        Iterable<HivePartitionName> names = transform(partitionNames, name -> HivePartitionName.partition(databaseName, tableName, name));

        ImmutableMap.Builder<String, Partition> partitionsByName = ImmutableMap.builder();
        Map<HivePartitionName, Partition> all = getAll(partitionCache, names, NoSuchObjectException.class);
        for (Entry<HivePartitionName, Partition> entry : all.entrySet()) {
            partitionsByName.put(entry.getKey().getPartitionName(), entry.getValue());
        }
        return partitionsByName.build();
    }

    private Partition loadPartitionByName(final HivePartitionName partitionName)
            throws Exception
    {
        checkNotNull(partitionName, "partitionName is null");
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartitionsByNames", stats.getGetPartitionByName().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            return client.get_partition_by_name(partitionName.getHiveTableName().getDatabaseName(),
                                    partitionName.getHiveTableName().getTableName(),
                                    partitionName.getPartitionName());
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            throw e;
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    private Map<HivePartitionName, Partition> loadPartitionsByNames(Iterable<? extends HivePartitionName> partitionNames)
            throws Exception
    {
        checkNotNull(partitionNames, "partitionNames is null");
        checkArgument(!Iterables.isEmpty(partitionNames), "partitionNames is empty");

        HivePartitionName firstPartition = Iterables.get(partitionNames, 0);

        HiveTableName hiveTableName = firstPartition.getHiveTableName();
        final String databaseName = hiveTableName.getDatabaseName();
        final String tableName = hiveTableName.getTableName();

        final List<String> partitionsToFetch = new ArrayList<>();
        for (HivePartitionName partitionName : partitionNames) {
            checkArgument(partitionName.getHiveTableName().equals(hiveTableName), "Expected table name %s but got %s", hiveTableName, partitionName.getHiveTableName());
            partitionsToFetch.add(partitionName.getPartitionName());
        }

        final List<String> partitionColumnNames = ImmutableList.copyOf(Warehouse.makeSpecFromName(firstPartition.getPartitionName()).keySet());

        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartitionsByNames", stats.getGetPartitionsByNames().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            ImmutableMap.Builder<HivePartitionName, Partition> partitions = ImmutableMap.builder();
                            for (Partition partition : client.get_partitions_by_names(databaseName, tableName, partitionsToFetch)) {
                                String partitionId = FileUtils.makePartName(partitionColumnNames, partition.getValues(), null);
                                partitions.put(HivePartitionName.partition(databaseName, tableName, partitionId), partition);
                            }
                            return partitions.build();
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            throw e;
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
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
}
