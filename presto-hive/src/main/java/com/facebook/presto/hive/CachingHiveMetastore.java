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
package com.facebook.presto.hive;

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Function;
import com.google.common.base.Objects;
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
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.weakref.jmx.Managed;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.hive.RetryDriver.retry;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Hive Metastore Cache
 */
@ThreadSafe
public class CachingHiveMetastore
{
    private final HiveCluster clientProvider;
    private final LoadingCache<String, List<String>> databaseNamesCache;
    private final LoadingCache<String, List<String>> tableNamesCache;
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
    public void flushCache()
    {
        databaseNamesCache.invalidateAll();
        tableNamesCache.invalidateAll();
        partitionNamesCache.invalidateAll();
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

    public List<String> getAllDatabases()
    {
        return get(databaseNamesCache, "", RuntimeException.class);
    }

    private List<String> loadAllDatabases()
            throws Exception
    {
        return retry().stopOnIllegalExceptions().run("getAllDatabases", new Callable<List<String>>()
        {
            @Override
            public List<String> call()
                    throws Exception
            {
                try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                    return client.get_all_databases();
                }
            }
        });
    }

    public List<String> getAllTables(String databaseName)
            throws NoSuchObjectException
    {
        return get(tableNamesCache, databaseName, NoSuchObjectException.class);
    }

    private List<String> loadAllTables(final String databaseName)
            throws Exception
    {
        return retry().stopOn(NoSuchObjectException.class).stopOnIllegalExceptions().run("getAllTables", new Callable<List<String>>()
        {
            @Override
            public List<String> call()
                    throws Exception
            {
                try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                    List<String> tables = client.get_all_tables(databaseName);
                    if (tables.isEmpty()) {
                        // Check to see if the database exists
                        client.get_database(databaseName);
                    }
                    return tables;
                }
            }
        });
    }

    public Table getTable(String databaseName, String tableName)
            throws NoSuchObjectException
    {
        return get(tableCache, HiveTableName.table(databaseName, tableName), NoSuchObjectException.class);
    }

    private Table loadTable(final HiveTableName hiveTableName)
            throws Exception
    {
        return retry().stopOn(NoSuchObjectException.class, HiveViewNotSupportedException.class).stopOnIllegalExceptions().run("getTable", new Callable<Table>()
        {
            @Override
            public Table call()
                    throws Exception
            {
                try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                    Table table = client.get_table(hiveTableName.getDatabaseName(), hiveTableName.getTableName());
                    if (table.getTableType().equals(TableType.VIRTUAL_VIEW.toString())) {
                        throw new HiveViewNotSupportedException(new SchemaTableName(hiveTableName.getDatabaseName(), hiveTableName.getTableName()));
                    }
                    return table;
                }
            }
        });
    }

    public List<String> getPartitionNames(String databaseName, String tableName)
            throws NoSuchObjectException
    {
        return get(partitionNamesCache, HiveTableName.table(databaseName, tableName), NoSuchObjectException.class);
    }

    private List<String> loadPartitionNames(final HiveTableName hiveTableName)
            throws Exception
    {
        return retry().stopOn(NoSuchObjectException.class).stopOnIllegalExceptions().run("getPartitionNames", new Callable<List<String>>()
        {
            @Override
            public List<String> call()
                    throws Exception
            {
                try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                    return client.get_partition_names(hiveTableName.getDatabaseName(), hiveTableName.getTableName(), (short) 0);
                }
            }
        });
    }

    public List<String> getPartitionNamesByParts(String databaseName, String tableName, List<String> parts)
            throws NoSuchObjectException
    {
        return get(partitionFilterCache, PartitionFilter.partitionFilter(databaseName, tableName, parts), NoSuchObjectException.class);
    }

    private List<String> loadPartitionNamesByParts(final PartitionFilter partitionFilter)
            throws Exception
    {
        return retry().stopOn(NoSuchObjectException.class).stopOnIllegalExceptions().run("getPartitionNamesByParts", new Callable<List<String>>()
        {
            @Override
            public List<String> call()
                    throws Exception
            {
                try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                    return client.get_partition_names_ps(partitionFilter.getHiveTableName().getDatabaseName(),
                            partitionFilter.getHiveTableName().getTableName(),
                            partitionFilter.getParts(),
                            (short) -1);
                }
            }
        });
    }

    /**
     * Note: the returned partitions may not be in the same order as the specified partition names.
     */
    public List<Partition> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
            throws NoSuchObjectException
    {
        Iterable<HivePartitionName> names = transform(partitionNames, partitionNameCreator(databaseName, tableName));
        return ImmutableList.copyOf(getAll(partitionCache, names, NoSuchObjectException.class).values());
    }

    private Partition loadPartitionByName(final HivePartitionName partitionName)
            throws Exception
    {
        checkNotNull(partitionName, "partitionName is null");
        return retry().stopOn(NoSuchObjectException.class).stopOnIllegalExceptions().run("getPartitionsByNames", new Callable<Partition>()
        {
            @Override
            public Partition call()
                    throws Exception
            {
                try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                    return client.get_partition_by_name(partitionName.getHiveTableName().getDatabaseName(),
                            partitionName.getHiveTableName().getTableName(),
                            partitionName.getPartitionName());
                }
            }
        });
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

        return retry().stopOn(NoSuchObjectException.class).stopOnIllegalExceptions().run("getPartitionsByNames", new Callable<Map<HivePartitionName, Partition>>()
        {
            @Override
            public Map<HivePartitionName, Partition> call()
                    throws Exception
            {
                try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                    ImmutableMap.Builder<HivePartitionName, Partition> partitions = ImmutableMap.builder();
                    for (Partition partition : client.get_partitions_by_names(databaseName, tableName, partitionsToFetch)) {
                        String partitionId = FileUtils.makePartName(partitionColumnNames, partition.getValues(), null);
                        partitions.put(HivePartitionName.partition(databaseName, tableName, partitionId), partition);
                    }
                    return partitions.build();
                }
            }
        });
    }

    private static Function<String, HivePartitionName> partitionNameCreator(final String databaseName, final String tableName)
    {
        return new Function<String, HivePartitionName>()
        {
            @SuppressWarnings("ClassEscapesDefinedScope")
            @Override
            public HivePartitionName apply(String partitionName)
            {
                return HivePartitionName.partition(databaseName, tableName, partitionName);
            }
        };
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
            return Objects.toStringHelper(this)
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

            HiveTableName that = (HiveTableName) o;

            return Objects.equal(databaseName, that.databaseName) && Objects.equal(tableName, that.tableName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(databaseName, tableName);
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
            return Objects.toStringHelper(this)
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

            HivePartitionName that = (HivePartitionName) o;

            return Objects.equal(hiveTableName, that.hiveTableName) && Objects.equal(partitionName, that.partitionName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(hiveTableName, partitionName);
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
            return Objects.toStringHelper(this)
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

            PartitionFilter that = (PartitionFilter) o;

            return Objects.equal(hiveTableName, that.hiveTableName) && Objects.equal(parts, that.parts);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(hiveTableName, parts);
        }
    }
}
