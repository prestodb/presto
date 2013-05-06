package com.facebook.presto.hive;

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.units.Duration;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static com.facebook.presto.hive.HivePartition.UNPARTITIONED_ID;
import static com.facebook.presto.hive.RetryDriver.retry;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Hive Metastore Cache
 */
@ThreadSafe
public class CachingHiveMetastore
{
    private final HiveCluster clientProvider;
    private final Cache<String, List<String>> databaseNamesCache;
    private final Cache<String, List<String>> tableNamesCache;
    private final Cache<HiveTableName, List<String>> partitionNamesCache;
    private final Cache<HiveTableName, Table> tableCache;
    private final Cache<HivePartitionName, Partition> partitionCache;
    private final Cache<PartitionFilter, List<String>> partitionFilterCache;

    @Inject
    public CachingHiveMetastore(HiveCluster hiveCluster, HiveClientConfig hiveClientConfig)
    {
        this(checkNotNull(hiveCluster, "hiveCluster is null"),
                checkNotNull(hiveClientConfig, "hiveClientConfig is null").getMetastoreCacheTtl());
    }

    public CachingHiveMetastore(HiveCluster hiveCluster, Duration cacheTtl)
    {
        this.clientProvider = checkNotNull(hiveCluster, "hiveCluster is null");

        long expiresAfterWriteMillis = (long) checkNotNull(cacheTtl, "cacheTtl is null").toMillis();

        databaseNamesCache = CacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .build();
        tableNamesCache = CacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .build();
        partitionNamesCache = CacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .build();
        tableCache = CacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .build();
        partitionCache = CacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .build();
        partitionFilterCache = CacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .build();
    }

    // TODO: make this flushable via JMX
    public void flushCache()
    {
        databaseNamesCache.invalidateAll();
        tableNamesCache.invalidateAll();
        partitionNamesCache.invalidateAll();
        tableCache.invalidateAll();
        partitionCache.invalidateAll();
        partitionFilterCache.invalidateAll();
    }

    private static <K, V, E extends Exception> V getWithCallable(Cache<K, V> cache, K key, Callable<V> loader, Class<E> exceptionClass)
            throws E
    {
        try {
            return cache.get(key, loader);
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            Throwable t = e.getCause();
            Throwables.propagateIfInstanceOf(t, exceptionClass);
            throw Throwables.propagate(t);
        }
    }

    public List<String> getAllDatabases()
    {
        return getWithCallable(databaseNamesCache, "", new Callable<List<String>>()
        {
            @Override
            public List<String> call()
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
        }, RuntimeException.class);
    }

    public List<String> getAllTables(final String databaseName)
            throws NoSuchObjectException
    {
        return getWithCallable(tableNamesCache, databaseName, new Callable<List<String>>()
        {
            @Override
            public List<String> call()
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
        }, NoSuchObjectException.class);
    }

    public Table getTable(final String databaseName, final String tableName)
            throws NoSuchObjectException
    {
        return getWithCallable(tableCache, HiveTableName.table(databaseName, tableName), new Callable<Table>()
        {
            @Override
            public Table call()
                    throws Exception
            {
                return retry().stopOn(NoSuchObjectException.class).stopOnIllegalExceptions().run("getTable", new Callable<Table>()
                {
                    @Override
                    public Table call()
                            throws Exception
                    {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            Table table = client.get_table(databaseName, tableName);
                            if (table.getTableType().equals(TableType.VIRTUAL_VIEW.toString())) {
                                throw new TableNotFoundException(new SchemaTableName(databaseName, tableName), HiveClient.HIVE_VIEWS_NOT_SUPPORTED);
                            }
                            return table;
                        }
                    }
                });
            }
        }, NoSuchObjectException.class);
    }

    public List<String> getPartitionNames(final String databaseName, final String tableName)
            throws NoSuchObjectException
    {
        return getWithCallable(partitionNamesCache, HiveTableName.table(databaseName, tableName), new Callable<List<String>>()
        {
            @Override
            public List<String> call()
                    throws Exception
            {
                return retry().stopOn(NoSuchObjectException.class).stopOnIllegalExceptions().run("getPartitionNames", new Callable<List<String>>()
                {
                    @Override
                    public List<String> call()
                            throws Exception
                    {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            List<String> partitionNames = client.get_partition_names(databaseName, tableName, (short) 0);
                            if (partitionNames.isEmpty()) {
                                // Check if the table exists
                                getTable(databaseName, tableName);
                                return ImmutableList.of(UNPARTITIONED_ID);
                            }
                            return partitionNames;
                        }
                    }
                });
            }
        }, NoSuchObjectException.class);
    }

    public List<String> getPartitionNamesByParts(final String databaseName, final String tableName, final List<String> parts)
            throws NoSuchObjectException
    {
        return getWithCallable(partitionFilterCache, PartitionFilter.partitionFilter(databaseName, tableName, parts), new Callable<List<String>>()
        {
            @Override
            public List<String> call()
                    throws Exception
            {
                return retry().stopOn(NoSuchObjectException.class).stopOnIllegalExceptions().run("getPartitionNamesByParts", new Callable<List<String>>()
                {
                    @Override
                    public List<String> call()
                            throws Exception
                    {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            return client.get_partition_names_ps(databaseName, tableName, parts, (short) -1);
                        }
                    }
                });
            }
        }, NoSuchObjectException.class);
    }

    public List<Partition> getPartitionsByNames(final String databaseName, final String tableName, List<String> partitionNames)
            throws NoSuchObjectException
    {
        // Pre-populate some results with already cached partitions
        final List<String> partitionsToFetch = new ArrayList<>();
        Partition[] partitions = new Partition[partitionNames.size()];
        for (int i = 0; i < partitionNames.size(); i++) {
            String partitionName = partitionNames.get(i);
            Partition partition = partitionCache.getIfPresent(HivePartitionName.partition(databaseName, tableName, partitionName));
            if (partition == null) {
                partitionsToFetch.add(partitionName);
            }
            else {
                partitions[i] = partition;
            }
        }

        if (!partitionsToFetch.isEmpty()) {
            List<Partition> fetchedPartitions;
            try {
                fetchedPartitions = retry()
                        .stopOn(NoSuchObjectException.class)
                        .stopOnIllegalExceptions().run("getPartitionsByNames", new Callable<List<Partition>>()
                        {
                            @Override
                            public List<Partition> call()
                                    throws Exception
                            {
                                try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                                    return client.get_partitions_by_names(databaseName, tableName, partitionsToFetch);
                                }
                            }
                        });
            }
            catch (Exception e) {
                Throwables.propagateIfInstanceOf(e, NoSuchObjectException.class);
                throw Throwables.propagate(e);
            }

            // Cache the results
            checkState(fetchedPartitions.size() == partitionsToFetch.size());
            for (int i = 0; i < fetchedPartitions.size(); i++) {
                partitionCache.put(HivePartitionName.partition(databaseName, tableName, partitionsToFetch.get(i)), fetchedPartitions.get(i));
            }

            // Merge the results
            Iterator<Partition> fetchedPartitionsIterator = fetchedPartitions.iterator();
            for (int i = 0; i < partitionNames.size(); i++) {
                if (partitions[i] == null) {
                    checkState(fetchedPartitionsIterator.hasNext(), "iterator should always have next");
                    partitions[i] = fetchedPartitionsIterator.next();
                }
            }
            checkState(!fetchedPartitionsIterator.hasNext(), "iterator not have any more elements");
        }

        return ImmutableList.copyOf(partitions);
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
