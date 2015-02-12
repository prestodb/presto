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
package com.facebook.presto.cassandra;

import com.facebook.presto.spi.NotFoundException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.cassandra.RetryDriver.retry;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Cassandra Schema Cache
 */
@ThreadSafe
public class CachingCassandraSchemaProvider
{
    private final String connectorId;
    private final CassandraSession session;

    /**
     * Mapping from an empty string to all schema names.  Each schema name is a
     * mapping from the lower case schema name to the case sensitive schema name.
     * This mapping is necessary because Presto currently does not properly handle
     * case sensitive names.
     */
    private final LoadingCache<String, Map<String, String>> schemaNamesCache;

    /**
     * Mapping from lower case schema name to all tables in that schema.  Each
     * table name is a mapping from the lower case table name to the case
     * sensitive table name. This mapping is necessary because Presto currently
     * does not properly handle case sensitive names.
     */
    private final LoadingCache<String, Map<String, String>> tableNamesCache;

    private final LoadingCache<PartitionListKey, List<CassandraPartition>> partitionsCache;
    private final LoadingCache<PartitionListKey, List<CassandraPartition>> partitionsCacheFull;
    private final LoadingCache<SchemaTableName, CassandraTable> tableCache;

    @Inject
    public CachingCassandraSchemaProvider(
            CassandraConnectorId connectorId,
            CassandraSession session,
            @ForCassandra ExecutorService executor,
            CassandraClientConfig cassandraClientConfig)
    {
        this(checkNotNull(connectorId, "connectorId is null").toString(),
                session,
                executor,
                checkNotNull(cassandraClientConfig, "cassandraClientConfig is null").getSchemaCacheTtl(),
                cassandraClientConfig.getSchemaRefreshInterval());
    }

    public CachingCassandraSchemaProvider(String connectorId, CassandraSession session, ExecutorService executor, Duration cacheTtl, Duration refreshInterval)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.session = checkNotNull(session, "cassandraSession is null");

        checkNotNull(executor, "executor is null");

        long expiresAfterWriteMillis = checkNotNull(cacheTtl, "cacheTtl is null").toMillis();
        long refreshMills = checkNotNull(refreshInterval, "refreshInterval is null").toMillis();

        ListeningExecutorService listeningExecutor = listeningDecorator(executor);

        schemaNamesCache = CacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .refreshAfterWrite(refreshMills, MILLISECONDS)
                .build(new BackgroundCacheLoader<String, Map<String, String>>(listeningExecutor)
                {
                    @Override
                    public Map<String, String> load(String key)
                            throws Exception
                    {
                        return loadAllSchemas();
                    }
                });

        tableNamesCache = CacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .refreshAfterWrite(refreshMills, MILLISECONDS)
                .build(new BackgroundCacheLoader<String, Map<String, String>>(listeningExecutor)
                {
                    @Override
                    public Map<String, String> load(String databaseName)
                            throws Exception
                    {
                        return loadAllTables(databaseName);
                    }
                });

        tableCache = CacheBuilder
                .newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .refreshAfterWrite(refreshMills, MILLISECONDS)
                .build(new BackgroundCacheLoader<SchemaTableName, CassandraTable>(listeningExecutor)
                {
                    @Override
                    public CassandraTable load(SchemaTableName tableName)
                            throws Exception
                    {
                        return loadTable(tableName);
                    }
                });

        partitionsCache = CacheBuilder
                .newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .refreshAfterWrite(refreshMills, MILLISECONDS)
                .build(new BackgroundCacheLoader<PartitionListKey, List<CassandraPartition>>(listeningExecutor)
                {
                    @Override
                    public List<CassandraPartition> load(PartitionListKey key)
                            throws Exception
                    {
                        return loadPartitions(key);
                    }
                });

        partitionsCacheFull = CacheBuilder
                .newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .build(new BackgroundCacheLoader<PartitionListKey, List<CassandraPartition>>(listeningExecutor)
                {
                    @Override
                    public List<CassandraPartition> load(PartitionListKey key)
                            throws Exception
                    {
                        return loadPartitions(key);
                    }
                });
    }

    @Managed
    public void flushCache()
    {
        schemaNamesCache.invalidateAll();
        tableNamesCache.invalidateAll();
        partitionsCache.invalidateAll();
        tableCache.invalidateAll();
    }

    public List<String> getAllSchemas()
    {
        return ImmutableList.copyOf(getCacheValue(schemaNamesCache, "", RuntimeException.class).keySet());
    }

    private Map<String, String> loadAllSchemas()
            throws Exception
    {
        return retry()
                .stopOnIllegalExceptions()
                .run("getAllSchemas", () -> Maps.uniqueIndex(session.getAllSchemas(), CachingCassandraSchemaProvider::toLowerCase));
    }

    public List<String> getAllTables(String databaseName)
            throws SchemaNotFoundException
    {
        return ImmutableList.copyOf(getCacheValue(tableNamesCache, databaseName, SchemaNotFoundException.class).keySet());
    }

    private Map<String, String> loadAllTables(final String databaseName)
            throws Exception
    {
        return retry().stopOn(NotFoundException.class).stopOnIllegalExceptions()
                .run("getAllTables", () -> {
                    String caseSensitiveDatabaseName = getCaseSensitiveSchemaName(databaseName);
                    if (caseSensitiveDatabaseName == null) {
                        caseSensitiveDatabaseName = databaseName;
                    }
                    List<String> tables = session.getAllTables(caseSensitiveDatabaseName);
                    Map<String, String> nameMap = Maps.uniqueIndex(tables, CachingCassandraSchemaProvider::toLowerCase);

                    if (tables.isEmpty()) {
                        // Check to see if the database exists
                        session.getSchema(databaseName);
                    }
                    return nameMap;
                });
    }

    public CassandraTableHandle getTableHandle(SchemaTableName schemaTableName)
    {
        checkNotNull(schemaTableName, "schemaTableName is null");
        String schemaName = getCaseSensitiveSchemaName(schemaTableName.getSchemaName());
        String tableName = getCaseSensitiveTableName(schemaTableName);
        CassandraTableHandle tableHandle = new CassandraTableHandle(connectorId, schemaName, tableName);
        return tableHandle;
    }

    public String getCaseSensitiveSchemaName(String caseInsensitiveName)
    {
        String caseSensitiveSchemaName = getCacheValue(schemaNamesCache, "", RuntimeException.class).get(caseInsensitiveName.toLowerCase(ENGLISH));
        return caseSensitiveSchemaName == null ? caseInsensitiveName : caseSensitiveSchemaName;
    }

    public String getCaseSensitiveTableName(SchemaTableName schemaTableName)
    {
        String  caseSensitiveTableName = getCacheValue(tableNamesCache, schemaTableName.getSchemaName(), SchemaNotFoundException.class).get(schemaTableName.getTableName().toLowerCase(ENGLISH));
        return caseSensitiveTableName == null ? schemaTableName.getTableName() : caseSensitiveTableName;
    }

    public CassandraTable getTable(CassandraTableHandle tableHandle)
            throws TableNotFoundException
    {
        return getCacheValue(tableCache, tableHandle.getSchemaTableName(), TableNotFoundException.class);
    }

    public void flushTable(SchemaTableName tableName)
    {
        tableCache.asMap().remove(tableName);

        tableNamesCache.asMap().remove(tableName.getSchemaName());

        for (Iterator<PartitionListKey> iterator = partitionsCache.asMap().keySet().iterator(); iterator.hasNext(); ) {
            PartitionListKey partitionListKey = iterator.next();
            if (partitionListKey.getTable().getTableHandle().getSchemaTableName().equals(tableName)) {
                iterator.remove();
            }
        }
        for (Iterator<PartitionListKey> iterator = partitionsCacheFull.asMap().keySet().iterator(); iterator.hasNext(); ) {
            PartitionListKey partitionListKey = iterator.next();
            if (partitionListKey.getTable().getTableHandle().getSchemaTableName().equals(tableName)) {
                iterator.remove();
            }
        }
    }

    private CassandraTable loadTable(final SchemaTableName tableName)
            throws Exception
    {
        return retry()
                .stopOn(NotFoundException.class)
                .stopOnIllegalExceptions()
                .run("getTable", () -> session.getTable(tableName));
    }

    public List<CassandraPartition> getAllPartitions(CassandraTable table)
    {
        PartitionListKey key = new PartitionListKey(table, ImmutableList.<Comparable<?>>of());
        return getCacheValue(partitionsCache, key, RuntimeException.class);
    }

    public List<CassandraPartition> getPartitions(CassandraTable table, List<Comparable<?>> partitionKeys)
    {
        checkNotNull(table, "table is null");
        checkNotNull(partitionKeys, "partitionKeys is null");
        checkArgument(partitionKeys.size() == table.getPartitionKeyColumns().size());

        PartitionListKey key = new PartitionListKey(table, partitionKeys);
        return getCacheValue(partitionsCacheFull, key, RuntimeException.class);
    }

    private List<CassandraPartition> loadPartitions(final PartitionListKey key)
            throws Exception
    {
        return retry()
                .stopOnIllegalExceptions()
                .run("getPartitions", () -> session.getPartitions(key.getTable(), key.getFilterPrefix()));
    }

    private static <K, V, E extends Exception> V getCacheValue(LoadingCache<K, V> cache, K key, Class<E> exceptionClass)
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

    private static String toLowerCase(String value)
    {
        return value.toLowerCase(ENGLISH);
    }

    private static final class PartitionListKey
    {
        private final CassandraTable table;
        private final List<Comparable<?>> filterPrefix;

        PartitionListKey(CassandraTable table, List<Comparable<?>> filterPrefix)
        {
            this.table = table;
            this.filterPrefix = ImmutableList.copyOf(filterPrefix);
        }

        public List<Comparable<?>> getFilterPrefix()
        {
            return filterPrefix;
        }

        public CassandraTable getTable()
        {
            return table;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(table, filterPrefix);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            PartitionListKey other = (PartitionListKey) obj;
            return Objects.equals(this.table, other.table) &&
                    Objects.equals(this.filterPrefix, other.filterPrefix);
        }
    }
}
