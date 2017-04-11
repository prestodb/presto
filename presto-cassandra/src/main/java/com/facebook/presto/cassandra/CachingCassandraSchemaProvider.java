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
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.cassandra.RetryDriver.retry;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
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
    private final LoadingCache<SchemaTableName, CassandraTable> tableCache;

    @Inject
    public CachingCassandraSchemaProvider(
            CassandraConnectorId connectorId,
            CassandraSession session,
            @ForCassandra ExecutorService executor,
            CassandraClientConfig cassandraClientConfig)
    {
        this(requireNonNull(connectorId, "connectorId is null").toString(),
                session,
                executor,
                requireNonNull(cassandraClientConfig, "cassandraClientConfig is null").getSchemaCacheTtl(),
                cassandraClientConfig.getSchemaRefreshInterval());
    }

    public CachingCassandraSchemaProvider(String connectorId, CassandraSession session, ExecutorService executor, Duration cacheTtl, Duration refreshInterval)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.session = requireNonNull(session, "cassandraSession is null");

        requireNonNull(executor, "executor is null");

        long expiresAfterWriteMillis = requireNonNull(cacheTtl, "cacheTtl is null").toMillis();
        long refreshMills = requireNonNull(refreshInterval, "refreshInterval is null").toMillis();

        schemaNamesCache = CacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .refreshAfterWrite(refreshMills, MILLISECONDS)
                .build(asyncReloading(new CacheLoader<String, Map<String, String>>()
                {
                    @Override
                    public Map<String, String> load(String key)
                            throws Exception
                    {
                        return loadAllSchemas();
                    }
                }, executor));

        tableNamesCache = CacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .refreshAfterWrite(refreshMills, MILLISECONDS)
                .build(asyncReloading(new CacheLoader<String, Map<String, String>>()
                {
                    @Override
                    public Map<String, String> load(String databaseName)
                            throws Exception
                    {
                        return loadAllTables(databaseName);
                    }
                }, executor));

        tableCache = CacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .refreshAfterWrite(refreshMills, MILLISECONDS)
                .build(asyncReloading(new CacheLoader<SchemaTableName, CassandraTable>()
                {
                    @Override
                    public CassandraTable load(SchemaTableName tableName)
                            throws Exception
                    {
                        return loadTable(tableName);
                    }
                }, executor));
    }

    @Managed
    public void flushCache()
    {
        schemaNamesCache.invalidateAll();
        tableNamesCache.invalidateAll();
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
        requireNonNull(schemaTableName, "schemaTableName is null");
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
        String caseSensitiveTableName = getCacheValue(tableNamesCache, schemaTableName.getSchemaName(), SchemaNotFoundException.class).get(schemaTableName.getTableName().toLowerCase(ENGLISH));
        return caseSensitiveTableName == null ? schemaTableName.getTableName() : caseSensitiveTableName;
    }

    public CassandraTable getTable(CassandraTableHandle tableHandle)
            throws TableNotFoundException
    {
        return getCacheValue(tableCache, tableHandle.getSchemaTableName(), TableNotFoundException.class);
    }

    public void flushTable(SchemaTableName tableName)
    {
        tableCache.invalidate(tableName);
        tableNamesCache.invalidate(tableName.getSchemaName());
        schemaNamesCache.invalidateAll();
    }

    private CassandraTable loadTable(final SchemaTableName tableName)
            throws Exception
    {
        return retry()
                .stopOn(NotFoundException.class)
                .stopOnIllegalExceptions()
                .run("getTable", () -> session.getTable(tableName));
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
}
