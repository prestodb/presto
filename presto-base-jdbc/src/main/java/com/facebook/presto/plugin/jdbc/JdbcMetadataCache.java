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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class JdbcMetadataCache
{
    private final JdbcClient jdbcClient;
    private final JdbcMetadataCache delegate;

    private final LoadingCache<KeyAndSession<SchemaTableName>, Optional<JdbcTableHandle>> tableHandleCache;
    private final LoadingCache<KeyAndSession<JdbcTableHandle>, List<JdbcColumnHandle>> columnHandlesCache;

    @Inject
    public JdbcMetadataCache(JdbcClient jdbcClient, JdbcMetadataConfig config, JdbcMetadataCacheStats stats)
    {
        this(
                newCachedThreadPool(daemonThreadsNamed("jdbc-metadata-cache" + "-%s")),
                jdbcClient,
                stats,
                OptionalLong.of(config.getMetadataCacheTtl().toMillis()),
                config.getMetadataCacheRefreshInterval().toMillis() >= config.getMetadataCacheTtl().toMillis() ? OptionalLong.empty() : OptionalLong.of(config.getMetadataCacheRefreshInterval().toMillis()),
                config.getMetadataCacheMaximumSize(),
                null);
    }

    public JdbcMetadataCache(
            ExecutorService executor,
            JdbcClient jdbcClient,
            JdbcMetadataCacheStats stats,
            OptionalLong cacheTtl,
            OptionalLong refreshInterval,
            long cacheMaximumSize)
    {
        this(
                executor,
                jdbcClient,
                stats,
                cacheTtl,
                refreshInterval,
                cacheMaximumSize,
                null);
    }

    private JdbcMetadataCache(
            ExecutorService executor,
            JdbcClient jdbcClient,
            JdbcMetadataCacheStats stats,
            OptionalLong cacheTtl,
            OptionalLong refreshInterval,
            long cacheMaximumSize,
            @Nullable JdbcMetadataCache delegate)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        this.delegate = delegate;

        this.tableHandleCache = newCacheBuilder(cacheTtl, refreshInterval, cacheMaximumSize)
                .build(asyncReloading(CacheLoader.from(this::loadTableHandle), executor));
        stats.setTableHandleCache(tableHandleCache);

        this.columnHandlesCache = newCacheBuilder(cacheTtl, refreshInterval, cacheMaximumSize)
                .build(asyncReloading(CacheLoader.from(this::loadColumnHandles), executor));
        stats.setColumnHandlesCache(columnHandlesCache);
    }

    public static JdbcMetadataCache createTransactionCache(JdbcMetadataCache delegate, long maximumSize)
    {
        return createTransactionCache(delegate, new JdbcMetadataCacheStats(), maximumSize);
    }

    static JdbcMetadataCache createTransactionCache(JdbcMetadataCache delegate, JdbcMetadataCacheStats stats, long maximumSize)
    {
        requireNonNull(delegate, "delegate is null");
        return new JdbcMetadataCache(
                newDirectExecutorService(),
                delegate.jdbcClient,
                stats,
                OptionalLong.empty(),
                OptionalLong.empty(),
                maximumSize,
                delegate);
    }

    public JdbcTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        KeyAndSession<SchemaTableName> key = new KeyAndSession<>(session, tableName);
        Optional<JdbcTableHandle> tableHandle = get(tableHandleCache, key);
        if (!tableHandle.isPresent()) {
            tableHandleCache.invalidate(key);
        }
        return tableHandle.orElse(null);
    }

    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle jdbcTableHandle)
    {
        return get(columnHandlesCache, new KeyAndSession<>(session, jdbcTableHandle));
    }

    public void invalidateTable(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        tableHandleCache.invalidate(new KeyAndSession<>(session, tableHandle.getSchemaTableName()));
        columnHandlesCache.invalidate(new KeyAndSession<>(session, tableHandle));
        if (delegate != null) {
            delegate.invalidateTable(session, tableHandle);
        }
    }

    public void invalidateTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        tableHandleCache.invalidate(new KeyAndSession<>(session, tableName));
        if (delegate != null) {
            delegate.invalidateTableHandle(session, tableName);
        }
    }

    private Optional<JdbcTableHandle> loadTableHandle(KeyAndSession<SchemaTableName> tableName)
    {
        // The returned tableHandle can be null if it does not contain the table
        if (delegate != null) {
            return Optional.ofNullable(delegate.getTableHandle(tableName.getSession(), tableName.getKey()));
        }
        return Optional.ofNullable(jdbcClient.getTableHandle(tableName.getSession(), JdbcIdentity.from(tableName.getSession()), tableName.getKey()));
    }

    private List<JdbcColumnHandle> loadColumnHandles(KeyAndSession<JdbcTableHandle> tableHandle)
    {
        if (delegate != null) {
            return delegate.getColumns(tableHandle.getSession(), tableHandle.getKey());
        }
        return jdbcClient.getColumns(tableHandle.getSession(), tableHandle.getKey());
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
        return cacheBuilder.maximumSize(maximumSize).recordStats();
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

    private static class KeyAndSession<T>
    {
        private final ConnectorSession session;
        private final T key;

        public KeyAndSession(ConnectorSession session, T key)
        {
            this.session = requireNonNull(session, "session is null");
            this.key = requireNonNull(key, "key is null");
        }

        public ConnectorSession getSession()
        {
            return session;
        }

        public T getKey()
        {
            return key;
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
            KeyAndSession<?> other = (KeyAndSession<?>) o;
            return Objects.equals(key, other.key);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(key);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("session", session)
                    .add("key", key)
                    .toString();
        }
    }
}
