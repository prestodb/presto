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

package com.facebook.presto.iceberg;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.iceberg.IcebergSessionProperties.isUseFileListCache;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class IcebergFileListCache
{
    private final Cache<String, List<FileScanTask>> cache;
    private final com.facebook.presto.iceberg.IcebergFileListCache.CachedTableChecker cachedTableChecker;

    @Inject
    public IcebergFileListCache(IcebergConfig icebergConfig)
    {
        this(
                icebergConfig.getFileStatusCacheExpireAfterWrite(),
                icebergConfig.getFileStatusCacheMaxSize(),
                icebergConfig.getFileStatusCacheTables());
    }

    public IcebergFileListCache(Duration expireAfterWrite, long maxSize, List<String> tables)
    {
        cache = CacheBuilder.newBuilder()
                .maximumWeight(maxSize)
                .weigher((Weigher<String, List<FileScanTask>>) (key, value) -> value.size())
                .expireAfterWrite(expireAfterWrite.toMillis(), TimeUnit.MILLISECONDS)
                .recordStats()
                .build();
        this.cachedTableChecker = new com.facebook.presto.iceberg.IcebergFileListCache.CachedTableChecker(requireNonNull(tables, "tables is null"));
    }

    public CloseableIterable<FileScanTask> planFiles(
            TableScan tableScan,
            IcebergTableHandle tableHandle,
            Expression expression,
            ConnectorSession session)
    {
        String key = tableHandle.getSchemaTableName().toString() + tableHandle.getSnapshotId().get() + expression;
        boolean cacheEnabled = isUseFileListCache(session);

        if (cacheEnabled) {
            // Use caching only when cache is enabled.
            // This is useful for debugging issues, when cache is explicitly disabled via session property.
            List<FileScanTask> files = cache.getIfPresent(key);
            if (files != null) {
                return CloseableIterable.withNoopClose(files);
            }
        }

        CloseableIterable<FileScanTask> iterable = tableScan.planFiles();
        if (cacheEnabled && cachedTableChecker.isCachedTable(tableHandle.getSchemaTableName())) {
            return cachingIterator(iterable, key);
        }
        return iterable;
    }

    private CloseableIterable<FileScanTask> cachingIterator(CloseableIterable<FileScanTask> iterable, String key)
    {
        return new CloseableIterable<FileScanTask>()
        {
            @Override
            public void close() throws IOException
            {
                iterable.close();
            }

            @Override
            public CloseableIterator<FileScanTask> iterator()
            {
                final List<FileScanTask> files = new ArrayList<>();
                return new CloseableIterator<FileScanTask>() {
                    private final CloseableIterator<FileScanTask> inner = iterable.iterator();

                    @Override
                    public void close() throws IOException
                    {
                        inner.close();
                    }

                    @Override
                    public boolean hasNext()
                    {
                        boolean hasNext = inner.hasNext();
                        if (!hasNext) {
                            cache.put(key, ImmutableList.copyOf(files));
                        }
                        return hasNext;
                    }

                    @Override
                    public FileScanTask next()
                    {
                        FileScanTask next = inner.next();
                        files.add(next);
                        return next;
                    }
                };
            }
        };
    }

    @Managed
    public void flushCache()
    {
        cache.invalidateAll();
    }

    @Managed
    public Double getHitRate()
    {
        return cache.stats().hitRate();
    }

    @Managed
    public Double getMissRate()
    {
        return cache.stats().missRate();
    }

    @Managed
    public long getHitCount()
    {
        return cache.stats().hitCount();
    }

    @Managed
    public long getMissCount()
    {
        return cache.stats().missCount();
    }

    @Managed
    public long getRequestCount()
    {
        return cache.stats().requestCount();
    }

    @Managed
    public long getSize()
    {
        return cache.size();
    }

    private static class CachedTableChecker
    {
        private final Set<SchemaTableName> cachedTableNames;
        private final boolean cacheAllTables;

        public CachedTableChecker(List<String> cachedTables)
        {
            cacheAllTables = cachedTables.contains("*");
            if (cacheAllTables) {
                checkArgument(cachedTables.size() == 1, "Only '*' is expected when caching all tables");
                this.cachedTableNames = ImmutableSet.of();
            }
            else {
                this.cachedTableNames = cachedTables.stream()
                        .map(SchemaTableName::valueOf)
                        .collect(toImmutableSet());
            }
        }

        public boolean isCachedTable(SchemaTableName schemaTableName)
        {
            return cacheAllTables || cachedTableNames.contains(schemaTableName);
        }
    }
}
