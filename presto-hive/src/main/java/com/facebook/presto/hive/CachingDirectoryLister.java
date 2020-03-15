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

import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.util.HiveFileIterator.NestedDirectoryPolicy;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class CachingDirectoryLister
        implements DirectoryLister
{
    private final Cache<Path, List<HiveFileInfo>> cache;
    private final Set<SchemaTableName> cachedTableNames;

    protected final DirectoryLister delegate;

    @Inject
    public CachingDirectoryLister(@ForCachingDirectoryLister DirectoryLister delegate, HiveClientConfig hiveClientConfig)
    {
        this(
                delegate,
                hiveClientConfig.getFileStatusCacheExpireAfterWrite(),
                hiveClientConfig.getFileStatusCacheMaxSize(),
                hiveClientConfig.getFileStatusCacheTables().stream()
                        .map(CachingDirectoryLister::parseTableName)
                        .collect(Collectors.toSet()));
    }

    public CachingDirectoryLister(DirectoryLister delegate, Duration expireAfterWrite, long maxSize, Set<SchemaTableName> tables)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        cache = CacheBuilder.newBuilder()
                .maximumWeight(maxSize)
                .weigher((Weigher<Path, List<HiveFileInfo>>) (key, value) -> value.size())
                .expireAfterWrite(expireAfterWrite.toMillis(), TimeUnit.MILLISECONDS)
                .recordStats()
                .build();
        cachedTableNames = ImmutableSet.copyOf(requireNonNull(tables, "cachedTableNames is null"));
    }

    private static SchemaTableName parseTableName(String tableName)
    {
        String[] parts = tableName.split("\\.");
        checkArgument(parts.length == 2, "Invalid schemaTableName: %s", tableName);
        return new SchemaTableName(parts[0], parts[1]);
    }

    @Override
    public Iterator<HiveFileInfo> list(FileSystem fileSystem, Table table, Path path, NamenodeStats namenodeStats, NestedDirectoryPolicy nestedDirectoryPolicy, PathFilter pathFilter)
    {
        SchemaTableName schemaTableName = new SchemaTableName(table.getDatabaseName(), table.getTableName());

        List<HiveFileInfo> files = cache.getIfPresent(path);
        if (files != null) {
            return files.iterator();
        }

        Iterator<HiveFileInfo> iterator = delegate.list(fileSystem, table, path, namenodeStats, nestedDirectoryPolicy, pathFilter);
        if (cachedTableNames.contains(schemaTableName)) {
            return cachingIterator(iterator, path);
        }
        return iterator;
    }

    private Iterator<HiveFileInfo> cachingIterator(Iterator<HiveFileInfo> iterator, Path path)
    {
        return new Iterator<HiveFileInfo>()
        {
            private final List<HiveFileInfo> files = new ArrayList<>();

            @Override
            public boolean hasNext()
            {
                boolean hasNext = iterator.hasNext();
                if (!hasNext) {
                    cache.put(path, ImmutableList.copyOf(files));
                }
                return hasNext;
            }

            @Override
            public HiveFileInfo next()
            {
                HiveFileInfo next = iterator.next();
                files.add(next);
                return next;
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
}
