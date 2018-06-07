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
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CachedDirectoryLister
        implements DirectoryLister
{
    private final Cache<Path, List<LocatedFileStatus>> locatedFileStatusCache;
    private final Set<SchemaTableName> tableNames;

    @Inject
    public CachedDirectoryLister(HiveClientConfig hiveClientConfig)
    {
        this(hiveClientConfig.getFileStatusCacheExpireAfterWrite(), hiveClientConfig.getFileStatusCacheMaxSize(), hiveClientConfig.getCachedTables());
    }

    private CachedDirectoryLister(Duration expireAfterWrite, long maxSize, List<String> tables)
    {
        this.locatedFileStatusCache = CacheBuilder.newBuilder().maximumWeight(maxSize).weigher((Weigher<Path, List<LocatedFileStatus>>) (key, value) -> value.size()).expireAfterWrite(expireAfterWrite.toMillis(), TimeUnit.MILLISECONDS).recordStats().build();
        this.tableNames = tables == null ? ImmutableSet.of() : tables.stream().map(CachedDirectoryLister::parseTableName).collect(Collectors.toSet());
    }

    private static SchemaTableName parseTableName(String tableName)
    {
        String[] parts = tableName.split("\\.");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid schemaTableName " + tableName);
        }
        return new SchemaTableName(parts[0], parts[1]);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> list(FileSystem fs, Table table, Path path)
            throws IOException
    {
        List<LocatedFileStatus> files = locatedFileStatusCache.getIfPresent(path);
        if (files != null) {
            return new RemoteIterator<LocatedFileStatus>()
            {
                private final Iterator<LocatedFileStatus> iterator = ImmutableList.copyOf(files).iterator();

                @Override
                public boolean hasNext()
                        throws IOException
                {
                    return iterator.hasNext();
                }

                @Override
                public LocatedFileStatus next()
                        throws IOException
                {
                    return iterator.next();
                }
            };
        }
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listLocatedStatus(path);

        SchemaTableName schemaTableName = new SchemaTableName(table.getDatabaseName(), table.getTableName());
        if (tableNames.contains(schemaTableName)) {
            return new RemoteIterator<LocatedFileStatus>()
            {
                private final List<LocatedFileStatus> files = new ArrayList<>();

                @Override
                public boolean hasNext()
                        throws IOException
                {
                    boolean hasNext = locatedFileStatusRemoteIterator.hasNext();
                    if (!hasNext) {
                        locatedFileStatusCache.put(path, ImmutableList.copyOf(files));
                    }
                    return hasNext;
                }

                @Override
                public LocatedFileStatus next()
                        throws IOException
                {
                    LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
                    files.add(next);
                    return next;
                }
            };
        }
        return locatedFileStatusRemoteIterator;
    }

    @Managed
    public Double getHitRate()
    {
        return locatedFileStatusCache.stats().hitRate();
    }

    @Managed
    public Double getMissRate()
    {
        return locatedFileStatusCache.stats().missRate();
    }

    @Managed
    public long getHitCount()
    {
        return locatedFileStatusCache.stats().hitCount();
    }

    @Managed
    public long getRequestCount()
    {
        return locatedFileStatusCache.stats().requestCount();
    }
}
