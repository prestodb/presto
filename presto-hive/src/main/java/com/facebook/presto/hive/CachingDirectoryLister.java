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
import com.facebook.presto.hive.util.HiveFileIterator;
import com.facebook.presto.spi.PrestoException;
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
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILE_NOT_FOUND;
import static com.facebook.presto.hive.HiveFileInfo.createHiveFileInfo;
import static com.facebook.presto.hive.util.HiveFileIterator.NestedDirectoryPolicy;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class CachingDirectoryLister
        implements DirectoryLister
{
    private final Cache<Path, List<LocatedFileStatus>> cache;
    private final Set<SchemaTableName> cachedTableNames;

    @Inject
    public CachingDirectoryLister(HiveClientConfig hiveClientConfig)
    {
        this(
                hiveClientConfig.getFileStatusCacheExpireAfterWrite(),
                hiveClientConfig.getFileStatusCacheMaxSize(),
                hiveClientConfig.getFileStatusCacheTables().stream()
                        .map(CachingDirectoryLister::parseTableName)
                        .collect(Collectors.toSet()));
    }

    public CachingDirectoryLister(Duration expireAfterWrite, long maxSize, Set<SchemaTableName> tables)
    {
        cache = CacheBuilder.newBuilder()
                .maximumWeight(maxSize)
                .weigher((Weigher<Path, List<LocatedFileStatus>>) (key, value) -> value.size())
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

        List<LocatedFileStatus> files = cache.getIfPresent(path);
        if (files != null) {
            return new HiveFileIterator(path, p -> new HadoopFileInfoIterator(simpleRemoteIterator(files)), namenodeStats, nestedDirectoryPolicy, pathFilter);
        }
        RemoteIterator<LocatedFileStatus> iterator = null;
        try {
            iterator = fileSystem.listLocatedStatus(path);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_FILE_NOT_FOUND, "Hive file location does not exist: " + path);
        }

        if (!cachedTableNames.contains(schemaTableName)) {
            HadoopFileInfoIterator fileIterator = new HadoopFileInfoIterator(iterator);
            return new HiveFileIterator(path, p -> fileIterator, namenodeStats, nestedDirectoryPolicy, pathFilter);
        }
        HadoopFileInfoIterator fileIterator = new HadoopFileInfoIterator(cachingRemoteIterator(iterator, path));
        return new HiveFileIterator(path, p -> fileIterator, namenodeStats, nestedDirectoryPolicy, pathFilter);
    }

    public static class HadoopFileInfoIterator
            implements RemoteIterator<HiveFileInfo>
    {
        private final RemoteIterator<LocatedFileStatus> locatedFileStatusIterator;

        public HadoopFileInfoIterator(RemoteIterator<LocatedFileStatus> locatedFileStatusIterator)
        {
            this.locatedFileStatusIterator = requireNonNull(locatedFileStatusIterator, "locatedFileStatusIterator is null");
        }

        @Override
        public boolean hasNext()
                throws IOException
        {
            return locatedFileStatusIterator.hasNext();
        }

        @Override
        public HiveFileInfo next()
                throws IOException
        {
            return createHiveFileInfo(locatedFileStatusIterator.next(), Optional.empty());
        }
    }

    private RemoteIterator<LocatedFileStatus> cachingRemoteIterator(RemoteIterator<LocatedFileStatus> iterator, Path path)
    {
        return new RemoteIterator<LocatedFileStatus>()
        {
            private final List<LocatedFileStatus> files = new ArrayList<>();

            @Override
            public boolean hasNext()
                    throws IOException
            {
                boolean hasNext = iterator.hasNext();
                if (!hasNext) {
                    cache.put(path, ImmutableList.copyOf(files));
                }
                return hasNext;
            }

            @Override
            public LocatedFileStatus next()
                    throws IOException
            {
                LocatedFileStatus next = iterator.next();
                files.add(next);
                return next;
            }
        };
    }

    private static RemoteIterator<LocatedFileStatus> simpleRemoteIterator(List<LocatedFileStatus> files)
    {
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
