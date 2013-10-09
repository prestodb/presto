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

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Provide our own local caching of Hadoop FileSystems because the Hadoop default cache is 10x slower.
 */
public class FileSystemCache
{
    private final Cache<FileSystemKey, FileSystem> cache;

    @Inject
    public FileSystemCache(HiveClientConfig hiveClientConfig)
    {
        cache = CacheBuilder.newBuilder()
                .expireAfterAccess(hiveClientConfig.getFileSystemCacheTtl().toMillis(), TimeUnit.MILLISECONDS)
                .build();
    }

    public Function<Path, Path> createPathWrapper()
    {
        return new Function<Path, Path>()
        {
            @Override
            public Path apply(final Path path)
            {
                return new ForwardingPath(path)
                {
                    @Override
                    public FileSystem getFileSystem(Configuration conf)
                            throws IOException
                    {
                        try {
                            return cache.get(FileSystemKey.create(path, conf), createFileSystemFromPath(path, conf));
                        }
                        catch (ExecutionException | UncheckedExecutionException | ExecutionError e) {
                            throw new IOException(e.getCause());
                        }
                    }
                };
            }
        };
    }

    public List<FileSystem> getFileSystems()
    {
        return ImmutableList.copyOf(cache.asMap().values());
    }

    private static Callable<FileSystem> createFileSystemFromPath(final Path path, final Configuration conf)
    {
        return new Callable<FileSystem>()
        {
            @Override
            public FileSystem call()
                    throws Exception
            {
                return path.getFileSystem(conf);
            }
        };
    }

    private static class FileSystemKey
    {
        @Nullable
        private final String fsType;
        private final String scheme;
        private final String authority;

        private FileSystemKey(@Nullable String fsType, String scheme, String authority)
        {
            this.fsType = fsType;
            this.scheme = checkNotNull(scheme, "scheme is null");
            this.authority = checkNotNull(authority, "authority is null");
        }

        private static FileSystemKey create(Path path, Configuration conf)
        {
            String scheme = Strings.nullToEmpty(path.toUri().getScheme()).toLowerCase();
            String authority = Strings.nullToEmpty(path.toUri().getAuthority()).toLowerCase();
            String fsType = scheme.isEmpty() ? null : conf.get("fs." + scheme + ".impl");
            return new FileSystemKey(fsType, scheme, authority);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(fsType, scheme, authority);
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
            FileSystemKey other = (FileSystemKey) obj;
            return Objects.equal(this.fsType, other.fsType) &&
                    Objects.equal(this.scheme, other.scheme) &&
                    Objects.equal(this.authority, other.authority);
        }
    }
}
