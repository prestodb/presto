package com.facebook.presto.hive;

import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Provide our own local caching of Hadoop FileSystems because the Hadoop default
 * cache is 10x slower.
 */
public class FileSystemCache
{
    private final Cache<FileSystemKey, FileSystem> cache;

    public FileSystemCache()
    {
        this(new HiveClientConfig());
    }

    @Inject
    public FileSystemCache(HiveClientConfig hiveClientConfig)
    {
        cache = CacheBuilder.newBuilder()
                .expireAfterAccess((long) hiveClientConfig.getFileSystemCacheTtl().toMillis(), TimeUnit.MILLISECONDS)
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
                        // This method assumes the supplied conf arg will be the same as the static Configuration
                        // provided by the HdfsEnvironment
                        try {
                            return cache.get(new FileSystemKey(path), createFileSystemFromPath(path, conf));
                        }
                        catch (ExecutionException | UncheckedExecutionException e) {
                            throw new IOException(e.getCause());
                        }
                    }
                };
            }
        };
    }

    private Callable<FileSystem> createFileSystemFromPath(final Path path, final Configuration conf)
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
        private final String scheme;
        @Nullable
        private final String authority;

        // Typically we also consider a username here, but since we always use an empty configuration, it is unneeded.

        private FileSystemKey(String scheme, String authority)
        {
            this.scheme = scheme == null ? null : scheme.toLowerCase();
            this.authority = authority == null ? null : authority.toLowerCase();
        }

        private FileSystemKey(Path path)
        {
            this(path.toUri().getScheme(), path.toUri().getAuthority());
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

            FileSystemKey that = (FileSystemKey) o;

            if (authority != null ? !authority.equals(that.authority) : that.authority != null) {
                return false;
            }
            if (scheme != null ? !scheme.equals(that.scheme) : that.scheme != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = scheme != null ? scheme.hashCode() : 0;
            result = 31 * result + (authority != null ? authority.hashCode() : 0);
            return result;
        }
    }
}
