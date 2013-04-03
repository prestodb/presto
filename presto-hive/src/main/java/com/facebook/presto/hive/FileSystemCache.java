package com.facebook.presto.hive;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.common.primitives.Ints;
import io.airlift.units.Duration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.SocksSocketFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.net.SocketFactory;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Provide our own local caching of Hadoop FileSystems because the Hadoop default
 * cache is 10x slower.
 */
public class FileSystemCache
{
    private final HostAndPort socksProxy;
    private final Duration dfsTimeout;
    private final LoadingCache<PathAndKey, FileSystem> cache;

    private final ThreadLocal<Configuration> hadoopConfiguration = new ThreadLocal<Configuration>()
    {
        @Override
        protected Configuration initialValue()
        {
            return createConfiguration();
        }
    };

    public FileSystemCache()
    {
        this(new HiveClientConfig());
    }

    @Inject
    public FileSystemCache(HiveClientConfig hiveClientConfig)
    {
        checkNotNull(hiveClientConfig, "hiveClientConfig is null");
        checkArgument(hiveClientConfig.getDfsTimeout().toMillis() >= 1, "dfsTimeout must be at least 1 ms");

        this.socksProxy = hiveClientConfig.getMetastoreSocksProxy();
        this.dfsTimeout = hiveClientConfig.getDfsTimeout();
        cache = CacheBuilder.newBuilder()
                .expireAfterAccess((long) hiveClientConfig.getFileSystemCacheTtl().toMillis(), TimeUnit.MILLISECONDS)
                .build(new CacheLoader<PathAndKey, FileSystem>() {
                    @Override
                    public FileSystem load(PathAndKey pathAndKey)
                            throws Exception
                    {
                        return FileSystem.get(pathAndKey.getPath().toUri(), hadoopConfiguration.get());
                    }
                });
    }

    public Configuration getConfiguration()
    {
        return hadoopConfiguration.get();
    }

    private Configuration createConfiguration()
    {
        Configuration config = new Configuration();

        // this is to prevent dfs client from doing reverse DNS lookups to determine whether nodes are rack local
        config.setClass("topology.node.switch.mapping.impl", NoOpDNSwitchMapping.class, DNSToSwitchMapping.class);

        if (socksProxy != null) {
            config.setClass("hadoop.rpc.socket.factory.class.default", SocksSocketFactory.class, SocketFactory.class);
            config.set("hadoop.socks.server", socksProxy.toString());
        }
        config.setBoolean("dfs.read.shortcircuit", true);
        config.setBoolean("dfs.read.shortcircuit.fallbackwhenfail", true);
        config.setInt("dfs.socket.timeout", Ints.saturatedCast((long) dfsTimeout.toMillis()));
        config.setInt("ipc.ping.interval", Ints.saturatedCast((long) dfsTimeout.toMillis()));

        return config;
    }

    public FileSystem getFileSystem(Path path)
    {
        return cache.getUnchecked(new PathAndKey(path));
    }

    // Carries the Path, but uses the FileSystemKey for identity
    private static class PathAndKey
    {
        private final Path path;
        private final FileSystemKey key;

        private PathAndKey(Path path)
        {
            this.path = path;
            key = new FileSystemKey(path);
        }

        public Path getPath()
        {
            return path;
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

            PathAndKey that = (PathAndKey) o;

            if (!key.equals(that.key)) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            return key.hashCode();
        }
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

    public static class NoOpDNSwitchMapping
            implements DNSToSwitchMapping
    {
        @Override
        public List<String> resolve(List<String> names)
        {
            // dfs client expects an empty list as an indication that the host->switch mapping for the given names are not known
            return ImmutableList.of();
        }
    }
}
