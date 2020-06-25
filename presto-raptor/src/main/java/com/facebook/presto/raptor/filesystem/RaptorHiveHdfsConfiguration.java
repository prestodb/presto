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
package com.facebook.presto.raptor.filesystem;

import com.facebook.presto.hadoop.HadoopNative;
import com.facebook.presto.hive.HdfsContext;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.HadoopExtendedFileSystemCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.SocksSocketFactory;

import javax.inject.Inject;
import javax.net.SocketFactory;

import java.net.URI;
import java.util.List;

import static com.facebook.presto.raptor.filesystem.FileSystemUtil.copy;
import static com.facebook.presto.raptor.filesystem.FileSystemUtil.getInitialConfiguration;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_PING_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_RPC_PROTECTION;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SOCKS_SERVER_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_TIMEOUT_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY;

public class RaptorHiveHdfsConfiguration
        implements RaptorHdfsConfiguration
{
    static {
        HadoopNative.requireHadoopNative();
        HadoopExtendedFileSystemCache.initialize();
    }

    private static final Configuration INITIAL_CONFIGURATION = getInitialConfiguration();

    @SuppressWarnings("ThreadLocalNotStaticFinal")
    private final ThreadLocal<Configuration> hadoopConfiguration = new ThreadLocal<Configuration>()
    {
        @Override
        protected Configuration initialValue()
        {
            Configuration configuration = new Configuration(false);
            copy(INITIAL_CONFIGURATION, configuration);
            initializer.updateConfiguration(configuration);
            return configuration;
        }
    };

    private final HdfsConfigurationInitializer initializer;

    @Inject
    public RaptorHiveHdfsConfiguration(RaptorHdfsConfig config)
    {
        this.initializer = new HdfsConfigurationInitializer(requireNonNull(config, "config is null"));
    }

    // TODO: Support DynamicConfigurationProvider which consumes context and URI
    @Override
    public Configuration getConfiguration(HdfsContext context, URI uri)
    {
        return hadoopConfiguration.get();
    }

    private static class HdfsConfigurationInitializer
    {
        private final HostAndPort socksProxy;
        private final Duration ipcPingInterval;
        private final Duration dfsTimeout;
        private final Duration dfsConnectTimeout;
        private final int dfsConnectMaxRetries;
        private final String domainSocketPath;
        private final Configuration resourcesConfiguration;
        private final int fileSystemMaxCacheSize;
        private final boolean isHdfsWireEncryptionEnabled;
        private int textMaxLineLength;

        public HdfsConfigurationInitializer(RaptorHdfsConfig config)
        {
            requireNonNull(config, "config is null");
            checkArgument(config.getDfsTimeout().toMillis() >= 1, "dfsTimeout must be at least 1 ms");
            checkArgument(toIntExact(config.getTextMaxLineLength().toBytes()) >= 1, "textMaxLineLength must be at least 1 byte");

            this.socksProxy = config.getSocksProxy();
            this.ipcPingInterval = config.getIpcPingInterval();
            this.dfsTimeout = config.getDfsTimeout();
            this.dfsConnectTimeout = config.getDfsConnectTimeout();
            this.dfsConnectMaxRetries = config.getDfsConnectMaxRetries();
            this.domainSocketPath = config.getDomainSocketPath();
            this.resourcesConfiguration = readConfiguration(config.getResourceConfigFiles());
            this.fileSystemMaxCacheSize = config.getFileSystemMaxCacheSize();
            this.isHdfsWireEncryptionEnabled = config.isHdfsWireEncryptionEnabled();
            this.textMaxLineLength = toIntExact(config.getTextMaxLineLength().toBytes());
        }

        private static Configuration readConfiguration(List<String> resourcePaths)
        {
            Configuration result = new Configuration(false);

            for (String resourcePath : resourcePaths) {
                Configuration resourceProperties = new Configuration(false);
                resourceProperties.addResource(new Path(resourcePath));
                copy(resourceProperties, result);
            }

            return result;
        }

        public void updateConfiguration(Configuration config)
        {
            copy(resourcesConfiguration, config);

            // this is to prevent dfs client from doing reverse DNS lookups to determine whether nodes are rack local
            config.setClass(NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY, NoOpDNSToSwitchMapping.class, DNSToSwitchMapping.class);

            if (socksProxy != null) {
                config.setClass(HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY, SocksSocketFactory.class, SocketFactory.class);
                config.set(HADOOP_SOCKS_SERVER_KEY, socksProxy.toString());
            }

            if (domainSocketPath != null) {
                config.setStrings(DFS_DOMAIN_SOCKET_PATH_KEY, domainSocketPath);
            }

            // only enable short circuit reads if domain socket path is properly configured
            if (!config.get(DFS_DOMAIN_SOCKET_PATH_KEY, "").trim().isEmpty()) {
                config.setBooleanIfUnset(DFS_CLIENT_READ_SHORTCIRCUIT_KEY, true);
            }

            config.setInt(DFS_CLIENT_SOCKET_TIMEOUT_KEY, toIntExact(dfsTimeout.toMillis()));
            config.setInt(IPC_PING_INTERVAL_KEY, toIntExact(ipcPingInterval.toMillis()));
            config.setInt(IPC_CLIENT_CONNECT_TIMEOUT_KEY, toIntExact(dfsConnectTimeout.toMillis()));
            config.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, dfsConnectMaxRetries);

            if (isHdfsWireEncryptionEnabled) {
                config.set(HADOOP_RPC_PROTECTION, "privacy");
                config.setBoolean("dfs.encrypt.data.transfer", true);
            }

            config.setInt("fs.cache.max-size", fileSystemMaxCacheSize);

            config.setInt(LineRecordReader.MAX_LINE_LENGTH, textMaxLineLength);
        }

        public static class NoOpDNSToSwitchMapping
                implements DNSToSwitchMapping
        {
            @Override
            public List<String> resolve(List<String> names)
            {
                // dfs client expects an empty list as an indication that the host->switch mapping for the given names are not known
                return ImmutableList.of();
            }

            @Override
            public void reloadCachedMappings()
            {
                // no-op
            }

            @Override
            public void reloadCachedMappings(List<String> names)
            {
                // no-op
            }
        }
    }
}
