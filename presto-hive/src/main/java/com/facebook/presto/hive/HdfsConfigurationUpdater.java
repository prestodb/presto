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

import com.facebook.presto.hive.s3.S3ConfigurationUpdater;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.OrcTableProperties;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.SocksSocketFactory;
import parquet.hadoop.ParquetOutputFormat;

import javax.inject.Inject;
import javax.net.SocketFactory;

import java.util.List;

import static com.facebook.hive.orc.OrcConf.ConfVars.HIVE_ORC_COMPRESSION;
import static com.facebook.presto.hive.util.ConfigurationUtils.copy;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_PING_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SOCKS_SERVER_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_TIMEOUT_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.COMPRESSRESULT;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_ORC_DEFAULT_COMPRESS;
import static org.apache.hadoop.io.SequenceFile.CompressionType.BLOCK;

public class HdfsConfigurationUpdater
{
    private final HostAndPort socksProxy;
    private final Duration ipcPingInterval;
    private final Duration dfsTimeout;
    private final Duration dfsConnectTimeout;
    private final int dfsConnectMaxRetries;
    private final String domainSocketPath;
    private final Configuration resourcesConfiguration;
    private final HiveCompressionCodec compressionCodec;
    private final int fileSystemMaxCacheSize;
    private final S3ConfigurationUpdater s3ConfigurationUpdater;

    @VisibleForTesting
    public HdfsConfigurationUpdater(HiveClientConfig config)
    {
        this(config, ignored -> {});
    }

    @Inject
    public HdfsConfigurationUpdater(HiveClientConfig config, S3ConfigurationUpdater s3ConfigurationUpdater)
    {
        requireNonNull(config, "config is null");
        checkArgument(config.getDfsTimeout().toMillis() >= 1, "dfsTimeout must be at least 1 ms");

        this.socksProxy = config.getMetastoreSocksProxy();
        this.ipcPingInterval = config.getIpcPingInterval();
        this.dfsTimeout = config.getDfsTimeout();
        this.dfsConnectTimeout = config.getDfsConnectTimeout();
        this.dfsConnectMaxRetries = config.getDfsConnectMaxRetries();
        this.domainSocketPath = config.getDomainSocketPath();
        this.resourcesConfiguration = readConfiguration(config.getResourceConfigFiles());
        this.compressionCodec = config.getHiveCompressionCodec();
        this.fileSystemMaxCacheSize = config.getFileSystemMaxCacheSize();

        this.s3ConfigurationUpdater = requireNonNull(s3ConfigurationUpdater, "s3ConfigurationUpdater is null");
    }

    private static Configuration readConfiguration(List<String> resourcePaths)
    {
        Configuration result = new Configuration(false);
        if (resourcePaths == null) {
            return result;
        }

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

        config.setInt("fs.cache.max-size", fileSystemMaxCacheSize);

        configureCompression(config, compressionCodec);

        s3ConfigurationUpdater.updateConfiguration(config);
    }

    public static void configureCompression(Configuration config, HiveCompressionCodec compressionCodec)
    {
        boolean compression = compressionCodec != HiveCompressionCodec.NONE;
        config.setBoolean(COMPRESSRESULT.varname, compression);
        config.setBoolean("mapred.output.compress", compression);
        config.setBoolean(FileOutputFormat.COMPRESS, compression);
        // For DWRF
        config.set(HIVE_ORC_DEFAULT_COMPRESS.varname, compressionCodec.getOrcCompressionKind().name());
        config.set(HIVE_ORC_COMPRESSION.varname, compressionCodec.getOrcCompressionKind().name());
        // For ORC
        config.set(OrcTableProperties.COMPRESSION.getPropName(), compressionCodec.getOrcCompressionKind().name());
        // For RCFile and Text
        if (compressionCodec.getCodec().isPresent()) {
            config.set("mapred.output.compression.codec", compressionCodec.getCodec().get().getName());
            config.set(FileOutputFormat.COMPRESS_CODEC, compressionCodec.getCodec().get().getName());
        }
        else {
            config.unset("mapred.output.compression.codec");
            config.unset(FileOutputFormat.COMPRESS_CODEC);
        }
        // For Parquet
        config.set(ParquetOutputFormat.COMPRESSION, compressionCodec.getParquetCompressionCodec().name());
        // For SequenceFile
        config.set(FileOutputFormat.COMPRESS_TYPE, BLOCK.toString());
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
