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

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
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

import java.io.File;
import java.util.List;

import static com.facebook.hive.orc.OrcConf.ConfVars.HIVE_ORC_COMPRESSION;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.COMPRESSRESULT;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_ORC_DEFAULT_COMPRESS;
import static org.apache.hadoop.io.SequenceFile.CompressionType.BLOCK;

public class HdfsConfigurationUpdater
{
    private final HostAndPort socksProxy;
    private final Duration dfsTimeout;
    private final Duration dfsConnectTimeout;
    private final int dfsConnectMaxRetries;
    private final String domainSocketPath;
    private final String s3AwsAccessKey;
    private final String s3AwsSecretKey;
    private final boolean s3UseInstanceCredentials;
    private final boolean s3SslEnabled;
    private final boolean s3SseEnabled;
    private final int s3MaxClientRetries;
    private final int s3MaxErrorRetries;
    private final Duration s3MaxBackoffTime;
    private final Duration s3MaxRetryTime;
    private final Duration s3ConnectTimeout;
    private final Duration s3SocketTimeout;
    private final int s3MaxConnections;
    private final DataSize s3MultipartMinFileSize;
    private final DataSize s3MultipartMinPartSize;
    private final File s3StagingDirectory;
    private final List<String> resourcePaths;
    private final boolean pinS3ClientToCurrentRegion;
    private final HiveCompressionCodec compressionCodec;

    @Inject
    public HdfsConfigurationUpdater(HiveClientConfig hiveClientConfig)
    {
        requireNonNull(hiveClientConfig, "hiveClientConfig is null");
        checkArgument(hiveClientConfig.getDfsTimeout().toMillis() >= 1, "dfsTimeout must be at least 1 ms");

        this.socksProxy = hiveClientConfig.getMetastoreSocksProxy();
        this.dfsTimeout = hiveClientConfig.getDfsTimeout();
        this.dfsConnectTimeout = hiveClientConfig.getDfsConnectTimeout();
        this.dfsConnectMaxRetries = hiveClientConfig.getDfsConnectMaxRetries();
        this.domainSocketPath = hiveClientConfig.getDomainSocketPath();
        this.s3AwsAccessKey = hiveClientConfig.getS3AwsAccessKey();
        this.s3AwsSecretKey = hiveClientConfig.getS3AwsSecretKey();
        this.s3UseInstanceCredentials = hiveClientConfig.isS3UseInstanceCredentials();
        this.s3SslEnabled = hiveClientConfig.isS3SslEnabled();
        this.s3SseEnabled = hiveClientConfig.isS3SseEnabled();
        this.s3MaxClientRetries = hiveClientConfig.getS3MaxClientRetries();
        this.s3MaxErrorRetries = hiveClientConfig.getS3MaxErrorRetries();
        this.s3MaxBackoffTime = hiveClientConfig.getS3MaxBackoffTime();
        this.s3MaxRetryTime = hiveClientConfig.getS3MaxRetryTime();
        this.s3ConnectTimeout = hiveClientConfig.getS3ConnectTimeout();
        this.s3SocketTimeout = hiveClientConfig.getS3SocketTimeout();
        this.s3MaxConnections = hiveClientConfig.getS3MaxConnections();
        this.s3MultipartMinFileSize = hiveClientConfig.getS3MultipartMinFileSize();
        this.s3MultipartMinPartSize = hiveClientConfig.getS3MultipartMinPartSize();
        this.s3StagingDirectory = hiveClientConfig.getS3StagingDirectory();
        this.resourcePaths = hiveClientConfig.getResourceConfigFiles();
        this.pinS3ClientToCurrentRegion = hiveClientConfig.isPinS3ClientToCurrentRegion();
        this.compressionCodec = hiveClientConfig.getHiveCompressionCodec();
    }

    public void updateConfiguration(Configuration config)
    {
        if (resourcePaths != null) {
            for (String resourcePath : resourcePaths) {
                config.addResource(new Path(resourcePath));
            }
        }

        // this is to prevent dfs client from doing reverse DNS lookups to determine whether nodes are rack local
        config.setClass("topology.node.switch.mapping.impl", NoOpDNSToSwitchMapping.class, DNSToSwitchMapping.class);

        if (socksProxy != null) {
            config.setClass("hadoop.rpc.socket.factory.class.default", SocksSocketFactory.class, SocketFactory.class);
            config.set("hadoop.socks.server", socksProxy.toString());
        }

        if (domainSocketPath != null) {
            config.setStrings("dfs.domain.socket.path", domainSocketPath);
        }

        // only enable short circuit reads if domain socket path is properly configured
        if (!config.get("dfs.domain.socket.path", "").trim().isEmpty()) {
            config.setBooleanIfUnset("dfs.client.read.shortcircuit", true);
        }

        config.setInt("dfs.socket.timeout", Ints.checkedCast(dfsTimeout.toMillis()));
        config.setInt("ipc.ping.interval", Ints.checkedCast(dfsTimeout.toMillis()));
        config.setInt("ipc.client.connect.timeout", Ints.checkedCast(dfsConnectTimeout.toMillis()));
        config.setInt("ipc.client.connect.max.retries", dfsConnectMaxRetries);

        // re-map filesystem schemes to match Amazon Elastic MapReduce
        config.set("fs.s3.impl", PrestoS3FileSystem.class.getName());
        config.set("fs.s3a.impl", PrestoS3FileSystem.class.getName());
        config.set("fs.s3n.impl", PrestoS3FileSystem.class.getName());
        config.set("fs.s3bfs.impl", "org.apache.hadoop.fs.s3.S3FileSystem");

        // set AWS credentials for S3
        if (s3AwsAccessKey != null) {
            config.set(PrestoS3FileSystem.S3_ACCESS_KEY, s3AwsAccessKey);
            config.set("fs.s3bfs.awsAccessKeyId", s3AwsAccessKey);
        }
        if (s3AwsSecretKey != null) {
            config.set(PrestoS3FileSystem.S3_SECRET_KEY, s3AwsSecretKey);
            config.set("fs.s3bfs.awsSecretAccessKey", s3AwsSecretKey);
        }

        configureCompression(config, compressionCodec);

        // set config for S3
        config.setBoolean(PrestoS3FileSystem.S3_USE_INSTANCE_CREDENTIALS, s3UseInstanceCredentials);
        config.setBoolean(PrestoS3FileSystem.S3_SSL_ENABLED, s3SslEnabled);
        config.setBoolean(PrestoS3FileSystem.S3_SSE_ENABLED, s3SseEnabled);
        config.setInt(PrestoS3FileSystem.S3_MAX_CLIENT_RETRIES, s3MaxClientRetries);
        config.setInt(PrestoS3FileSystem.S3_MAX_ERROR_RETRIES, s3MaxErrorRetries);
        config.set(PrestoS3FileSystem.S3_MAX_BACKOFF_TIME, s3MaxBackoffTime.toString());
        config.set(PrestoS3FileSystem.S3_MAX_RETRY_TIME, s3MaxRetryTime.toString());
        config.set(PrestoS3FileSystem.S3_CONNECT_TIMEOUT, s3ConnectTimeout.toString());
        config.set(PrestoS3FileSystem.S3_SOCKET_TIMEOUT, s3SocketTimeout.toString());
        config.set(PrestoS3FileSystem.S3_STAGING_DIRECTORY, s3StagingDirectory.toString());
        config.setInt(PrestoS3FileSystem.S3_MAX_CONNECTIONS, s3MaxConnections);
        config.setLong(PrestoS3FileSystem.S3_MULTIPART_MIN_FILE_SIZE, s3MultipartMinFileSize.toBytes());
        config.setLong(PrestoS3FileSystem.S3_MULTIPART_MIN_PART_SIZE, s3MultipartMinPartSize.toBytes());
        config.setBoolean(PrestoS3FileSystem.S3_PIN_CLIENT_TO_CURRENT_REGION, pinS3ClientToCurrentRegion);
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
        // For RCFile
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
    }
}
