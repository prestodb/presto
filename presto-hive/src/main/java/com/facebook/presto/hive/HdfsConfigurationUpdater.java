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
import static com.facebook.presto.hive.util.ConfigurationUtils.copy;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
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

    private final String s3AwsAccessKey;
    private final String s3AwsSecretKey;
    private final String s3Endpoint;
    private final PrestoS3SignerType s3SignerType;
    private final boolean s3UseInstanceCredentials;
    private final boolean s3SslEnabled;
    private final boolean s3SseEnabled;
    private final PrestoS3SseType s3SseType;
    private final String s3EncryptionMaterialsProvider;
    private final String s3KmsKeyId;
    private final String s3SseKmsKeyId;
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
    private final boolean pinS3ClientToCurrentRegion;
    private final String s3UserAgentPrefix;

    @Inject
    public HdfsConfigurationUpdater(HiveClientConfig hiveClientConfig, HiveS3Config s3Config)
    {
        requireNonNull(hiveClientConfig, "hiveClientConfig is null");
        checkArgument(hiveClientConfig.getDfsTimeout().toMillis() >= 1, "dfsTimeout must be at least 1 ms");

        this.socksProxy = hiveClientConfig.getMetastoreSocksProxy();
        this.ipcPingInterval = hiveClientConfig.getIpcPingInterval();
        this.dfsTimeout = hiveClientConfig.getDfsTimeout();
        this.dfsConnectTimeout = hiveClientConfig.getDfsConnectTimeout();
        this.dfsConnectMaxRetries = hiveClientConfig.getDfsConnectMaxRetries();
        this.domainSocketPath = hiveClientConfig.getDomainSocketPath();
        this.resourcesConfiguration = readConfiguration(hiveClientConfig.getResourceConfigFiles());
        this.compressionCodec = hiveClientConfig.getHiveCompressionCodec();
        this.fileSystemMaxCacheSize = hiveClientConfig.getFileSystemMaxCacheSize();

        this.s3AwsAccessKey = s3Config.getS3AwsAccessKey();
        this.s3AwsSecretKey = s3Config.getS3AwsSecretKey();
        this.s3Endpoint = s3Config.getS3Endpoint();
        this.s3SignerType = s3Config.getS3SignerType();
        this.s3UseInstanceCredentials = s3Config.isS3UseInstanceCredentials();
        this.s3SslEnabled = s3Config.isS3SslEnabled();
        this.s3SseEnabled = s3Config.isS3SseEnabled();
        this.s3SseType = s3Config.getS3SseType();
        this.s3EncryptionMaterialsProvider = s3Config.getS3EncryptionMaterialsProvider();
        this.s3KmsKeyId = s3Config.getS3KmsKeyId();
        this.s3SseKmsKeyId = s3Config.getS3SseKmsKeyId();
        this.s3MaxClientRetries = s3Config.getS3MaxClientRetries();
        this.s3MaxErrorRetries = s3Config.getS3MaxErrorRetries();
        this.s3MaxBackoffTime = s3Config.getS3MaxBackoffTime();
        this.s3MaxRetryTime = s3Config.getS3MaxRetryTime();
        this.s3ConnectTimeout = s3Config.getS3ConnectTimeout();
        this.s3SocketTimeout = s3Config.getS3SocketTimeout();
        this.s3MaxConnections = s3Config.getS3MaxConnections();
        this.s3MultipartMinFileSize = s3Config.getS3MultipartMinFileSize();
        this.s3MultipartMinPartSize = s3Config.getS3MultipartMinPartSize();
        this.s3StagingDirectory = s3Config.getS3StagingDirectory();
        this.pinS3ClientToCurrentRegion = s3Config.isPinS3ClientToCurrentRegion();
        this.s3UserAgentPrefix = s3Config.getS3UserAgentPrefix();
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

    public void updateConfiguration(PrestoHadoopConfiguration config)
    {
        copy(resourcesConfiguration, config);

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

        config.setInt("dfs.socket.timeout", toIntExact(dfsTimeout.toMillis()));
        config.setInt("ipc.ping.interval", toIntExact(ipcPingInterval.toMillis()));
        config.setInt("ipc.client.connect.timeout", toIntExact(dfsConnectTimeout.toMillis()));
        config.setInt("ipc.client.connect.max.retries", dfsConnectMaxRetries);

        // re-map filesystem schemes to match Amazon Elastic MapReduce
        config.set("fs.s3.impl", PrestoS3FileSystem.class.getName());
        config.set("fs.s3a.impl", PrestoS3FileSystem.class.getName());
        config.set("fs.s3n.impl", PrestoS3FileSystem.class.getName());

        // set AWS credentials for S3
        if (s3AwsAccessKey != null) {
            config.set(PrestoS3FileSystem.S3_ACCESS_KEY, s3AwsAccessKey);
        }
        if (s3AwsSecretKey != null) {
            config.set(PrestoS3FileSystem.S3_SECRET_KEY, s3AwsSecretKey);
        }
        if (s3Endpoint != null) {
            config.set(PrestoS3FileSystem.S3_ENDPOINT, s3Endpoint);
        }
        if (s3SignerType != null) {
            config.set(PrestoS3FileSystem.S3_SIGNER_TYPE, s3SignerType.name());
        }

        config.setInt("fs.cache.max-size", fileSystemMaxCacheSize);

        configureCompression(config, compressionCodec);

        // set config for S3
        config.setBoolean(PrestoS3FileSystem.S3_USE_INSTANCE_CREDENTIALS, s3UseInstanceCredentials);
        config.setBoolean(PrestoS3FileSystem.S3_SSL_ENABLED, s3SslEnabled);
        config.setBoolean(PrestoS3FileSystem.S3_SSE_ENABLED, s3SseEnabled);
        config.set(PrestoS3FileSystem.S3_SSE_TYPE, s3SseType.name());
        if (s3EncryptionMaterialsProvider != null) {
            config.set(PrestoS3FileSystem.S3_ENCRYPTION_MATERIALS_PROVIDER, s3EncryptionMaterialsProvider);
        }
        if (s3KmsKeyId != null) {
            config.set(PrestoS3FileSystem.S3_KMS_KEY_ID, s3KmsKeyId);
        }
        if (s3SseKmsKeyId != null) {
            config.set(PrestoS3FileSystem.S3_SSE_KMS_KEY_ID, s3SseKmsKeyId);
        }
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
        config.set(PrestoS3FileSystem.S3_USER_AGENT_PREFIX, s3UserAgentPrefix);
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
    }
}
