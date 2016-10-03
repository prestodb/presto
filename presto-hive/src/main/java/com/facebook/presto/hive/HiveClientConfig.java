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

import com.google.common.base.Splitter;
import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDataSize;
import io.airlift.units.MinDuration;
import org.joda.time.DateTimeZone;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

@DefunctConfig({
        "hive.file-system-cache-ttl",
        "hive.max-global-split-iterator-threads",
        "hive.optimized-reader.enabled"
})
public class HiveClientConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private String timeZone = TimeZone.getDefault().getID();

    private DataSize maxSplitSize = new DataSize(64, MEGABYTE);
    private int maxOutstandingSplits = 1_000;
    private int maxSplitIteratorThreads = 1_000;
    private int minPartitionBatchSize = 10;
    private int maxPartitionBatchSize = 100;
    private int maxInitialSplits = 200;
    private DataSize maxInitialSplitSize;
    private int domainCompactionThreshold = 100;
    private boolean forceLocalScheduling;
    private boolean recursiveDirWalkerEnabled;

    private int maxConcurrentFileRenames = 20;

    private boolean allowCorruptWritesForTesting;

    private Duration metastoreCacheTtl = new Duration(1, TimeUnit.HOURS);
    private Duration metastoreRefreshInterval = new Duration(1, TimeUnit.SECONDS);
    private int maxMetastoreRefreshThreads = 100;
    private HostAndPort metastoreSocksProxy;
    private Duration metastoreTimeout = new Duration(10, TimeUnit.SECONDS);

    private Duration ipcPingInterval = new Duration(10, TimeUnit.SECONDS);
    private Duration dfsTimeout = new Duration(60, TimeUnit.SECONDS);
    private Duration dfsConnectTimeout = new Duration(500, TimeUnit.MILLISECONDS);
    private int dfsConnectMaxRetries = 5;
    private boolean verifyChecksum = true;
    private String domainSocketPath;

    private String s3AwsAccessKey;
    private String s3AwsSecretKey;
    private String s3Endpoint;
    private PrestoS3SignerType s3SignerType;
    private boolean s3UseInstanceCredentials = true;
    private boolean s3SslEnabled = true;
    private boolean s3SseEnabled;
    private String s3EncryptionMaterialsProvider;
    private String s3KmsKeyId;
    private int s3MaxClientRetries = 3;
    private int s3MaxErrorRetries = 10;
    private Duration s3MaxBackoffTime = new Duration(10, TimeUnit.MINUTES);
    private Duration s3MaxRetryTime = new Duration(10, TimeUnit.MINUTES);
    private Duration s3ConnectTimeout = new Duration(5, TimeUnit.SECONDS);
    private Duration s3SocketTimeout = new Duration(5, TimeUnit.SECONDS);
    private int s3MaxConnections = 500;
    private File s3StagingDirectory = new File(StandardSystemProperty.JAVA_IO_TMPDIR.value());
    private DataSize s3MultipartMinFileSize = new DataSize(16, MEGABYTE);
    private DataSize s3MultipartMinPartSize = new DataSize(5, MEGABYTE);
    private boolean useParquetColumnNames;
    private boolean pinS3ClientToCurrentRegion;
    private String s3UserAgentPrefix = "";

    private HiveStorageFormat hiveStorageFormat = HiveStorageFormat.RCBINARY;
    private HiveCompressionCodec hiveCompressionCodec = HiveCompressionCodec.GZIP;
    private boolean respectTableFormat = true;
    private boolean immutablePartitions;
    private int maxPartitionsPerWriter = 100;

    private List<String> resourceConfigFiles;

    private boolean parquetOptimizedReaderEnabled;

    private boolean parquetPredicatePushdownEnabled;

    private boolean assumeCanonicalPartitionKeys;

    private boolean useOrcColumnNames;
    private boolean orcBloomFiltersEnabled;
    private DataSize orcMaxMergeDistance = new DataSize(1, MEGABYTE);
    private DataSize orcMaxBufferSize = new DataSize(8, MEGABYTE);
    private DataSize orcStreamBufferSize = new DataSize(8, MEGABYTE);

    private boolean rcfileOptimizedReaderEnabled;

    private HiveMetastoreAuthenticationType hiveMetastoreAuthenticationType = HiveMetastoreAuthenticationType.NONE;
    private String hiveMetastoreServicePrincipal;
    private String hiveMetastoreClientPrincipal;
    private String hiveMetastoreClientKeytab;

    private HdfsAuthenticationType hdfsAuthenticationType = HdfsAuthenticationType.NONE;
    private boolean hdfsImpersonationEnabled;
    private String hdfsPrestoPrincipal;
    private String hdfsPrestoKeytab;

    private boolean skipDeletionForAlter;

    private boolean bucketExecutionEnabled = true;
    private boolean bucketWritingEnabled = true;

    public int getMaxInitialSplits()
    {
        return maxInitialSplits;
    }

    @Config("hive.max-initial-splits")
    public HiveClientConfig setMaxInitialSplits(int maxInitialSplits)
    {
        this.maxInitialSplits = maxInitialSplits;
        return this;
    }

    public DataSize getMaxInitialSplitSize()
    {
        if (maxInitialSplitSize == null) {
            return new DataSize(maxSplitSize.getValue() / 2, maxSplitSize.getUnit());
        }
        return maxInitialSplitSize;
    }

    @Config("hive.max-initial-split-size")
    public HiveClientConfig setMaxInitialSplitSize(DataSize maxInitialSplitSize)
    {
        this.maxInitialSplitSize = maxInitialSplitSize;
        return this;
    }

    @Min(1)
    public int getDomainCompactionThreshold()
    {
        return domainCompactionThreshold;
    }

    @Config("hive.domain-compaction-threshold")
    @ConfigDescription("Maximum ranges to allow in a tuple domain without compacting it")
    public HiveClientConfig setDomainCompactionThreshold(int domainCompactionThreshold)
    {
        this.domainCompactionThreshold = domainCompactionThreshold;
        return this;
    }

    public boolean isForceLocalScheduling()
    {
        return forceLocalScheduling;
    }

    @Config("hive.force-local-scheduling")
    public HiveClientConfig setForceLocalScheduling(boolean forceLocalScheduling)
    {
        this.forceLocalScheduling = forceLocalScheduling;
        return this;
    }

    @Min(1)
    public int getMaxConcurrentFileRenames()
    {
        return maxConcurrentFileRenames;
    }

    @Config("hive.max-concurrent-file-renames")
    public HiveClientConfig setMaxConcurrentFileRenames(int maxConcurrentFileRenames)
    {
        this.maxConcurrentFileRenames = maxConcurrentFileRenames;
        return this;
    }

    @Config("hive.recursive-directories")
    public HiveClientConfig setRecursiveDirWalkerEnabled(boolean recursiveDirWalkerEnabled)
    {
        this.recursiveDirWalkerEnabled = recursiveDirWalkerEnabled;
        return this;
    }

    public boolean getRecursiveDirWalkerEnabled()
    {
        return recursiveDirWalkerEnabled;
    }

    public DateTimeZone getDateTimeZone()
    {
        return DateTimeZone.forTimeZone(TimeZone.getTimeZone(timeZone));
    }

    @NotNull
    public String getTimeZone()
    {
        return timeZone;
    }

    @Config("hive.time-zone")
    public HiveClientConfig setTimeZone(String id)
    {
        this.timeZone = (id != null) ? id : TimeZone.getDefault().getID();
        return this;
    }

    @NotNull
    public DataSize getMaxSplitSize()
    {
        return maxSplitSize;
    }

    @Config("hive.max-split-size")
    public HiveClientConfig setMaxSplitSize(DataSize maxSplitSize)
    {
        this.maxSplitSize = maxSplitSize;
        return this;
    }

    @Min(1)
    public int getMaxOutstandingSplits()
    {
        return maxOutstandingSplits;
    }

    @Config("hive.max-outstanding-splits")
    public HiveClientConfig setMaxOutstandingSplits(int maxOutstandingSplits)
    {
        this.maxOutstandingSplits = maxOutstandingSplits;
        return this;
    }

    @Min(1)
    public int getMaxSplitIteratorThreads()
    {
        return maxSplitIteratorThreads;
    }

    @Config("hive.max-split-iterator-threads")
    public HiveClientConfig setMaxSplitIteratorThreads(int maxSplitIteratorThreads)
    {
        this.maxSplitIteratorThreads = maxSplitIteratorThreads;
        return this;
    }

    @Deprecated
    public boolean getAllowCorruptWritesForTesting()
    {
        return allowCorruptWritesForTesting;
    }

    @Deprecated
    @Config("hive.allow-corrupt-writes-for-testing")
    @ConfigDescription("Allow Hive connector to write data even when data will likely be corrupt")
    public HiveClientConfig setAllowCorruptWritesForTesting(boolean allowCorruptWritesForTesting)
    {
        this.allowCorruptWritesForTesting = allowCorruptWritesForTesting;
        return this;
    }

    @NotNull
    public Duration getMetastoreCacheTtl()
    {
        return metastoreCacheTtl;
    }

    @MinDuration("0ms")
    @Config("hive.metastore-cache-ttl")
    public HiveClientConfig setMetastoreCacheTtl(Duration metastoreCacheTtl)
    {
        this.metastoreCacheTtl = metastoreCacheTtl;
        return this;
    }

    @NotNull
    public Duration getMetastoreRefreshInterval()
    {
        return metastoreRefreshInterval;
    }

    @MinDuration("1ms")
    @Config("hive.metastore-refresh-interval")
    public HiveClientConfig setMetastoreRefreshInterval(Duration metastoreRefreshInterval)
    {
        this.metastoreRefreshInterval = metastoreRefreshInterval;
        return this;
    }

    @Min(1)
    public int getMaxMetastoreRefreshThreads()
    {
        return maxMetastoreRefreshThreads;
    }

    @Config("hive.metastore-refresh-max-threads")
    public HiveClientConfig setMaxMetastoreRefreshThreads(int maxMetastoreRefreshThreads)
    {
        this.maxMetastoreRefreshThreads = maxMetastoreRefreshThreads;
        return this;
    }

    public HostAndPort getMetastoreSocksProxy()
    {
        return metastoreSocksProxy;
    }

    @Config("hive.metastore.thrift.client.socks-proxy")
    public HiveClientConfig setMetastoreSocksProxy(HostAndPort metastoreSocksProxy)
    {
        this.metastoreSocksProxy = metastoreSocksProxy;
        return this;
    }

    @NotNull
    public Duration getMetastoreTimeout()
    {
        return metastoreTimeout;
    }

    @Config("hive.metastore-timeout")
    public HiveClientConfig setMetastoreTimeout(Duration metastoreTimeout)
    {
        this.metastoreTimeout = metastoreTimeout;
        return this;
    }

    @Min(1)
    public int getMinPartitionBatchSize()
    {
        return minPartitionBatchSize;
    }

    @Config("hive.metastore.partition-batch-size.min")
    public HiveClientConfig setMinPartitionBatchSize(int minPartitionBatchSize)
    {
        this.minPartitionBatchSize = minPartitionBatchSize;
        return this;
    }

    @Min(1)
    public int getMaxPartitionBatchSize()
    {
        return maxPartitionBatchSize;
    }

    @Config("hive.metastore.partition-batch-size.max")
    public HiveClientConfig setMaxPartitionBatchSize(int maxPartitionBatchSize)
    {
        this.maxPartitionBatchSize = maxPartitionBatchSize;
        return this;
    }

    public List<String> getResourceConfigFiles()
    {
        return resourceConfigFiles;
    }

    @Config("hive.config.resources")
    public HiveClientConfig setResourceConfigFiles(String files)
    {
        this.resourceConfigFiles = (files == null) ? null : SPLITTER.splitToList(files);
        return this;
    }

    public HiveClientConfig setResourceConfigFiles(List<String> files)
    {
        this.resourceConfigFiles = (files == null) ? null : ImmutableList.copyOf(files);
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getIpcPingInterval()
    {
        return ipcPingInterval;
    }

    @Config("hive.dfs.ipc-ping-interval")
    public HiveClientConfig setIpcPingInterval(Duration pingInterval)
    {
        this.ipcPingInterval = pingInterval;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getDfsTimeout()
    {
        return dfsTimeout;
    }

    @Config("hive.dfs-timeout")
    public HiveClientConfig setDfsTimeout(Duration dfsTimeout)
    {
        this.dfsTimeout = dfsTimeout;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getDfsConnectTimeout()
    {
        return dfsConnectTimeout;
    }

    @Config("hive.dfs.connect.timeout")
    public HiveClientConfig setDfsConnectTimeout(Duration dfsConnectTimeout)
    {
        this.dfsConnectTimeout = dfsConnectTimeout;
        return this;
    }

    @Min(0)
    public int getDfsConnectMaxRetries()
    {
        return dfsConnectMaxRetries;
    }

    @Config("hive.dfs.connect.max-retries")
    public HiveClientConfig setDfsConnectMaxRetries(int dfsConnectMaxRetries)
    {
        this.dfsConnectMaxRetries = dfsConnectMaxRetries;
        return this;
    }

    public HiveStorageFormat getHiveStorageFormat()
    {
        return hiveStorageFormat;
    }

    @Config("hive.storage-format")
    public HiveClientConfig setHiveStorageFormat(HiveStorageFormat hiveStorageFormat)
    {
        this.hiveStorageFormat = hiveStorageFormat;
        return this;
    }

    public HiveCompressionCodec getHiveCompressionCodec()
    {
        return hiveCompressionCodec;
    }

    @Config("hive.compression-codec")
    public HiveClientConfig setHiveCompressionCodec(HiveCompressionCodec hiveCompressionCodec)
    {
        this.hiveCompressionCodec = hiveCompressionCodec;
        return this;
    }

    public boolean isRespectTableFormat()
    {
        return respectTableFormat;
    }

    @Config("hive.respect-table-format")
    @ConfigDescription("Should new partitions be written using the existing table format or the default Presto format")
    public HiveClientConfig setRespectTableFormat(boolean respectTableFormat)
    {
        this.respectTableFormat = respectTableFormat;
        return this;
    }

    public boolean isImmutablePartitions()
    {
        return immutablePartitions;
    }

    @Config("hive.immutable-partitions")
    @ConfigDescription("Can new data be inserted into existing partitions or existing unpartitioned tables")
    public HiveClientConfig setImmutablePartitions(boolean immutablePartitions)
    {
        this.immutablePartitions = immutablePartitions;
        return this;
    }

    @Min(1)
    public int getMaxPartitionsPerWriter()
    {
        return maxPartitionsPerWriter;
    }

    @Config("hive.max-partitions-per-writers")
    @ConfigDescription("Maximum number of partitions per writer")
    public HiveClientConfig setMaxPartitionsPerWriter(int maxPartitionsPerWriter)
    {
        this.maxPartitionsPerWriter = maxPartitionsPerWriter;
        return this;
    }

    public String getDomainSocketPath()
    {
        return domainSocketPath;
    }

    @Config("hive.dfs.domain-socket-path")
    @LegacyConfig("dfs.domain-socket-path")
    public HiveClientConfig setDomainSocketPath(String domainSocketPath)
    {
        this.domainSocketPath = domainSocketPath;
        return this;
    }

    public boolean isVerifyChecksum()
    {
        return verifyChecksum;
    }

    @Config("hive.dfs.verify-checksum")
    public HiveClientConfig setVerifyChecksum(boolean verifyChecksum)
    {
        this.verifyChecksum = verifyChecksum;
        return this;
    }

    public String getS3AwsAccessKey()
    {
        return s3AwsAccessKey;
    }

    @Config("hive.s3.aws-access-key")
    public HiveClientConfig setS3AwsAccessKey(String s3AwsAccessKey)
    {
        this.s3AwsAccessKey = s3AwsAccessKey;
        return this;
    }

    public String getS3AwsSecretKey()
    {
        return s3AwsSecretKey;
    }

    @Config("hive.s3.aws-secret-key")
    public HiveClientConfig setS3AwsSecretKey(String s3AwsSecretKey)
    {
        this.s3AwsSecretKey = s3AwsSecretKey;
        return this;
    }

    public String getS3Endpoint()
    {
        return s3Endpoint;
    }

    @Config("hive.s3.endpoint")
    public HiveClientConfig setS3Endpoint(String s3Endpoint)
    {
        this.s3Endpoint = s3Endpoint;
        return this;
    }

    public PrestoS3SignerType getS3SignerType()
    {
        return s3SignerType;
    }

    @Config("hive.s3.signer-type")
    public HiveClientConfig setS3SignerType(PrestoS3SignerType s3SignerType)
    {
        this.s3SignerType = s3SignerType;
        return this;
    }

    public boolean isS3UseInstanceCredentials()
    {
        return s3UseInstanceCredentials;
    }

    @Config("hive.s3.use-instance-credentials")
    public HiveClientConfig setS3UseInstanceCredentials(boolean s3UseInstanceCredentials)
    {
        this.s3UseInstanceCredentials = s3UseInstanceCredentials;
        return this;
    }

    public boolean isS3SslEnabled()
    {
        return s3SslEnabled;
    }

    @Config("hive.s3.ssl.enabled")
    public HiveClientConfig setS3SslEnabled(boolean s3SslEnabled)
    {
        this.s3SslEnabled = s3SslEnabled;
        return this;
    }

    public String getS3EncryptionMaterialsProvider()
    {
        return s3EncryptionMaterialsProvider;
    }

    @Config("hive.s3.encryption-materials-provider")
    @ConfigDescription("Use a custom encryption materials provider for S3 data encryption")
    public HiveClientConfig setS3EncryptionMaterialsProvider(String s3EncryptionMaterialsProvider)
    {
        this.s3EncryptionMaterialsProvider = s3EncryptionMaterialsProvider;
        return this;
    }

    public String getS3KmsKeyId()
    {
        return s3KmsKeyId;
    }

    @Config("hive.s3.kms-key-id")
    @ConfigDescription("Use an AWS KMS key for S3 data encryption")
    public HiveClientConfig setS3KmsKeyId(String s3KmsKeyId)
    {
        this.s3KmsKeyId = s3KmsKeyId;
        return this;
    }

    public boolean isS3SseEnabled()
    {
        return s3SseEnabled;
    }

    @Config("hive.s3.sse.enabled")
    @ConfigDescription("Enable S3 server side encryption")
    public HiveClientConfig setS3SseEnabled(boolean s3SseEnabled)
    {
        this.s3SseEnabled = s3SseEnabled;
        return this;
    }

    @Min(0)
    public int getS3MaxClientRetries()
    {
        return s3MaxClientRetries;
    }

    @Config("hive.s3.max-client-retries")
    public HiveClientConfig setS3MaxClientRetries(int s3MaxClientRetries)
    {
        this.s3MaxClientRetries = s3MaxClientRetries;
        return this;
    }

    @Min(0)
    public int getS3MaxErrorRetries()
    {
        return s3MaxErrorRetries;
    }

    @Config("hive.s3.max-error-retries")
    public HiveClientConfig setS3MaxErrorRetries(int s3MaxErrorRetries)
    {
        this.s3MaxErrorRetries = s3MaxErrorRetries;
        return this;
    }

    @MinDuration("1s")
    @NotNull
    public Duration getS3MaxBackoffTime()
    {
        return s3MaxBackoffTime;
    }

    @Config("hive.s3.max-backoff-time")
    public HiveClientConfig setS3MaxBackoffTime(Duration s3MaxBackoffTime)
    {
        this.s3MaxBackoffTime = s3MaxBackoffTime;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getS3MaxRetryTime()
    {
        return s3MaxRetryTime;
    }

    @Config("hive.s3.max-retry-time")
    public HiveClientConfig setS3MaxRetryTime(Duration s3MaxRetryTime)
    {
        this.s3MaxRetryTime = s3MaxRetryTime;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getS3ConnectTimeout()
    {
        return s3ConnectTimeout;
    }

    @Config("hive.s3.connect-timeout")
    public HiveClientConfig setS3ConnectTimeout(Duration s3ConnectTimeout)
    {
        this.s3ConnectTimeout = s3ConnectTimeout;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getS3SocketTimeout()
    {
        return s3SocketTimeout;
    }

    @Config("hive.s3.socket-timeout")
    public HiveClientConfig setS3SocketTimeout(Duration s3SocketTimeout)
    {
        this.s3SocketTimeout = s3SocketTimeout;
        return this;
    }

    @Min(1)
    public int getS3MaxConnections()
    {
        return s3MaxConnections;
    }

    @Config("hive.s3.max-connections")
    public HiveClientConfig setS3MaxConnections(int s3MaxConnections)
    {
        this.s3MaxConnections = s3MaxConnections;
        return this;
    }

    @NotNull
    public File getS3StagingDirectory()
    {
        return s3StagingDirectory;
    }

    @Config("hive.s3.staging-directory")
    @ConfigDescription("Temporary directory for staging files before uploading to S3")
    public HiveClientConfig setS3StagingDirectory(File s3StagingDirectory)
    {
        this.s3StagingDirectory = s3StagingDirectory;
        return this;
    }

    @NotNull
    @MinDataSize("16MB")
    public DataSize getS3MultipartMinFileSize()
    {
        return s3MultipartMinFileSize;
    }

    @Config("hive.s3.multipart.min-file-size")
    @ConfigDescription("Minimum file size for an S3 multipart upload")
    public HiveClientConfig setS3MultipartMinFileSize(DataSize size)
    {
        this.s3MultipartMinFileSize = size;
        return this;
    }

    @NotNull
    @MinDataSize("5MB")
    public DataSize getS3MultipartMinPartSize()
    {
        return s3MultipartMinPartSize;
    }

    @Config("hive.s3.multipart.min-part-size")
    @ConfigDescription("Minimum part size for an S3 multipart upload")
    public HiveClientConfig setS3MultipartMinPartSize(DataSize size)
    {
        this.s3MultipartMinPartSize = size;
        return this;
    }

    public boolean isPinS3ClientToCurrentRegion()
    {
        return pinS3ClientToCurrentRegion;
    }

    @Config("hive.s3.pin-client-to-current-region")
    @ConfigDescription("Should the S3 client be pinned to the current EC2 region")
    public HiveClientConfig setPinS3ClientToCurrentRegion(boolean pinS3ClientToCurrentRegion)
    {
        this.pinS3ClientToCurrentRegion = pinS3ClientToCurrentRegion;
        return this;
    }

    @NotNull
    public String getS3UserAgentPrefix()
    {
        return s3UserAgentPrefix;
    }

    @Config("hive.s3.user-agent-prefix")
    @ConfigDescription("The user agent prefix to use for S3 calls")
    public HiveClientConfig setS3UserAgentPrefix(String s3UserAgentPrefix)
    {
        this.s3UserAgentPrefix = s3UserAgentPrefix;
        return this;
    }

    @Deprecated
    public boolean isParquetPredicatePushdownEnabled()
    {
        return parquetPredicatePushdownEnabled;
    }

    @Deprecated
    @Config("hive.parquet-predicate-pushdown.enabled")
    public HiveClientConfig setParquetPredicatePushdownEnabled(boolean parquetPredicatePushdownEnabled)
    {
        this.parquetPredicatePushdownEnabled = parquetPredicatePushdownEnabled;
        return this;
    }

    @Deprecated
    public boolean isParquetOptimizedReaderEnabled()
    {
        return parquetOptimizedReaderEnabled;
    }

    @Deprecated
    @Config("hive.parquet-optimized-reader.enabled")
    public HiveClientConfig setParquetOptimizedReaderEnabled(boolean parquetOptimizedReaderEnabled)
    {
        this.parquetOptimizedReaderEnabled = parquetOptimizedReaderEnabled;
        return this;
    }

    public boolean isUseOrcColumnNames()
    {
        return useOrcColumnNames;
    }

    @Config("hive.orc.use-column-names")
    @ConfigDescription("Access ORC columns using names from the file")
    public HiveClientConfig setUseOrcColumnNames(boolean useOrcColumnNames)
    {
        this.useOrcColumnNames = useOrcColumnNames;
        return this;
    }

    @NotNull
    public DataSize getOrcMaxMergeDistance()
    {
        return orcMaxMergeDistance;
    }

    @Config("hive.orc.max-merge-distance")
    public HiveClientConfig setOrcMaxMergeDistance(DataSize orcMaxMergeDistance)
    {
        this.orcMaxMergeDistance = orcMaxMergeDistance;
        return this;
    }

    @NotNull
    public DataSize getOrcMaxBufferSize()
    {
        return orcMaxBufferSize;
    }

    @Config("hive.orc.max-buffer-size")
    public HiveClientConfig setOrcMaxBufferSize(DataSize orcMaxBufferSize)
    {
        this.orcMaxBufferSize = orcMaxBufferSize;
        return this;
    }

    @NotNull
    public DataSize getOrcStreamBufferSize()
    {
        return orcStreamBufferSize;
    }

    @Config("hive.orc.stream-buffer-size")
    public HiveClientConfig setOrcStreamBufferSize(DataSize orcStreamBufferSize)
    {
        this.orcStreamBufferSize = orcStreamBufferSize;
        return this;
    }

    public boolean isOrcBloomFiltersEnabled()
    {
        return orcBloomFiltersEnabled;
    }

    @Config("hive.orc.bloom-filters.enabled")
    public HiveClientConfig setOrcBloomFiltersEnabled(boolean orcBloomFiltersEnabled)
    {
        this.orcBloomFiltersEnabled = orcBloomFiltersEnabled;
        return this;
    }

    @Deprecated
    public boolean isRcfileOptimizedReaderEnabled()
    {
        return rcfileOptimizedReaderEnabled;
    }

    @Deprecated
    @Config("hive.rcfile-optimized-reader.enabled")
    public HiveClientConfig setRcfileOptimizedReaderEnabled(boolean rcfileOptimizedReaderEnabled)
    {
        this.rcfileOptimizedReaderEnabled = rcfileOptimizedReaderEnabled;
        return this;
    }

    public boolean isAssumeCanonicalPartitionKeys()
    {
        return assumeCanonicalPartitionKeys;
    }

    @Config("hive.assume-canonical-partition-keys")
    public HiveClientConfig setAssumeCanonicalPartitionKeys(boolean assumeCanonicalPartitionKeys)
    {
        this.assumeCanonicalPartitionKeys = assumeCanonicalPartitionKeys;
        return this;
    }

    public boolean isUseParquetColumnNames()
    {
        return useParquetColumnNames;
    }

    @Config("hive.parquet.use-column-names")
    @ConfigDescription("Access Parquet columns using names from the file")
    public HiveClientConfig setUseParquetColumnNames(boolean useParquetColumnNames)
    {
        this.useParquetColumnNames = useParquetColumnNames;
        return this;
    }

    public enum HiveMetastoreAuthenticationType
    {
        NONE,
        KERBEROS
    }

    public HiveMetastoreAuthenticationType getHiveMetastoreAuthenticationType()
    {
        return hiveMetastoreAuthenticationType;
    }

    @Config("hive.metastore.authentication.type")
    @ConfigDescription("Hive Metastore authentication type")
    public HiveClientConfig setHiveMetastoreAuthenticationType(HiveMetastoreAuthenticationType hiveMetastoreAuthenticationType)
    {
        this.hiveMetastoreAuthenticationType = hiveMetastoreAuthenticationType;
        return this;
    }

    public String getHiveMetastoreServicePrincipal()
    {
        return hiveMetastoreServicePrincipal;
    }

    @Config("hive.metastore.service.principal")
    @ConfigDescription("Hive Metastore service principal")
    public HiveClientConfig setHiveMetastoreServicePrincipal(String hiveMetastoreServicePrincipal)
    {
        this.hiveMetastoreServicePrincipal = hiveMetastoreServicePrincipal;
        return this;
    }

    public String getHiveMetastoreClientPrincipal()
    {
        return hiveMetastoreClientPrincipal;
    }

    @Config("hive.metastore.client.principal")
    @ConfigDescription("Hive Metastore client principal")
    public HiveClientConfig setHiveMetastoreClientPrincipal(String hiveMetastoreClientPrincipal)
    {
        this.hiveMetastoreClientPrincipal = hiveMetastoreClientPrincipal;
        return this;
    }

    public String getHiveMetastoreClientKeytab()
    {
        return hiveMetastoreClientKeytab;
    }

    @Config("hive.metastore.client.keytab")
    @ConfigDescription("Hive Metastore client keytab location")
    public HiveClientConfig setHiveMetastoreClientKeytab(String hiveMetastoreClientKeytab)
    {
        this.hiveMetastoreClientKeytab = hiveMetastoreClientKeytab;
        return this;
    }

    public enum HdfsAuthenticationType
    {
        NONE,
        KERBEROS,
    }

    public HdfsAuthenticationType getHdfsAuthenticationType()
    {
        return hdfsAuthenticationType;
    }

    @Config("hive.hdfs.authentication.type")
    @ConfigDescription("HDFS authentication type")
    public HiveClientConfig setHdfsAuthenticationType(HdfsAuthenticationType hdfsAuthenticationType)
    {
        this.hdfsAuthenticationType = hdfsAuthenticationType;
        return this;
    }

    public boolean isHdfsImpersonationEnabled()
    {
        return hdfsImpersonationEnabled;
    }

    @Config("hive.hdfs.impersonation.enabled")
    @ConfigDescription("Should Presto user be impersonated when communicating with HDFS")
    public HiveClientConfig setHdfsImpersonationEnabled(boolean hdfsImpersonationEnabled)
    {
        this.hdfsImpersonationEnabled = hdfsImpersonationEnabled;
        return this;
    }

    public String getHdfsPrestoPrincipal()
    {
        return hdfsPrestoPrincipal;
    }

    @Config("hive.hdfs.presto.principal")
    @ConfigDescription("Presto principal used to access HDFS")
    public HiveClientConfig setHdfsPrestoPrincipal(String hdfsPrestoPrincipal)
    {
        this.hdfsPrestoPrincipal = hdfsPrestoPrincipal;
        return this;
    }

    public String getHdfsPrestoKeytab()
    {
        return hdfsPrestoKeytab;
    }

    @Config("hive.hdfs.presto.keytab")
    @ConfigDescription("Presto keytab used to access HDFS")
    public HiveClientConfig setHdfsPrestoKeytab(String hdfsPrestoKeytab)
    {
        this.hdfsPrestoKeytab = hdfsPrestoKeytab;
        return this;
    }

    public boolean isSkipDeletionForAlter()
    {
        return skipDeletionForAlter;
    }

    @Config("hive.skip-deletion-for-alter")
    @ConfigDescription("Skip deletion of old partition data when a partition is deleted and then inserted in the same transaction")
    public HiveClientConfig setSkipDeletionForAlter(boolean skipDeletionForAlter)
    {
        this.skipDeletionForAlter = skipDeletionForAlter;
        return this;
    }

    public boolean isBucketExecutionEnabled()
    {
        return bucketExecutionEnabled;
    }

    @Config("hive.bucket-execution")
    @ConfigDescription("Use bucketing to speed up execution")
    public HiveClientConfig setBucketExecutionEnabled(boolean bucketExecutionEnabled)
    {
        this.bucketExecutionEnabled = bucketExecutionEnabled;
        return this;
    }

    public boolean isBucketWritingEnabled()
    {
        return bucketWritingEnabled;
    }

    @Config("hive.bucket-writing")
    @ConfigDescription("Enable writing to bucketed tables")
    public HiveClientConfig setBucketWritingEnabled(boolean bucketWritingEnabled)
    {
        this.bucketWritingEnabled = bucketWritingEnabled;
        return this;
    }
}
