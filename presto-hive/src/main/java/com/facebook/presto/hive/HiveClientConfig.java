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

import com.facebook.presto.hive.s3.S3FileSystemType;
import com.google.common.base.Splitter;
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

import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

@DefunctConfig({
        "hive.file-system-cache-ttl",
        "hive.max-global-split-iterator-threads",
        "hive.optimized-reader.enabled"})
public class HiveClientConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private String timeZone = TimeZone.getDefault().getID();

    private DataSize maxSplitSize = new DataSize(64, MEGABYTE);
    private int maxPartitionsPerScan = 100_000;
    private int maxOutstandingSplits = 1_000;
    private DataSize maxOutstandingSplitsSize = new DataSize(256, MEGABYTE);
    private int maxSplitIteratorThreads = 1_000;
    private int minPartitionBatchSize = 10;
    private int maxPartitionBatchSize = 100;
    private int maxInitialSplits = 200;
    private int splitLoaderConcurrency = 4;
    private DataSize maxInitialSplitSize;
    private int domainCompactionThreshold = 100;
    private boolean forceLocalScheduling;
    private boolean recursiveDirWalkerEnabled;

    private int maxConcurrentFileRenames = 20;

    private boolean allowCorruptWritesForTesting;

    private Duration metastoreCacheTtl = new Duration(0, TimeUnit.SECONDS);
    private Duration metastoreRefreshInterval = new Duration(0, TimeUnit.SECONDS);
    private long metastoreCacheMaximumSize = 10000;
    private long perTransactionMetastoreCacheMaximumSize = 1000;
    private int maxMetastoreRefreshThreads = 100;
    private HostAndPort metastoreSocksProxy;
    private Duration metastoreTimeout = new Duration(10, TimeUnit.SECONDS);

    private Duration ipcPingInterval = new Duration(10, TimeUnit.SECONDS);
    private Duration dfsTimeout = new Duration(60, TimeUnit.SECONDS);
    private Duration dfsConnectTimeout = new Duration(500, TimeUnit.MILLISECONDS);
    private int dfsConnectMaxRetries = 5;
    private boolean verifyChecksum = true;
    private String domainSocketPath;

    private S3FileSystemType s3FileSystemType = S3FileSystemType.PRESTO;

    private HiveStorageFormat hiveStorageFormat = HiveStorageFormat.RCBINARY;
    private HiveCompressionCodec hiveCompressionCodec = HiveCompressionCodec.GZIP;
    private boolean respectTableFormat = true;
    private boolean immutablePartitions;
    private int maxPartitionsPerWriter = 100;
    private int writeValidationThreads = 16;

    private List<String> resourceConfigFiles;

    private boolean useParquetColumnNames;
    private boolean parquetOptimizedReaderEnabled;
    private boolean parquetPredicatePushdownEnabled;

    private boolean assumeCanonicalPartitionKeys;

    private boolean useOrcColumnNames;
    private boolean orcBloomFiltersEnabled;
    private double orcDefaultBloomFilterFpp = 0.05;
    private DataSize orcMaxMergeDistance = new DataSize(1, MEGABYTE);
    private DataSize orcMaxBufferSize = new DataSize(8, MEGABYTE);
    private DataSize orcStreamBufferSize = new DataSize(8, MEGABYTE);
    private DataSize orcMaxReadBlockSize = new DataSize(16, MEGABYTE);
    private boolean orcLazyReadSmallRanges = true;
    private boolean orcOptimizedWriterEnabled;

    private boolean rcfileOptimizedWriterEnabled = true;
    private boolean rcfileWriterValidate;

    private HiveMetastoreAuthenticationType hiveMetastoreAuthenticationType = HiveMetastoreAuthenticationType.NONE;
    private HdfsAuthenticationType hdfsAuthenticationType = HdfsAuthenticationType.NONE;
    private boolean hdfsImpersonationEnabled;

    private boolean skipDeletionForAlter;

    private boolean bucketExecutionEnabled = true;
    private boolean bucketWritingEnabled = true;

    private int fileSystemMaxCacheSize = 1000;

    private boolean writesToNonManagedTablesEnabled;
    private boolean tableStatisticsEnabled = true;

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
    public int getSplitLoaderConcurrency()
    {
        return splitLoaderConcurrency;
    }

    @Config("hive.split-loader-concurrency")
    public HiveClientConfig setSplitLoaderConcurrency(int splitLoaderConcurrency)
    {
        this.splitLoaderConcurrency = splitLoaderConcurrency;
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
    public int getMaxPartitionsPerScan()
    {
        return maxPartitionsPerScan;
    }

    @Config("hive.max-partitions-per-scan")
    @ConfigDescription("Maximum allowed partitions for a single table scan")
    public HiveClientConfig setMaxPartitionsPerScan(int maxPartitionsPerScan)
    {
        this.maxPartitionsPerScan = maxPartitionsPerScan;
        return this;
    }

    @Min(1)
    public int getMaxOutstandingSplits()
    {
        return maxOutstandingSplits;
    }

    @Config("hive.max-outstanding-splits")
    @ConfigDescription("Target number of buffered splits for each table scan in a query, before the scheduler tries to pause itself")
    public HiveClientConfig setMaxOutstandingSplits(int maxOutstandingSplits)
    {
        this.maxOutstandingSplits = maxOutstandingSplits;
        return this;
    }

    @MinDataSize("1MB")
    public DataSize getMaxOutstandingSplitsSize()
    {
        return maxOutstandingSplitsSize;
    }

    @Config("hive.max-outstanding-splits-size")
    @ConfigDescription("Maximum amount of memory allowed for split buffering for each table scan in a query, before the query is failed")
    public HiveClientConfig setMaxOutstandingSplitsSize(DataSize maxOutstandingSplits)
    {
        this.maxOutstandingSplitsSize = maxOutstandingSplits;
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

    public long getMetastoreCacheMaximumSize()
    {
        return metastoreCacheMaximumSize;
    }

    @Min(1)
    @Config("hive.metastore-cache-maximum-size")
    public HiveClientConfig setMetastoreCacheMaximumSize(long metastoreCacheMaximumSize)
    {
        this.metastoreCacheMaximumSize = metastoreCacheMaximumSize;
        return this;
    }

    public long getPerTransactionMetastoreCacheMaximumSize()
    {
        return perTransactionMetastoreCacheMaximumSize;
    }

    @Min(1)
    @Config("hive.per-transaction-metastore-cache-maximum-size")
    public HiveClientConfig setPerTransactionMetastoreCacheMaximumSize(long perTransactionMetastoreCacheMaximumSize)
    {
        this.perTransactionMetastoreCacheMaximumSize = perTransactionMetastoreCacheMaximumSize;
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

    public int getWriteValidationThreads()
    {
        return writeValidationThreads;
    }

    @Config("hive.write-validation-threads")
    @ConfigDescription("Number of threads used for verifying data after a write")
    public HiveClientConfig setWriteValidationThreads(int writeValidationThreads)
    {
        this.writeValidationThreads = writeValidationThreads;
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

    @NotNull
    public S3FileSystemType getS3FileSystemType()
    {
        return s3FileSystemType;
    }

    @Config("hive.s3-file-system-type")
    public HiveClientConfig setS3FileSystemType(S3FileSystemType s3FileSystemType)
    {
        this.s3FileSystemType = s3FileSystemType;
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

    @NotNull
    public DataSize getOrcMaxReadBlockSize()
    {
        return orcMaxReadBlockSize;
    }

    @Config("hive.orc.max-read-block-size")
    public HiveClientConfig setOrcMaxReadBlockSize(DataSize orcMaxReadBlockSize)
    {
        this.orcMaxReadBlockSize = orcMaxReadBlockSize;
        return this;
    }

    @Deprecated
    public boolean isOrcLazyReadSmallRanges()
    {
        return orcLazyReadSmallRanges;
    }

    // TODO remove config option once efficacy is proven
    @Deprecated
    @Config("hive.orc.lazy-read-small-ranges")
    @ConfigDescription("ORC read small disk ranges lazily")
    public HiveClientConfig setOrcLazyReadSmallRanges(boolean orcLazyReadSmallRanges)
    {
        this.orcLazyReadSmallRanges = orcLazyReadSmallRanges;
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

    public double getOrcDefaultBloomFilterFpp()
    {
        return orcDefaultBloomFilterFpp;
    }

    @Config("hive.orc.default-bloom-filter-fpp")
    @ConfigDescription("ORC Bloom filter false positive probability")
    public HiveClientConfig setOrcDefaultBloomFilterFpp(double orcDefaultBloomFilterFpp)
    {
        this.orcDefaultBloomFilterFpp = orcDefaultBloomFilterFpp;
        return this;
    }

    @Deprecated
    public boolean isOrcOptimizedWriterEnabled()
    {
        return orcOptimizedWriterEnabled;
    }

    @Deprecated
    @Config("hive.orc.optimized-writer.enabled")
    public HiveClientConfig setOrcOptimizedWriterEnabled(boolean orcOptimizedWriterEnabled)
    {
        this.orcOptimizedWriterEnabled = orcOptimizedWriterEnabled;
        return this;
    }

    @Deprecated
    public boolean isRcfileOptimizedWriterEnabled()
    {
        return rcfileOptimizedWriterEnabled;
    }

    @Deprecated
    @Config("hive.rcfile-optimized-writer.enabled")
    public HiveClientConfig setRcfileOptimizedWriterEnabled(boolean rcfileOptimizedWriterEnabled)
    {
        this.rcfileOptimizedWriterEnabled = rcfileOptimizedWriterEnabled;
        return this;
    }

    public boolean isRcfileWriterValidate()
    {
        return rcfileWriterValidate;
    }

    @Config("hive.rcfile.writer.validate")
    @ConfigDescription("Validate RCFile after write by re-reading the whole file")
    public HiveClientConfig setRcfileWriterValidate(boolean rcfileWriterValidate)
    {
        this.rcfileWriterValidate = rcfileWriterValidate;
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

    @NotNull
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

    public enum HdfsAuthenticationType
    {
        NONE,
        KERBEROS,
    }

    @NotNull
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
    @ConfigDescription("Enable bucket-aware execution: only use a single worker per bucket")
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

    public int getFileSystemMaxCacheSize()
    {
        return fileSystemMaxCacheSize;
    }

    @Config("hive.fs.cache.max-size")
    @ConfigDescription("Hadoop FileSystem cache size")
    public HiveClientConfig setFileSystemMaxCacheSize(int fileSystemMaxCacheSize)
    {
        this.fileSystemMaxCacheSize = fileSystemMaxCacheSize;
        return this;
    }

    @Config("hive.non-managed-table-writes-enabled")
    @ConfigDescription("Enable writes to non-managed (external) tables")
    public HiveClientConfig setWritesToNonManagedTablesEnabled(boolean writesToNonManagedTablesEnabled)
    {
        this.writesToNonManagedTablesEnabled = writesToNonManagedTablesEnabled;
        return this;
    }

    public boolean getWritesToNonManagedTablesEnabled()
    {
        return writesToNonManagedTablesEnabled;
    }

    @Config("hive.table-statistics-enabled")
    @ConfigDescription("Enable use of table statistics")
    public HiveClientConfig setTableStatisticsEnabled(boolean tableStatisticsEnabled)
    {
        this.tableStatisticsEnabled = tableStatisticsEnabled;
        return this;
    }

    public boolean isTableStatisticsEnabled()
    {
        return tableStatisticsEnabled;
    }
}
