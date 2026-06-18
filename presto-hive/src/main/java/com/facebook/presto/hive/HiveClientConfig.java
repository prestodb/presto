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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.configuration.DefunctConfig;
import com.facebook.airlift.configuration.LegacyConfig;
import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.airlift.units.MaxDataSize;
import com.facebook.airlift.units.MinDataSize;
import com.facebook.airlift.units.MinDuration;
import com.facebook.drift.transport.netty.codec.Protocol;
import com.facebook.presto.hive.s3.S3FileSystemType;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.units.DataSize.Unit.BYTE;
import static com.facebook.airlift.units.DataSize.Unit.KILOBYTE;
import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static com.facebook.presto.hive.BucketFunctionType.HIVE_COMPATIBLE;
import static com.facebook.presto.hive.BucketFunctionType.PRESTO_NATIVE;
import static com.facebook.presto.hive.HiveClientConfig.InsertExistingPartitionsBehavior.APPEND;
import static com.facebook.presto.hive.HiveClientConfig.InsertExistingPartitionsBehavior.ERROR;
import static com.facebook.presto.hive.HiveClientConfig.InsertExistingPartitionsBehavior.OVERWRITE;
import static com.facebook.presto.hive.HiveSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR;
import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

@DefunctConfig({
        "hive.file-system-cache-ttl",
        "hive.max-global-split-iterator-threads",
        "hive.max-sort-files-per-bucket",
        "hive.bucket-writing",
        "hive.parquet.fail-on-corrupted-statistics",
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
    private DataSize writerSortBufferSize = new DataSize(64, MEGABYTE);
    private boolean recursiveDirWalkerEnabled;

    private int maxConcurrentFileRenames = 20;
    private int maxConcurrentZeroRowFileCreations = 20;

    private boolean allowCorruptWritesForTesting;

    private Duration ipcPingInterval = new Duration(10, TimeUnit.SECONDS);
    private Duration dfsTimeout = new Duration(60, TimeUnit.SECONDS);
    private Duration dfsConnectTimeout = new Duration(500, TimeUnit.MILLISECONDS);
    private int dfsConnectMaxRetries = 5;
    private String domainSocketPath;

    private S3FileSystemType s3FileSystemType = S3FileSystemType.PRESTO;

    private HiveStorageFormat hiveStorageFormat = ORC;
    private HiveCompressionCodec compressionCodec = HiveCompressionCodec.GZIP;
    private HiveCompressionCodec orcCompressionCodec = HiveCompressionCodec.GZIP;
    private boolean respectTableFormat = true;
    private boolean immutablePartitions;
    private boolean createEmptyBucketFiles = true;
    private boolean insertOverwriteImmutablePartitions;
    private boolean failFastOnInsertIntoImmutablePartitionsEnabled = true;
    private InsertExistingPartitionsBehavior insertExistingPartitionsBehavior;
    private int maxPartitionsPerWriter = 100;
    private int maxOpenSortFiles = 50;
    private int writeValidationThreads = 16;

    private List<String> resourceConfigFiles = ImmutableList.of();

    private DataSize textMaxLineLength = new DataSize(100, MEGABYTE);
    private boolean assumeCanonicalPartitionKeys;
    private double orcDefaultBloomFilterFpp = 0.05;
    private boolean rcfileOptimizedWriterEnabled = true;
    private boolean rcfileWriterValidate;
    private HdfsAuthenticationType hdfsAuthenticationType = HdfsAuthenticationType.NONE;
    private boolean hdfsImpersonationEnabled;
    private boolean hdfsWireEncryptionEnabled;

    private boolean skipDeletionForAlter;
    private boolean skipTargetCleanupOnRollback;

    private boolean bucketExecutionEnabled = true;
    private boolean sortedWritingEnabled = true;
    private BucketFunctionType bucketFunctionTypeForExchange = HIVE_COMPATIBLE;

    private BucketFunctionType bucketFunctionTypeForCteMaterialization = PRESTO_NATIVE;
    private boolean ignoreTableBucketing;
    private boolean ignoreUnreadablePartition;
    private int minBucketCountToNotIgnoreTableBucketing;
    private int maxBucketsForGroupedExecution = 1_000_000;
    // TODO: Clean up this gatekeeper config and related code/session property once the roll out is done.
    private boolean sortedWriteToTempPathEnabled;
    private int sortedWriteTempPathSubdirectoryCount = 10;

    private int fileSystemMaxCacheSize = 1000;

    private boolean optimizeMismatchedBucketCount;
    private boolean writesToNonManagedTablesEnabled;
    private boolean createsOfNonManagedTablesEnabled = true;

    private boolean tableStatisticsEnabled = true;
    private int partitionStatisticsSampleSize = 100;
    private boolean ignoreCorruptedStatistics;
    private boolean collectColumnStatisticsOnWrite;
    private boolean partitionStatisticsBasedOptimizationEnabled;

    private boolean s3SelectPushdownEnabled;
    private int s3SelectPushdownMaxConnections = 500;
    private boolean orderBasedExecutionEnabled;

    private boolean isTemporaryStagingDirectoryEnabled = true;
    private String temporaryStagingDirectoryPath = "/tmp/presto-${USER}";

    private String temporaryTableSchema = "default";
    private HiveStorageFormat temporaryTableStorageFormat = ORC;
    private HiveCompressionCodec temporaryTableCompressionCodec = HiveCompressionCodec.SNAPPY;
    private boolean shouldCreateEmptyBucketFilesForTemporaryTable;

    private int cteVirtualBucketCount = 128;
    private boolean usePageFileForHiveUnsupportedType = true;

    private boolean pushdownFilterEnabled;
    private boolean parquetPushdownFilterEnabled;
    private boolean adaptiveFilterReorderingEnabled = true;
    private Duration fileStatusCacheExpireAfterWrite = new Duration(0, TimeUnit.SECONDS);
    private DataSize fileStatusCacheMaxRetainedSize = new DataSize(0, KILOBYTE);
    private List<String> fileStatusCacheTables = ImmutableList.of();

    private DataSize pageFileStripeMaxSize = new DataSize(24, MEGABYTE);
    private boolean parquetDereferencePushdownEnabled;

    private boolean isPartialAggregationPushdownEnabled;
    private boolean isPartialAggregationPushdownForVariableLengthDatatypesEnabled;

    private boolean fileRenamingEnabled;
    private boolean preferManifestToListFiles;
    private boolean manifestVerificationEnabled;
    private boolean undoMetastoreOperationsEnabled = true;

    private boolean optimizedPartitionUpdateSerializationEnabled;

    private Duration partitionLeaseDuration = new Duration(0, TimeUnit.SECONDS);

    private boolean enableLooseMemoryAccounting;
    private int materializedViewMissingPartitionsThreshold = 100;

    private boolean verboseRuntimeStatsEnabled;
    private boolean useRecordPageSourceForCustomSplit = true;
    private boolean hudiMetadataEnabled;
    private String hudiTablesUseMergedView;

    private boolean sizeBasedSplitWeightsEnabled = true;
    private double minimumAssignedSplitWeight = 0.05;
    private boolean dynamicSplitSizesEnabled;

    private boolean userDefinedTypeEncodingEnabled;

    private boolean columnIndexFilterEnabled;
    private boolean fileSplittable = true;
    private Protocol thriftProtocol = Protocol.BINARY;
    private DataSize thriftBufferSize = new DataSize(128, BYTE);

    private boolean copyOnFirstWriteConfigurationEnabled;

    private boolean partitionFilteringFromMetastoreEnabled = true;

    private boolean skipEmptyFiles;

    private boolean parallelParsingOfPartitionValuesEnabled;
    private int maxParallelParsingConcurrency = 100;
    private boolean quickStatsEnabled;
    // Duration the initiator query of the quick stats fetch for a partition should wait for stats to be built, before failing and returning EMPTY PartitionStats
    private Duration quickStatsInlineBuildTimeout = new Duration(60, TimeUnit.SECONDS);
    // If an in-progress background build is already observed for a partition, this duration is what the current query will wait for the background build to finish
    // before giving up and returning EMPTY stats
    private Duration quickStatsBackgroundBuildTimeout = new Duration(0, TimeUnit.SECONDS);
    private Duration quickStatsCacheExpiry = new Duration(24, TimeUnit.HOURS);
    private Duration quickStatsInProgressReaperExpiry = new Duration(5, TimeUnit.MINUTES);
    private Duration parquetQuickStatsFileMetadataFetchTimeout = new Duration(60, TimeUnit.SECONDS);
    private int parquetQuickStatsMaxConcurrentCalls = 500;
    private int quickStatsMaxConcurrentCalls = 100;
    private boolean legacyTimestampBucketing;
    private boolean optimizeParsingOfPartitionValues;
    private int optimizeParsingOfPartitionValuesThreshold = 500;
    private boolean symlinkOptimizedReaderEnabled = true;

    @Min(0)
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

    @Min(1)
    public int getMaxConcurrentZeroRowFileCreations()
    {
        return maxConcurrentZeroRowFileCreations;
    }

    @Config("hive.max-concurrent-zero-row-file-creations")
    public HiveClientConfig setMaxConcurrentZeroRowFileCreations(int maxConcurrentZeroRowFileCreations)
    {
        this.maxConcurrentZeroRowFileCreations = maxConcurrentZeroRowFileCreations;
        return this;
    }

    public boolean getRecursiveDirWalkerEnabled()
    {
        return recursiveDirWalkerEnabled;
    }

    @Config("hive.recursive-directories")
    public HiveClientConfig setRecursiveDirWalkerEnabled(boolean recursiveDirWalkerEnabled)
    {
        this.recursiveDirWalkerEnabled = recursiveDirWalkerEnabled;
        return this;
    }

    public boolean isUserDefinedTypeEncodingEnabled()
    {
        return userDefinedTypeEncodingEnabled;
    }

    @Config("hive.user-defined-type-encoding-enabled")
    public HiveClientConfig setUserDefinedTypeEncodingEnabled(boolean userDefinedTypeEncodingEnabled)
    {
        this.userDefinedTypeEncodingEnabled = userDefinedTypeEncodingEnabled;
        return this;
    }

    public DateTimeZone getDateTimeZone()
    {
        return DateTimeZone.forID(timeZone);
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

    @NotNull
    public List<String> getResourceConfigFiles()
    {
        return resourceConfigFiles;
    }

    @Config("hive.config.resources")
    public HiveClientConfig setResourceConfigFiles(String files)
    {
        this.resourceConfigFiles = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(files);
        return this;
    }

    public HiveClientConfig setResourceConfigFiles(List<String> files)
    {
        this.resourceConfigFiles = ImmutableList.copyOf(files);
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

    public HiveCompressionCodec getCompressionCodec()
    {
        return compressionCodec;
    }

    @Config("hive.compression-codec")
    public HiveClientConfig setCompressionCodec(HiveCompressionCodec compressionCodec)
    {
        this.compressionCodec = compressionCodec;
        return this;
    }

    public HiveCompressionCodec getOrcCompressionCodec()
    {
        return orcCompressionCodec;
    }

    @Config("hive.orc-compression-codec")
    public HiveClientConfig setOrcCompressionCodec(HiveCompressionCodec orcCompressionCodec)
    {
        this.orcCompressionCodec = orcCompressionCodec;
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

    @Deprecated
    public boolean isInsertOverwriteImmutablePartitionEnabled()
    {
        return insertOverwriteImmutablePartitions;
    }

    @Deprecated
    @Config("hive.insert-overwrite-immutable-partitions-enabled")
    @ConfigDescription("When enabled, insertion query will overwrite existing partitions when partitions are immutable. This config only takes effect with hive.immutable-partitions set to true")
    public HiveClientConfig setInsertOverwriteImmutablePartitionEnabled(boolean insertOverwriteImmutablePartitions)
    {
        this.insertOverwriteImmutablePartitions = insertOverwriteImmutablePartitions;
        return this;
    }

    public enum InsertExistingPartitionsBehavior
    {
        ERROR,
        APPEND,
        OVERWRITE,
        /**/;

        public static InsertExistingPartitionsBehavior valueOf(String value, boolean immutablePartition)
        {
            InsertExistingPartitionsBehavior enumValue = valueOf(value.toUpperCase(ENGLISH));
            if (immutablePartition) {
                checkArgument(enumValue != APPEND, format("Presto is configured to treat Hive partitions as immutable. %s is not allowed to be set to %s", INSERT_EXISTING_PARTITIONS_BEHAVIOR, APPEND));
            }

            return enumValue;
        }
    }

    public InsertExistingPartitionsBehavior getInsertExistingPartitionsBehavior()
    {
        if (insertExistingPartitionsBehavior != null) {
            return insertExistingPartitionsBehavior;
        }

        return immutablePartitions ? (isInsertOverwriteImmutablePartitionEnabled() ? OVERWRITE : ERROR) : APPEND;
    }

    @Config("hive.insert-existing-partitions-behavior")
    @ConfigDescription("Default value for insert existing partitions behavior")
    public HiveClientConfig setInsertExistingPartitionsBehavior(InsertExistingPartitionsBehavior insertExistingPartitionsBehavior)
    {
        this.insertExistingPartitionsBehavior = insertExistingPartitionsBehavior;
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

    public boolean isCreateEmptyBucketFiles()
    {
        return createEmptyBucketFiles;
    }

    @Config("hive.create-empty-bucket-files")
    @ConfigDescription("Create empty files for buckets that have no data")
    public HiveClientConfig setCreateEmptyBucketFiles(boolean createEmptyBucketFiles)
    {
        this.createEmptyBucketFiles = createEmptyBucketFiles;
        return this;
    }

    public boolean isFailFastOnInsertIntoImmutablePartitionsEnabled()
    {
        return failFastOnInsertIntoImmutablePartitionsEnabled;
    }

    @Config("hive.fail-fast-on-insert-into-immutable-partitions-enabled")
    @ConfigDescription("Fail fast when inserting into an immutable partition. Increases load on the metastore")
    public HiveClientConfig setFailFastOnInsertIntoImmutablePartitionsEnabled(boolean failFastOnInsertIntoImmutablePartitionsEnabled)
    {
        this.failFastOnInsertIntoImmutablePartitionsEnabled = failFastOnInsertIntoImmutablePartitionsEnabled;
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

    @MinDataSize("1B")
    @MaxDataSize("1GB")
    @NotNull
    public DataSize getTextMaxLineLength()
    {
        return textMaxLineLength;
    }

    @Config("hive.text.max-line-length")
    @ConfigDescription("Maximum line length for text files")
    public HiveClientConfig setTextMaxLineLength(DataSize textMaxLineLength)
    {
        this.textMaxLineLength = textMaxLineLength;
        return this;
    }

    @Deprecated
    public boolean isOptimizeMismatchedBucketCount()
    {
        return optimizeMismatchedBucketCount;
    }

    @Deprecated
    @Config("hive.optimize-mismatched-bucket-count")
    public HiveClientConfig setOptimizeMismatchedBucketCount(boolean optimizeMismatchedBucketCount)
    {
        this.optimizeMismatchedBucketCount = optimizeMismatchedBucketCount;
        return this;
    }

    public List<String> getFileStatusCacheTables()
    {
        return fileStatusCacheTables;
    }

    @Config("hive.file-status-cache-tables")
    @ConfigDescription("The tables that have file status cache enabled. Setting to '*' includes all tables.")
    public HiveClientConfig setFileStatusCacheTables(String fileStatusCacheTables)
    {
        this.fileStatusCacheTables = SPLITTER.splitToList(fileStatusCacheTables);
        return this;
    }

    public DataSize getFileStatusCacheMaxRetainedSize()
    {
        return fileStatusCacheMaxRetainedSize;
    }

    @Config("hive.file-status-cache.max-retained-size")
    public HiveClientConfig setFileStatusCacheMaxRetainedSize(DataSize fileStatusCacheMaxRetainedSize)
    {
        this.fileStatusCacheMaxRetainedSize = fileStatusCacheMaxRetainedSize;
        return this;
    }

    public Duration getFileStatusCacheExpireAfterWrite()
    {
        return fileStatusCacheExpireAfterWrite;
    }

    @Config("hive.file-status-cache-expire-time")
    public HiveClientConfig setFileStatusCacheExpireAfterWrite(Duration fileStatusCacheExpireAfterWrite)
    {
        this.fileStatusCacheExpireAfterWrite = fileStatusCacheExpireAfterWrite;
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

    public boolean isHdfsWireEncryptionEnabled()
    {
        return hdfsWireEncryptionEnabled;
    }

    @Config("hive.hdfs.wire-encryption.enabled")
    @ConfigDescription("Should be turned on when HDFS wire encryption is enabled")
    public HiveClientConfig setHdfsWireEncryptionEnabled(boolean hdfsWireEncryptionEnabled)
    {
        this.hdfsWireEncryptionEnabled = hdfsWireEncryptionEnabled;
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

    public boolean isSkipTargetCleanupOnRollback()
    {
        return skipTargetCleanupOnRollback;
    }

    @Config("hive.skip-target-cleanup-on-rollback")
    @ConfigDescription("Skip deletion of target directories when a metastore operation fails and the write mode is DIRECT_TO_TARGET_NEW_DIRECTORY")
    public HiveClientConfig setSkipTargetCleanupOnRollback(boolean skipTargetCleanupOnRollback)
    {
        this.skipTargetCleanupOnRollback = skipTargetCleanupOnRollback;
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

    public boolean isSortedWritingEnabled()
    {
        return sortedWritingEnabled;
    }

    @Config("hive.sorted-writing")
    @ConfigDescription("Enable writing to bucketed sorted tables")
    public HiveClientConfig setSortedWritingEnabled(boolean sortedWritingEnabled)
    {
        this.sortedWritingEnabled = sortedWritingEnabled;
        return this;
    }

    @Config("hive.bucket-function-type-for-exchange")
    @ConfigDescription("Hash function type for exchange")
    public HiveClientConfig setBucketFunctionTypeForExchange(BucketFunctionType bucketFunctionTypeForExchange)
    {
        this.bucketFunctionTypeForExchange = bucketFunctionTypeForExchange;
        return this;
    }

    public BucketFunctionType getBucketFunctionTypeForExchange()
    {
        return bucketFunctionTypeForExchange;
    }

    @Config("hive.bucket-function-type-for-cte-materialization")
    @ConfigDescription("Hash function type for cte materialization")
    public HiveClientConfig setBucketFunctionTypeForCteMaterialization(BucketFunctionType bucketFunctionTypeForCteMaterialization)
    {
        this.bucketFunctionTypeForCteMaterialization = bucketFunctionTypeForCteMaterialization;
        return this;
    }

    public BucketFunctionType getBucketFunctionTypeForCteMaterialization()
    {
        return bucketFunctionTypeForCteMaterialization;
    }

    @Config("hive.ignore-unreadable-partition")
    @ConfigDescription("Ignore unreadable partitions and report as warnings instead of failing the query")
    public HiveClientConfig setIgnoreUnreadablePartition(boolean ignoreUnreadablePartition)
    {
        this.ignoreUnreadablePartition = ignoreUnreadablePartition;
        return this;
    }

    public boolean isIgnoreUnreadablePartition()
    {
        return ignoreUnreadablePartition;
    }

    @Config("hive.ignore-table-bucketing")
    @ConfigDescription("Ignore table bucketing to allow reading from unbucketed partitions")
    public HiveClientConfig setIgnoreTableBucketing(boolean ignoreTableBucketing)
    {
        this.ignoreTableBucketing = ignoreTableBucketing;
        return this;
    }

    public boolean isIgnoreTableBucketing()
    {
        return ignoreTableBucketing;
    }

    @Config("hive.min-bucket-count-to-not-ignore-table-bucketing")
    @ConfigDescription("Ignore table bucketing when table bucket count is less than the value specified, " +
            "otherwise, it is controlled by property hive.ignore-table-bucketing")
    public HiveClientConfig setMinBucketCountToNotIgnoreTableBucketing(int minBucketCountToNotIgnoreTableBucketing)
    {
        this.minBucketCountToNotIgnoreTableBucketing = minBucketCountToNotIgnoreTableBucketing;
        return this;
    }

    public int getMinBucketCountToNotIgnoreTableBucketing()
    {
        return minBucketCountToNotIgnoreTableBucketing;
    }

    @Config("hive.max-buckets-for-grouped-execution")
    @ConfigDescription("Maximum number of buckets to run with grouped execution")
    public HiveClientConfig setMaxBucketsForGroupedExecution(int maxBucketsForGroupedExecution)
    {
        this.maxBucketsForGroupedExecution = maxBucketsForGroupedExecution;
        return this;
    }

    public int getMaxBucketsForGroupedExecution()
    {
        return maxBucketsForGroupedExecution;
    }

    @Config("hive.sorted-write-to-temp-path-enabled")
    @ConfigDescription("Enable writing temp files to temp path when writing to bucketed sorted tables")
    public HiveClientConfig setSortedWriteToTempPathEnabled(boolean sortedWriteToTempPathEnabled)
    {
        this.sortedWriteToTempPathEnabled = sortedWriteToTempPathEnabled;
        return this;
    }

    public boolean isSortedWriteToTempPathEnabled()
    {
        return sortedWriteToTempPathEnabled;
    }

    @Config("hive.sorted-write-temp-path-subdirectory-count")
    @ConfigDescription("Number of directories per partition for temp files generated by writing sorted table")
    public HiveClientConfig setSortedWriteTempPathSubdirectoryCount(int sortedWriteTempPathSubdirectoryCount)
    {
        this.sortedWriteTempPathSubdirectoryCount = sortedWriteTempPathSubdirectoryCount;
        return this;
    }

    public int getSortedWriteTempPathSubdirectoryCount()
    {
        return sortedWriteTempPathSubdirectoryCount;
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

    public boolean getWritesToNonManagedTablesEnabled()
    {
        return writesToNonManagedTablesEnabled;
    }

    @Config("hive.non-managed-table-writes-enabled")
    @ConfigDescription("Enable writes to non-managed (external) tables")
    public HiveClientConfig setWritesToNonManagedTablesEnabled(boolean writesToNonManagedTablesEnabled)
    {
        this.writesToNonManagedTablesEnabled = writesToNonManagedTablesEnabled;
        return this;
    }

    public boolean getCreatesOfNonManagedTablesEnabled()
    {
        return createsOfNonManagedTablesEnabled;
    }

    @Config("hive.non-managed-table-creates-enabled")
    @ConfigDescription("Enable non-managed (external) table creates")
    public HiveClientConfig setCreatesOfNonManagedTablesEnabled(boolean createsOfNonManagedTablesEnabled)
    {
        this.createsOfNonManagedTablesEnabled = createsOfNonManagedTablesEnabled;
        return this;
    }

    public boolean isTableStatisticsEnabled()
    {
        return tableStatisticsEnabled;
    }

    @Config("hive.table-statistics-enabled")
    @ConfigDescription("Enable use of table statistics")
    public HiveClientConfig setTableStatisticsEnabled(boolean tableStatisticsEnabled)
    {
        this.tableStatisticsEnabled = tableStatisticsEnabled;
        return this;
    }

    @Min(1)
    public int getPartitionStatisticsSampleSize()
    {
        return partitionStatisticsSampleSize;
    }

    @Config("hive.partition-statistics-sample-size")
    @ConfigDescription("Maximum sample size of the partitions column statistics")
    public HiveClientConfig setPartitionStatisticsSampleSize(int partitionStatisticsSampleSize)
    {
        this.partitionStatisticsSampleSize = partitionStatisticsSampleSize;
        return this;
    }

    public boolean isIgnoreCorruptedStatistics()
    {
        return ignoreCorruptedStatistics;
    }

    @Config("hive.ignore-corrupted-statistics")
    @ConfigDescription("Ignore corrupted statistics rather than failing")
    public HiveClientConfig setIgnoreCorruptedStatistics(boolean ignoreCorruptedStatistics)
    {
        this.ignoreCorruptedStatistics = ignoreCorruptedStatistics;
        return this;
    }

    public boolean isCollectColumnStatisticsOnWrite()
    {
        return collectColumnStatisticsOnWrite;
    }

    @Config("hive.collect-column-statistics-on-write")
    @ConfigDescription("Enables automatic column level statistics collection on write")
    public HiveClientConfig setCollectColumnStatisticsOnWrite(boolean collectColumnStatisticsOnWrite)
    {
        this.collectColumnStatisticsOnWrite = collectColumnStatisticsOnWrite;
        return this;
    }

    public boolean isPartitionStatisticsBasedOptimizationEnabled()
    {
        return partitionStatisticsBasedOptimizationEnabled;
    }

    @Config("hive.partition-statistics-based-optimization-enabled")
    @ConfigDescription("Enables partition statistics based optimization, including partition pruning and predicate stripping")
    public HiveClientConfig setPartitionStatisticsBasedOptimizationEnabled(boolean partitionStatisticsBasedOptimizationEnabled)
    {
        this.partitionStatisticsBasedOptimizationEnabled = partitionStatisticsBasedOptimizationEnabled;
        return this;
    }

    public boolean isS3SelectPushdownEnabled()
    {
        return s3SelectPushdownEnabled;
    }

    @Config("hive.s3select-pushdown.enabled")
    @ConfigDescription("Enable query pushdown to AWS S3 Select service")
    public HiveClientConfig setS3SelectPushdownEnabled(boolean s3SelectPushdownEnabled)
    {
        this.s3SelectPushdownEnabled = s3SelectPushdownEnabled;
        return this;
    }

    @Min(1)
    public int getS3SelectPushdownMaxConnections()
    {
        return s3SelectPushdownMaxConnections;
    }

    @Config("hive.s3select-pushdown.max-connections")
    public HiveClientConfig setS3SelectPushdownMaxConnections(int s3SelectPushdownMaxConnections)
    {
        this.s3SelectPushdownMaxConnections = s3SelectPushdownMaxConnections;
        return this;
    }

    public boolean isOrderBasedExecutionEnabled()
    {
        return orderBasedExecutionEnabled;
    }

    @Config("hive.order-based-execution-enabled")
    @ConfigDescription("Enable order-based execution. When it's enabled, hive files become non-splittable and the table ordering properties would be exposed to plan optimizer " +
            "for features like streaming aggregation and merge join")
    public HiveClientConfig setOrderBasedExecutionEnabled(boolean orderBasedExecutionEnabled)
    {
        this.orderBasedExecutionEnabled = orderBasedExecutionEnabled;
        return this;
    }

    public boolean isTemporaryStagingDirectoryEnabled()
    {
        return isTemporaryStagingDirectoryEnabled;
    }

    @Config("hive.temporary-staging-directory-enabled")
    @ConfigDescription("Should use (if possible) temporary staging directory for write operations")
    public HiveClientConfig setTemporaryStagingDirectoryEnabled(boolean temporaryStagingDirectoryEnabled)
    {
        this.isTemporaryStagingDirectoryEnabled = temporaryStagingDirectoryEnabled;
        return this;
    }

    @NotNull
    public String getTemporaryStagingDirectoryPath()
    {
        return temporaryStagingDirectoryPath;
    }

    @Config("hive.temporary-staging-directory-path")
    @ConfigDescription("Location of temporary staging directory for write operations. Use ${USER} placeholder to use different location for each user.")
    public HiveClientConfig setTemporaryStagingDirectoryPath(String temporaryStagingDirectoryPath)
    {
        this.temporaryStagingDirectoryPath = temporaryStagingDirectoryPath;
        return this;
    }

    @NotNull
    public String getTemporaryTableSchema()
    {
        return temporaryTableSchema;
    }

    @Config("hive.temporary-table-schema")
    public HiveClientConfig setTemporaryTableSchema(String temporaryTableSchema)
    {
        this.temporaryTableSchema = temporaryTableSchema;
        return this;
    }

    @NotNull
    public HiveStorageFormat getTemporaryTableStorageFormat()
    {
        return temporaryTableStorageFormat;
    }

    @Config("hive.temporary-table-storage-format")
    public HiveClientConfig setTemporaryTableStorageFormat(HiveStorageFormat temporaryTableStorageFormat)
    {
        this.temporaryTableStorageFormat = temporaryTableStorageFormat;
        return this;
    }

    @NotNull
    public HiveCompressionCodec getTemporaryTableCompressionCodec()
    {
        return temporaryTableCompressionCodec;
    }

    @Config("hive.temporary-table-compression-codec")
    public HiveClientConfig setTemporaryTableCompressionCodec(HiveCompressionCodec temporaryTableCompressionCodec)
    {
        this.temporaryTableCompressionCodec = temporaryTableCompressionCodec;
        return this;
    }

    public boolean isCreateEmptyBucketFilesForTemporaryTable()
    {
        return shouldCreateEmptyBucketFilesForTemporaryTable;
    }

    @Config("hive.create-empty-bucket-files-for-temporary-table")
    @ConfigDescription("Create empty files when there is no data for temporary table buckets")
    public HiveClientConfig setCreateEmptyBucketFilesForTemporaryTable(boolean shouldCreateEmptyBucketFilesForTemporaryTable)
    {
        this.shouldCreateEmptyBucketFilesForTemporaryTable = shouldCreateEmptyBucketFilesForTemporaryTable;
        return this;
    }

    public boolean getUsePageFileForHiveUnsupportedType()
    {
        return usePageFileForHiveUnsupportedType;
    }

    @Config("hive.use-pagefile-for-hive-unsupported-type")
    public HiveClientConfig setUsePageFileForHiveUnsupportedType(boolean usePageFileForHiveUnsupportedType)
    {
        this.usePageFileForHiveUnsupportedType = usePageFileForHiveUnsupportedType;
        return this;
    }

    public boolean isPushdownFilterEnabled()
    {
        return pushdownFilterEnabled;
    }

    @Config("hive.pushdown-filter-enabled")
    @ConfigDescription("Experimental: enable complex filter pushdown")
    public HiveClientConfig setPushdownFilterEnabled(boolean pushdownFilterEnabled)
    {
        this.pushdownFilterEnabled = pushdownFilterEnabled;
        return this;
    }

    @NotNull
    public boolean isParquetPushdownFilterEnabled()
    {
        return parquetPushdownFilterEnabled;
    }

    @Config("hive.cte-virtual-bucket-count")
    @ConfigDescription("Number of buckets allocated per materialized CTE. (Recommended value: 4 - 10x times the size of the cluster)")
    public HiveClientConfig setCteVirtualBucketCount(int cteVirtualBucketCount)
    {
        this.cteVirtualBucketCount = cteVirtualBucketCount;
        return this;
    }

    @NotNull
    public int getCteVirtualBucketCount()
    {
        return cteVirtualBucketCount;
    }

    @Config("hive.parquet.pushdown-filter-enabled")
    @ConfigDescription("Experimental: enable complex filter pushdown for Parquet")
    public HiveClientConfig setParquetPushdownFilterEnabled(boolean parquetPushdownFilterEnabled)
    {
        this.parquetPushdownFilterEnabled = parquetPushdownFilterEnabled;
        return this;
    }

    public boolean isAdaptiveFilterReorderingEnabled()
    {
        return adaptiveFilterReorderingEnabled;
    }

    @Config("hive.adaptive-filter-reordering-enabled")
    @ConfigDescription("Experimental: enable adaptive filter reordering")
    public HiveClientConfig setAdaptiveFilterReorderingEnabled(boolean adaptiveFilterReorderingEnabled)
    {
        this.adaptiveFilterReorderingEnabled = adaptiveFilterReorderingEnabled;
        return this;
    }

    public DataSize getPageFileStripeMaxSize()
    {
        return pageFileStripeMaxSize;
    }

    @Config("hive.pagefile.writer.stripe-max-size")
    public HiveClientConfig setPageFileStripeMaxSize(DataSize pageFileStripeMaxSize)
    {
        this.pageFileStripeMaxSize = pageFileStripeMaxSize;
        return this;
    }

    @Config("hive.enable-parquet-dereference-pushdown")
    @ConfigDescription("enable parquet dereference pushdown")
    public HiveClientConfig setParquetDereferencePushdownEnabled(boolean parquetDereferencePushdownEnabled)
    {
        this.parquetDereferencePushdownEnabled = parquetDereferencePushdownEnabled;
        return this;
    }

    public boolean isParquetDereferencePushdownEnabled()
    {
        return this.parquetDereferencePushdownEnabled;
    }

    @Config("hive.partial_aggregation_pushdown_enabled")
    @ConfigDescription("enable partial aggregation pushdown")
    public HiveClientConfig setPartialAggregationPushdownEnabled(boolean partialAggregationPushdownEnabled)
    {
        this.isPartialAggregationPushdownEnabled = partialAggregationPushdownEnabled;
        return this;
    }

    public boolean isPartialAggregationPushdownEnabled()
    {
        return this.isPartialAggregationPushdownEnabled;
    }

    @Config("hive.partial_aggregation_pushdown_for_variable_length_datatypes_enabled")
    @ConfigDescription("enable partial aggregation pushdown for variable length datatypes")
    public HiveClientConfig setPartialAggregationPushdownForVariableLengthDatatypesEnabled(boolean partialAggregationPushdownForVariableLengthDatatypesEnabled)
    {
        this.isPartialAggregationPushdownForVariableLengthDatatypesEnabled = partialAggregationPushdownForVariableLengthDatatypesEnabled;
        return this;
    }

    public boolean isPartialAggregationPushdownForVariableLengthDatatypesEnabled()
    {
        return this.isPartialAggregationPushdownForVariableLengthDatatypesEnabled;
    }

    @Config("hive.file_renaming_enabled")
    @ConfigDescription("enable file renaming")
    public HiveClientConfig setFileRenamingEnabled(boolean fileRenamingEnabled)
    {
        this.fileRenamingEnabled = fileRenamingEnabled;
        return this;
    }

    public boolean isFileRenamingEnabled()
    {
        return this.fileRenamingEnabled;
    }

    @Config("hive.prefer-manifests-to-list-files")
    @ConfigDescription("Prefer to fetch the list of file names and sizes from manifests rather than storage")
    public HiveClientConfig setPreferManifestsToListFiles(boolean preferManifestToListFiles)
    {
        this.preferManifestToListFiles = preferManifestToListFiles;
        return this;
    }

    public boolean isPreferManifestsToListFiles()
    {
        return this.preferManifestToListFiles;
    }

    @Config("hive.manifest-verification-enabled")
    @ConfigDescription("Enable verification of file names and sizes in manifest / partition parameters")
    public HiveClientConfig setManifestVerificationEnabled(boolean manifestVerificationEnabled)
    {
        this.manifestVerificationEnabled = manifestVerificationEnabled;
        return this;
    }

    public boolean isManifestVerificationEnabled()
    {
        return this.manifestVerificationEnabled;
    }

    @Config("hive.undo-metastore-operations-enabled")
    @ConfigDescription("Enable undo metastore operations")
    public HiveClientConfig setUndoMetastoreOperationsEnabled(boolean undoMetastoreOperationsEnabled)
    {
        this.undoMetastoreOperationsEnabled = undoMetastoreOperationsEnabled;
        return this;
    }

    public boolean isUndoMetastoreOperationsEnabled()
    {
        return undoMetastoreOperationsEnabled;
    }

    @Config("hive.experimental-optimized-partition-update-serialization-enabled")
    @ConfigDescription("Serialize PartitionUpdate objects using binary SMILE encoding and compress with the ZSTD compression")
    public HiveClientConfig setOptimizedPartitionUpdateSerializationEnabled(boolean optimizedPartitionUpdateSerializationEnabled)
    {
        this.optimizedPartitionUpdateSerializationEnabled = optimizedPartitionUpdateSerializationEnabled;
        return this;
    }

    public boolean isOptimizedPartitionUpdateSerializationEnabled()
    {
        return optimizedPartitionUpdateSerializationEnabled;
    }

    @Config("hive.verbose-runtime-stats-enabled")
    @ConfigDescription("Enable tracking all runtime stats. Note that this may affect query performance")
    public HiveClientConfig setVerboseRuntimeStatsEnabled(boolean verboseRuntimeStatsEnabled)
    {
        this.verboseRuntimeStatsEnabled = verboseRuntimeStatsEnabled;
        return this;
    }

    public boolean isVerboseRuntimeStatsEnabled()
    {
        return verboseRuntimeStatsEnabled;
    }

    @Config("hive.partition-lease-duration")
    @ConfigDescription("Partition lease duration")
    public HiveClientConfig setPartitionLeaseDuration(Duration partitionLeaseDuration)
    {
        this.partitionLeaseDuration = partitionLeaseDuration;
        return this;
    }

    public Duration getPartitionLeaseDuration()
    {
        return partitionLeaseDuration;
    }

    public boolean isLooseMemoryAccountingEnabled()
    {
        return enableLooseMemoryAccounting;
    }

    @Config("hive.loose-memory-accounting-enabled")
    @ConfigDescription("When enabled relaxes memory accounting for queries violating memory limits to run that previously honored memory thresholds.")
    public HiveClientConfig setLooseMemoryAccountingEnabled(boolean enableLooseMemoryAccounting)
    {
        this.enableLooseMemoryAccounting = enableLooseMemoryAccounting;
        return this;
    }

    @Config("hive.materialized-view-missing-partitions-threshold")
    @ConfigDescription("Materialized views with missing partitions more than this threshold falls back to the base tables at read time")
    public HiveClientConfig setMaterializedViewMissingPartitionsThreshold(int materializedViewMissingPartitionsThreshold)
    {
        this.materializedViewMissingPartitionsThreshold = materializedViewMissingPartitionsThreshold;
        return this;
    }

    public int getMaterializedViewMissingPartitionsThreshold()
    {
        return this.materializedViewMissingPartitionsThreshold;
    }

    @Config("hive.parquet-column-index-filter-enabled")
    @ConfigDescription("enable using parquet column index filter")
    public HiveClientConfig setReadColumnIndexFilter(boolean columnIndexFilterEnabled)
    {
        this.columnIndexFilterEnabled = columnIndexFilterEnabled;
        return this;
    }

    public boolean getReadColumnIndexFilter()
    {
        return this.columnIndexFilterEnabled;
    }

    @Config("hive.size-based-split-weights-enabled")
    public HiveClientConfig setSizeBasedSplitWeightsEnabled(boolean sizeBasedSplitWeightsEnabled)
    {
        this.sizeBasedSplitWeightsEnabled = sizeBasedSplitWeightsEnabled;
        return this;
    }

    public boolean isSizeBasedSplitWeightsEnabled()
    {
        return sizeBasedSplitWeightsEnabled;
    }

    @Config("hive.dynamic-split-sizes-enabled")
    public HiveClientConfig setDynamicSplitSizesEnabled(boolean dynamicSplitSizesEnabled)
    {
        this.dynamicSplitSizesEnabled = dynamicSplitSizesEnabled;
        return this;
    }

    public boolean isDynamicSplitSizesEnabled()
    {
        return dynamicSplitSizesEnabled;
    }

    @Config("hive.minimum-assigned-split-weight")
    @ConfigDescription("Minimum weight that a split can be assigned when size based split weights are enabled")
    public HiveClientConfig setMinimumAssignedSplitWeight(double minimumAssignedSplitWeight)
    {
        this.minimumAssignedSplitWeight = minimumAssignedSplitWeight;
        return this;
    }

    @DecimalMax("1.0") // standard split weight
    @DecimalMin(value = "0", inclusive = false)
    public double getMinimumAssignedSplitWeight()
    {
        return minimumAssignedSplitWeight;
    }

    public boolean isUseRecordPageSourceForCustomSplit()
    {
        return this.useRecordPageSourceForCustomSplit;
    }

    @Config("hive.use-record-page-source-for-custom-split")
    @ConfigDescription("Use record page source for custom split. By default, true. Used to query MOR tables in Hudi.")
    public HiveClientConfig setUseRecordPageSourceForCustomSplit(boolean useRecordPageSourceForCustomSplit)
    {
        this.useRecordPageSourceForCustomSplit = useRecordPageSourceForCustomSplit;
        return this;
    }

    public boolean isFileSplittable()
    {
        return fileSplittable;
    }

    @Config("hive.file-splittable")
    @ConfigDescription("By default, this value is true. Set to false to make a hive file un-splittable when coordinator schedules splits.")
    public HiveClientConfig setFileSplittable(boolean fileSplittable)
    {
        this.fileSplittable = fileSplittable;
        return this;
    }

    @Config("hive.hudi-metadata-enabled")
    @ConfigDescription("For Hudi tables prefer to fetch the list of file names, sizes and other metadata from the internal metadata table rather than storage")
    public HiveClientConfig setHudiMetadataEnabled(boolean hudiMetadataEnabled)
    {
        this.hudiMetadataEnabled = hudiMetadataEnabled;
        return this;
    }

    public boolean isHudiMetadataEnabled()
    {
        return this.hudiMetadataEnabled;
    }

    @Config("hive.hudi-tables-use-merged-view")
    @ConfigDescription("For Hudi tables, a comma-separated list in the form of <schema>.<table> which should prefer to fetch the list of files from the merged file system view")
    public HiveClientConfig setHudiTablesUseMergedView(String hudiTablesUseMergedView)
    {
        this.hudiTablesUseMergedView = hudiTablesUseMergedView;
        return this;
    }

    public String getHudiTablesUseMergedView()
    {
        return this.hudiTablesUseMergedView;
    }

    @Config("hive.quick-stats.enabled")
    @ConfigDescription("Use quick stats to resolve stats")
    public HiveClientConfig setQuickStatsEnabled(boolean quickStatsEnabled)
    {
        this.quickStatsEnabled = quickStatsEnabled;
        return this;
    }

    public boolean isQuickStatsEnabled()
    {
        return this.quickStatsEnabled;
    }

    @Config("hive.quick-stats.inline-build-timeout")
    public HiveClientConfig setQuickStatsInlineBuildTimeout(Duration buildTimeout)
    {
        this.quickStatsInlineBuildTimeout = buildTimeout;
        return this;
    }

    public Duration getQuickStatsInlineBuildTimeout()
    {
        return this.quickStatsInlineBuildTimeout;
    }

    @Config("hive.quick-stats.background-build-timeout")
    public HiveClientConfig setQuickStatsBackgroundBuildTimeout(Duration buildTimeout)
    {
        this.quickStatsBackgroundBuildTimeout = buildTimeout;
        return this;
    }

    public Duration getQuickStatsBackgroundBuildTimeout()
    {
        return this.quickStatsBackgroundBuildTimeout;
    }

    @Config("hive.quick-stats.cache-expiry")
    public HiveClientConfig setQuickStatsCacheExpiry(Duration cacheExpiry)
    {
        this.quickStatsCacheExpiry = cacheExpiry;
        return this;
    }

    public Duration getQuickStatsCacheExpiry()
    {
        return this.quickStatsCacheExpiry;
    }

    @Config("hive.quick-stats.reaper-expiry")
    public HiveClientConfig setQuickStatsReaperExpiry(Duration reaperExpiry)
    {
        this.quickStatsInProgressReaperExpiry = reaperExpiry;
        return this;
    }

    public Duration getQuickStatsReaperExpiry()
    {
        return this.quickStatsInProgressReaperExpiry;
    }

    @Config("hive.quick-stats.parquet.file-metadata-fetch-timeout")
    public HiveClientConfig setParquetQuickStatsFileMetadataFetchTimeout(Duration fileMetadataFetchTimeout)
    {
        this.parquetQuickStatsFileMetadataFetchTimeout = fileMetadataFetchTimeout;
        return this;
    }

    public Duration getParquetQuickStatsFileMetadataFetchTimeout()
    {
        return this.parquetQuickStatsFileMetadataFetchTimeout;
    }

    @Min(1)
    public int getMaxConcurrentParquetQuickStatsCalls()
    {
        return parquetQuickStatsMaxConcurrentCalls;
    }

    @Config("hive.quick-stats.parquet.max-concurrent-calls")
    public HiveClientConfig setMaxConcurrentParquetQuickStatsCalls(int maxConcurrentCalls)
    {
        this.parquetQuickStatsMaxConcurrentCalls = maxConcurrentCalls;
        return this;
    }

    @Min(1)
    public int getMaxConcurrentQuickStatsCalls()
    {
        return quickStatsMaxConcurrentCalls;
    }

    @Config("hive.quick-stats.max-concurrent-calls")
    public HiveClientConfig setMaxConcurrentQuickStatsCalls(int maxConcurrentFooterFetchCalls)
    {
        this.quickStatsMaxConcurrentCalls = maxConcurrentFooterFetchCalls;
        return this;
    }

    public Protocol getThriftProtocol()
    {
        return thriftProtocol;
    }

    @Config("hive.internal-communication.thrift-transport-protocol")
    @ConfigDescription("Thrift encoding type for internal communication")
    public HiveClientConfig setThriftProtocol(Protocol thriftProtocol)
    {
        this.thriftProtocol = thriftProtocol;
        return this;
    }

    public DataSize getThriftBufferSize()
    {
        return thriftBufferSize;
    }

    @Config("hive.internal-communication.thrift-transport-buffer-size")
    @ConfigDescription("Thrift buffer size for internal communication")
    public HiveClientConfig setThriftBufferSize(DataSize thriftBufferSize)
    {
        this.thriftBufferSize = thriftBufferSize;
        return this;
    }

    @Config("hive.copy-on-first-write-configuration-enabled")
    @ConfigDescription("Optimize the number of configuration copies by enabling copy-on-write technique")
    public HiveClientConfig setCopyOnFirstWriteConfigurationEnabled(boolean copyOnFirstWriteConfigurationEnabled)
    {
        this.copyOnFirstWriteConfigurationEnabled = copyOnFirstWriteConfigurationEnabled;
        return this;
    }

    public boolean isCopyOnFirstWriteConfigurationEnabled()
    {
        return copyOnFirstWriteConfigurationEnabled;
    }

    public boolean isPartitionFilteringFromMetastoreEnabled()
    {
        return partitionFilteringFromMetastoreEnabled;
    }

    @Config("hive.partition-filtering-from-metastore-enabled")
    @ConfigDescription("When enabled attempts to retrieve partition metadata only for partitions that satisfy the query predicates")
    public HiveClientConfig setPartitionFilteringFromMetastoreEnabled(boolean partitionFilteringFromMetastoreEnabled)
    {
        this.partitionFilteringFromMetastoreEnabled = partitionFilteringFromMetastoreEnabled;
        return this;
    }

    @Config("hive.parallel-parsing-of-partition-values-enabled")
    @ConfigDescription("Enables parallel parsing of partition values using a thread pool")
    public HiveClientConfig setParallelParsingOfPartitionValuesEnabled(boolean parallelParsingOfPartitionValuesEnabled)
    {
        this.parallelParsingOfPartitionValuesEnabled = parallelParsingOfPartitionValuesEnabled;
        return this;
    }

    public boolean isParallelParsingOfPartitionValuesEnabled()
    {
        return this.parallelParsingOfPartitionValuesEnabled;
    }

    @Config("hive.max-parallel-parsing-concurrency")
    @ConfigDescription("Maximum size of the thread pool used for parallel parsing of partition values")
    public HiveClientConfig setMaxParallelParsingConcurrency(int maxParallelParsingConcurrency)
    {
        this.maxParallelParsingConcurrency = maxParallelParsingConcurrency;
        return this;
    }

    public int getMaxParallelParsingConcurrency()
    {
        return this.maxParallelParsingConcurrency;
    }

    @Config("hive.skip-empty-files")
    @ConfigDescription("Enables skip of empty files avoiding output error")
    public HiveClientConfig setSkipEmptyFilesEnabled(boolean skipEmptyFiles)
    {
        this.skipEmptyFiles = skipEmptyFiles;
        return this;
    }

    public boolean isSkipEmptyFilesEnabled()
    {
        return this.skipEmptyFiles;
    }

    public boolean isLegacyTimestampBucketing()
    {
        return legacyTimestampBucketing;
    }

    @Config("hive.legacy-timestamp-bucketing")
    @ConfigDescription("Use legacy timestamp bucketing algorithm (which is not Hive compatible) for table bucketed by timestamp type.")
    public HiveClientConfig setLegacyTimestampBucketing(boolean legacyTimestampBucketing)
    {
        this.legacyTimestampBucketing = legacyTimestampBucketing;
        return this;
    }

    @Config("hive.optimize-parsing-of-partition-values-enabled")
    @ConfigDescription("Enables optimization of parsing partition values when number of candidate partitions is large")
    public HiveClientConfig setOptimizeParsingOfPartitionValues(boolean optimizeParsingOfPartitionValues)
    {
        this.optimizeParsingOfPartitionValues = optimizeParsingOfPartitionValues;
        return this;
    }

    public boolean isOptimizeParsingOfPartitionValues()
    {
        return optimizeParsingOfPartitionValues;
    }

    @Config("hive.optimize-parsing-of-partition-values-threshold")
    @ConfigDescription("Enables optimization of parsing partition values when number of candidate partitions exceed the threshold set here")
    public HiveClientConfig setOptimizeParsingOfPartitionValuesThreshold(int optimizeParsingOfPartitionValuesThreshold)
    {
        this.optimizeParsingOfPartitionValuesThreshold = optimizeParsingOfPartitionValuesThreshold;
        return this;
    }

    @Min(1)
    public int getOptimizeParsingOfPartitionValuesThreshold()
    {
        return optimizeParsingOfPartitionValuesThreshold;
    }

    public boolean isSymlinkOptimizedReaderEnabled()
    {
        return symlinkOptimizedReaderEnabled;
    }

    @Config("hive.experimental.symlink.optimized-reader.enabled")
    @ConfigDescription("Experimental: Enable optimized SymlinkTextInputFormat reader")
    public HiveClientConfig setSymlinkOptimizedReaderEnabled(boolean symlinkOptimizedReaderEnabled)
    {
        this.symlinkOptimizedReaderEnabled = symlinkOptimizedReaderEnabled;
        return this;
    }
}
