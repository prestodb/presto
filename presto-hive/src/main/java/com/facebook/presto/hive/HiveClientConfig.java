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
import com.facebook.presto.hive.s3.S3FileSystemType;
import com.facebook.presto.orc.OrcWriteValidation.OrcWriteValidationMode;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;
import io.airlift.units.MinDuration;
import org.joda.time.DateTimeZone;

import javax.validation.constraints.DecimalMax;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.BucketFunctionType.HIVE_COMPATIBLE;
import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

@DefunctConfig({
        "hive.file-system-cache-ttl",
        "hive.max-global-split-iterator-threads",
        "hive.max-sort-files-per-bucket",
        "hive.bucket-writing",
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
    private NodeSelectionStrategy nodeSelectionStrategy = NO_PREFERENCE;
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
    private boolean insertOverwriteImmutablePartitions;
    private boolean failFastOnInsertIntoImmutablePartitionsEnabled = true;
    private int maxPartitionsPerWriter = 100;
    private int maxOpenSortFiles = 50;
    private int writeValidationThreads = 16;

    private List<String> resourceConfigFiles = ImmutableList.of();

    private DataSize textMaxLineLength = new DataSize(100, MEGABYTE);

    private boolean useParquetColumnNames;
    private boolean failOnCorruptedParquetStatistics = true;
    private DataSize parquetMaxReadBlockSize = new DataSize(16, MEGABYTE);

    private boolean assumeCanonicalPartitionKeys;

    private boolean useOrcColumnNames;
    private boolean orcBloomFiltersEnabled;
    private double orcDefaultBloomFilterFpp = 0.05;
    private DataSize orcMaxMergeDistance = new DataSize(1, MEGABYTE);
    private DataSize orcMaxBufferSize = new DataSize(8, MEGABYTE);
    private DataSize orcTinyStripeThreshold = new DataSize(8, MEGABYTE);
    private DataSize orcStreamBufferSize = new DataSize(8, MEGABYTE);
    private DataSize orcMaxReadBlockSize = new DataSize(16, MEGABYTE);
    private boolean orcLazyReadSmallRanges = true;
    private boolean orcOptimizedWriterEnabled = true;
    private double orcWriterValidationPercentage;
    private OrcWriteValidationMode orcWriterValidationMode = OrcWriteValidationMode.BOTH;

    private boolean rcfileOptimizedWriterEnabled = true;
    private boolean rcfileWriterValidate;

    private HiveMetastoreAuthenticationType hiveMetastoreAuthenticationType = HiveMetastoreAuthenticationType.NONE;
    private HdfsAuthenticationType hdfsAuthenticationType = HdfsAuthenticationType.NONE;
    private boolean hdfsImpersonationEnabled;
    private boolean hdfsWireEncryptionEnabled;

    private boolean skipDeletionForAlter;
    private boolean skipTargetCleanupOnRollback;

    private boolean bucketExecutionEnabled = true;
    private boolean sortedWritingEnabled = true;
    private BucketFunctionType bucketFunctionTypeForExchange = HIVE_COMPATIBLE;
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

    private boolean isTemporaryStagingDirectoryEnabled = true;
    private String temporaryStagingDirectoryPath = "/tmp/presto-${USER}";

    private String temporaryTableSchema = "default";
    private HiveStorageFormat temporaryTableStorageFormat = ORC;
    private HiveCompressionCodec temporaryTableCompressionCodec = HiveCompressionCodec.SNAPPY;
    private boolean shouldCreateEmptyBucketFilesForTemporaryTable = true;
    private boolean usePageFileForHiveUnsupportedType = true;

    private boolean pushdownFilterEnabled;
    private boolean rangeFiltersOnSubscriptsEnabled;
    private boolean adaptiveFilterReorderingEnabled = true;
    private boolean zstdJniDecompressionEnabled;

    private Duration fileStatusCacheExpireAfterWrite = new Duration(0, TimeUnit.SECONDS);
    private long fileStatusCacheMaxSize;
    private List<String> fileStatusCacheTables = ImmutableList.of();

    private DataSize pageFileStripeMaxSize = new DataSize(24, MEGABYTE);
    private boolean parquetBatchReadOptimizationEnabled;
    private boolean parquetEnableBatchReaderVerification;
    private boolean parquetDereferencePushdownEnabled;

    private int maxMetadataUpdaterThreads = 100;

    private boolean isPartialAggregationPushdownEnabled;
    private boolean isPartialAggregationPushdownForVariableLengthDatatypesEnabled;

    private boolean fileRenamingEnabled;
    private boolean preferManifestToListFiles;
    private boolean manifestVerificationEnabled;
    private boolean preferMetadataToListHudiFiles;
    private boolean hudiMetadataVerificationEnabled;

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

    @MinDataSize("1MB")
    @MaxDataSize("1GB")
    public DataSize getWriterSortBufferSize()
    {
        return writerSortBufferSize;
    }

    @Config("hive.writer-sort-buffer-size")
    public HiveClientConfig setWriterSortBufferSize(DataSize writerSortBufferSize)
    {
        this.writerSortBufferSize = writerSortBufferSize;
        return this;
    }

    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return nodeSelectionStrategy;
    }

    @Config("hive.node-selection-strategy")
    public HiveClientConfig setNodeSelectionStrategy(NodeSelectionStrategy nodeSelectionStrategy)
    {
        this.nodeSelectionStrategy = nodeSelectionStrategy;
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

    public boolean isInsertOverwriteImmutablePartitionEnabled()
    {
        return insertOverwriteImmutablePartitions;
    }

    @Config("hive.insert-overwrite-immutable-partitions-enabled")
    @ConfigDescription("When enabled, insertion query will overwrite existing partitions when partitions are immutable. This config only takes effect with hive.immutable-partitions set to true")
    public HiveClientConfig setInsertOverwriteImmutablePartitionEnabled(boolean insertOverwriteImmutablePartitions)
    {
        this.insertOverwriteImmutablePartitions = insertOverwriteImmutablePartitions;
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

    @Min(2)
    @Max(1000)
    public int getMaxOpenSortFiles()
    {
        return maxOpenSortFiles;
    }

    @Config("hive.max-open-sort-files")
    @ConfigDescription("Maximum number of writer temporary files to read in one pass")
    public HiveClientConfig setMaxOpenSortFiles(int maxOpenSortFiles)
    {
        this.maxOpenSortFiles = maxOpenSortFiles;
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
    public DataSize getOrcTinyStripeThreshold()
    {
        return orcTinyStripeThreshold;
    }

    @Config("hive.orc.tiny-stripe-threshold")
    public HiveClientConfig setOrcTinyStripeThreshold(DataSize orcTinyStripeThreshold)
    {
        this.orcTinyStripeThreshold = orcTinyStripeThreshold;
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

    @DecimalMin("0.0")
    @DecimalMax("100.0")
    public double getOrcWriterValidationPercentage()
    {
        return orcWriterValidationPercentage;
    }

    @Config("hive.orc.writer.validation-percentage")
    @ConfigDescription("Percentage of ORC files to validate after write by re-reading the whole file")
    public HiveClientConfig setOrcWriterValidationPercentage(double orcWriterValidationPercentage)
    {
        this.orcWriterValidationPercentage = orcWriterValidationPercentage;
        return this;
    }

    @NotNull
    public OrcWriteValidationMode getOrcWriterValidationMode()
    {
        return orcWriterValidationMode;
    }

    @Config("hive.orc.writer.validation-mode")
    @ConfigDescription("Level of detail in ORC validation. Lower levels require more memory.")
    public HiveClientConfig setOrcWriterValidationMode(OrcWriteValidationMode orcWriterValidationMode)
    {
        this.orcWriterValidationMode = orcWriterValidationMode;
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

    public boolean isFailOnCorruptedParquetStatistics()
    {
        return failOnCorruptedParquetStatistics;
    }

    @Config("hive.parquet.fail-on-corrupted-statistics")
    @ConfigDescription("Fail when scanning Parquet files with corrupted statistics")
    public HiveClientConfig setFailOnCorruptedParquetStatistics(boolean failOnCorruptedParquetStatistics)
    {
        this.failOnCorruptedParquetStatistics = failOnCorruptedParquetStatistics;
        return this;
    }

    @NotNull
    public DataSize getParquetMaxReadBlockSize()
    {
        return parquetMaxReadBlockSize;
    }

    @Config("hive.parquet.max-read-block-size")
    public HiveClientConfig setParquetMaxReadBlockSize(DataSize parquetMaxReadBlockSize)
    {
        this.parquetMaxReadBlockSize = parquetMaxReadBlockSize;
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

    public boolean isZstdJniDecompressionEnabled()
    {
        return zstdJniDecompressionEnabled;
    }

    @Config("hive.zstd-jni-decompression-enabled")
    public HiveClientConfig setZstdJniDecompressionEnabled(boolean zstdJniDecompressionEnabled)
    {
        this.zstdJniDecompressionEnabled = zstdJniDecompressionEnabled;
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

    public long getFileStatusCacheMaxSize()
    {
        return fileStatusCacheMaxSize;
    }

    @Config("hive.file-status-cache-size")
    public HiveClientConfig setFileStatusCacheMaxSize(long fileStatusCacheMaxSize)
    {
        this.fileStatusCacheMaxSize = fileStatusCacheMaxSize;
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

    public boolean isRangeFiltersOnSubscriptsEnabled()
    {
        return rangeFiltersOnSubscriptsEnabled;
    }

    @Config("hive.range-filters-on-subscripts-enabled")
    @ConfigDescription("Experimental: enable pushdown of range filters on subscripts (a[2] = 5) into ORC column readers")
    public HiveClientConfig setRangeFiltersOnSubscriptsEnabled(boolean rangeFiltersOnSubscriptsEnabled)
    {
        this.rangeFiltersOnSubscriptsEnabled = rangeFiltersOnSubscriptsEnabled;
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

    @Config("hive.parquet-batch-read-optimization-enabled")
    @ConfigDescription("enable parquet batch reads optimization")
    public HiveClientConfig setParquetBatchReadOptimizationEnabled(boolean parquetBatchReadOptimizationEnabled)
    {
        this.parquetBatchReadOptimizationEnabled = parquetBatchReadOptimizationEnabled;
        return this;
    }

    public boolean isParquetBatchReadOptimizationEnabled()
    {
        return this.parquetBatchReadOptimizationEnabled;
    }

    @Config("hive.enable-parquet-batch-reader-verification")
    @ConfigDescription("enable optimized parquet reader")
    public HiveClientConfig setParquetBatchReaderVerificationEnabled(boolean parquetEnableBatchReaderVerification)
    {
        this.parquetEnableBatchReaderVerification = parquetEnableBatchReaderVerification;
        return this;
    }

    public boolean isParquetBatchReaderVerificationEnabled()
    {
        return this.parquetEnableBatchReaderVerification;
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

    @Min(1)
    public int getMaxMetadataUpdaterThreads()
    {
        return maxMetadataUpdaterThreads;
    }

    @Config("hive.max-metadata-updater-threads")
    public HiveClientConfig setMaxMetadataUpdaterThreads(int maxMetadataUpdaterThreads)
    {
        this.maxMetadataUpdaterThreads = maxMetadataUpdaterThreads;
        return this;
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

    @Config("hive.prefer-metadata-to-list-hudi-files")
    @ConfigDescription("For Hudi tables prefer to fetch the list of file names and sizes from metadata rather than storage")
    public HiveClientConfig setPreferMetadataToListHudiFiles(boolean preferMetadataToListHudiFiles)
    {
        this.preferMetadataToListHudiFiles = preferMetadataToListHudiFiles;
        return this;
    }

    public boolean isPreferMetadataToListHudiFiles()
    {
        return this.preferMetadataToListHudiFiles;
    }

    @Config("hive.hudi-metadata-verification-enabled")
    @ConfigDescription("Enable verification of file names and sizes in Hudi metadata")
    public HiveClientConfig setHudiMetadataVerificationEnabled(boolean hudiMetadataVerificationEnabled)
    {
        this.hudiMetadataVerificationEnabled = hudiMetadataVerificationEnabled;
        return this;
    }

    public boolean isHudiMetadataVerificationEnabled()
    {
        return this.hudiMetadataVerificationEnabled;
    }
}
