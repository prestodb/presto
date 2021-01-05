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

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.facebook.presto.hive.HiveClientConfig.HdfsAuthenticationType;
import com.facebook.presto.hive.HiveClientConfig.HiveMetastoreAuthenticationType;
import com.facebook.presto.hive.s3.S3FileSystemType;
import com.facebook.presto.orc.OrcWriteValidation.OrcWriteValidationMode;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.time.ZoneId;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.BucketFunctionType.HIVE_COMPATIBLE;
import static com.facebook.presto.hive.BucketFunctionType.PRESTO_NATIVE;
import static com.facebook.presto.hive.HiveCompressionCodec.NONE;
import static com.facebook.presto.hive.HiveCompressionCodec.SNAPPY;
import static com.facebook.presto.hive.HiveStorageFormat.DWRF;
import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.hive.TestHiveUtil.nonDefaultTimeZone;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.HARD_AFFINITY;

public class TestHiveClientConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(HiveClientConfig.class)
                .setTimeZone(TimeZone.getDefault().getID())
                .setMaxSplitSize(new DataSize(64, Unit.MEGABYTE))
                .setMaxPartitionsPerScan(100_000)
                .setMaxOutstandingSplits(1_000)
                .setMaxOutstandingSplitsSize(new DataSize(256, Unit.MEGABYTE))
                .setMaxSplitIteratorThreads(1_000)
                .setAllowCorruptWritesForTesting(false)
                .setMinPartitionBatchSize(10)
                .setMaxPartitionBatchSize(100)
                .setMaxInitialSplits(200)
                .setMaxInitialSplitSize(new DataSize(32, Unit.MEGABYTE))
                .setSplitLoaderConcurrency(4)
                .setDomainCompactionThreshold(100)
                .setWriterSortBufferSize(new DataSize(64, Unit.MEGABYTE))
                .setNodeSelectionStrategy(NodeSelectionStrategy.valueOf("NO_PREFERENCE"))
                .setMaxConcurrentFileRenames(20)
                .setMaxConcurrentZeroRowFileCreations(20)
                .setRecursiveDirWalkerEnabled(false)
                .setDfsTimeout(new Duration(60, TimeUnit.SECONDS))
                .setIpcPingInterval(new Duration(10, TimeUnit.SECONDS))
                .setDfsConnectTimeout(new Duration(500, TimeUnit.MILLISECONDS))
                .setDfsConnectMaxRetries(5)
                .setDomainSocketPath(null)
                .setS3FileSystemType(S3FileSystemType.PRESTO)
                .setResourceConfigFiles("")
                .setHiveStorageFormat(ORC)
                .setCompressionCodec(HiveCompressionCodec.GZIP)
                .setOrcCompressionCodec(HiveCompressionCodec.GZIP)
                .setRespectTableFormat(true)
                .setImmutablePartitions(false)
                .setCreateEmptyBucketFiles(true)
                .setInsertOverwriteImmutablePartitionEnabled(false)
                .setFailFastOnInsertIntoImmutablePartitionsEnabled(true)
                .setSortedWritingEnabled(true)
                .setMaxPartitionsPerWriter(100)
                .setMaxOpenSortFiles(50)
                .setWriteValidationThreads(16)
                .setTextMaxLineLength(new DataSize(100, Unit.MEGABYTE))
                .setUseParquetColumnNames(false)
                .setParquetMaxReadBlockSize(new DataSize(16, Unit.MEGABYTE))
                .setUseOrcColumnNames(false)
                .setAssumeCanonicalPartitionKeys(false)
                .setOrcBloomFiltersEnabled(false)
                .setOrcDefaultBloomFilterFpp(0.05)
                .setOrcMaxMergeDistance(new DataSize(1, Unit.MEGABYTE))
                .setOrcMaxBufferSize(new DataSize(8, Unit.MEGABYTE))
                .setOrcStreamBufferSize(new DataSize(8, Unit.MEGABYTE))
                .setOrcTinyStripeThreshold(new DataSize(8, Unit.MEGABYTE))
                .setOrcMaxReadBlockSize(new DataSize(16, Unit.MEGABYTE))
                .setOrcLazyReadSmallRanges(true)
                .setRcfileOptimizedWriterEnabled(true)
                .setRcfileWriterValidate(false)
                .setOrcOptimizedWriterEnabled(true)
                .setOrcWriterValidationPercentage(0.0)
                .setOrcWriterValidationMode(OrcWriteValidationMode.BOTH)
                .setHiveMetastoreAuthenticationType(HiveMetastoreAuthenticationType.NONE)
                .setHdfsAuthenticationType(HdfsAuthenticationType.NONE)
                .setHdfsImpersonationEnabled(false)
                .setSkipDeletionForAlter(false)
                .setSkipTargetCleanupOnRollback(false)
                .setBucketExecutionEnabled(true)
                .setIgnoreTableBucketing(false)
                .setMinBucketCountToNotIgnoreTableBucketing(0)
                .setMaxBucketsForGroupedExecution(1_000_000)
                .setSortedWriteToTempPathEnabled(false)
                .setSortedWriteTempPathSubdirectoryCount(10)
                .setFileSystemMaxCacheSize(1000)
                .setTableStatisticsEnabled(true)
                .setOptimizeMismatchedBucketCount(false)
                .setWritesToNonManagedTablesEnabled(false)
                .setCreatesOfNonManagedTablesEnabled(true)
                .setHdfsWireEncryptionEnabled(false)
                .setPartitionStatisticsSampleSize(100)
                .setIgnoreCorruptedStatistics(false)
                .setCollectColumnStatisticsOnWrite(false)
                .setPartitionStatisticsBasedOptimizationEnabled(false)
                .setS3SelectPushdownEnabled(false)
                .setS3SelectPushdownMaxConnections(500)
                .setTemporaryStagingDirectoryEnabled(true)
                .setTemporaryStagingDirectoryPath("/tmp/presto-${USER}")
                .setTemporaryTableSchema("default")
                .setTemporaryTableStorageFormat(ORC)
                .setTemporaryTableCompressionCodec(SNAPPY)
                .setCreateEmptyBucketFilesForTemporaryTable(false)
                .setUsePageFileForHiveUnsupportedType(true)
                .setPushdownFilterEnabled(false)
                .setParquetPushdownFilterEnabled(false)
                .setZstdJniDecompressionEnabled(false)
                .setRangeFiltersOnSubscriptsEnabled(false)
                .setAdaptiveFilterReorderingEnabled(true)
                .setFileStatusCacheExpireAfterWrite(new Duration(0, TimeUnit.SECONDS))
                .setFileStatusCacheMaxSize(0)
                .setFileStatusCacheTables("")
                .setPageFileStripeMaxSize(new DataSize(24, Unit.MEGABYTE))
                .setParquetBatchReaderVerificationEnabled(false)
                .setParquetBatchReadOptimizationEnabled(false)
                .setBucketFunctionTypeForExchange(HIVE_COMPATIBLE)
                .setParquetDereferencePushdownEnabled(false)
                .setIgnoreUnreadablePartition(false)
                .setMaxMetadataUpdaterThreads(100)
                .setPartialAggregationPushdownEnabled(false)
                .setPartialAggregationPushdownForVariableLengthDatatypesEnabled(false)
                .setFileRenamingEnabled(false)
                .setPreferManifestsToListFiles(false)
                .setManifestVerificationEnabled(false)
                .setUndoMetastoreOperationsEnabled(true)
                .setOptimizedPartitionUpdateSerializationEnabled(false)
                .setVerboseRuntimeStatsEnabled(false)
                .setPartitionLeaseDuration(new Duration(0, TimeUnit.SECONDS))
                .setMaterializedViewMissingPartitionsThreshold(100)
                .setLooseMemoryAccountingEnabled(false)
                .setReadColumnIndexFilter(false)
                .setSizeBasedSplitWeightsEnabled(true)
                .setMinimumAssignedSplitWeight(0.05)
                .setUserDefinedTypeEncodingEnabled(false)
                .setUseRecordPageSourceForCustomSplit(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.time-zone", nonDefaultTimeZone().getID())
                .put("hive.max-split-size", "256MB")
                .put("hive.max-partitions-per-scan", "123")
                .put("hive.max-outstanding-splits", "10")
                .put("hive.max-outstanding-splits-size", "32MB")
                .put("hive.max-split-iterator-threads", "10")
                .put("hive.allow-corrupt-writes-for-testing", "true")
                .put("hive.metastore.partition-batch-size.min", "1")
                .put("hive.metastore.partition-batch-size.max", "1000")
                .put("hive.dfs.ipc-ping-interval", "34s")
                .put("hive.dfs-timeout", "33s")
                .put("hive.dfs.connect.timeout", "20s")
                .put("hive.dfs.connect.max-retries", "10")
                .put("hive.dfs.domain-socket-path", "/foo")
                .put("hive.s3-file-system-type", "EMRFS")
                .put("hive.config.resources", "/foo.xml,/bar.xml")
                .put("hive.max-initial-splits", "10")
                .put("hive.max-initial-split-size", "16MB")
                .put("hive.split-loader-concurrency", "1")
                .put("hive.domain-compaction-threshold", "42")
                .put("hive.writer-sort-buffer-size", "13MB")
                .put("hive.recursive-directories", "true")
                .put("hive.storage-format", "SEQUENCEFILE")
                .put("hive.compression-codec", "NONE")
                .put("hive.orc-compression-codec", "ZSTD")
                .put("hive.respect-table-format", "false")
                .put("hive.immutable-partitions", "true")
                .put("hive.create-empty-bucket-files", "false")
                .put("hive.insert-overwrite-immutable-partitions-enabled", "true")
                .put("hive.fail-fast-on-insert-into-immutable-partitions-enabled", "false")
                .put("hive.max-partitions-per-writers", "222")
                .put("hive.max-open-sort-files", "333")
                .put("hive.write-validation-threads", "11")
                .put("hive.node-selection-strategy", "HARD_AFFINITY")
                .put("hive.max-concurrent-file-renames", "100")
                .put("hive.max-concurrent-zero-row-file-creations", "100")
                .put("hive.assume-canonical-partition-keys", "true")
                .put("hive.text.max-line-length", "13MB")
                .put("hive.parquet.use-column-names", "true")
                .put("hive.parquet.max-read-block-size", "66kB")
                .put("hive.orc.use-column-names", "true")
                .put("hive.orc.bloom-filters.enabled", "true")
                .put("hive.orc.default-bloom-filter-fpp", "0.96")
                .put("hive.orc.max-merge-distance", "22kB")
                .put("hive.orc.max-buffer-size", "44kB")
                .put("hive.orc.stream-buffer-size", "55kB")
                .put("hive.orc.tiny-stripe-threshold", "61kB")
                .put("hive.orc.max-read-block-size", "66kB")
                .put("hive.orc.lazy-read-small-ranges", "false")
                .put("hive.rcfile-optimized-writer.enabled", "false")
                .put("hive.rcfile.writer.validate", "true")
                .put("hive.orc.optimized-writer.enabled", "false")
                .put("hive.orc.writer.validation-percentage", "0.16")
                .put("hive.orc.writer.validation-mode", "DETAILED")
                .put("hive.metastore.authentication.type", "KERBEROS")
                .put("hive.hdfs.authentication.type", "KERBEROS")
                .put("hive.hdfs.impersonation.enabled", "true")
                .put("hive.skip-deletion-for-alter", "true")
                .put("hive.skip-target-cleanup-on-rollback", "true")
                .put("hive.bucket-execution", "false")
                .put("hive.sorted-writing", "false")
                .put("hive.ignore-table-bucketing", "true")
                .put("hive.min-bucket-count-to-not-ignore-table-bucketing", "1024")
                .put("hive.max-buckets-for-grouped-execution", "100")
                .put("hive.sorted-write-to-temp-path-enabled", "true")
                .put("hive.sorted-write-temp-path-subdirectory-count", "50")
                .put("hive.fs.cache.max-size", "1010")
                .put("hive.table-statistics-enabled", "false")
                .put("hive.optimize-mismatched-bucket-count", "true")
                .put("hive.non-managed-table-writes-enabled", "true")
                .put("hive.non-managed-table-creates-enabled", "false")
                .put("hive.hdfs.wire-encryption.enabled", "true")
                .put("hive.partition-statistics-sample-size", "1234")
                .put("hive.ignore-corrupted-statistics", "true")
                .put("hive.collect-column-statistics-on-write", "true")
                .put("hive.partition-statistics-based-optimization-enabled", "true")
                .put("hive.s3select-pushdown.enabled", "true")
                .put("hive.s3select-pushdown.max-connections", "1234")
                .put("hive.temporary-staging-directory-enabled", "false")
                .put("hive.temporary-staging-directory-path", "updated")
                .put("hive.temporary-table-schema", "other")
                .put("hive.temporary-table-storage-format", "DWRF")
                .put("hive.temporary-table-compression-codec", "NONE")
                .put("hive.create-empty-bucket-files-for-temporary-table", "true")
                .put("hive.use-pagefile-for-hive-unsupported-type", "false")
                .put("hive.pushdown-filter-enabled", "true")
                .put("hive.parquet.pushdown-filter-enabled", "true")
                .put("hive.range-filters-on-subscripts-enabled", "true")
                .put("hive.adaptive-filter-reordering-enabled", "false")
                .put("hive.zstd-jni-decompression-enabled", "true")
                .put("hive.file-status-cache-tables", "foo.bar1, foo.bar2")
                .put("hive.file-status-cache-size", "1000")
                .put("hive.file-status-cache-expire-time", "30m")
                .put("hive.pagefile.writer.stripe-max-size", "1kB")
                .put("hive.parquet-batch-read-optimization-enabled", "true")
                .put("hive.enable-parquet-batch-reader-verification", "true")
                .put("hive.bucket-function-type-for-exchange", "PRESTO_NATIVE")
                .put("hive.enable-parquet-dereference-pushdown", "true")
                .put("hive.ignore-unreadable-partition", "true")
                .put("hive.max-metadata-updater-threads", "1000")
                .put("hive.partial_aggregation_pushdown_enabled", "true")
                .put("hive.partial_aggregation_pushdown_for_variable_length_datatypes_enabled", "true")
                .put("hive.file_renaming_enabled", "true")
                .put("hive.prefer-manifests-to-list-files", "true")
                .put("hive.manifest-verification-enabled", "true")
                .put("hive.undo-metastore-operations-enabled", "false")
                .put("hive.experimental-optimized-partition-update-serialization-enabled", "true")
                .put("hive.partition-lease-duration", "4h")
                .put("hive.loose-memory-accounting-enabled", "true")
                .put("hive.verbose-runtime-stats-enabled", "true")
                .put("hive.materialized-view-missing-partitions-threshold", "50")
                .put("hive.parquet-column-index-filter-enabled", "true")
                .put("hive.size-based-split-weights-enabled", "false")
                .put("hive.user-defined-type-encoding-enabled", "true")
                .put("hive.minimum-assigned-split-weight", "1.0")
                .put("hive.use-record-page-source-for-custom-split", "false")
                .build();

        HiveClientConfig expected = new HiveClientConfig()
                .setTimeZone(TimeZone.getTimeZone(ZoneId.of(nonDefaultTimeZone().getID())).getID())
                .setMaxSplitSize(new DataSize(256, Unit.MEGABYTE))
                .setMaxPartitionsPerScan(123)
                .setMaxOutstandingSplits(10)
                .setMaxOutstandingSplitsSize(new DataSize(32, Unit.MEGABYTE))
                .setMaxSplitIteratorThreads(10)
                .setAllowCorruptWritesForTesting(true)
                .setMinPartitionBatchSize(1)
                .setMaxPartitionBatchSize(1000)
                .setMaxInitialSplits(10)
                .setMaxInitialSplitSize(new DataSize(16, Unit.MEGABYTE))
                .setSplitLoaderConcurrency(1)
                .setDomainCompactionThreshold(42)
                .setWriterSortBufferSize(new DataSize(13, Unit.MEGABYTE))
                .setNodeSelectionStrategy(HARD_AFFINITY)
                .setMaxConcurrentFileRenames(100)
                .setMaxConcurrentZeroRowFileCreations(100)
                .setRecursiveDirWalkerEnabled(true)
                .setIpcPingInterval(new Duration(34, TimeUnit.SECONDS))
                .setDfsTimeout(new Duration(33, TimeUnit.SECONDS))
                .setDfsConnectTimeout(new Duration(20, TimeUnit.SECONDS))
                .setDfsConnectMaxRetries(10)
                .setResourceConfigFiles(ImmutableList.of("/foo.xml", "/bar.xml"))
                .setHiveStorageFormat(HiveStorageFormat.SEQUENCEFILE)
                .setCompressionCodec(HiveCompressionCodec.NONE)
                .setOrcCompressionCodec(HiveCompressionCodec.ZSTD)
                .setRespectTableFormat(false)
                .setImmutablePartitions(true)
                .setCreateEmptyBucketFiles(false)
                .setInsertOverwriteImmutablePartitionEnabled(true)
                .setFailFastOnInsertIntoImmutablePartitionsEnabled(false)
                .setMaxPartitionsPerWriter(222)
                .setMaxOpenSortFiles(333)
                .setWriteValidationThreads(11)
                .setDomainSocketPath("/foo")
                .setS3FileSystemType(S3FileSystemType.EMRFS)
                .setTextMaxLineLength(new DataSize(13, Unit.MEGABYTE))
                .setUseParquetColumnNames(true)
                .setParquetMaxReadBlockSize(new DataSize(66, Unit.KILOBYTE))
                .setUseOrcColumnNames(true)
                .setAssumeCanonicalPartitionKeys(true)
                .setOrcBloomFiltersEnabled(true)
                .setOrcDefaultBloomFilterFpp(0.96)
                .setOrcMaxMergeDistance(new DataSize(22, Unit.KILOBYTE))
                .setOrcMaxBufferSize(new DataSize(44, Unit.KILOBYTE))
                .setOrcStreamBufferSize(new DataSize(55, Unit.KILOBYTE))
                .setOrcTinyStripeThreshold(new DataSize(61, Unit.KILOBYTE))
                .setOrcMaxReadBlockSize(new DataSize(66, Unit.KILOBYTE))
                .setOrcLazyReadSmallRanges(false)
                .setRcfileOptimizedWriterEnabled(false)
                .setRcfileWriterValidate(true)
                .setOrcOptimizedWriterEnabled(false)
                .setOrcWriterValidationPercentage(0.16)
                .setOrcWriterValidationMode(OrcWriteValidationMode.DETAILED)
                .setHiveMetastoreAuthenticationType(HiveMetastoreAuthenticationType.KERBEROS)
                .setHdfsAuthenticationType(HdfsAuthenticationType.KERBEROS)
                .setHdfsImpersonationEnabled(true)
                .setSkipDeletionForAlter(true)
                .setSkipTargetCleanupOnRollback(true)
                .setBucketExecutionEnabled(false)
                .setSortedWritingEnabled(false)
                .setIgnoreTableBucketing(true)
                .setMaxBucketsForGroupedExecution(100)
                .setSortedWriteToTempPathEnabled(true)
                .setSortedWriteTempPathSubdirectoryCount(50)
                .setFileSystemMaxCacheSize(1010)
                .setTableStatisticsEnabled(false)
                .setOptimizeMismatchedBucketCount(true)
                .setWritesToNonManagedTablesEnabled(true)
                .setCreatesOfNonManagedTablesEnabled(false)
                .setHdfsWireEncryptionEnabled(true)
                .setPartitionStatisticsSampleSize(1234)
                .setIgnoreCorruptedStatistics(true)
                .setMinBucketCountToNotIgnoreTableBucketing(1024)
                .setCollectColumnStatisticsOnWrite(true)
                .setPartitionStatisticsBasedOptimizationEnabled(true)
                .setS3SelectPushdownEnabled(true)
                .setS3SelectPushdownMaxConnections(1234)
                .setTemporaryStagingDirectoryEnabled(false)
                .setTemporaryStagingDirectoryPath("updated")
                .setTemporaryTableSchema("other")
                .setTemporaryTableStorageFormat(DWRF)
                .setTemporaryTableCompressionCodec(NONE)
                .setCreateEmptyBucketFilesForTemporaryTable(true)
                .setUsePageFileForHiveUnsupportedType(false)
                .setPushdownFilterEnabled(true)
                .setParquetPushdownFilterEnabled(true)
                .setZstdJniDecompressionEnabled(true)
                .setRangeFiltersOnSubscriptsEnabled(true)
                .setAdaptiveFilterReorderingEnabled(false)
                .setFileStatusCacheTables("foo.bar1,foo.bar2")
                .setFileStatusCacheMaxSize(1000)
                .setFileStatusCacheExpireAfterWrite(new Duration(30, TimeUnit.MINUTES))
                .setPageFileStripeMaxSize(new DataSize(1, Unit.KILOBYTE))
                .setParquetBatchReaderVerificationEnabled(true)
                .setParquetBatchReadOptimizationEnabled(true)
                .setBucketFunctionTypeForExchange(PRESTO_NATIVE)
                .setParquetDereferencePushdownEnabled(true)
                .setIgnoreUnreadablePartition(true)
                .setMaxMetadataUpdaterThreads(1000)
                .setPartialAggregationPushdownEnabled(true)
                .setPartialAggregationPushdownForVariableLengthDatatypesEnabled(true)
                .setFileRenamingEnabled(true)
                .setPreferManifestsToListFiles(true)
                .setManifestVerificationEnabled(true)
                .setUndoMetastoreOperationsEnabled(false)
                .setOptimizedPartitionUpdateSerializationEnabled(true)
                .setVerboseRuntimeStatsEnabled(true)
                .setPartitionLeaseDuration(new Duration(4, TimeUnit.HOURS))
                .setMaterializedViewMissingPartitionsThreshold(50)
                .setLooseMemoryAccountingEnabled(true)
                .setReadColumnIndexFilter(true)
                .setSizeBasedSplitWeightsEnabled(false)
                .setMinimumAssignedSplitWeight(1.0)
                .setUserDefinedTypeEncodingEnabled(true)
                .setUseRecordPageSourceForCustomSplit(false);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
