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
import com.facebook.drift.transport.netty.codec.Protocol;
import com.facebook.presto.hive.HiveClientConfig.HdfsAuthenticationType;
import com.facebook.presto.hive.s3.S3FileSystemType;
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
import static com.facebook.presto.hive.HiveClientConfig.InsertExistingPartitionsBehavior.APPEND;
import static com.facebook.presto.hive.HiveCompressionCodec.NONE;
import static com.facebook.presto.hive.HiveCompressionCodec.SNAPPY;
import static com.facebook.presto.hive.HiveStorageFormat.DWRF;
import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.hive.TestHiveUtil.nonDefaultTimeZone;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

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
                .setInsertExistingPartitionsBehavior(APPEND)
                .setCreateEmptyBucketFiles(true)
                .setInsertOverwriteImmutablePartitionEnabled(false)
                .setFailFastOnInsertIntoImmutablePartitionsEnabled(true)
                .setSortedWritingEnabled(true)
                .setMaxPartitionsPerWriter(100)
                .setMaxOpenSortFiles(50)
                .setWriteValidationThreads(16)
                .setTextMaxLineLength(new DataSize(100, Unit.MEGABYTE))
                .setUseOrcColumnNames(false)
                .setAssumeCanonicalPartitionKeys(false)
                .setOrcDefaultBloomFilterFpp(0.05)
                .setRcfileOptimizedWriterEnabled(true)
                .setRcfileWriterValidate(false)
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
                .setOrderBasedExecutionEnabled(false)
                .setTemporaryStagingDirectoryEnabled(true)
                .setTemporaryStagingDirectoryPath("/tmp/presto-${USER}")
                .setTemporaryTableSchema("default")
                .setTemporaryTableStorageFormat(ORC)
                .setTemporaryTableCompressionCodec(SNAPPY)
                .setCreateEmptyBucketFilesForTemporaryTable(false)
                .setUsePageFileForHiveUnsupportedType(true)
                .setPushdownFilterEnabled(false)
                .setParquetPushdownFilterEnabled(false)
                .setAdaptiveFilterReorderingEnabled(true)
                .setFileStatusCacheExpireAfterWrite(new Duration(0, TimeUnit.SECONDS))
                .setFileStatusCacheMaxRetainedSize(new DataSize(0, KILOBYTE))
                .setFileStatusCacheTables("")
                .setPageFileStripeMaxSize(new DataSize(24, Unit.MEGABYTE))
                .setBucketFunctionTypeForExchange(HIVE_COMPATIBLE)
                .setBucketFunctionTypeForCteMaterialization(PRESTO_NATIVE)
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
                .setDynamicSplitSizesEnabled(false)
                .setMinimumAssignedSplitWeight(0.05)
                .setUserDefinedTypeEncodingEnabled(false)
                .setUseRecordPageSourceForCustomSplit(true)
                .setFileSplittable(true)
                .setHudiMetadataEnabled(false)
                .setThriftProtocol(Protocol.BINARY)
                .setThriftBufferSize(new DataSize(128, BYTE))
                .setCopyOnFirstWriteConfigurationEnabled(true)
                .setPartitionFilteringFromMetastoreEnabled(true)
                .setParallelParsingOfPartitionValuesEnabled(false)
                .setMaxParallelParsingConcurrency(100)
                .setQuickStatsEnabled(false)
                .setQuickStatsInlineBuildTimeout(new Duration(60, TimeUnit.SECONDS))
                .setQuickStatsBackgroundBuildTimeout(new Duration(0, TimeUnit.SECONDS))
                .setQuickStatsCacheExpiry(new Duration(24, TimeUnit.HOURS))
                .setQuickStatsReaperExpiry(new Duration(5, TimeUnit.MINUTES))
                .setParquetQuickStatsFileMetadataFetchTimeout(new Duration(60, TimeUnit.SECONDS))
                .setMaxConcurrentQuickStatsCalls(100)
                .setMaxConcurrentParquetQuickStatsCalls(500)
                .setCteVirtualBucketCount(128)
                .setSkipEmptyFilesEnabled(false)
                .setAffinitySchedulingFileSectionSize(new DataSize(256, MEGABYTE))
                .setLegacyTimestampBucketing(false)
                .setOrcUseVectorFilter(false));
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
                .put("hive.insert-existing-partitions-behavior", "OVERWRITE")
                .put("hive.create-empty-bucket-files", "false")
                .put("hive.insert-overwrite-immutable-partitions-enabled", "true")
                .put("hive.fail-fast-on-insert-into-immutable-partitions-enabled", "false")
                .put("hive.max-partitions-per-writers", "222")
                .put("hive.max-open-sort-files", "333")
                .put("hive.write-validation-threads", "11")
                .put("hive.max-concurrent-file-renames", "100")
                .put("hive.max-concurrent-zero-row-file-creations", "100")
                .put("hive.assume-canonical-partition-keys", "true")
                .put("hive.text.max-line-length", "13MB")
                .put("hive.orc.use-column-names", "true")
                .put("hive.orc.default-bloom-filter-fpp", "0.96")
                .put("hive.rcfile-optimized-writer.enabled", "false")
                .put("hive.rcfile.writer.validate", "true")
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
                .put("hive.order-based-execution-enabled", "true")
                .put("hive.temporary-staging-directory-enabled", "false")
                .put("hive.temporary-staging-directory-path", "updated")
                .put("hive.temporary-table-schema", "other")
                .put("hive.temporary-table-storage-format", "DWRF")
                .put("hive.temporary-table-compression-codec", "NONE")
                .put("hive.create-empty-bucket-files-for-temporary-table", "true")
                .put("hive.use-pagefile-for-hive-unsupported-type", "false")
                .put("hive.pushdown-filter-enabled", "true")
                .put("hive.parquet.pushdown-filter-enabled", "true")
                .put("hive.adaptive-filter-reordering-enabled", "false")
                .put("hive.file-status-cache-tables", "foo.bar1, foo.bar2")
                .put("hive.file-status-cache.max-retained-size", "500MB")
                .put("hive.file-status-cache-expire-time", "30m")
                .put("hive.pagefile.writer.stripe-max-size", "1kB")
                .put("hive.bucket-function-type-for-exchange", "PRESTO_NATIVE")
                .put("hive.bucket-function-type-for-cte-materialization", "HIVE_COMPATIBLE")
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
                .put("hive.dynamic-split-sizes-enabled", "true")
                .put("hive.user-defined-type-encoding-enabled", "true")
                .put("hive.minimum-assigned-split-weight", "1.0")
                .put("hive.use-record-page-source-for-custom-split", "false")
                .put("hive.file-splittable", "false")
                .put("hive.hudi-metadata-enabled", "true")
                .put("hive.internal-communication.thrift-transport-protocol", "COMPACT")
                .put("hive.internal-communication.thrift-transport-buffer-size", "256B")
                .put("hive.copy-on-first-write-configuration-enabled", "false")
                .put("hive.partition-filtering-from-metastore-enabled", "false")
                .put("hive.parallel-parsing-of-partition-values-enabled", "true")
                .put("hive.max-parallel-parsing-concurrency", "200")
                .put("hive.quick-stats.enabled", "true")
                .put("hive.quick-stats.inline-build-timeout", "61s")
                .put("hive.quick-stats.background-build-timeout", "1s")
                .put("hive.quick-stats.cache-expiry", "5h")
                .put("hive.quick-stats.reaper-expiry", "15m")
                .put("hive.quick-stats.parquet.file-metadata-fetch-timeout", "30s")
                .put("hive.quick-stats.parquet.max-concurrent-calls", "399")
                .put("hive.quick-stats.max-concurrent-calls", "101")
                .put("hive.cte-virtual-bucket-count", "256")
                .put("hive.affinity-scheduling-file-section-size", "512MB")
                .put("hive.skip-empty-files", "true")
                .put("hive.legacy-timestamp-bucketing", "true")
                .put("hive.orc-use-vector-filter", "true")
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
                .setUseOrcColumnNames(true)
                .setAssumeCanonicalPartitionKeys(true)
                .setOrcDefaultBloomFilterFpp(0.96)
                .setRcfileOptimizedWriterEnabled(false)
                .setRcfileWriterValidate(true)
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
                .setOrderBasedExecutionEnabled(true)
                .setTemporaryStagingDirectoryEnabled(false)
                .setTemporaryStagingDirectoryPath("updated")
                .setTemporaryTableSchema("other")
                .setTemporaryTableStorageFormat(DWRF)
                .setTemporaryTableCompressionCodec(NONE)
                .setCreateEmptyBucketFilesForTemporaryTable(true)
                .setUsePageFileForHiveUnsupportedType(false)
                .setPushdownFilterEnabled(true)
                .setParquetPushdownFilterEnabled(true)
                .setAdaptiveFilterReorderingEnabled(false)
                .setFileStatusCacheTables("foo.bar1,foo.bar2")
                .setFileStatusCacheMaxRetainedSize((new DataSize(500, MEGABYTE)))
                .setFileStatusCacheExpireAfterWrite(new Duration(30, TimeUnit.MINUTES))
                .setPageFileStripeMaxSize(new DataSize(1, Unit.KILOBYTE))
                .setBucketFunctionTypeForExchange(PRESTO_NATIVE)
                .setBucketFunctionTypeForCteMaterialization(HIVE_COMPATIBLE)
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
                .setDynamicSplitSizesEnabled(true)
                .setMinimumAssignedSplitWeight(1.0)
                .setUserDefinedTypeEncodingEnabled(true)
                .setUseRecordPageSourceForCustomSplit(false)
                .setFileSplittable(false)
                .setHudiMetadataEnabled(true)
                .setThriftProtocol(Protocol.COMPACT)
                .setThriftBufferSize(new DataSize(256, BYTE))
                .setCopyOnFirstWriteConfigurationEnabled(false)
                .setPartitionFilteringFromMetastoreEnabled(false)
                .setParallelParsingOfPartitionValuesEnabled(true)
                .setMaxParallelParsingConcurrency(200)
                .setQuickStatsEnabled(true)
                .setQuickStatsInlineBuildTimeout(new Duration(61, TimeUnit.SECONDS))
                .setQuickStatsBackgroundBuildTimeout(new Duration(1, TimeUnit.SECONDS))
                .setQuickStatsCacheExpiry(new Duration(5, TimeUnit.HOURS))
                .setQuickStatsReaperExpiry(new Duration(15, TimeUnit.MINUTES))
                .setParquetQuickStatsFileMetadataFetchTimeout(new Duration(30, TimeUnit.SECONDS))
                .setMaxConcurrentParquetQuickStatsCalls(399)
                .setMaxConcurrentQuickStatsCalls(101)
                .setAffinitySchedulingFileSectionSize(new DataSize(512, MEGABYTE))
                .setSkipEmptyFilesEnabled(true)
                .setCteVirtualBucketCount(256)
                .setAffinitySchedulingFileSectionSize(new DataSize(512, MEGABYTE))
                .setLegacyTimestampBucketing(true)
                .setOrcUseVectorFilter(true);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
