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

import com.facebook.presto.orc.OrcWriteValidation.OrcWriteValidationMode;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.hive.HiveSessionProperties.InsertExistingPartitionsBehavior.APPEND;
import static com.facebook.presto.hive.HiveSessionProperties.InsertExistingPartitionsBehavior.ERROR;
import static com.facebook.presto.hive.HiveSessionProperties.InsertExistingPartitionsBehavior.OVERWRITE;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public final class HiveSessionProperties
{
    private static final String IGNORE_TABLE_BUCKETING = "ignore_table_bucketing";
    private static final String MIN_BUCKET_COUNT_TO_NOT_IGNORE_TABLE_BUCKETING = "min_bucket_count_to_not_ignore_table_bucketing";
    private static final String BUCKET_EXECUTION_ENABLED = "bucket_execution_enabled";
    private static final String NODE_SELECTION_STRATEGY = "node_selection_strategy";
    private static final String INSERT_EXISTING_PARTITIONS_BEHAVIOR = "insert_existing_partitions_behavior";
    private static final String ORC_BLOOM_FILTERS_ENABLED = "orc_bloom_filters_enabled";
    private static final String ORC_MAX_MERGE_DISTANCE = "orc_max_merge_distance";
    private static final String ORC_MAX_BUFFER_SIZE = "orc_max_buffer_size";
    private static final String ORC_STREAM_BUFFER_SIZE = "orc_stream_buffer_size";
    private static final String ORC_TINY_STRIPE_THRESHOLD = "orc_tiny_stripe_threshold";
    private static final String ORC_MAX_READ_BLOCK_SIZE = "orc_max_read_block_size";
    private static final String ORC_LAZY_READ_SMALL_RANGES = "orc_lazy_read_small_ranges";
    private static final String ORC_ZSTD_JNI_DECOMPRESSION_ENABLED = "orc_zstd_jni_decompression_enabled";
    private static final String ORC_STRING_STATISTICS_LIMIT = "orc_string_statistics_limit";
    private static final String ORC_OPTIMIZED_WRITER_ENABLED = "orc_optimized_writer_enabled";
    private static final String ORC_OPTIMIZED_WRITER_VALIDATE = "orc_optimized_writer_validate";
    private static final String ORC_OPTIMIZED_WRITER_VALIDATE_PERCENTAGE = "orc_optimized_writer_validate_percentage";
    private static final String ORC_OPTIMIZED_WRITER_VALIDATE_MODE = "orc_optimized_writer_validate_mode";
    private static final String ORC_OPTIMIZED_WRITER_MIN_STRIPE_SIZE = "orc_optimized_writer_min_stripe_size";
    private static final String ORC_OPTIMIZED_WRITER_MAX_STRIPE_SIZE = "orc_optimized_writer_max_stripe_size";
    private static final String ORC_OPTIMIZED_WRITER_MAX_STRIPE_ROWS = "orc_optimized_writer_max_stripe_rows";
    private static final String ORC_OPTIMIZED_WRITER_MAX_DICTIONARY_MEMORY = "orc_optimized_writer_max_dictionary_memory";
    private static final String PAGEFILE_WRITER_MAX_STRIPE_SIZE = "pagefile_writer_max_stripe_size";
    public static final String HIVE_STORAGE_FORMAT = "hive_storage_format";
    private static final String COMPRESSION_CODEC = "compression_codec";
    private static final String ORC_COMPRESSION_CODEC = "orc_compression_codec";
    public static final String RESPECT_TABLE_FORMAT = "respect_table_format";
    private static final String PARQUET_USE_COLUMN_NAME = "parquet_use_column_names";
    private static final String PARQUET_FAIL_WITH_CORRUPTED_STATISTICS = "parquet_fail_with_corrupted_statistics";
    private static final String PARQUET_MAX_READ_BLOCK_SIZE = "parquet_max_read_block_size";
    private static final String PARQUET_WRITER_BLOCK_SIZE = "parquet_writer_block_size";
    private static final String PARQUET_WRITER_PAGE_SIZE = "parquet_writer_page_size";
    private static final String PARQUET_OPTIMIZED_WRITER_ENABLED = "parquet_optimized_writer_enabled";
    private static final String MAX_SPLIT_SIZE = "max_split_size";
    private static final String MAX_INITIAL_SPLIT_SIZE = "max_initial_split_size";
    public static final String RCFILE_OPTIMIZED_WRITER_ENABLED = "rcfile_optimized_writer_enabled";
    private static final String RCFILE_OPTIMIZED_WRITER_VALIDATE = "rcfile_optimized_writer_validate";
    private static final String SORTED_WRITING_ENABLED = "sorted_writing_enabled";
    public static final String SORTED_WRITE_TO_TEMP_PATH_ENABLED = "sorted_write_to_temp_path_enabled";
    public static final String SORTED_WRITE_TEMP_PATH_SUBDIRECTORY_COUNT = "sorted_write_temp_path_subdirectory_count";
    private static final String STATISTICS_ENABLED = "statistics_enabled";
    private static final String PARTITION_STATISTICS_SAMPLE_SIZE = "partition_statistics_sample_size";
    private static final String IGNORE_CORRUPTED_STATISTICS = "ignore_corrupted_statistics";
    public static final String COLLECT_COLUMN_STATISTICS_ON_WRITE = "collect_column_statistics_on_write";
    public static final String PARTITION_STATISTICS_BASED_OPTIMIZATION_ENABLED = "partition_stats_based_optimization_enabled";
    private static final String OPTIMIZE_MISMATCHED_BUCKET_COUNT = "optimize_mismatched_bucket_count";
    private static final String S3_SELECT_PUSHDOWN_ENABLED = "s3_select_pushdown_enabled";
    public static final String SHUFFLE_PARTITIONED_COLUMNS_FOR_TABLE_WRITE = "shuffle_partitioned_columns_for_table_write";
    private static final String TEMPORARY_STAGING_DIRECTORY_ENABLED = "temporary_staging_directory_enabled";
    private static final String TEMPORARY_STAGING_DIRECTORY_PATH = "temporary_staging_directory_path";
    private static final String TEMPORARY_TABLE_SCHEMA = "temporary_table_schema";
    private static final String TEMPORARY_TABLE_STORAGE_FORMAT = "temporary_table_storage_format";
    private static final String TEMPORARY_TABLE_COMPRESSION_CODEC = "temporary_table_compression_codec";
    private static final String TEMPORARY_TABLE_CREATE_EMPTY_BUCKET_FILES = "temporary_table_create_empty_bucket_files";
    private static final String USE_PAGEFILE_FOR_HIVE_UNSUPPORTED_TYPE = "use_pagefile_for_hive_unsupported_type";
    public static final String PUSHDOWN_FILTER_ENABLED = "pushdown_filter_enabled";
    public static final String RANGE_FILTERS_ON_SUBSCRIPTS_ENABLED = "range_filters_on_subscripts_enabled";
    public static final String ADAPTIVE_FILTER_REORDERING_ENABLED = "adaptive_filter_reordering_enabled";
    public static final String VIRTUAL_BUCKET_COUNT = "virtual_bucket_count";
    public static final String MAX_BUCKETS_FOR_GROUPED_EXECUTION = "max_buckets_for_grouped_execution";
    public static final String OFFLINE_DATA_DEBUG_MODE_ENABLED = "offline_data_debug_mode_enabled";
    public static final String FAIL_FAST_ON_INSERT_INTO_IMMUTABLE_PARTITIONS_ENABLED = "fail_fast_on_insert_into_immutable_partitions_enabled";
    public static final String USE_LIST_DIRECTORY_CACHE = "use_list_directory_cache";
    private static final String PARQUET_BATCH_READ_OPTIMIZATION_ENABLED = "parquet_batch_read_optimization_enabled";
    private static final String PARQUET_BATCH_READER_VERIFICATION_ENABLED = "parquet_batch_reader_verification_enabled";
    private static final String BUCKET_FUNCTION_TYPE_FOR_EXCHANGE = "bucket_function_type_for_exchange";
    public static final String PARQUET_DEREFERENCE_PUSHDOWN_ENABLED = "parquet_dereference_pushdown_enabled";
    public static final String IGNORE_UNREADABLE_PARTITION = "ignore_unreadable_partition";
    public static final String PARTIAL_AGGREGATION_PUSHDOWN_ENABLED = "partial_aggregation_pushdown_enabled";
    public static final String PARTIAL_AGGREGATION_PUSHDOWN_FOR_VARIABLE_LENGTH_DATATYPES_ENABLED = "partial_aggregation_pushdown_for_variable_length_datatypes_enabled";
    public static final String FILE_RENAMING_ENABLED = "file_renaming_enabled";
    public static final String PREFER_MANIFESTS_TO_LIST_FILES = "prefer_manifests_to_list_files";
    public static final String MANIFEST_VERIFICATION_ENABLED = "manifest_verification_enabled";
    public static final String NEW_PARTITION_USER_SUPPLIED_PARAMETER = "new_partition_user_supplied_parameter";
    public static final String PREFER_METADATA_TO_LIST_HUDI_FILES = "prefer_metadata_to_list_hudi_files";
    public static final String HUDI_METADATA_VERIFICATION_ENABLED = "hudi_metadata_verification_enabled";

    private final List<PropertyMetadata<?>> sessionProperties;

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

    @Inject
    public HiveSessionProperties(HiveClientConfig hiveClientConfig, OrcFileWriterConfig orcFileWriterConfig, ParquetFileWriterConfig parquetFileWriterConfig)
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        IGNORE_TABLE_BUCKETING,
                        "Ignore table bucketing to enable reading from unbucketed partitions",
                        hiveClientConfig.isIgnoreTableBucketing(),
                        false),
                integerProperty(
                        MIN_BUCKET_COUNT_TO_NOT_IGNORE_TABLE_BUCKETING,
                        "Ignore table bucketing when table bucket count is less than the value specified",
                        hiveClientConfig.getMinBucketCountToNotIgnoreTableBucketing(),
                        true),
                booleanProperty(
                        BUCKET_EXECUTION_ENABLED,
                        "Enable bucket-aware execution: only use a single worker per bucket",
                        hiveClientConfig.isBucketExecutionEnabled(),
                        false),
                new PropertyMetadata<>(
                        NODE_SELECTION_STRATEGY,
                        "Node affinity selection strategy",
                        VARCHAR,
                        NodeSelectionStrategy.class,
                        hiveClientConfig.getNodeSelectionStrategy(),
                        false,
                        value -> NodeSelectionStrategy.valueOf((String) value),
                        NodeSelectionStrategy::toString),
                new PropertyMetadata<>(
                        INSERT_EXISTING_PARTITIONS_BEHAVIOR,
                        "Behavior on insert existing partitions; this session property doesn't control behavior on insert existing unpartitioned table",
                        VARCHAR,
                        InsertExistingPartitionsBehavior.class,
                        getDefaultInsertExistingPartitionsBehavior(hiveClientConfig),
                        false,
                        value -> InsertExistingPartitionsBehavior.valueOf((String) value, hiveClientConfig.isImmutablePartitions()),
                        InsertExistingPartitionsBehavior::toString),
                booleanProperty(
                        ORC_BLOOM_FILTERS_ENABLED,
                        "ORC: Enable bloom filters for predicate pushdown",
                        hiveClientConfig.isOrcBloomFiltersEnabled(),
                        false),
                dataSizeSessionProperty(
                        ORC_MAX_MERGE_DISTANCE,
                        "ORC: Maximum size of gap between two reads to merge into a single read",
                        hiveClientConfig.getOrcMaxMergeDistance(),
                        false),
                dataSizeSessionProperty(
                        ORC_MAX_BUFFER_SIZE,
                        "ORC: Maximum size of a single read",
                        hiveClientConfig.getOrcMaxBufferSize(),
                        false),
                dataSizeSessionProperty(
                        ORC_STREAM_BUFFER_SIZE,
                        "ORC: Size of buffer for streaming reads",
                        hiveClientConfig.getOrcStreamBufferSize(),
                        false),
                dataSizeSessionProperty(
                        ORC_TINY_STRIPE_THRESHOLD,
                        "ORC: Threshold below which an ORC stripe or file will read in its entirety",
                        hiveClientConfig.getOrcTinyStripeThreshold(),
                        false),
                dataSizeSessionProperty(
                        ORC_MAX_READ_BLOCK_SIZE,
                        "ORC: Soft max size of Presto blocks produced by ORC reader",
                        hiveClientConfig.getOrcMaxReadBlockSize(),
                        false),
                booleanProperty(
                        ORC_LAZY_READ_SMALL_RANGES,
                        "Experimental: ORC: Read small file segments lazily",
                        hiveClientConfig.isOrcLazyReadSmallRanges(),
                        false),
                dataSizeSessionProperty(
                        ORC_STRING_STATISTICS_LIMIT,
                        "ORC: Maximum size of string statistics; drop if exceeding",
                        orcFileWriterConfig.getStringStatisticsLimit(),
                        false),
                booleanProperty(
                        ORC_OPTIMIZED_WRITER_ENABLED,
                        "Experimental: ORC: Enable optimized writer",
                        hiveClientConfig.isOrcOptimizedWriterEnabled(),
                        false),
                booleanProperty(
                        ORC_OPTIMIZED_WRITER_VALIDATE,
                        "Experimental: ORC: Force all validation for files",
                        hiveClientConfig.getOrcWriterValidationPercentage() > 0.0,
                        false),
                new PropertyMetadata<>(
                        ORC_OPTIMIZED_WRITER_VALIDATE_PERCENTAGE,
                        "Experimental: ORC: sample percentage for validation for files",
                        DOUBLE,
                        Double.class,
                        hiveClientConfig.getOrcWriterValidationPercentage(),
                        false,
                        value -> {
                            double doubleValue = ((Number) value).doubleValue();
                            if (doubleValue < 0.0 || doubleValue > 100.0) {
                                throw new PrestoException(
                                        INVALID_SESSION_PROPERTY,
                                        format("%s must be between 0.0 and 100.0 inclusive: %s", ORC_OPTIMIZED_WRITER_VALIDATE_PERCENTAGE, doubleValue));
                            }
                            return doubleValue;
                        },
                        value -> value),
                stringProperty(
                        ORC_OPTIMIZED_WRITER_VALIDATE_MODE,
                        "Experimental: ORC: Level of detail in ORC validation",
                        hiveClientConfig.getOrcWriterValidationMode().toString(),
                        false),
                dataSizeSessionProperty(
                        ORC_OPTIMIZED_WRITER_MIN_STRIPE_SIZE,
                        "Experimental: ORC: Min stripe size",
                        orcFileWriterConfig.getStripeMinSize(),
                        false),
                dataSizeSessionProperty(
                        ORC_OPTIMIZED_WRITER_MAX_STRIPE_SIZE,
                        "Experimental: ORC: Max stripe size",
                        orcFileWriterConfig.getStripeMaxSize(),
                        false),
                integerProperty(
                        ORC_OPTIMIZED_WRITER_MAX_STRIPE_ROWS,
                        "Experimental: ORC: Max stripe row count",
                        orcFileWriterConfig.getStripeMaxRowCount(),
                        false),
                dataSizeSessionProperty(
                        ORC_OPTIMIZED_WRITER_MAX_DICTIONARY_MEMORY,
                        "Experimental: ORC: Max dictionary memory",
                        orcFileWriterConfig.getDictionaryMaxMemory(),
                        false),
                dataSizeSessionProperty(
                        PAGEFILE_WRITER_MAX_STRIPE_SIZE,
                        "PAGEFILE: Max stripe size",
                        hiveClientConfig.getPageFileStripeMaxSize(),
                        false),
                stringProperty(
                        HIVE_STORAGE_FORMAT,
                        "Default storage format for new tables or partitions",
                        hiveClientConfig.getHiveStorageFormat().toString(),
                        false),
                new PropertyMetadata<>(
                        COMPRESSION_CODEC,
                        "The compression codec to use when writing files",
                        VARCHAR,
                        HiveCompressionCodec.class,
                        hiveClientConfig.getCompressionCodec(),
                        false,
                        value -> HiveCompressionCodec.valueOf(((String) value).toUpperCase()),
                        HiveCompressionCodec::name),
                new PropertyMetadata<>(
                        ORC_COMPRESSION_CODEC,
                        "The preferred compression codec to use when writing ORC and DWRF files",
                        VARCHAR,
                        HiveCompressionCodec.class,
                        hiveClientConfig.getOrcCompressionCodec(),
                        false,
                        value -> HiveCompressionCodec.valueOf(((String) value).toUpperCase()),
                        HiveCompressionCodec::name),
                booleanProperty(
                        RESPECT_TABLE_FORMAT,
                        "Write new partitions using table format rather than default storage format",
                        hiveClientConfig.isRespectTableFormat(),
                        false),
                booleanProperty(
                        PARQUET_USE_COLUMN_NAME,
                        "Experimental: Parquet: Access Parquet columns using names from the file",
                        hiveClientConfig.isUseParquetColumnNames(),
                        false),
                booleanProperty(
                        PARQUET_FAIL_WITH_CORRUPTED_STATISTICS,
                        "Parquet: Fail when scanning Parquet files with corrupted statistics",
                        hiveClientConfig.isFailOnCorruptedParquetStatistics(),
                        false),
                dataSizeSessionProperty(
                        PARQUET_MAX_READ_BLOCK_SIZE,
                        "Parquet: Maximum size of a block to read",
                        hiveClientConfig.getParquetMaxReadBlockSize(),
                        false),
                dataSizeSessionProperty(
                        PARQUET_WRITER_BLOCK_SIZE,
                        "Parquet: Writer block size",
                        parquetFileWriterConfig.getBlockSize(),
                        false),
                dataSizeSessionProperty(
                        PARQUET_WRITER_PAGE_SIZE,
                        "Parquet: Writer page size",
                        parquetFileWriterConfig.getPageSize(),
                        false),
                dataSizeSessionProperty(
                        MAX_SPLIT_SIZE,
                        "Max split size",
                        hiveClientConfig.getMaxSplitSize(),
                        true),
                dataSizeSessionProperty(
                        MAX_INITIAL_SPLIT_SIZE,
                        "Max initial split size",
                        hiveClientConfig.getMaxInitialSplitSize(),
                        true),
                booleanProperty(
                        RCFILE_OPTIMIZED_WRITER_ENABLED,
                        "Experimental: RCFile: Enable optimized writer",
                        hiveClientConfig.isRcfileOptimizedWriterEnabled(),
                        false),
                booleanProperty(
                        RCFILE_OPTIMIZED_WRITER_VALIDATE,
                        "Experimental: RCFile: Validate writer files",
                        hiveClientConfig.isRcfileWriterValidate(),
                        false),
                booleanProperty(
                        SORTED_WRITING_ENABLED,
                        "Enable writing to bucketed sorted tables",
                        hiveClientConfig.isSortedWritingEnabled(),
                        false),
                booleanProperty(
                        SORTED_WRITE_TO_TEMP_PATH_ENABLED,
                        "Enable writing temp files to temp path when writing to bucketed sorted tables",
                        hiveClientConfig.isSortedWriteToTempPathEnabled(),
                        false),
                integerProperty(
                        SORTED_WRITE_TEMP_PATH_SUBDIRECTORY_COUNT,
                        "Number of directories per partition for temp files generated by writing sorted table",
                        hiveClientConfig.getSortedWriteTempPathSubdirectoryCount(),
                        false),
                booleanProperty(
                        STATISTICS_ENABLED,
                        "Experimental: Expose table statistics",
                        hiveClientConfig.isTableStatisticsEnabled(),
                        false),
                integerProperty(
                        PARTITION_STATISTICS_SAMPLE_SIZE,
                        "Maximum sample size of the partitions column statistics",
                        hiveClientConfig.getPartitionStatisticsSampleSize(),
                        false),
                booleanProperty(
                        IGNORE_CORRUPTED_STATISTICS,
                        "Experimental: Ignore corrupted statistics rather than failing",
                        hiveClientConfig.isIgnoreCorruptedStatistics(),
                        false),
                booleanProperty(
                        COLLECT_COLUMN_STATISTICS_ON_WRITE,
                        "Experimental: Enables automatic column level statistics collection on write",
                        hiveClientConfig.isCollectColumnStatisticsOnWrite(),
                        false),
                booleanProperty(
                        PARTITION_STATISTICS_BASED_OPTIMIZATION_ENABLED,
                        "Enables partition stats based optimization, including partition pruning and predicate stripping",
                        hiveClientConfig.isPartitionStatisticsBasedOptimizationEnabled(),
                        false),
                booleanProperty(
                        OPTIMIZE_MISMATCHED_BUCKET_COUNT,
                        "Experimental: Enable optimization to avoid shuffle when bucket count is compatible but not the same",
                        hiveClientConfig.isOptimizeMismatchedBucketCount(),
                        false),
                booleanProperty(
                        S3_SELECT_PUSHDOWN_ENABLED,
                        "S3 Select pushdown enabled",
                        hiveClientConfig.isS3SelectPushdownEnabled(),
                        false),
                booleanProperty(
                        TEMPORARY_STAGING_DIRECTORY_ENABLED,
                        "Should use temporary staging directory for write operations",
                        hiveClientConfig.isTemporaryStagingDirectoryEnabled(),
                        false),
                stringProperty(
                        TEMPORARY_STAGING_DIRECTORY_PATH,
                        "Temporary staging directory location",
                        hiveClientConfig.getTemporaryStagingDirectoryPath(),
                        false),
                stringProperty(
                        TEMPORARY_TABLE_SCHEMA,
                        "Schema where to create temporary tables",
                        hiveClientConfig.getTemporaryTableSchema(),
                        false),
                new PropertyMetadata<>(
                        TEMPORARY_TABLE_STORAGE_FORMAT,
                        "Storage format used to store data in temporary tables",
                        VARCHAR,
                        HiveStorageFormat.class,
                        hiveClientConfig.getTemporaryTableStorageFormat(),
                        false,
                        value -> HiveStorageFormat.valueOf(((String) value).toUpperCase()),
                        HiveStorageFormat::name),
                new PropertyMetadata<>(
                        TEMPORARY_TABLE_COMPRESSION_CODEC,
                        "Compression codec used to store data in temporary tables",
                        VARCHAR,
                        HiveCompressionCodec.class,
                        hiveClientConfig.getTemporaryTableCompressionCodec(),
                        false,
                        value -> HiveCompressionCodec.valueOf(((String) value).toUpperCase()),
                        HiveCompressionCodec::name),
                booleanProperty(
                        TEMPORARY_TABLE_CREATE_EMPTY_BUCKET_FILES,
                        "Create empty files when there is no data for temporary table buckets",
                        hiveClientConfig.isCreateEmptyBucketFilesForTemporaryTable(),
                        false),
                booleanProperty(
                        USE_PAGEFILE_FOR_HIVE_UNSUPPORTED_TYPE,
                        "Automatically switch to PAGEFILE format for materialized exchange when encountering unsupported types",
                        hiveClientConfig.getUsePageFileForHiveUnsupportedType(),
                        true),
                booleanProperty(
                        PUSHDOWN_FILTER_ENABLED,
                        "Experimental: enable complex filter pushdown",
                        hiveClientConfig.isPushdownFilterEnabled(),
                        false),
                booleanProperty(
                        RANGE_FILTERS_ON_SUBSCRIPTS_ENABLED,
                        "Experimental: enable pushdown of range filters on subscripts (a[2] = 5) into ORC column readers",
                        hiveClientConfig.isRangeFiltersOnSubscriptsEnabled(),
                        false),
                booleanProperty(
                        ADAPTIVE_FILTER_REORDERING_ENABLED,
                        "Experimental: enable adaptive filter reordering",
                        hiveClientConfig.isAdaptiveFilterReorderingEnabled(),
                        false),
                integerProperty(
                        VIRTUAL_BUCKET_COUNT,
                        "Number of virtual bucket assigned for unbucketed tables",
                        0,
                        false),
                integerProperty(
                        MAX_BUCKETS_FOR_GROUPED_EXECUTION,
                        "maximum total buckets to allow using grouped execution",
                        hiveClientConfig.getMaxBucketsForGroupedExecution(),
                        false),
                booleanProperty(
                        OFFLINE_DATA_DEBUG_MODE_ENABLED,
                        "allow reading from tables or partitions that are marked as offline or not readable",
                        false,
                        true),
                booleanProperty(
                        ORC_ZSTD_JNI_DECOMPRESSION_ENABLED,
                        "use JNI based zstd decompression for reading ORC files",
                        hiveClientConfig.isZstdJniDecompressionEnabled(),
                        true),
                booleanProperty(
                        SHUFFLE_PARTITIONED_COLUMNS_FOR_TABLE_WRITE,
                        "Shuffle the data on partitioned columns",
                        false,
                        false),
                booleanProperty(
                        FAIL_FAST_ON_INSERT_INTO_IMMUTABLE_PARTITIONS_ENABLED,
                        "Fail fast when trying to insert into an immutable partition. Increases load on the metastore",
                        hiveClientConfig.isFailFastOnInsertIntoImmutablePartitionsEnabled(),
                        false),
                booleanProperty(
                        USE_LIST_DIRECTORY_CACHE,
                        "Use list directory cache if available when set to true",
                        !hiveClientConfig.getFileStatusCacheTables().isEmpty(),
                        false),
                booleanProperty(
                        PARQUET_OPTIMIZED_WRITER_ENABLED,
                        "Experimental: Enable optimized writer",
                        parquetFileWriterConfig.isParquetOptimizedWriterEnabled(),
                        false),
                booleanProperty(
                        PARQUET_BATCH_READ_OPTIMIZATION_ENABLED,
                        "Is Parquet batch read optimization enabled",
                        hiveClientConfig.isParquetBatchReadOptimizationEnabled(),
                        false),
                booleanProperty(
                        PARQUET_BATCH_READER_VERIFICATION_ENABLED,
                        "Is Parquet batch reader verification enabled? This is for testing purposes only, not to be used in production",
                        hiveClientConfig.isParquetBatchReaderVerificationEnabled(),
                        false),
                booleanProperty(
                        IGNORE_UNREADABLE_PARTITION,
                        "Ignore unreadable partitions and report as warnings instead of failing the query",
                        hiveClientConfig.isIgnoreUnreadablePartition(),
                        false),
                new PropertyMetadata<>(
                        BUCKET_FUNCTION_TYPE_FOR_EXCHANGE,
                        "hash function type for bucketed table exchange",
                        VARCHAR,
                        BucketFunctionType.class,
                        hiveClientConfig.getBucketFunctionTypeForExchange(),
                        false,
                        value -> BucketFunctionType.valueOf((String) value),
                        BucketFunctionType::toString),
                booleanProperty(
                        PARQUET_DEREFERENCE_PUSHDOWN_ENABLED,
                        "Is dereference pushdown expression pushdown into Parquet reader enabled?",
                        hiveClientConfig.isParquetDereferencePushdownEnabled(),
                        false),
                booleanProperty(
                        PARTIAL_AGGREGATION_PUSHDOWN_ENABLED,
                        "Is partial aggregation pushdown enabled for Hive file formats",
                        hiveClientConfig.isPartialAggregationPushdownEnabled(),
                        false),
                booleanProperty(
                        PARTIAL_AGGREGATION_PUSHDOWN_FOR_VARIABLE_LENGTH_DATATYPES_ENABLED,
                        "Is partial aggregation pushdown enabled for variable length datatypes",
                        hiveClientConfig.isPartialAggregationPushdownForVariableLengthDatatypesEnabled(),
                        false),
                booleanProperty(
                        FILE_RENAMING_ENABLED,
                        "Enable renaming the files written by writers",
                        hiveClientConfig.isFileRenamingEnabled(),
                        false),
                booleanProperty(
                        PREFER_MANIFESTS_TO_LIST_FILES,
                        "Prefer to fetch the list of file names and sizes from manifest",
                        hiveClientConfig.isPreferManifestsToListFiles(),
                        false),
                booleanProperty(
                        MANIFEST_VERIFICATION_ENABLED,
                        "Enable manifest verification",
                        hiveClientConfig.isManifestVerificationEnabled(),
                        false),
                stringProperty(
                        NEW_PARTITION_USER_SUPPLIED_PARAMETER,
                        "\"user_supplied\" parameter added to all newly created partitions",
                        null,
                        true),
                booleanProperty(
                        PREFER_METADATA_TO_LIST_HUDI_FILES,
                        "For Hudi tables prefer to fetch the list of files from its metadata",
                        hiveClientConfig.isPreferMetadataToListHudiFiles(),
                        false),
                booleanProperty(
                        HUDI_METADATA_VERIFICATION_ENABLED,
                        "Verify file listing maintained in Hudi table metadata against the file system",
                        hiveClientConfig.isHudiMetadataVerificationEnabled(),
                        false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isBucketExecutionEnabled(ConnectorSession session)
    {
        return session.getProperty(BUCKET_EXECUTION_ENABLED, Boolean.class);
    }

    public static boolean shouldIgnoreTableBucketing(ConnectorSession session)
    {
        return session.getProperty(IGNORE_TABLE_BUCKETING, Boolean.class);
    }

    public static Integer getMinBucketCountToNotIgnoreTableBucketing(ConnectorSession session)
    {
        return session.getProperty(MIN_BUCKET_COUNT_TO_NOT_IGNORE_TABLE_BUCKETING, Integer.class);
    }

    public static int getMaxBucketsForGroupedExecution(ConnectorSession session)
    {
        return session.getProperty(MAX_BUCKETS_FOR_GROUPED_EXECUTION, Integer.class);
    }

    public static NodeSelectionStrategy getNodeSelectionStrategy(ConnectorSession session)
    {
        return session.getProperty(NODE_SELECTION_STRATEGY, NodeSelectionStrategy.class);
    }

    public static InsertExistingPartitionsBehavior getInsertExistingPartitionsBehavior(ConnectorSession session)
    {
        return session.getProperty(INSERT_EXISTING_PARTITIONS_BEHAVIOR, InsertExistingPartitionsBehavior.class);
    }

    public static boolean isOrcBloomFiltersEnabled(ConnectorSession session)
    {
        return session.getProperty(ORC_BLOOM_FILTERS_ENABLED, Boolean.class);
    }

    public static DataSize getOrcMaxMergeDistance(ConnectorSession session)
    {
        return session.getProperty(ORC_MAX_MERGE_DISTANCE, DataSize.class);
    }

    public static DataSize getOrcMaxBufferSize(ConnectorSession session)
    {
        return session.getProperty(ORC_MAX_BUFFER_SIZE, DataSize.class);
    }

    public static DataSize getOrcStreamBufferSize(ConnectorSession session)
    {
        return session.getProperty(ORC_STREAM_BUFFER_SIZE, DataSize.class);
    }

    public static DataSize getOrcTinyStripeThreshold(ConnectorSession session)
    {
        return session.getProperty(ORC_TINY_STRIPE_THRESHOLD, DataSize.class);
    }

    public static DataSize getOrcMaxReadBlockSize(ConnectorSession session)
    {
        return session.getProperty(ORC_MAX_READ_BLOCK_SIZE, DataSize.class);
    }

    public static boolean getOrcLazyReadSmallRanges(ConnectorSession session)
    {
        return session.getProperty(ORC_LAZY_READ_SMALL_RANGES, Boolean.class);
    }

    public static boolean isOrcZstdJniDecompressionEnabled(ConnectorSession session)
    {
        return session.getProperty(ORC_ZSTD_JNI_DECOMPRESSION_ENABLED, Boolean.class);
    }

    public static DataSize getOrcStringStatisticsLimit(ConnectorSession session)
    {
        return session.getProperty(ORC_STRING_STATISTICS_LIMIT, DataSize.class);
    }

    public static boolean isOrcOptimizedWriterEnabled(ConnectorSession session)
    {
        return session.getProperty(ORC_OPTIMIZED_WRITER_ENABLED, Boolean.class);
    }

    public static boolean isOrcOptimizedWriterValidate(ConnectorSession session)
    {
        boolean validate = session.getProperty(ORC_OPTIMIZED_WRITER_VALIDATE, Boolean.class);
        double percentage = session.getProperty(ORC_OPTIMIZED_WRITER_VALIDATE_PERCENTAGE, Double.class);

        checkArgument(percentage >= 0.0 && percentage <= 100.0);

        // session property can disabled validation
        if (!validate) {
            return false;
        }

        // session property can not force validation when sampling is enabled
        // todo change this if session properties support null
        return ThreadLocalRandom.current().nextDouble(100) < percentage;
    }

    public static OrcWriteValidationMode getOrcOptimizedWriterValidateMode(ConnectorSession session)
    {
        return OrcWriteValidationMode.valueOf(session.getProperty(ORC_OPTIMIZED_WRITER_VALIDATE_MODE, String.class).toUpperCase(ENGLISH));
    }

    public static DataSize getOrcOptimizedWriterMinStripeSize(ConnectorSession session)
    {
        return session.getProperty(ORC_OPTIMIZED_WRITER_MIN_STRIPE_SIZE, DataSize.class);
    }

    public static DataSize getOrcOptimizedWriterMaxStripeSize(ConnectorSession session)
    {
        return session.getProperty(ORC_OPTIMIZED_WRITER_MAX_STRIPE_SIZE, DataSize.class);
    }

    public static int getOrcOptimizedWriterMaxStripeRows(ConnectorSession session)
    {
        return session.getProperty(ORC_OPTIMIZED_WRITER_MAX_STRIPE_ROWS, Integer.class);
    }

    public static DataSize getOrcOptimizedWriterMaxDictionaryMemory(ConnectorSession session)
    {
        return session.getProperty(ORC_OPTIMIZED_WRITER_MAX_DICTIONARY_MEMORY, DataSize.class);
    }

    public static DataSize getPageFileStripeMaxSize(ConnectorSession session)
    {
        return session.getProperty(PAGEFILE_WRITER_MAX_STRIPE_SIZE, DataSize.class);
    }

    public static HiveStorageFormat getHiveStorageFormat(ConnectorSession session)
    {
        return HiveStorageFormat.valueOf(session.getProperty(HIVE_STORAGE_FORMAT, String.class).toUpperCase(ENGLISH));
    }

    public static HiveCompressionCodec getCompressionCodec(ConnectorSession session)
    {
        return session.getProperty(COMPRESSION_CODEC, HiveCompressionCodec.class);
    }

    public static HiveCompressionCodec getOrcCompressionCodec(ConnectorSession session)
    {
        return session.getProperty(ORC_COMPRESSION_CODEC, HiveCompressionCodec.class);
    }

    public static boolean isRespectTableFormat(ConnectorSession session)
    {
        return session.getProperty(RESPECT_TABLE_FORMAT, Boolean.class);
    }

    public static boolean isUseParquetColumnNames(ConnectorSession session)
    {
        return session.getProperty(PARQUET_USE_COLUMN_NAME, Boolean.class);
    }

    public static boolean isFailOnCorruptedParquetStatistics(ConnectorSession session)
    {
        return session.getProperty(PARQUET_FAIL_WITH_CORRUPTED_STATISTICS, Boolean.class);
    }

    public static DataSize getParquetMaxReadBlockSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_MAX_READ_BLOCK_SIZE, DataSize.class);
    }

    public static DataSize getParquetWriterBlockSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_BLOCK_SIZE, DataSize.class);
    }

    public static DataSize getParquetWriterPageSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_PAGE_SIZE, DataSize.class);
    }

    public static DataSize getMaxSplitSize(ConnectorSession session)
    {
        return session.getProperty(MAX_SPLIT_SIZE, DataSize.class);
    }

    public static DataSize getMaxInitialSplitSize(ConnectorSession session)
    {
        return session.getProperty(MAX_INITIAL_SPLIT_SIZE, DataSize.class);
    }

    public static boolean isRcfileOptimizedWriterEnabled(ConnectorSession session)
    {
        return session.getProperty(RCFILE_OPTIMIZED_WRITER_ENABLED, Boolean.class);
    }

    public static boolean isRcfileOptimizedWriterValidate(ConnectorSession session)
    {
        return session.getProperty(RCFILE_OPTIMIZED_WRITER_VALIDATE, Boolean.class);
    }

    public static boolean isSortedWritingEnabled(ConnectorSession session)
    {
        return session.getProperty(SORTED_WRITING_ENABLED, Boolean.class);
    }

    public static boolean isSortedWriteToTempPathEnabled(ConnectorSession session)
    {
        return session.getProperty(SORTED_WRITE_TO_TEMP_PATH_ENABLED, Boolean.class);
    }

    public static int getSortedWriteTempPathSubdirectoryCount(ConnectorSession session)
    {
        return session.getProperty(SORTED_WRITE_TEMP_PATH_SUBDIRECTORY_COUNT, Integer.class);
    }

    public static boolean isS3SelectPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(S3_SELECT_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static boolean isStatisticsEnabled(ConnectorSession session)
    {
        return session.getProperty(STATISTICS_ENABLED, Boolean.class);
    }

    public static int getPartitionStatisticsSampleSize(ConnectorSession session)
    {
        int size = session.getProperty(PARTITION_STATISTICS_SAMPLE_SIZE, Integer.class);
        if (size < 1) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, format("%s must be greater than 0: %s", PARTITION_STATISTICS_SAMPLE_SIZE, size));
        }
        return size;
    }

    public static boolean isIgnoreCorruptedStatistics(ConnectorSession session)
    {
        return session.getProperty(IGNORE_CORRUPTED_STATISTICS, Boolean.class);
    }

    public static boolean isCollectColumnStatisticsOnWrite(ConnectorSession session)
    {
        return session.getProperty(COLLECT_COLUMN_STATISTICS_ON_WRITE, Boolean.class);
    }

    public static boolean isPartitionStatisticsBasedOptimizationEnabled(ConnectorSession session)
    {
        return session.getProperty(PARTITION_STATISTICS_BASED_OPTIMIZATION_ENABLED, Boolean.class);
    }

    @Deprecated
    public static boolean isOptimizedMismatchedBucketCount(ConnectorSession session)
    {
        return session.getProperty(OPTIMIZE_MISMATCHED_BUCKET_COUNT, Boolean.class);
    }

    public static boolean isTemporaryStagingDirectoryEnabled(ConnectorSession session)
    {
        return session.getProperty(TEMPORARY_STAGING_DIRECTORY_ENABLED, Boolean.class);
    }

    public static String getTemporaryStagingDirectoryPath(ConnectorSession session)
    {
        return session.getProperty(TEMPORARY_STAGING_DIRECTORY_PATH, String.class);
    }

    public static String getTemporaryTableSchema(ConnectorSession session)
    {
        return session.getProperty(TEMPORARY_TABLE_SCHEMA, String.class);
    }

    public static HiveStorageFormat getTemporaryTableStorageFormat(ConnectorSession session)
    {
        return session.getProperty(TEMPORARY_TABLE_STORAGE_FORMAT, HiveStorageFormat.class);
    }

    public static HiveCompressionCodec getTemporaryTableCompressionCodec(ConnectorSession session)
    {
        return session.getProperty(TEMPORARY_TABLE_COMPRESSION_CODEC, HiveCompressionCodec.class);
    }

    public static boolean shouldCreateEmptyBucketFilesForTemporaryTable(ConnectorSession session)
    {
        return session.getProperty(TEMPORARY_TABLE_CREATE_EMPTY_BUCKET_FILES, Boolean.class);
    }

    public static boolean isUsePageFileForHiveUnsupportedType(ConnectorSession session)
    {
        return session.getProperty(USE_PAGEFILE_FOR_HIVE_UNSUPPORTED_TYPE, Boolean.class);
    }

    public static boolean isPushdownFilterEnabled(ConnectorSession session)
    {
        return session.getProperty(PUSHDOWN_FILTER_ENABLED, Boolean.class);
    }

    public static boolean isRangeFiltersOnSubscriptsEnabled(ConnectorSession session)
    {
        return session.getProperty(RANGE_FILTERS_ON_SUBSCRIPTS_ENABLED, Boolean.class);
    }

    public static boolean isAdaptiveFilterReorderingEnabled(ConnectorSession session)
    {
        return session.getProperty(ADAPTIVE_FILTER_REORDERING_ENABLED, Boolean.class);
    }

    public static int getVirtualBucketCount(ConnectorSession session)
    {
        int virtualBucketCount = session.getProperty(VIRTUAL_BUCKET_COUNT, Integer.class);
        if (virtualBucketCount < 0) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, format("%s must not be negative: %s", VIRTUAL_BUCKET_COUNT, virtualBucketCount));
        }
        return virtualBucketCount;
    }

    public static boolean isOfflineDataDebugModeEnabled(ConnectorSession session)
    {
        return session.getProperty(OFFLINE_DATA_DEBUG_MODE_ENABLED, Boolean.class);
    }

    public static boolean shouldIgnoreUnreadablePartition(ConnectorSession session)
    {
        return session.getProperty(IGNORE_UNREADABLE_PARTITION, Boolean.class);
    }

    public static boolean isShufflePartitionedColumnsForTableWriteEnabled(ConnectorSession session)
    {
        return session.getProperty(SHUFFLE_PARTITIONED_COLUMNS_FOR_TABLE_WRITE, Boolean.class);
    }

    public static boolean isParquetBatchReadsEnabled(ConnectorSession session)
    {
        return session.getProperty(PARQUET_BATCH_READ_OPTIMIZATION_ENABLED, Boolean.class);
    }

    public static boolean isParquetBatchReaderVerificationEnabled(ConnectorSession session)
    {
        return session.getProperty(PARQUET_BATCH_READER_VERIFICATION_ENABLED, Boolean.class);
    }

    public static PropertyMetadata<DataSize> dataSizeSessionProperty(String name, String description, DataSize defaultValue, boolean hidden)
    {
        return new PropertyMetadata<>(
                name,
                description,
                createUnboundedVarcharType(),
                DataSize.class,
                defaultValue,
                hidden,
                value -> DataSize.valueOf((String) value),
                DataSize::toString);
    }

    private static InsertExistingPartitionsBehavior getDefaultInsertExistingPartitionsBehavior(HiveClientConfig hiveClientConfig)
    {
        if (!hiveClientConfig.isImmutablePartitions()) {
            return APPEND;
        }

        return hiveClientConfig.isInsertOverwriteImmutablePartitionEnabled() ? OVERWRITE : ERROR;
    }

    public static boolean isFailFastOnInsertIntoImmutablePartitionsEnabled(ConnectorSession session)
    {
        return session.getProperty(FAIL_FAST_ON_INSERT_INTO_IMMUTABLE_PARTITIONS_ENABLED, Boolean.class);
    }

    public static boolean isUseListDirectoryCache(ConnectorSession session)
    {
        return session.getProperty(USE_LIST_DIRECTORY_CACHE, Boolean.class);
    }

    public static boolean isParquetOptimizedWriterEnabled(ConnectorSession session)
    {
        return session.getProperty(PARQUET_OPTIMIZED_WRITER_ENABLED, Boolean.class);
    }

    public static BucketFunctionType getBucketFunctionTypeForExchange(ConnectorSession session)
    {
        return session.getProperty(BUCKET_FUNCTION_TYPE_FOR_EXCHANGE, BucketFunctionType.class);
    }

    public static boolean isParquetDereferencePushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(PARQUET_DEREFERENCE_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static boolean isPartialAggregationPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(PARTIAL_AGGREGATION_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static boolean isPartialAggregationPushdownForVariableLengthDatatypesEnabled(ConnectorSession session)
    {
        return session.getProperty(PARTIAL_AGGREGATION_PUSHDOWN_FOR_VARIABLE_LENGTH_DATATYPES_ENABLED, Boolean.class);
    }

    public static boolean isFileRenamingEnabled(ConnectorSession session)
    {
        return session.getProperty(FILE_RENAMING_ENABLED, Boolean.class);
    }

    public static boolean isPreferManifestsToListFiles(ConnectorSession session)
    {
        return session.getProperty(PREFER_MANIFESTS_TO_LIST_FILES, Boolean.class);
    }

    public static boolean isManifestVerificationEnabled(ConnectorSession session)
    {
        return session.getProperty(MANIFEST_VERIFICATION_ENABLED, Boolean.class);
    }

    public static Optional<String> getNewPartitionUserSuppliedParameter(ConnectorSession session)
    {
        return Optional.ofNullable(session.getProperty(NEW_PARTITION_USER_SUPPLIED_PARAMETER, String.class));
    }

    public static boolean isPreferMetadataToListHudiFiles(ConnectorSession session)
    {
        return session.getProperty(PREFER_METADATA_TO_LIST_HUDI_FILES, Boolean.class);
    }

    public static boolean isHudiMetadataVerificationEnabled(ConnectorSession session)
    {
        return session.getProperty(HUDI_METADATA_VERIFICATION_ENABLED, Boolean.class);
    }
}
