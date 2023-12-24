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
package com.facebook.presto.iceberg;

import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveCompressionCodec;
import com.facebook.presto.hive.OrcFileWriterConfig;
import com.facebook.presto.hive.ParquetFileWriterConfig;
import com.facebook.presto.iceberg.nessie.NessieConfig;
import com.facebook.presto.iceberg.util.HiveStatisticsMergeStrategy;
import com.facebook.presto.orc.OrcWriteValidation;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.apache.parquet.column.ParquetProperties;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.doubleProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public final class IcebergSessionProperties
{
    private static final String COMPRESSION_CODEC = "compression_codec";
    private static final String PARQUET_MAX_READ_BLOCK_SIZE = "parquet_max_read_block_size";
    private static final String PARQUET_WRITER_BLOCK_SIZE = "parquet_writer_block_size";
    private static final String PARQUET_WRITER_PAGE_SIZE = "parquet_writer_page_size";
    private static final String PARQUET_WRITER_VERSION = "parquet_writer_version";
    private static final String PARQUET_USE_COLUMN_NAMES = "parquet_use_column_names";
    private static final String PARQUET_BATCH_READ_OPTIMIZATION_ENABLED = "parquet_batch_read_optimization_enabled";
    private static final String PARQUET_BATCH_READER_VERIFICATION_ENABLED = "parquet_batch_reader_verification_enabled";
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
    private static final String ORC_COMPRESSION_CODEC = "orc_compression_codec";
    private static final String CACHE_ENABLED = "cache_enabled";
    private static final String MINIMUM_ASSIGNED_SPLIT_WEIGHT = "minimum_assigned_split_weight";
    private static final String NODE_SELECTION_STRATEGY = "node_selection_strategy";
    private static final String NESSIE_REFERENCE_NAME = "nessie_reference_name";
    private static final String NESSIE_REFERENCE_HASH = "nessie_reference_hash";
    public static final String READ_MASKED_VALUE_ENABLED = "read_null_masked_parquet_encrypted_value_enabled";
    public static final String PARQUET_DEREFERENCE_PUSHDOWN_ENABLED = "parquet_dereference_pushdown_enabled";
    public static final String MERGE_ON_READ_MODE_ENABLED = "merge_on_read_enabled";
    public static final String PUSHDOWN_FILTER_ENABLED = "pushdown_filter_enabled";
    public static final String HIVE_METASTORE_STATISTICS_MERGE_STRATEGY = "hive_statistics_merge_strategy";
    public static final String STATISTIC_SNAPSHOT_RECORD_DIFFERENCE_WEIGHT = "statistic_snapshot_record_difference_weight";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public IcebergSessionProperties(
            IcebergConfig icebergConfig,
            HiveClientConfig hiveClientConfig,
            ParquetFileWriterConfig parquetFileWriterConfig,
            OrcFileWriterConfig orcFileWriterConfig,
            CacheConfig cacheConfig,
            NessieConfig nessieConfig)
    {
        sessionProperties = ImmutableList.of(
                new PropertyMetadata<>(
                        COMPRESSION_CODEC,
                        "The compression codec to use when writing files",
                        VARCHAR,
                        HiveCompressionCodec.class,
                        icebergConfig.getCompressionCodec(),
                        false,
                        value -> HiveCompressionCodec.valueOf(((String) value).toUpperCase()),
                        HiveCompressionCodec::name),
                booleanProperty(
                        PARQUET_USE_COLUMN_NAMES,
                        "Experimental: Parquet: Access Parquet columns using names from the file",
                        hiveClientConfig.isUseParquetColumnNames(),
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
                new PropertyMetadata<>(
                        PARQUET_WRITER_VERSION,
                        "Parquet: Writer version",
                        VARCHAR,
                        ParquetProperties.WriterVersion.class,
                        parquetFileWriterConfig.getWriterVersion(),
                        false,
                        value -> ParquetProperties.WriterVersion.valueOf(((String) value).toUpperCase()),
                        ParquetProperties.WriterVersion::name),
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
                booleanProperty(
                        ORC_ZSTD_JNI_DECOMPRESSION_ENABLED,
                        "use JNI based zstd decompression for reading ORC files",
                        hiveClientConfig.isZstdJniDecompressionEnabled(),
                        true),
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
                new PropertyMetadata<>(
                        ORC_COMPRESSION_CODEC,
                        "The preferred compression codec to use when writing ORC and DWRF files",
                        VARCHAR,
                        HiveCompressionCodec.class,
                        hiveClientConfig.getOrcCompressionCodec(),
                        false,
                        value -> HiveCompressionCodec.valueOf(((String) value).toUpperCase()),
                        HiveCompressionCodec::name),
                new PropertyMetadata<>(
                        NODE_SELECTION_STRATEGY,
                        "Node affinity selection strategy",
                        VARCHAR,
                        NodeSelectionStrategy.class,
                        hiveClientConfig.getNodeSelectionStrategy(),
                        false,
                        value -> NodeSelectionStrategy.valueOf((String) value),
                        NodeSelectionStrategy::toString),
                booleanProperty(
                        CACHE_ENABLED,
                        "Enable cache for Iceberg",
                        cacheConfig.isCachingEnabled(),
                        false),
                doubleProperty(
                        MINIMUM_ASSIGNED_SPLIT_WEIGHT,
                        "Minimum assigned split weight",
                        icebergConfig.getMinimumAssignedSplitWeight(),
                        false),
                stringProperty(
                        NESSIE_REFERENCE_NAME,
                        "Nessie reference name to use",
                        nessieConfig.getDefaultReferenceName(),
                        false),
                stringProperty(
                        NESSIE_REFERENCE_HASH,
                        "Nessie reference hash to use",
                        null,
                        false),
                booleanProperty(
                        READ_MASKED_VALUE_ENABLED,
                        "Return null when access is denied for an encrypted parquet column",
                        hiveClientConfig.getReadNullMaskedParquetEncryptedValue(),
                        false),
                booleanProperty(
                        PARQUET_DEREFERENCE_PUSHDOWN_ENABLED,
                        "Is dereference pushdown expression pushdown into Parquet reader enabled?",
                        icebergConfig.isParquetDereferencePushdownEnabled(),
                        false),
                booleanProperty(
                        MERGE_ON_READ_MODE_ENABLED,
                        "Reads enabled for merge-on-read Iceberg tables",
                        icebergConfig.isMergeOnReadModeEnabled(),
                        false),
                new PropertyMetadata<>(
                        HIVE_METASTORE_STATISTICS_MERGE_STRATEGY,
                        "choose how to include statistics from the Hive Metastore when calculating table stats. Valid values are: "
                                + Joiner.on(", ").join(HiveStatisticsMergeStrategy.values()),
                        VARCHAR,
                        HiveStatisticsMergeStrategy.class,
                        icebergConfig.getHiveStatisticsMergeStrategy(),
                        false,
                        val -> HiveStatisticsMergeStrategy.valueOf((String) val),
                        HiveStatisticsMergeStrategy::name),
                booleanProperty(
                        PUSHDOWN_FILTER_ENABLED,
                        "Experimental: Enable Filter Pushdown for Iceberg. This is only supported with Native Worker.",
                        icebergConfig.isPushdownFilterEnabled(),
                        false),
                doubleProperty(
                        STATISTIC_SNAPSHOT_RECORD_DIFFERENCE_WEIGHT,
                        "the amount that the difference in total record count matters" +
                                "when calculating the closest snapshot when picking statistics. A " +
                                "value of 1 means a single record is equivalent to 1 millisecond of " +
                                "time difference.",
                        icebergConfig.getStatisticSnapshotRecordDifferenceWeight(),
                        false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static HiveCompressionCodec getCompressionCodec(ConnectorSession session)
    {
        return session.getProperty(COMPRESSION_CODEC, HiveCompressionCodec.class);
    }

    public static DataSize getParquetMaxReadBlockSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_MAX_READ_BLOCK_SIZE, DataSize.class);
    }

    public static DataSize getParquetWriterPageSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_PAGE_SIZE, DataSize.class);
    }

    public static DataSize getParquetWriterBlockSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_PAGE_SIZE, DataSize.class);
    }

    public static ParquetProperties.WriterVersion getParquetWriterVersion(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_VERSION, ParquetProperties.WriterVersion.class);
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

    public static OrcWriteValidation.OrcWriteValidationMode getOrcOptimizedWriterValidateMode(ConnectorSession session)
    {
        return OrcWriteValidation.OrcWriteValidationMode.valueOf(session.getProperty(ORC_OPTIMIZED_WRITER_VALIDATE_MODE, String.class).toUpperCase(ENGLISH));
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

    public static HiveCompressionCodec getOrcCompressionCodec(ConnectorSession session)
    {
        return session.getProperty(ORC_COMPRESSION_CODEC, HiveCompressionCodec.class);
    }

    public static boolean isCacheEnabled(ConnectorSession session)
    {
        return session.getProperty(CACHE_ENABLED, Boolean.class);
    }

    public static NodeSelectionStrategy getNodeSelectionStrategy(ConnectorSession session)
    {
        return session.getProperty(NODE_SELECTION_STRATEGY, NodeSelectionStrategy.class);
    }

    public static String getNessieReferenceName(ConnectorSession session)
    {
        return session.getProperty(NESSIE_REFERENCE_NAME, String.class);
    }

    public static String getNessieReferenceHash(ConnectorSession session)
    {
        return session.getProperty(NESSIE_REFERENCE_HASH, String.class);
    }

    public static double getMinimumAssignedSplitWeight(ConnectorSession session)
    {
        return session.getProperty(MINIMUM_ASSIGNED_SPLIT_WEIGHT, Double.class);
    }

    public static boolean getReadNullMaskedParquetEncryptedValue(ConnectorSession session)
    {
        return session.getProperty(READ_MASKED_VALUE_ENABLED, Boolean.class);
    }

    public static boolean isParquetDereferencePushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(PARQUET_DEREFERENCE_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static boolean isMergeOnReadModeEnabled(ConnectorSession session)
    {
        return session.getProperty(MERGE_ON_READ_MODE_ENABLED, Boolean.class);
    }

    public static HiveStatisticsMergeStrategy getHiveStatisticsMergeStrategy(ConnectorSession session)
    {
        return session.getProperty(HIVE_METASTORE_STATISTICS_MERGE_STRATEGY, HiveStatisticsMergeStrategy.class);
    }

    public static boolean isPushdownFilterEnabled(ConnectorSession session)
    {
        return session.getProperty(PUSHDOWN_FILTER_ENABLED, Boolean.class);
    }

    public static double getStatisticSnapshotRecordDifferenceWeight(ConnectorSession session)
    {
        return session.getProperty(STATISTIC_SNAPSHOT_RECORD_DIFFERENCE_WEIGHT, Double.class);
    }
}
