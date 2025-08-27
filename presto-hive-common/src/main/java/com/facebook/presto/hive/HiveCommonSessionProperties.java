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

import com.facebook.airlift.units.DataSize;
import com.facebook.presto.orc.OrcWriteValidation.OrcWriteValidationMode;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import jakarta.inject.Inject;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class HiveCommonSessionProperties
{
    @VisibleForTesting
    public static final String RANGE_FILTERS_ON_SUBSCRIPTS_ENABLED = "range_filters_on_subscripts_enabled";
    @VisibleForTesting
    public static final String PARQUET_BATCH_READ_OPTIMIZATION_ENABLED = "parquet_batch_read_optimization_enabled";
    @VisibleForTesting
    public static final String ORC_USE_COLUMN_NAMES = "orc_use_column_names";

    public static final String NODE_SELECTION_STRATEGY = "node_selection_strategy";
    private static final String ORC_BLOOM_FILTERS_ENABLED = "orc_bloom_filters_enabled";
    private static final String ORC_LAZY_READ_SMALL_RANGES = "orc_lazy_read_small_ranges";
    private static final String ORC_MAX_BUFFER_SIZE = "orc_max_buffer_size";
    private static final String ORC_MAX_MERGE_DISTANCE = "orc_max_merge_distance";
    private static final String ORC_MAX_READ_BLOCK_SIZE = "orc_max_read_block_size";
    private static final String ORC_OPTIMIZED_WRITER_ENABLED = "orc_optimized_writer_enabled";
    private static final String ORC_OPTIMIZED_WRITER_VALIDATE = "orc_optimized_writer_validate";
    private static final String ORC_OPTIMIZED_WRITER_VALIDATE_MODE = "orc_optimized_writer_validate_mode";
    private static final String ORC_OPTIMIZED_WRITER_VALIDATE_PERCENTAGE = "orc_optimized_writer_validate_percentage";
    private static final String ORC_STREAM_BUFFER_SIZE = "orc_stream_buffer_size";
    private static final String ORC_TINY_STRIPE_THRESHOLD = "orc_tiny_stripe_threshold";
    private static final String ORC_ZSTD_JNI_DECOMPRESSION_ENABLED = "orc_zstd_jni_decompression_enabled";
    private static final String PARQUET_BATCH_READER_VERIFICATION_ENABLED = "parquet_batch_reader_verification_enabled";
    private static final String PARQUET_MAX_READ_BLOCK_SIZE = "parquet_max_read_block_size";
    private static final String PARQUET_USE_COLUMN_NAMES = "parquet_use_column_names";
    public static final String READ_MASKED_VALUE_ENABLED = "read_null_masked_parquet_encrypted_value_enabled";
    public static final String AFFINITY_SCHEDULING_FILE_SECTION_SIZE = "affinity_scheduling_file_section_size";
    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public HiveCommonSessionProperties(HiveCommonClientConfig hiveCommonClientConfig)
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        RANGE_FILTERS_ON_SUBSCRIPTS_ENABLED,
                        "Experimental: enable pushdown of range filters on subscripts (a[2] = 5) into ORC column readers",
                        hiveCommonClientConfig.isRangeFiltersOnSubscriptsEnabled(),
                        false),
                new PropertyMetadata<>(
                        NODE_SELECTION_STRATEGY,
                        "Node affinity selection strategy",
                        VARCHAR,
                        NodeSelectionStrategy.class,
                        hiveCommonClientConfig.getNodeSelectionStrategy(),
                        false,
                        value -> NodeSelectionStrategy.valueOf((String) value),
                        NodeSelectionStrategy::toString),
                booleanProperty(
                        ORC_BLOOM_FILTERS_ENABLED,
                        "ORC: Enable bloom filters for predicate pushdown",
                        hiveCommonClientConfig.isOrcBloomFiltersEnabled(),
                        false),
                booleanProperty(
                        ORC_LAZY_READ_SMALL_RANGES,
                        "Experimental: ORC: Read small file segments lazily",
                        hiveCommonClientConfig.isOrcLazyReadSmallRanges(),
                        false),
                dataSizeSessionProperty(
                        ORC_MAX_BUFFER_SIZE,
                        "ORC: Maximum size of a single read",
                        hiveCommonClientConfig.getOrcMaxBufferSize(),
                        false),
                dataSizeSessionProperty(
                        ORC_MAX_MERGE_DISTANCE,
                        "ORC: Maximum size of gap between two reads to merge into a single read",
                        hiveCommonClientConfig.getOrcMaxMergeDistance(),
                        false),
                dataSizeSessionProperty(
                        ORC_MAX_READ_BLOCK_SIZE,
                        "ORC: Soft max size of Presto blocks produced by ORC reader",
                        hiveCommonClientConfig.getOrcMaxReadBlockSize(),
                        false),
                booleanProperty(
                        ORC_OPTIMIZED_WRITER_ENABLED,
                        "Experimental: ORC: Enable optimized writer",
                        hiveCommonClientConfig.isOrcOptimizedWriterEnabled(),
                        false),
                booleanProperty(
                        ORC_OPTIMIZED_WRITER_VALIDATE,
                        "Experimental: ORC: Force all validation for files",
                        hiveCommonClientConfig.getOrcWriterValidationPercentage() > 0.0,
                        false),
                stringProperty(
                        ORC_OPTIMIZED_WRITER_VALIDATE_MODE,
                        "Experimental: ORC: Level of detail in ORC validation",
                        hiveCommonClientConfig.getOrcWriterValidationMode().toString(),
                        false),
                new PropertyMetadata<>(
                        ORC_OPTIMIZED_WRITER_VALIDATE_PERCENTAGE,
                        "Experimental: ORC: sample percentage for validation for files",
                        DOUBLE,
                        Double.class,
                        hiveCommonClientConfig.getOrcWriterValidationPercentage(),
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
                dataSizeSessionProperty(
                        ORC_STREAM_BUFFER_SIZE,
                        "ORC: Size of buffer for streaming reads",
                        hiveCommonClientConfig.getOrcStreamBufferSize(),
                        false),
                dataSizeSessionProperty(
                        ORC_TINY_STRIPE_THRESHOLD,
                        "ORC: Threshold below which an ORC stripe or file will read in its entirety",
                        hiveCommonClientConfig.getOrcTinyStripeThreshold(),
                        false),
                booleanProperty(
                        ORC_ZSTD_JNI_DECOMPRESSION_ENABLED,
                        "use JNI based zstd decompression for reading ORC files",
                        hiveCommonClientConfig.isZstdJniDecompressionEnabled(),
                        true),
                booleanProperty(
                        ORC_USE_COLUMN_NAMES,
                        "Access ORC columns using names from the file first, and fallback to Hive schema column names if not found to ensure backward compatibility with old data",
                        hiveCommonClientConfig.isUseOrcColumnNames(),
                        false),
                booleanProperty(
                        PARQUET_BATCH_READ_OPTIMIZATION_ENABLED,
                        "Is Parquet batch read optimization enabled",
                        hiveCommonClientConfig.isParquetBatchReadOptimizationEnabled(),
                        false),
                booleanProperty(
                        PARQUET_BATCH_READER_VERIFICATION_ENABLED,
                        "Is Parquet batch reader verification enabled? This is for testing purposes only, not to be used in production",
                        hiveCommonClientConfig.isParquetBatchReaderVerificationEnabled(),
                        false),
                dataSizeSessionProperty(
                        PARQUET_MAX_READ_BLOCK_SIZE,
                        "Parquet: Maximum size of a block to read",
                        hiveCommonClientConfig.getParquetMaxReadBlockSize(),
                        false),
                booleanProperty(
                        PARQUET_USE_COLUMN_NAMES,
                        "Experimental: Parquet: Access Parquet columns using names from the file",
                        hiveCommonClientConfig.isUseParquetColumnNames(),
                        false),
                booleanProperty(
                        READ_MASKED_VALUE_ENABLED,
                        "Return null when access is denied for an encrypted parquet column",
                        hiveCommonClientConfig.getReadNullMaskedParquetEncryptedValue(),
                        false),
                dataSizeSessionProperty(
                        AFFINITY_SCHEDULING_FILE_SECTION_SIZE,
                        "Size of file section for affinity scheduling",
                        hiveCommonClientConfig.getAffinitySchedulingFileSectionSize(),
                        false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static NodeSelectionStrategy getNodeSelectionStrategy(ConnectorSession session)
    {
        return session.getProperty(NODE_SELECTION_STRATEGY, NodeSelectionStrategy.class);
    }

    public static boolean isOrcBloomFiltersEnabled(ConnectorSession session)
    {
        return session.getProperty(ORC_BLOOM_FILTERS_ENABLED, Boolean.class);
    }

    public static boolean getOrcLazyReadSmallRanges(ConnectorSession session)
    {
        return session.getProperty(ORC_LAZY_READ_SMALL_RANGES, Boolean.class);
    }

    public static DataSize getOrcMaxBufferSize(ConnectorSession session)
    {
        return session.getProperty(ORC_MAX_BUFFER_SIZE, DataSize.class);
    }

    public static DataSize getOrcMaxMergeDistance(ConnectorSession session)
    {
        return session.getProperty(ORC_MAX_MERGE_DISTANCE, DataSize.class);
    }

    public static DataSize getOrcMaxReadBlockSize(ConnectorSession session)
    {
        return session.getProperty(ORC_MAX_READ_BLOCK_SIZE, DataSize.class);
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

    public static DataSize getOrcStreamBufferSize(ConnectorSession session)
    {
        return session.getProperty(ORC_STREAM_BUFFER_SIZE, DataSize.class);
    }

    public static DataSize getOrcTinyStripeThreshold(ConnectorSession session)
    {
        return session.getProperty(ORC_TINY_STRIPE_THRESHOLD, DataSize.class);
    }

    public static boolean isOrcZstdJniDecompressionEnabled(ConnectorSession session)
    {
        return session.getProperty(ORC_ZSTD_JNI_DECOMPRESSION_ENABLED, Boolean.class);
    }

    public static boolean isUseOrcColumnNames(ConnectorSession session)
    {
        return session.getProperty(ORC_USE_COLUMN_NAMES, Boolean.class);
    }

    public static boolean isParquetBatchReadsEnabled(ConnectorSession session)
    {
        return session.getProperty(PARQUET_BATCH_READ_OPTIMIZATION_ENABLED, Boolean.class);
    }

    public static boolean isParquetBatchReaderVerificationEnabled(ConnectorSession session)
    {
        return session.getProperty(PARQUET_BATCH_READER_VERIFICATION_ENABLED, Boolean.class);
    }

    public static DataSize getParquetMaxReadBlockSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_MAX_READ_BLOCK_SIZE, DataSize.class);
    }

    public static boolean isUseParquetColumnNames(ConnectorSession session)
    {
        return session.getProperty(PARQUET_USE_COLUMN_NAMES, Boolean.class);
    }

    public static boolean isRangeFiltersOnSubscriptsEnabled(ConnectorSession session)
    {
        return session.getProperty(RANGE_FILTERS_ON_SUBSCRIPTS_ENABLED, Boolean.class);
    }

    public static boolean getReadNullMaskedParquetEncryptedValue(ConnectorSession session)
    {
        return session.getProperty(READ_MASKED_VALUE_ENABLED, Boolean.class);
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

    public static DataSize getAffinitySchedulingFileSectionSize(ConnectorSession session)
    {
        return session.getProperty(AFFINITY_SCHEDULING_FILE_SECTION_SIZE, DataSize.class);
    }
}
