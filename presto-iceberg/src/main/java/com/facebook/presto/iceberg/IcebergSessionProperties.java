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

import com.facebook.airlift.units.DataSize;
import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.hive.HiveCompressionCodec;
import com.facebook.presto.hive.OrcFileWriterConfig;
import com.facebook.presto.hive.ParquetFileWriterConfig;
import com.facebook.presto.iceberg.nessie.IcebergNessieConfig;
import com.facebook.presto.iceberg.util.StatisticsUtil;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import jakarta.inject.Inject;
import org.apache.parquet.column.ParquetProperties;

import java.util.EnumSet;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.iceberg.util.StatisticsUtil.SUPPORTED_MERGE_FLAGS;
import static com.facebook.presto.iceberg.util.StatisticsUtil.decodeMergeFlags;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.doubleProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.longProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static java.lang.String.format;

public final class IcebergSessionProperties
{
    private static final String PARQUET_WRITER_BLOCK_SIZE = "parquet_writer_block_size";
    private static final String PARQUET_WRITER_PAGE_SIZE = "parquet_writer_page_size";
    private static final String PARQUET_WRITER_VERSION = "parquet_writer_version";
    private static final String ORC_STRING_STATISTICS_LIMIT = "orc_string_statistics_limit";
    private static final String ORC_OPTIMIZED_WRITER_MIN_STRIPE_SIZE = "orc_optimized_writer_min_stripe_size";
    private static final String ORC_OPTIMIZED_WRITER_MAX_STRIPE_SIZE = "orc_optimized_writer_max_stripe_size";
    private static final String ORC_OPTIMIZED_WRITER_MAX_STRIPE_ROWS = "orc_optimized_writer_max_stripe_rows";
    private static final String ORC_OPTIMIZED_WRITER_MAX_DICTIONARY_MEMORY = "orc_optimized_writer_max_dictionary_memory";
    private static final String CACHE_ENABLED = "cache_enabled";
    private static final String MINIMUM_ASSIGNED_SPLIT_WEIGHT = "minimum_assigned_split_weight";
    private static final String NESSIE_REFERENCE_NAME = "nessie_reference_name";
    private static final String NESSIE_REFERENCE_HASH = "nessie_reference_hash";
    static final String COMPRESSION_CODEC = "compression_codec";
    public static final String PARQUET_DEREFERENCE_PUSHDOWN_ENABLED = "parquet_dereference_pushdown_enabled";
    public static final String MERGE_ON_READ_MODE_ENABLED = "merge_on_read_enabled";
    public static final String PUSHDOWN_FILTER_ENABLED = "pushdown_filter_enabled";
    public static final String DELETE_AS_JOIN_REWRITE_ENABLED = "delete_as_join_rewrite_enabled";
    public static final String DELETE_AS_JOIN_REWRITE_MAX_DELETE_COLUMNS = "delete_as_join_rewrite_max_delete_columns";
    public static final String HIVE_METASTORE_STATISTICS_MERGE_STRATEGY = "hive_statistics_merge_strategy";
    public static final String STATISTIC_SNAPSHOT_RECORD_DIFFERENCE_WEIGHT = "statistic_snapshot_record_difference_weight";
    public static final String ROWS_FOR_METADATA_OPTIMIZATION_THRESHOLD = "rows_for_metadata_optimization_threshold";
    public static final String STATISTICS_KLL_SKETCH_K_PARAMETER = "statistics_kll_sketch_k_parameter";
    public static final String TARGET_SPLIT_SIZE_BYTES = "target_split_size_bytes";
    public static final String MATERIALIZED_VIEW_STORAGE_PREFIX = "materialized_view_storage_prefix";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public IcebergSessionProperties(
            IcebergConfig icebergConfig,
            ParquetFileWriterConfig parquetFileWriterConfig,
            OrcFileWriterConfig orcFileWriterConfig,
            CacheConfig cacheConfig,
            Optional<IcebergNessieConfig> nessieConfig)
    {
        ImmutableList.Builder<PropertyMetadata<?>> propertiesBuilder = ImmutableList.<PropertyMetadata<?>>builder()
                .add(new PropertyMetadata<>(
                        COMPRESSION_CODEC,
                        "The compression codec to use when writing files",
                        VARCHAR,
                        HiveCompressionCodec.class,
                        icebergConfig.getCompressionCodec(),
                        false,
                        value -> HiveCompressionCodec.valueOf(((String) value).toUpperCase()),
                        HiveCompressionCodec::name))
                .add(dataSizeSessionProperty(
                        PARQUET_WRITER_BLOCK_SIZE,
                        "Parquet: Writer block size",
                        parquetFileWriterConfig.getBlockSize(),
                        false))
                .add(dataSizeSessionProperty(
                        PARQUET_WRITER_PAGE_SIZE,
                        "Parquet: Writer page size",
                        parquetFileWriterConfig.getPageSize(),
                        false))
                .add(new PropertyMetadata<>(
                        PARQUET_WRITER_VERSION,
                        "Parquet: Writer version",
                        VARCHAR,
                        ParquetProperties.WriterVersion.class,
                        parquetFileWriterConfig.getWriterVersion(),
                        false,
                        value -> ParquetProperties.WriterVersion.valueOf(((String) value).toUpperCase()),
                        ParquetProperties.WriterVersion::name))
                .add(dataSizeSessionProperty(
                        ORC_OPTIMIZED_WRITER_MIN_STRIPE_SIZE,
                        "Experimental: ORC: Min stripe size",
                        orcFileWriterConfig.getStripeMinSize(),
                        false))
                .add(dataSizeSessionProperty(
                        ORC_OPTIMIZED_WRITER_MAX_STRIPE_SIZE,
                        "Experimental: ORC: Max stripe size",
                        orcFileWriterConfig.getStripeMaxSize(),
                        false))
                .add(integerProperty(
                        ORC_OPTIMIZED_WRITER_MAX_STRIPE_ROWS,
                        "Experimental: ORC: Max stripe row count",
                        orcFileWriterConfig.getStripeMaxRowCount(),
                        false))
                .add(dataSizeSessionProperty(
                        ORC_OPTIMIZED_WRITER_MAX_DICTIONARY_MEMORY,
                        "Experimental: ORC: Max dictionary memory",
                        orcFileWriterConfig.getDictionaryMaxMemory(),
                        false))
                .add(booleanProperty(
                        // required by presto-hive module, might be removed in future
                        CACHE_ENABLED,
                        "Enable cache for Iceberg",
                        cacheConfig.isCachingEnabled(),
                        false))
                .add(doubleProperty(
                        MINIMUM_ASSIGNED_SPLIT_WEIGHT,
                        "Minimum assigned split weight",
                        icebergConfig.getMinimumAssignedSplitWeight(),
                        false))
                .add(dataSizeSessionProperty(
                        ORC_STRING_STATISTICS_LIMIT,
                        "ORC: Maximum size of string statistics; drop if exceeding",
                        orcFileWriterConfig.getStringStatisticsLimit(),
                        false))
                .add(booleanProperty(
                        PARQUET_DEREFERENCE_PUSHDOWN_ENABLED,
                        "Is dereference pushdown expression pushdown into Parquet reader enabled?",
                        icebergConfig.isParquetDereferencePushdownEnabled(),
                        false))
                .add(booleanProperty(
                        MERGE_ON_READ_MODE_ENABLED,
                        "Reads enabled for merge-on-read Iceberg tables",
                        icebergConfig.isMergeOnReadModeEnabled(),
                        false))
                .add(new PropertyMetadata<>(
                        HIVE_METASTORE_STATISTICS_MERGE_STRATEGY,
                        "Flags to choose which statistics from the Hive Metastore are used when calculating table stats. Valid values are: "
                                + Joiner.on(", ").join(SUPPORTED_MERGE_FLAGS),
                        VARCHAR,
                        EnumSet.class,
                        icebergConfig.getHiveStatisticsMergeFlags(),
                        false,
                        val -> decodeMergeFlags((String) val),
                        StatisticsUtil::encodeMergeFlags))
                .add(booleanProperty(
                        PUSHDOWN_FILTER_ENABLED,
                        "Experimental: Enable Filter Pushdown for Iceberg. This is only supported with Native Worker.",
                        icebergConfig.isPushdownFilterEnabled(),
                        false))
                .add(doubleProperty(
                        STATISTIC_SNAPSHOT_RECORD_DIFFERENCE_WEIGHT,
                        "the amount that the difference in total record count matters " +
                                "when calculating the closest snapshot when picking statistics. A " +
                                "value of 1 means a single record is equivalent to 1 millisecond of " +
                                "time difference.",
                        icebergConfig.getStatisticSnapshotRecordDifferenceWeight(),
                        false))
                .add(booleanProperty(
                        DELETE_AS_JOIN_REWRITE_ENABLED,
                        "When enabled equality delete row filtering will be pushed down into a join.",
                        icebergConfig.isDeleteAsJoinRewriteEnabled(),
                        false))
                .add(new PropertyMetadata<>(
                        DELETE_AS_JOIN_REWRITE_MAX_DELETE_COLUMNS,
                        "The maximum number of columns that can be used in a delete as join rewrite. " +
                                "If the number of columns exceeds this value, the delete as join rewrite will not be applied.",
                        INTEGER,
                        Integer.class,
                        icebergConfig.getDeleteAsJoinRewriteMaxDeleteColumns(),
                        false,
                        value -> {
                            int intValue = ((Number) value).intValue();
                            if (intValue < 0 || intValue > 400) {
                                throw new PrestoException(INVALID_SESSION_PROPERTY,
                                        format("Invalid value for %s: %s. It must be between 0 and 400.", DELETE_AS_JOIN_REWRITE_MAX_DELETE_COLUMNS, intValue));
                            }
                            return intValue;
                        },
                        integer -> integer))
                .add(integerProperty(
                        ROWS_FOR_METADATA_OPTIMIZATION_THRESHOLD,
                        "The max partitions number to utilize metadata optimization. When partitions number " +
                                "of an Iceberg table exceeds this threshold, metadata optimization would be skipped for " +
                                "the table. A value of 0 means skip metadata optimization directly.",
                        icebergConfig.getRowsForMetadataOptimizationThreshold(),
                        false))
                .add(integerProperty(STATISTICS_KLL_SKETCH_K_PARAMETER,
                        "The K parameter for the Apache DataSketches KLL sketch when computing histogram statistics",
                        icebergConfig.getStatisticsKllSketchKParameter(),
                        false))
                .add(longProperty(
                        TARGET_SPLIT_SIZE_BYTES,
                        "The target split size. Set to 0 to use the iceberg table's read.split.target-size property",
                        0L,
                        false))
                .add(stringProperty(
                        MATERIALIZED_VIEW_STORAGE_PREFIX,
                        "Default prefix for generated materialized view storage table names. " +
                                "This is only used when the storage_table table property is not explicitly set. " +
                                "When a custom table name is provided, it takes precedence over this prefix.",
                        icebergConfig.getMaterializedViewStoragePrefix(),
                        false));

        nessieConfig.ifPresent((config) -> propertiesBuilder
                .add(stringProperty(
                        NESSIE_REFERENCE_NAME,
                        "Nessie reference name to use",
                        config.getDefaultReferenceName(),
                        false))
                .add(stringProperty(
                        NESSIE_REFERENCE_HASH,
                        "Nessie reference hash to use",
                        null,
                        false)));

        sessionProperties = propertiesBuilder.build();
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static HiveCompressionCodec getCompressionCodec(ConnectorSession session)
    {
        return session.getProperty(COMPRESSION_CODEC, HiveCompressionCodec.class);
    }

    public static DataSize getParquetWriterPageSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_PAGE_SIZE, DataSize.class);
    }

    public static DataSize getParquetWriterBlockSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_BLOCK_SIZE, DataSize.class);
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

    public static DataSize getOrcStringStatisticsLimit(ConnectorSession session)
    {
        return session.getProperty(ORC_STRING_STATISTICS_LIMIT, DataSize.class);
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

    public static double getMinimumAssignedSplitWeight(ConnectorSession session)
    {
        return session.getProperty(MINIMUM_ASSIGNED_SPLIT_WEIGHT, Double.class);
    }

    public static boolean isParquetDereferencePushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(PARQUET_DEREFERENCE_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static boolean isMergeOnReadModeEnabled(ConnectorSession session)
    {
        return session.getProperty(MERGE_ON_READ_MODE_ENABLED, Boolean.class);
    }

    public static EnumSet<ColumnStatisticType> getHiveStatisticsMergeStrategy(ConnectorSession session)
    {
        return session.getProperty(HIVE_METASTORE_STATISTICS_MERGE_STRATEGY, EnumSet.class);
    }

    public static boolean isPushdownFilterEnabled(ConnectorSession session)
    {
        return session.getProperty(PUSHDOWN_FILTER_ENABLED, Boolean.class);
    }

    public static double getStatisticSnapshotRecordDifferenceWeight(ConnectorSession session)
    {
        return session.getProperty(STATISTIC_SNAPSHOT_RECORD_DIFFERENCE_WEIGHT, Double.class);
    }

    public static boolean isDeleteToJoinPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(DELETE_AS_JOIN_REWRITE_ENABLED, Boolean.class);
    }

    public static int getDeleteAsJoinRewriteMaxDeleteColumns(ConnectorSession session)
    {
        return session.getProperty(DELETE_AS_JOIN_REWRITE_MAX_DELETE_COLUMNS, Integer.class);
    }

    public static int getRowsForMetadataOptimizationThreshold(ConnectorSession session)
    {
        return session.getProperty(ROWS_FOR_METADATA_OPTIMIZATION_THRESHOLD, Integer.class);
    }

    public static String getNessieReferenceName(ConnectorSession session)
    {
        return session.getProperty(NESSIE_REFERENCE_NAME, String.class);
    }

    public static String getNessieReferenceHash(ConnectorSession session)
    {
        return session.getProperty(NESSIE_REFERENCE_HASH, String.class);
    }

    public static int getStatisticsKllSketchKParameter(ConnectorSession session)
    {
        return session.getProperty(STATISTICS_KLL_SKETCH_K_PARAMETER, Integer.class);
    }

    public static Long getTargetSplitSize(ConnectorSession session)
    {
        return session.getProperty(TARGET_SPLIT_SIZE_BYTES, Long.class);
    }

    public static String getMaterializedViewStoragePrefix(ConnectorSession session)
    {
        return session.getProperty(MATERIALIZED_VIEW_STORAGE_PREFIX, String.class);
    }
}
