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

package com.facebook.presto.hudi;

import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import jakarta.inject.Inject;

import java.util.List;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.hive.HiveSessionProperties.CACHE_ENABLED;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.dataSizeProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.durationProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerProperty;
import static java.lang.String.format;

public class HudiSessionProperties
{
    private final List<PropertyMetadata<?>> sessionProperties;

    private static final String HUDI_METADATA_TABLE_ENABLED = "hudi_metadata_table_enabled";
    private static final String SIZE_BASED_SPLIT_WEIGHTS_ENABLED = "size_based_split_weights_enabled";
    private static final String STANDARD_SPLIT_WEIGHT_SIZE = "standard_split_weight_size";
    private static final String MINIMUM_ASSIGNED_SPLIT_WEIGHT = "minimum_assigned_split_weight";
    private static final String MAX_OUTSTANDING_SPLITS = "max_outstanding_splits";
    private static final String SPLIT_GENERATOR_PARALLELISM = "split_generator_parallelism";

    static final String COLUMN_STATS_INDEX_ENABLED = "column_stats_index_enabled";
    static final String RECORD_LEVEL_INDEX_ENABLED = "record_level_index_enabled";
    static final String SECONDARY_INDEX_ENABLED = "secondary_index_enabled";
    static final String PARTITION_STATS_INDEX_ENABLED = "partition_stats_index_enabled";
    static final String COLUMN_STATS_WAIT_TIMEOUT = "column_stats_wait_timeout";
    static final String RECORD_INDEX_WAIT_TIMEOUT = "record_index_wait_timeout";
    static final String SECONDARY_INDEX_WAIT_TIMEOUT = "secondary_index_wait_timeout";
    static final String METADATA_PARTITION_LISTING_ENABLED = "metadata_partition_listing_enabled";
    static final String RESOLVE_COLUMN_NAME_CASING_ENABLED = "resolve_column_name_casing_enabled";

    @Inject
    public HudiSessionProperties(HudiConfig hudiConfig)
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        HUDI_METADATA_TABLE_ENABLED,
                        "Enable Hudi MetaTable Table",
                        hudiConfig.isMetadataTableEnabled(),
                        false),
                booleanProperty(
                        // required by presto-hive module, might be removed in future
                        CACHE_ENABLED,
                        "Enable cache for hive",
                        false,
                        true),
                booleanProperty(
                        SIZE_BASED_SPLIT_WEIGHTS_ENABLED,
                        format("If enabled, size-based splitting ensures that each batch of splits has enough data to process as defined by %s", STANDARD_SPLIT_WEIGHT_SIZE),
                        hudiConfig.isSizeBasedSplitWeightsEnabled(),
                        false),
                dataSizeProperty(
                        STANDARD_SPLIT_WEIGHT_SIZE,
                        "The split size corresponding to the standard weight (1.0) when size-based split weights are enabled",
                        hudiConfig.getStandardSplitWeightSize(),
                        false),
                new PropertyMetadata<>(
                        MINIMUM_ASSIGNED_SPLIT_WEIGHT,
                        "Minimum assigned split weight when size-based split weights are enabled",
                        DOUBLE,
                        Double.class,
                        hudiConfig.getMinimumAssignedSplitWeight(),
                        false,
                        value -> {
                            double doubleValue = ((Number) value).doubleValue();
                            if (!Double.isFinite(doubleValue) || doubleValue <= 0 || doubleValue > 1) {
                                throw new PrestoException(INVALID_SESSION_PROPERTY, format("%s must be > 0 and <= 1.0: %s", MINIMUM_ASSIGNED_SPLIT_WEIGHT, value));
                            }
                            return doubleValue;
                        },
                        value -> value),
                integerProperty(
                        MAX_OUTSTANDING_SPLITS,
                        "Maximum outstanding splits per batch for query",
                        hudiConfig.getMaxOutstandingSplits(),
                        false),
                integerProperty(
                        SPLIT_GENERATOR_PARALLELISM,
                        "Number of threads used to generate splits from partitions",
                        hudiConfig.getSplitGeneratorParallelism(),
                        false),
                booleanProperty(
                        RECORD_LEVEL_INDEX_ENABLED,
                        "Enable record level index for file skipping",
                        hudiConfig.isRecordLevelIndexEnabled(),
                        true),
                booleanProperty(
                        SECONDARY_INDEX_ENABLED,
                        "Enable secondary index for file skipping",
                        hudiConfig.isSecondaryIndexEnabled(),
                        true),
                booleanProperty(
                        COLUMN_STATS_INDEX_ENABLED,
                        "Enable column stats index for file skipping",
                        hudiConfig.isColumnStatsIndexEnabled(),
                        true),
                booleanProperty(
                        PARTITION_STATS_INDEX_ENABLED,
                        "Enable partition stats index for file skipping",
                        hudiConfig.isPartitionStatsIndexEnabled(),
                        true),
                durationProperty(
                        COLUMN_STATS_WAIT_TIMEOUT,
                        "Maximum timeout to wait for loading column stats",
                        hudiConfig.getColumnStatsWaitTimeout(),
                        false),
                durationProperty(
                        RECORD_INDEX_WAIT_TIMEOUT,
                        "Maximum timeout to wait for loading record index",
                        hudiConfig.getRecordIndexWaitTimeout(),
                        false),
                durationProperty(
                        SECONDARY_INDEX_WAIT_TIMEOUT,
                        "Maximum timeout to wait for loading secondary index",
                        hudiConfig.getSecondaryIndexWaitTimeout(),
                        false),
                booleanProperty(
                        METADATA_PARTITION_LISTING_ENABLED,
                        "Enable metadata table based partition listing",
                        hudiConfig.isMetadataPartitionListingEnabled(),
                        false),
                booleanProperty(
                        RESOLVE_COLUMN_NAME_CASING_ENABLED,
                        "Enable resolve column name casing",
                        hudiConfig.isResolveColumnNameCasingEnabled(),
                        true));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isHudiMetadataTableEnabled(ConnectorSession session)
    {
        return session.getProperty(HUDI_METADATA_TABLE_ENABLED, Boolean.class);
    }

    public static boolean isSizeBasedSplitWeightsEnabled(ConnectorSession session)
    {
        return session.getProperty(SIZE_BASED_SPLIT_WEIGHTS_ENABLED, Boolean.class);
    }

    public static DataSize getStandardSplitWeightSize(ConnectorSession session)
    {
        return session.getProperty(STANDARD_SPLIT_WEIGHT_SIZE, DataSize.class);
    }

    public static double getMinimumAssignedSplitWeight(ConnectorSession session)
    {
        return session.getProperty(MINIMUM_ASSIGNED_SPLIT_WEIGHT, Double.class);
    }

    public static int getMaxOutstandingSplits(ConnectorSession session)
    {
        return session.getProperty(MAX_OUTSTANDING_SPLITS, Integer.class);
    }

    public static int getSplitGeneratorParallelism(ConnectorSession session)
    {
        return session.getProperty(SPLIT_GENERATOR_PARALLELISM, Integer.class);
    }

    public static boolean isRecordLevelIndexEnabled(ConnectorSession session)
    {
        return session.getProperty(RECORD_LEVEL_INDEX_ENABLED, Boolean.class);
    }

    public static boolean isSecondaryIndexEnabled(ConnectorSession session)
    {
        return session.getProperty(SECONDARY_INDEX_ENABLED, Boolean.class);
    }

    public static boolean isColumnStatsIndexEnabled(ConnectorSession session)
    {
        return session.getProperty(COLUMN_STATS_INDEX_ENABLED, Boolean.class);
    }

    public static boolean isPartitionStatsIndexEnabled(ConnectorSession session)
    {
        return session.getProperty(PARTITION_STATS_INDEX_ENABLED, Boolean.class);
    }

    public static boolean isNoOpIndexEnabled(ConnectorSession session)
    {
        return !isRecordLevelIndexEnabled(session) && !isSecondaryIndexEnabled(session) && !isColumnStatsIndexEnabled(session);
    }

    public static Duration getColumnStatsWaitTimeout(ConnectorSession session)
    {
        return session.getProperty(COLUMN_STATS_WAIT_TIMEOUT, Duration.class);
    }

    public static Duration getRecordIndexWaitTimeout(ConnectorSession session)
    {
        return session.getProperty(RECORD_INDEX_WAIT_TIMEOUT, Duration.class);
    }

    public static Duration getSecondaryIndexWaitTimeout(ConnectorSession session)
    {
        return session.getProperty(SECONDARY_INDEX_WAIT_TIMEOUT, Duration.class);
    }

    public static boolean isMetadataPartitionListingEnabled(ConnectorSession session)
    {
        return session.getProperty(METADATA_PARTITION_LISTING_ENABLED, Boolean.class);
    }

    public static boolean isResolveColumnNameCasingEnabled(ConnectorSession session)
    {
        return session.getProperty(RESOLVE_COLUMN_NAME_CASING_ENABLED, Boolean.class);
    }
}
