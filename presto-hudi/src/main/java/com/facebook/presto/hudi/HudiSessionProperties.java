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

import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.hive.HiveSessionProperties.CACHE_ENABLED;
import static com.facebook.presto.hive.HiveSessionProperties.dataSizeSessionProperty;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.dataSizeProperty;
import static java.lang.String.format;

public class HudiSessionProperties
{
    private final List<PropertyMetadata<?>> sessionProperties;

    private static final String HUDI_METADATA_TABLE_ENABLED = "hudi_metadata_table_enabled";

    private static final String PARQUET_MAX_READ_BLOCK_SIZE = "parquet_max_read_block_size";
    private static final String PARQUET_BATCH_READ_OPTIMIZATION_ENABLED = "parquet_batch_read_optimization_enabled";
    private static final String PARQUET_BATCH_READER_VERIFICATION_ENABLED = "parquet_batch_reader_verification_enabled";

    private static final String SIZE_BASED_SPLIT_WEIGHTS_ENABLED = "size_based_split_weights_enabled";
    private static final String STANDARD_SPLIT_WEIGHT_SIZE = "standard_split_weight_size";
    private static final String MINIMUM_ASSIGNED_SPLIT_WEIGHT = "minimum_assigned_split_weight";

    @Inject
    public HudiSessionProperties(HiveClientConfig hiveClientConfig, HudiConfig hudiConfig)
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
                dataSizeSessionProperty(
                        PARQUET_MAX_READ_BLOCK_SIZE,
                        "Parquet: Maximum size of a block to read",
                        hiveClientConfig.getParquetMaxReadBlockSize(),
                        false),
                booleanProperty(
                        PARQUET_BATCH_READ_OPTIMIZATION_ENABLED,
                        "Is Parquet batch read optimization enabled",
                        false,
                        false),
                booleanProperty(
                        PARQUET_BATCH_READER_VERIFICATION_ENABLED,
                        "Is Parquet batch reader verification enabled? This is for testing purposes only, not to be used in production",
                        false,
                        false),
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
                        value -> value));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isHudiMetadataTableEnabled(ConnectorSession session)
    {
        return session.getProperty(HUDI_METADATA_TABLE_ENABLED, Boolean.class);
    }

    public static DataSize getParquetMaxReadBlockSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_MAX_READ_BLOCK_SIZE, DataSize.class);
    }

    public static boolean isParquetBatchReadsEnabled(ConnectorSession session)
    {
        return session.getProperty(PARQUET_BATCH_READ_OPTIMIZATION_ENABLED, Boolean.class);
    }

    public static boolean isParquetBatchReaderVerificationEnabled(ConnectorSession session)
    {
        return session.getProperty(PARQUET_BATCH_READER_VERIFICATION_ENABLED, Boolean.class);
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
}
