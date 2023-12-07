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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.hive.HiveSessionProperties.CACHE_ENABLED;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.dataSizeProperty;
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
                        false));
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
}
