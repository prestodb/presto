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
package com.facebook.presto.sessionpropertyproviders;

import com.facebook.presto.Session;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.session.WorkerSessionPropertyProvider;
import com.facebook.presto.spiller.NodeSpillConfig;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.JavaFeaturesConfig;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.units.DataSize;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.isSpillEnabled;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class JavaWorkerSessionPropertyProvider
        implements WorkerSessionPropertyProvider
{
    public static final String AGGREGATION_SPILL_ENABLED = "aggregation_spill_enabled";
    public static final String TOPN_SPILL_ENABLED = "topn_spill_enabled";
    public static final String DISTINCT_AGGREGATION_SPILL_ENABLED = "distinct_aggregation_spill_enabled";
    public static final String DEDUP_BASED_DISTINCT_AGGREGATION_SPILL_ENABLED = "dedup_based_distinct_aggregation_spill_enabled";
    public static final String DISTINCT_AGGREGATION_LARGE_BLOCK_SPILL_ENABLED = "distinct_aggregation_large_block_spill_enabled";
    public static final String DISTINCT_AGGREGATION_LARGE_BLOCK_SIZE_THRESHOLD = "distinct_aggregation_large_block_size_threshold";
    public static final String ORDER_BY_AGGREGATION_SPILL_ENABLED = "order_by_aggregation_spill_enabled";
    public static final String WINDOW_SPILL_ENABLED = "window_spill_enabled";
    public static final String ORDER_BY_SPILL_ENABLED = "order_by_spill_enabled";
    public static final String AGGREGATION_OPERATOR_UNSPILL_MEMORY_LIMIT = "aggregation_operator_unspill_memory_limit";
    public static final String TOPN_OPERATOR_UNSPILL_MEMORY_LIMIT = "topn_operator_unspill_memory_limit";
    public static final String TEMP_STORAGE_SPILLER_BUFFER_SIZE = "temp_storage_spiller_buffer_size";
    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public JavaWorkerSessionPropertyProvider(FeaturesConfig featuresConfig, JavaFeaturesConfig javaFeaturesConfig, NodeSpillConfig nodeSpillConfig)
    {
        boolean nativeExecution = requireNonNull(featuresConfig, "featuresConfig is null").isNativeExecutionEnabled();
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        TOPN_SPILL_ENABLED,
                        "Enable topN spilling if spill_enabled",
                        javaFeaturesConfig.isTopNSpillEnabled(),
                        nativeExecution),
                booleanProperty(
                        AGGREGATION_SPILL_ENABLED,
                        "Enable aggregate spilling if spill_enabled",
                        javaFeaturesConfig.isAggregationSpillEnabled(),
                        nativeExecution),
                booleanProperty(
                        DISTINCT_AGGREGATION_SPILL_ENABLED,
                        "Enable spill for distinct aggregations if spill_enabled and aggregation_spill_enabled",
                        javaFeaturesConfig.isDistinctAggregationSpillEnabled(),
                        nativeExecution),
                booleanProperty(
                        DEDUP_BASED_DISTINCT_AGGREGATION_SPILL_ENABLED,
                        "Perform deduplication of input data for distinct aggregates before spilling",
                        javaFeaturesConfig.isDedupBasedDistinctAggregationSpillEnabled(),
                        nativeExecution),
                booleanProperty(
                        DISTINCT_AGGREGATION_LARGE_BLOCK_SPILL_ENABLED,
                        "Spill large block to a separate spill file",
                        javaFeaturesConfig.isDistinctAggregationLargeBlockSpillEnabled(),
                        nativeExecution),
                new PropertyMetadata<>(
                        DISTINCT_AGGREGATION_LARGE_BLOCK_SIZE_THRESHOLD,
                        "Block size threshold beyond which it will be spilled into a separate spill file",
                        VARCHAR,
                        DataSize.class,
                        javaFeaturesConfig.getDistinctAggregationLargeBlockSizeThreshold(),
                        nativeExecution,
                        value -> DataSize.valueOf((String) value),
                        DataSize::toString),
                booleanProperty(
                        ORDER_BY_AGGREGATION_SPILL_ENABLED,
                        "Enable spill for order-by aggregations if spill_enabled and aggregation_spill_enabled",
                        javaFeaturesConfig.isOrderByAggregationSpillEnabled(),
                        nativeExecution),
                booleanProperty(
                        WINDOW_SPILL_ENABLED,
                        "Enable window spilling if spill_enabled",
                        javaFeaturesConfig.isWindowSpillEnabled(),
                        nativeExecution),
                booleanProperty(
                        ORDER_BY_SPILL_ENABLED,
                        "Enable order by spilling if spill_enabled",
                        javaFeaturesConfig.isOrderBySpillEnabled(),
                        nativeExecution),
                new PropertyMetadata<>(
                        AGGREGATION_OPERATOR_UNSPILL_MEMORY_LIMIT,
                        "Experimental: How much memory can should be allocated per aggregation operator in unspilling process",
                        VARCHAR,
                        DataSize.class,
                        javaFeaturesConfig.getAggregationOperatorUnspillMemoryLimit(),
                        nativeExecution,
                        value -> DataSize.valueOf((String) value),
                        DataSize::toString),
                new PropertyMetadata<>(
                        TOPN_OPERATOR_UNSPILL_MEMORY_LIMIT,
                        "How much memory can should be allocated per topN operator in unspilling process",
                        VARCHAR,
                        DataSize.class,
                        javaFeaturesConfig.getTopNOperatorUnspillMemoryLimit(),
                        nativeExecution,
                        value -> DataSize.valueOf((String) value),
                        DataSize::toString),
                new PropertyMetadata<>(
                        TEMP_STORAGE_SPILLER_BUFFER_SIZE,
                        "Experimental: Buffer size used by TempStorageSingleStreamSpiller",
                        VARCHAR,
                        DataSize.class,
                        nodeSpillConfig.getTempStorageBufferSize(),
                        nativeExecution,
                        value -> DataSize.valueOf((String) value),
                        DataSize::toString));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isTopNSpillEnabled(Session session)
    {
        return session.getSystemProperty(TOPN_SPILL_ENABLED, Boolean.class) && isSpillEnabled(session);
    }

    public static boolean isAggregationSpillEnabled(Session session)
    {
        return session.getSystemProperty(AGGREGATION_SPILL_ENABLED, Boolean.class) && isSpillEnabled(session);
    }

    public static boolean isDistinctAggregationSpillEnabled(Session session)
    {
        return session.getSystemProperty(DISTINCT_AGGREGATION_SPILL_ENABLED, Boolean.class) && isAggregationSpillEnabled(session);
    }

    public static boolean isDedupBasedDistinctAggregationSpillEnabled(Session session)
    {
        return session.getSystemProperty(DEDUP_BASED_DISTINCT_AGGREGATION_SPILL_ENABLED, Boolean.class);
    }

    public static boolean isDistinctAggregationLargeBlockSpillEnabled(Session session)
    {
        return session.getSystemProperty(DISTINCT_AGGREGATION_LARGE_BLOCK_SPILL_ENABLED, Boolean.class);
    }

    public static DataSize getDistinctAggregationLargeBlockSizeThreshold(Session session)
    {
        return session.getSystemProperty(DISTINCT_AGGREGATION_LARGE_BLOCK_SIZE_THRESHOLD, DataSize.class);
    }

    public static boolean isOrderByAggregationSpillEnabled(Session session)
    {
        return session.getSystemProperty(ORDER_BY_AGGREGATION_SPILL_ENABLED, Boolean.class) && isAggregationSpillEnabled(session);
    }

    public static boolean isWindowSpillEnabled(Session session)
    {
        return session.getSystemProperty(WINDOW_SPILL_ENABLED, Boolean.class) && isSpillEnabled(session);
    }

    public static boolean isOrderBySpillEnabled(Session session)
    {
        return session.getSystemProperty(ORDER_BY_SPILL_ENABLED, Boolean.class) && isSpillEnabled(session);
    }

    public static DataSize getAggregationOperatorUnspillMemoryLimit(Session session)
    {
        DataSize memoryLimitForMerge = session.getSystemProperty(AGGREGATION_OPERATOR_UNSPILL_MEMORY_LIMIT, DataSize.class);
        checkArgument(memoryLimitForMerge.toBytes() >= 0, "%s must be positive", AGGREGATION_OPERATOR_UNSPILL_MEMORY_LIMIT);
        return memoryLimitForMerge;
    }

    public static DataSize getTopNOperatorUnspillMemoryLimit(Session session)
    {
        DataSize unspillMemoryLimit = session.getSystemProperty(TOPN_OPERATOR_UNSPILL_MEMORY_LIMIT, DataSize.class);
        checkArgument(unspillMemoryLimit.toBytes() >= 0, "%s must be positive", TOPN_OPERATOR_UNSPILL_MEMORY_LIMIT);
        return unspillMemoryLimit;
    }

    public static DataSize getTempStorageSpillerBufferSize(Session session)
    {
        DataSize tempStorageSpillerBufferSize = session.getSystemProperty(TEMP_STORAGE_SPILLER_BUFFER_SIZE, DataSize.class);
        checkArgument(tempStorageSpillerBufferSize.toBytes() >= 0, "%s must be positive", TEMP_STORAGE_SPILLER_BUFFER_SIZE);
        return tempStorageSpillerBufferSize;
    }
}
