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
package com.facebook.presto.spark;

import com.facebook.presto.Session;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spark.SparkErrorCode.SPARK_EXECUTOR_OOM;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.dataSizeProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.doubleProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static com.google.common.base.Strings.nullToEmpty;
import static java.util.Collections.emptyList;

public class PrestoSparkSessionProperties
{
    private static final Splitter.MapSplitter MAP_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings().withKeyValueSeparator('=');

    public static final String SPARK_PARTITION_COUNT_AUTO_TUNE_ENABLED = "spark_partition_count_auto_tune_enabled";
    public static final String MIN_SPARK_INPUT_PARTITION_COUNT_FOR_AUTO_TUNE = "min_spark_input_partition_count_for_auto_tune";
    public static final String MAX_SPARK_INPUT_PARTITION_COUNT_FOR_AUTO_TUNE = "max_spark_input_partition_count_for_auto_tune";
    public static final String SPARK_INITIAL_PARTITION_COUNT = "spark_initial_partition_count";
    public static final String MAX_SPLITS_DATA_SIZE_PER_SPARK_PARTITION = "max_splits_data_size_per_spark_partition";
    public static final String SHUFFLE_OUTPUT_TARGET_AVERAGE_ROW_SIZE = "shuffle_output_target_average_row_size";
    public static final String STORAGE_BASED_BROADCAST_JOIN_ENABLED = "storage_based_broadcast_join_enabled";
    public static final String STORAGE_BASED_BROADCAST_JOIN_WRITE_BUFFER_SIZE = "storage_based_broadcast_join_write_buffer_size";
    public static final String SPARK_BROADCAST_JOIN_MAX_MEMORY_OVERRIDE = "spark_broadcast_join_max_memory_override";
    public static final String SPARK_SPLIT_ASSIGNMENT_BATCH_SIZE = "spark_split_assignment_batch_size";
    public static final String SPARK_MEMORY_REVOKING_THRESHOLD = "spark_memory_revoking_threshold";
    public static final String SPARK_MEMORY_REVOKING_TARGET = "spark_memory_revoking_target";
    public static final String SPARK_QUERY_EXECUTION_STRATEGIES = "spark_query_execution_strategies";
    public static final String SPARK_RETRY_ON_OUT_OF_MEMORY_BROADCAST_JOIN_ENABLED = "spark_retry_on_out_of_memory_broadcast_join_enabled";
    public static final String SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ENABLED = "spark_retry_on_out_of_memory_with_increased_memory_settings_enabled";
    public static final String OUT_OF_MEMORY_RETRY_PRESTO_SESSION_PROPERTIES = "out_of_memory_retry_presto_session_properties";
    public static final String OUT_OF_MEMORY_RETRY_SPARK_CONFIGS = "out_of_memory_retry_spark_configs";
    public static final String SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ERROR_CODES = "spark_retry_on_out_of_memory_with_increased_memory_settings_error_codes";
    public static final String SPARK_AVERAGE_INPUT_DATA_SIZE_PER_EXECUTOR = "spark_average_input_data_size_per_executor";
    public static final String SPARK_MAX_EXECUTOR_COUNT = "spark_max_executor_count";
    public static final String SPARK_MIN_EXECUTOR_COUNT = "spark_min_executor_count";
    public static final String SPARK_AVERAGE_INPUT_DATA_SIZE_PER_PARTITION = "spark_average_input_data_size_per_partition";
    public static final String SPARK_MAX_HASH_PARTITION_COUNT = "spark_max_hash_partition_count";
    public static final String SPARK_MIN_HASH_PARTITION_COUNT = "spark_min_hash_partition_count";
    public static final String SPARK_RESOURCE_ALLOCATION_STRATEGY_ENABLED = "spark_resource_allocation_strategy_enabled";
    public static final String SPARK_EXECUTOR_ALLOCATION_STRATEGY_ENABLED = "spark_executor_allocation_strategy_enabled";
    public static final String SPARK_HASH_PARTITION_COUNT_ALLOCATION_STRATEGY_ENABLED = "spark_hash_partition_count_allocation_strategy_enabled";
    public static final String SPARK_RETRY_ON_OUT_OF_MEMORY_HIGHER_PARTITION_COUNT_ENABLED = "spark_retry_on_out_of_memory_higher_hash_partition_count_enabled";
    public static final String SPARK_HASH_PARTITION_COUNT_SCALING_FACTOR_ON_OUT_OF_MEMORY = "spark_hash_partition_count_scaling_factor_on_out_of_memory";
    public static final String SPARK_ADAPTIVE_QUERY_EXECUTION_ENABLED = "spark_adaptive_query_execution_enabled";
    public static final String ADAPTIVE_JOIN_SIDE_SWITCHING_ENABLED = "adaptive_join_side_switching_enabled";
    public static final String NATIVE_EXECUTION_BROADCAST_BASE_PATH = "native_execution_broadcast_base_path";
    public static final String NATIVE_TRIGGER_COREDUMP_WHEN_UNRESPONSIVE_ENABLED = "native_trigger_coredump_when_unresponsive_enabled";

    private final List<PropertyMetadata<?>> sessionProperties;
    private final ExecutionStrategyValidator executionStrategyValidator;

    public PrestoSparkSessionProperties()
    {
        this(new PrestoSparkConfig());
    }

    @Inject
    public PrestoSparkSessionProperties(PrestoSparkConfig prestoSparkConfig)
    {
        executionStrategyValidator = new ExecutionStrategyValidator();
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        SPARK_PARTITION_COUNT_AUTO_TUNE_ENABLED,
                        "Automatic tuning of spark initial partition count based on splits size per partition",
                        prestoSparkConfig.isSparkPartitionCountAutoTuneEnabled(),
                        false),
                integerProperty(
                        MIN_SPARK_INPUT_PARTITION_COUNT_FOR_AUTO_TUNE,
                        "Minimal Spark input partition count when Spark partition auto tune is enabled",
                        prestoSparkConfig.getMinSparkInputPartitionCountForAutoTune(),
                        false),
                integerProperty(
                        MAX_SPARK_INPUT_PARTITION_COUNT_FOR_AUTO_TUNE,
                        "Max Spark input partition count when Spark partition auto tune is enabled",
                        prestoSparkConfig.getMaxSparkInputPartitionCountForAutoTune(),
                        false),
                integerProperty(
                        SPARK_INITIAL_PARTITION_COUNT,
                        "Initial partition count for Spark RDD when reading table",
                        prestoSparkConfig.getInitialSparkPartitionCount(),
                        false),
                dataSizeProperty(
                        MAX_SPLITS_DATA_SIZE_PER_SPARK_PARTITION,
                        "Maximal size in bytes for splits assigned to one partition",
                        prestoSparkConfig.getMaxSplitsDataSizePerSparkPartition(),
                        false),
                dataSizeProperty(
                        SHUFFLE_OUTPUT_TARGET_AVERAGE_ROW_SIZE,
                        "Target average size for row entries produced by Presto on Spark for shuffle",
                        prestoSparkConfig.getShuffleOutputTargetAverageRowSize(),
                        false),
                booleanProperty(
                        STORAGE_BASED_BROADCAST_JOIN_ENABLED,
                        "Use storage for distributing broadcast table",
                        prestoSparkConfig.isStorageBasedBroadcastJoinEnabled(),
                        false),
                dataSizeProperty(
                        STORAGE_BASED_BROADCAST_JOIN_WRITE_BUFFER_SIZE,
                        "Maximum size in bytes to buffer before flushing pages to disk",
                        prestoSparkConfig.getStorageBasedBroadcastJoinWriteBufferSize(),
                        false),
                dataSizeProperty(
                        SPARK_BROADCAST_JOIN_MAX_MEMORY_OVERRIDE,
                        "Maximum size of broadcast table in Presto on Spark",
                        prestoSparkConfig.getSparkBroadcastJoinMaxMemoryOverride(),
                        false),
                integerProperty(
                        SPARK_SPLIT_ASSIGNMENT_BATCH_SIZE,
                        "Number of splits are processed in a single iteration",
                        prestoSparkConfig.getSplitAssignmentBatchSize(),
                        false),
                doubleProperty(
                        SPARK_MEMORY_REVOKING_THRESHOLD,
                        "Revoke memory when memory pool is filled over threshold",
                        prestoSparkConfig.getMemoryRevokingThreshold(),
                        false),
                doubleProperty(
                        SPARK_MEMORY_REVOKING_TARGET,
                        "When revoking memory, try to revoke so much that memory pool is filled below target at the end",
                        prestoSparkConfig.getMemoryRevokingTarget(),
                        false),
                new PropertyMetadata<>(
                        SPARK_QUERY_EXECUTION_STRATEGIES,
                        "Execution strategies to be applied while running the query",
                        VARCHAR,
                        List.class,
                        emptyList(),
                        false,
                        value -> {
                            List<String> specifiedStrategies = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(value.toString());
                            specifiedStrategies.forEach(strategy -> executionStrategyValidator.accept(strategy));
                            return specifiedStrategies;
                        },
                        value -> value),
                booleanProperty(
                        SPARK_RETRY_ON_OUT_OF_MEMORY_BROADCAST_JOIN_ENABLED,
                        "Disable broadcast join on broadcast OOM and re-submit the query again within the same spark session",
                        prestoSparkConfig.isRetryOnOutOfMemoryBroadcastJoinEnabled(),
                        false),
                booleanProperty(
                        SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ENABLED,
                        "Retry OOMs with increased memory settings and re-submit the query again within the same spark session",
                        prestoSparkConfig.isRetryOnOutOfMemoryWithIncreasedMemorySettingsEnabled(),
                        false),
                new PropertyMetadata<>(
                        OUT_OF_MEMORY_RETRY_PRESTO_SESSION_PROPERTIES,
                        "Presto session properties to use on OOM query retry, if spark_retry_on_out_of_memory_with_increased_memory_settings_enabled",
                        VARCHAR,
                        Map.class,
                        prestoSparkConfig.getOutOfMemoryRetryPrestoSessionProperties(),
                        true,
                        value -> MAP_SPLITTER.split(nullToEmpty((String) value)),
                        value -> value),
                new PropertyMetadata<>(
                        OUT_OF_MEMORY_RETRY_SPARK_CONFIGS,
                        "Spark Configs to use on OOM query retry, if spark_retry_on_out_of_memory_with_increased_memory_settings_enabled",
                        VARCHAR,
                        Map.class,
                        prestoSparkConfig.getOutOfMemoryRetrySparkConfigs(),
                        true,
                        value -> MAP_SPLITTER.split(nullToEmpty((String) value)),
                        value -> value),
                new PropertyMetadata<>(
                        SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ERROR_CODES,
                        "Error Codes to retry with increase memory settings",
                        VARCHAR,
                        List.class,
                        ImmutableList.of(EXCEEDED_LOCAL_MEMORY_LIMIT.name(), SPARK_EXECUTOR_OOM.name()),
                        false,
                        value -> Splitter.on(',').trimResults().omitEmptyStrings().splitToList(value.toString().toUpperCase()),
                        value -> value),
                dataSizeProperty(
                        SPARK_AVERAGE_INPUT_DATA_SIZE_PER_EXECUTOR,
                        "Average input data size per executor",
                        prestoSparkConfig.getAverageInputDataSizePerExecutor(),
                        false),
                integerProperty(
                        SPARK_MAX_EXECUTOR_COUNT,
                        "Maximum count of executors to run a query",
                        prestoSparkConfig.getMaxExecutorCount(),
                        false),
                integerProperty(
                        SPARK_MIN_EXECUTOR_COUNT,
                        "Minimum count of executors to run a query",
                        prestoSparkConfig.getMinExecutorCount(),
                        false),
                dataSizeProperty(
                        SPARK_AVERAGE_INPUT_DATA_SIZE_PER_PARTITION,
                        "Average input data size per partition",
                        prestoSparkConfig.getAverageInputDataSizePerPartition(),
                        false),
                integerProperty(
                        SPARK_MAX_HASH_PARTITION_COUNT,
                        "Maximum hash partition count required by the query",
                        prestoSparkConfig.getMaxHashPartitionCount(),
                        false),
                integerProperty(
                        SPARK_MIN_HASH_PARTITION_COUNT,
                        "Minimum hash partition count required by the query",
                        prestoSparkConfig.getMinHashPartitionCount(),
                        false),
                booleanProperty(
                        SPARK_RESOURCE_ALLOCATION_STRATEGY_ENABLED,
                        "Flag to enable optimized resource allocation strategy",
                        prestoSparkConfig.isSparkResourceAllocationStrategyEnabled(),
                        false),
                booleanProperty(
                        SPARK_EXECUTOR_ALLOCATION_STRATEGY_ENABLED,
                        "Flag to enable optimized executor allocation strategy",
                        prestoSparkConfig.isExecutorAllocationStrategyEnabled(),
                        false),
                booleanProperty(
                        SPARK_HASH_PARTITION_COUNT_ALLOCATION_STRATEGY_ENABLED,
                        "Flag to enable optimized hash partition count allocation strategy",
                        prestoSparkConfig.isHashPartitionCountAllocationStrategyEnabled(),
                        false),
                booleanProperty(
                        SPARK_RETRY_ON_OUT_OF_MEMORY_HIGHER_PARTITION_COUNT_ENABLED,
                        "Increases hash partition count by scaling factor specified by spark.hash-partition-count-scaling-factor-on-out-of-memory if query fails due to low hash partition count",
                        prestoSparkConfig.isRetryOnOutOfMemoryWithHigherHashPartitionCountEnabled(),
                        false),
                doubleProperty(
                        SPARK_HASH_PARTITION_COUNT_SCALING_FACTOR_ON_OUT_OF_MEMORY,
                        "Scaling factor for hash partition count when a query fails with out of memory error due to low hash partition count",
                        prestoSparkConfig.getHashPartitionCountScalingFactorOnOutOfMemory(),
                        false),
                booleanProperty(
                        SPARK_ADAPTIVE_QUERY_EXECUTION_ENABLED,
                        "Flag to enable adaptive query execution",
                        prestoSparkConfig.isAdaptiveQueryExecutionEnabled(),
                        false),
                booleanProperty(
                        ADAPTIVE_JOIN_SIDE_SWITCHING_ENABLED,
                        "Enables the adaptive optimizer to switch the build and probe sides of a join",
                        prestoSparkConfig.isAdaptiveJoinSideSwitchingEnabled(),
                        false),
                stringProperty(
                        NATIVE_EXECUTION_BROADCAST_BASE_PATH,
                        "Base path for temporary storage of broadcast data",
                        prestoSparkConfig.getNativeExecutionBroadcastBasePath(),
                        false),
                booleanProperty(
                        NATIVE_TRIGGER_COREDUMP_WHEN_UNRESPONSIVE_ENABLED,
                        "Trigger coredump of the native execution process when it becomes unresponsive",
                        prestoSparkConfig.isNativeTriggerCoredumpWhenUnresponsiveEnabled(),
                        false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isSparkPartitionCountAutoTuneEnabled(Session session)
    {
        return session.getSystemProperty(SPARK_PARTITION_COUNT_AUTO_TUNE_ENABLED, Boolean.class);
    }

    public static int getMinSparkInputPartitionCountForAutoTune(Session session)
    {
        return session.getSystemProperty(MIN_SPARK_INPUT_PARTITION_COUNT_FOR_AUTO_TUNE, Integer.class);
    }

    public static int getMaxSparkInputPartitionCountForAutoTune(Session session)
    {
        return session.getSystemProperty(MAX_SPARK_INPUT_PARTITION_COUNT_FOR_AUTO_TUNE, Integer.class);
    }

    public static int getSparkInitialPartitionCount(Session session)
    {
        return session.getSystemProperty(SPARK_INITIAL_PARTITION_COUNT, Integer.class);
    }

    public static DataSize getMaxSplitsDataSizePerSparkPartition(Session session)
    {
        return session.getSystemProperty(MAX_SPLITS_DATA_SIZE_PER_SPARK_PARTITION, DataSize.class);
    }

    public static DataSize getShuffleOutputTargetAverageRowSize(Session session)
    {
        return session.getSystemProperty(SHUFFLE_OUTPUT_TARGET_AVERAGE_ROW_SIZE, DataSize.class);
    }

    public static boolean isStorageBasedBroadcastJoinEnabled(Session session)
    {
        return session.getSystemProperty(STORAGE_BASED_BROADCAST_JOIN_ENABLED, Boolean.class);
    }

    public static DataSize getStorageBasedBroadcastJoinWriteBufferSize(Session session)
    {
        return session.getSystemProperty(STORAGE_BASED_BROADCAST_JOIN_WRITE_BUFFER_SIZE, DataSize.class);
    }

    public static DataSize getSparkBroadcastJoinMaxMemoryOverride(Session session)
    {
        return session.getSystemProperty(SPARK_BROADCAST_JOIN_MAX_MEMORY_OVERRIDE, DataSize.class);
    }

    public static int getSplitAssignmentBatchSize(Session session)
    {
        return session.getSystemProperty(SPARK_SPLIT_ASSIGNMENT_BATCH_SIZE, Integer.class);
    }

    public static double getMemoryRevokingThreshold(Session session)
    {
        return session.getSystemProperty(SPARK_MEMORY_REVOKING_THRESHOLD, Double.class);
    }

    public static double getMemoryRevokingTarget(Session session)
    {
        return session.getSystemProperty(SPARK_MEMORY_REVOKING_TARGET, Double.class);
    }

    public static List<String> getQueryExecutionStrategies(Session session)
    {
        return session.getSystemProperty(SPARK_QUERY_EXECUTION_STRATEGIES, List.class);
    }

    public static boolean isRetryOnOutOfMemoryBroadcastJoinEnabled(Session session)
    {
        return session.getSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_BROADCAST_JOIN_ENABLED, Boolean.class);
    }

    public static boolean isRetryOnOutOfMemoryWithIncreasedMemoryEnabled(Session session)
    {
        return session.getSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ENABLED, Boolean.class);
    }

    public static Map<String, String> getOutOfMemoryRetryPrestoSessionProperties(Session session)
    {
        return session.getSystemProperty(OUT_OF_MEMORY_RETRY_PRESTO_SESSION_PROPERTIES, Map.class);
    }

    public static Map<String, String> getOutOfMemoryRetrySparkConfigs(Session session)
    {
        return session.getSystemProperty(OUT_OF_MEMORY_RETRY_SPARK_CONFIGS, Map.class);
    }

    public static List<String> getRetryOnOutOfMemoryWithIncreasedMemoryErrorCodes(Session session)
    {
        return session.getSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ERROR_CODES, List.class);
    }

    public static DataSize getAverageInputDataSizePerExecutor(Session session)
    {
        return session.getSystemProperty(SPARK_AVERAGE_INPUT_DATA_SIZE_PER_EXECUTOR, DataSize.class);
    }

    public static int getMaxExecutorCount(Session session)
    {
        return session.getSystemProperty(SPARK_MAX_EXECUTOR_COUNT, Integer.class);
    }

    public static int getMinExecutorCount(Session session)
    {
        return session.getSystemProperty(SPARK_MIN_EXECUTOR_COUNT, Integer.class);
    }

    public static DataSize getAverageInputDataSizePerPartition(Session session)
    {
        return session.getSystemProperty(SPARK_AVERAGE_INPUT_DATA_SIZE_PER_PARTITION, DataSize.class);
    }

    public static int getMaxHashPartitionCount(Session session)
    {
        return session.getSystemProperty(SPARK_MAX_HASH_PARTITION_COUNT, Integer.class);
    }

    public static int getMinHashPartitionCount(Session session)
    {
        return session.getSystemProperty(SPARK_MIN_HASH_PARTITION_COUNT, Integer.class);
    }

    public static boolean isSparkResourceAllocationStrategyEnabled(Session session)
    {
        return session.getSystemProperty(SPARK_RESOURCE_ALLOCATION_STRATEGY_ENABLED, Boolean.class);
    }

    public static boolean isSparkExecutorAllocationStrategyEnabled(Session session)
    {
        return session.getSystemProperty(SPARK_EXECUTOR_ALLOCATION_STRATEGY_ENABLED, Boolean.class);
    }

    public static boolean isSparkHashPartitionCountAllocationStrategyEnabled(Session session)
    {
        return session.getSystemProperty(SPARK_HASH_PARTITION_COUNT_ALLOCATION_STRATEGY_ENABLED, Boolean.class);
    }

    public static boolean isRetryOnOutOfMemoryWithHigherHashPartitionCountEnabled(Session session)
    {
        return session.getSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_HIGHER_PARTITION_COUNT_ENABLED, Boolean.class);
    }

    public static double getHashPartitionCountScalingFactorOnOutOfMemory(Session session)
    {
        return session.getSystemProperty(SPARK_HASH_PARTITION_COUNT_SCALING_FACTOR_ON_OUT_OF_MEMORY, Double.class);
    }

    public static boolean isAdaptiveQueryExecutionEnabled(Session session)
    {
        return session.getSystemProperty(SPARK_ADAPTIVE_QUERY_EXECUTION_ENABLED, Boolean.class);
    }

    public static boolean isAdaptiveJoinSideSwitchingEnabled(Session session)
    {
        return session.getSystemProperty(ADAPTIVE_JOIN_SIDE_SWITCHING_ENABLED, Boolean.class);
    }

    public static String getNativeExecutionBroadcastBasePath(Session session)
    {
        return session.getSystemProperty(NATIVE_EXECUTION_BROADCAST_BASE_PATH, String.class);
    }

    public static boolean isNativeTriggerCoredumpWhenUnresponsiveEnabled(Session session)
    {
        return session.getSystemProperty(NATIVE_TRIGGER_COREDUMP_WHEN_UNRESPONSIVE_ENABLED, Boolean.class);
    }
}
