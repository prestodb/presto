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
package com.facebook.presto;

import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.QueryManagerConfig.ExchangeMaterializationStrategy;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig.ResourceAwareSchedulingStrategy;
import com.facebook.presto.execution.warnings.WarningCollectorConfig;
import com.facebook.presto.execution.warnings.WarningHandlingLevel;
import com.facebook.presto.memory.MemoryManagerConfig;
import com.facebook.presto.memory.NodeMemoryConfig;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spiller.NodeSpillConfig;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.FeaturesConfig.AggregationIfToFilterRewriteStrategy;
import com.facebook.presto.sql.analyzer.FeaturesConfig.AggregationPartitioningMergingStrategy;
import com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType;
import com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy;
import com.facebook.presto.sql.analyzer.FeaturesConfig.PartialAggregationStrategy;
import com.facebook.presto.sql.analyzer.FeaturesConfig.PartialMergePushdownStrategy;
import com.facebook.presto.sql.analyzer.FeaturesConfig.PartitioningPrecisionStrategy;
import com.facebook.presto.sql.analyzer.FeaturesConfig.SingleStreamSpillerChoice;
import com.facebook.presto.tracing.TracingConfig;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.dataSizeProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.doubleProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.BROADCAST;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.PARTITIONED;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.ELIMINATE_CROSS_JOINS;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.NONE;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.PartialAggregationStrategy.ALWAYS;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.PartialAggregationStrategy.NEVER;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public final class SystemSessionProperties
{
    public static final String OPTIMIZE_HASH_GENERATION = "optimize_hash_generation";
    public static final String JOIN_DISTRIBUTION_TYPE = "join_distribution_type";
    public static final String JOIN_MAX_BROADCAST_TABLE_SIZE = "join_max_broadcast_table_size";
    public static final String DISTRIBUTED_JOIN = "distributed_join";
    public static final String DISTRIBUTED_INDEX_JOIN = "distributed_index_join";
    public static final String HASH_PARTITION_COUNT = "hash_partition_count";
    public static final String PARTITIONING_PROVIDER_CATALOG = "partitioning_provider_catalog";
    public static final String EXCHANGE_MATERIALIZATION_STRATEGY = "exchange_materialization_strategy";
    public static final String USE_STREAMING_EXCHANGE_FOR_MARK_DISTINCT = "use_stream_exchange_for_mark_distinct";
    public static final String GROUPED_EXECUTION = "grouped_execution";
    public static final String RECOVERABLE_GROUPED_EXECUTION = "recoverable_grouped_execution";
    public static final String MAX_FAILED_TASK_PERCENTAGE = "max_failed_task_percentage";
    public static final String MAX_STAGE_RETRIES = "max_stage_retries";
    public static final String PREFER_STREAMING_OPERATORS = "prefer_streaming_operators";
    public static final String TASK_WRITER_COUNT = "task_writer_count";
    public static final String TASK_PARTITIONED_WRITER_COUNT = "task_partitioned_writer_count";
    public static final String TASK_CONCURRENCY = "task_concurrency";
    public static final String TASK_SHARE_INDEX_LOADING = "task_share_index_loading";
    public static final String QUERY_MAX_MEMORY = "query_max_memory";
    public static final String QUERY_MAX_MEMORY_PER_NODE = "query_max_memory_per_node";
    public static final String QUERY_MAX_BROADCAST_MEMORY = "query_max_broadcast_memory";
    public static final String QUERY_MAX_TOTAL_MEMORY = "query_max_total_memory";
    public static final String QUERY_MAX_TOTAL_MEMORY_PER_NODE = "query_max_total_memory_per_node";
    public static final String QUERY_MAX_EXECUTION_TIME = "query_max_execution_time";
    public static final String QUERY_MAX_RUN_TIME = "query_max_run_time";
    public static final String RESOURCE_OVERCOMMIT = "resource_overcommit";
    public static final String QUERY_MAX_CPU_TIME = "query_max_cpu_time";
    public static final String QUERY_MAX_SCAN_RAW_INPUT_BYTES = "query_max_scan_raw_input_bytes";
    public static final String QUERY_MAX_OUTPUT_SIZE = "query_max_output_size";
    public static final String QUERY_MAX_STAGE_COUNT = "query_max_stage_count";
    public static final String REDISTRIBUTE_WRITES = "redistribute_writes";
    public static final String SCALE_WRITERS = "scale_writers";
    public static final String WRITER_MIN_SIZE = "writer_min_size";
    public static final String OPTIMIZED_SCALE_WRITER_PRODUCER_BUFFER = "optimized_scale_writer_producer_buffer";
    public static final String PUSH_TABLE_WRITE_THROUGH_UNION = "push_table_write_through_union";
    public static final String EXECUTION_POLICY = "execution_policy";
    public static final String DICTIONARY_AGGREGATION = "dictionary_aggregation";
    public static final String PLAN_WITH_TABLE_NODE_PARTITIONING = "plan_with_table_node_partitioning";
    public static final String SPATIAL_JOIN = "spatial_join";
    public static final String SPATIAL_PARTITIONING_TABLE_NAME = "spatial_partitioning_table_name";
    public static final String COLOCATED_JOIN = "colocated_join";
    public static final String CONCURRENT_LIFESPANS_PER_NODE = "concurrent_lifespans_per_task";
    public static final String REORDER_JOINS = "reorder_joins";
    public static final String JOIN_REORDERING_STRATEGY = "join_reordering_strategy";
    public static final String PARTIAL_MERGE_PUSHDOWN_STRATEGY = "partial_merge_pushdown_strategy";
    public static final String MAX_REORDERED_JOINS = "max_reordered_joins";
    public static final String INITIAL_SPLITS_PER_NODE = "initial_splits_per_node";
    public static final String SPLIT_CONCURRENCY_ADJUSTMENT_INTERVAL = "split_concurrency_adjustment_interval";
    public static final String OPTIMIZE_METADATA_QUERIES = "optimize_metadata_queries";
    public static final String OPTIMIZE_METADATA_QUERIES_IGNORE_STATS = "optimize_metadata_queries_ignore_stats";
    public static final String FAST_INEQUALITY_JOINS = "fast_inequality_joins";
    public static final String QUERY_PRIORITY = "query_priority";
    public static final String SPILL_ENABLED = "spill_enabled";
    public static final String JOIN_SPILL_ENABLED = "join_spill_enabled";
    public static final String AGGREGATION_SPILL_ENABLED = "aggregation_spill_enabled";
    public static final String DISTINCT_AGGREGATION_SPILL_ENABLED = "distinct_aggregation_spill_enabled";
    public static final String DEDUP_BASED_DISTINCT_AGGREGATION_SPILL_ENABLED = "dedup_based_distinct_aggregation_spill_enabled";
    public static final String ORDER_BY_AGGREGATION_SPILL_ENABLED = "order_by_aggregation_spill_enabled";
    public static final String WINDOW_SPILL_ENABLED = "window_spill_enabled";
    public static final String ORDER_BY_SPILL_ENABLED = "order_by_spill_enabled";
    public static final String AGGREGATION_OPERATOR_UNSPILL_MEMORY_LIMIT = "aggregation_operator_unspill_memory_limit";
    public static final String QUERY_MAX_REVOCABLE_MEMORY_PER_NODE = "query_max_revocable_memory_per_node";
    public static final String TEMP_STORAGE_SPILLER_BUFFER_SIZE = "temp_storage_spiller_buffer_size";
    public static final String OPTIMIZE_DISTINCT_AGGREGATIONS = "optimize_mixed_distinct_aggregations";
    public static final String LEGACY_ROW_FIELD_ORDINAL_ACCESS = "legacy_row_field_ordinal_access";
    public static final String LEGACY_MAP_SUBSCRIPT = "do_not_use_legacy_map_subscript";
    public static final String ITERATIVE_OPTIMIZER = "iterative_optimizer_enabled";
    public static final String ITERATIVE_OPTIMIZER_TIMEOUT = "iterative_optimizer_timeout";
    public static final String RUNTIME_OPTIMIZER_ENABLED = "runtime_optimizer_enabled";
    public static final String EXCHANGE_COMPRESSION = "exchange_compression";
    public static final String EXCHANGE_CHECKSUM = "exchange_checksum";
    public static final String LEGACY_TIMESTAMP = "legacy_timestamp";
    public static final String ENABLE_INTERMEDIATE_AGGREGATIONS = "enable_intermediate_aggregations";
    public static final String PUSH_AGGREGATION_THROUGH_JOIN = "push_aggregation_through_join";
    public static final String PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN = "push_partial_aggregation_through_join";
    public static final String PARSE_DECIMAL_LITERALS_AS_DOUBLE = "parse_decimal_literals_as_double";
    public static final String FORCE_SINGLE_NODE_OUTPUT = "force_single_node_output";
    public static final String FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_SIZE = "filter_and_project_min_output_page_size";
    public static final String FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_ROW_COUNT = "filter_and_project_min_output_page_row_count";
    public static final String DISTRIBUTED_SORT = "distributed_sort";
    public static final String USE_MARK_DISTINCT = "use_mark_distinct";
    public static final String PREFER_PARTIAL_AGGREGATION = "prefer_partial_aggregation";
    public static final String PARTIAL_AGGREGATION_STRATEGY = "partial_aggregation_strategy";
    public static final String PARTIAL_AGGREGATION_BYTE_REDUCTION_THRESHOLD = "partial_aggregation_byte_reduction_threshold";
    public static final String OPTIMIZE_TOP_N_ROW_NUMBER = "optimize_top_n_row_number";
    public static final String MAX_GROUPING_SETS = "max_grouping_sets";
    public static final String LEGACY_UNNEST = "legacy_unnest";
    public static final String STATISTICS_CPU_TIMER_ENABLED = "statistics_cpu_timer_enabled";
    public static final String ENABLE_STATS_CALCULATOR = "enable_stats_calculator";
    public static final String ENABLE_STATS_COLLECTION_FOR_TEMPORARY_TABLE = "enable_stats_collection_for_temporary_table";
    public static final String IGNORE_STATS_CALCULATOR_FAILURES = "ignore_stats_calculator_failures";
    public static final String PRINT_STATS_FOR_NON_JOIN_QUERY = "print_stats_for_non_join_query";
    public static final String MAX_DRIVERS_PER_TASK = "max_drivers_per_task";
    public static final String MAX_TASKS_PER_STAGE = "max_tasks_per_stage";
    public static final String DEFAULT_FILTER_FACTOR_ENABLED = "default_filter_factor_enabled";
    public static final String PUSH_LIMIT_THROUGH_OUTER_JOIN = "push_limit_through_outer_join";
    public static final String MAX_CONCURRENT_MATERIALIZATIONS = "max_concurrent_materializations";
    public static final String PUSHDOWN_SUBFIELDS_ENABLED = "pushdown_subfields_enabled";
    public static final String TABLE_WRITER_MERGE_OPERATOR_ENABLED = "table_writer_merge_operator_enabled";
    public static final String INDEX_LOADER_TIMEOUT = "index_loader_timeout";
    public static final String OPTIMIZED_REPARTITIONING_ENABLED = "optimized_repartitioning";
    public static final String AGGREGATION_PARTITIONING_MERGING_STRATEGY = "aggregation_partitioning_merging_strategy";
    public static final String LIST_BUILT_IN_FUNCTIONS_ONLY = "list_built_in_functions_only";
    public static final String PARTITIONING_PRECISION_STRATEGY = "partitioning_precision_strategy";
    public static final String EXPERIMENTAL_FUNCTIONS_ENABLED = "experimental_functions_enabled";
    public static final String USE_LEGACY_SCHEDULER = "use_legacy_scheduler";
    public static final String OPTIMIZE_COMMON_SUB_EXPRESSIONS = "optimize_common_sub_expressions";
    public static final String PREFER_DISTRIBUTED_UNION = "prefer_distributed_union";
    public static final String WARNING_HANDLING = "warning_handling";
    public static final String OPTIMIZE_NULLS_IN_JOINS = "optimize_nulls_in_join";
    public static final String TARGET_RESULT_SIZE = "target_result_size";
    public static final String PUSHDOWN_DEREFERENCE_ENABLED = "pushdown_dereference_enabled";
    public static final String ENABLE_DYNAMIC_FILTERING = "enable_dynamic_filtering";
    public static final String DYNAMIC_FILTERING_MAX_PER_DRIVER_ROW_COUNT = "dynamic_filtering_max_per_driver_row_count";
    public static final String DYNAMIC_FILTERING_MAX_PER_DRIVER_SIZE = "dynamic_filtering_max_per_driver_size";
    public static final String DYNAMIC_FILTERING_RANGE_ROW_LIMIT_PER_DRIVER = "dynamic_filtering_range_row_limit_per_driver";
    public static final String FRAGMENT_RESULT_CACHING_ENABLED = "fragment_result_caching_enabled";
    public static final String LEGACY_TYPE_COERCION_WARNING_ENABLED = "legacy_type_coercion_warning_enabled";
    public static final String INLINE_SQL_FUNCTIONS = "inline_sql_functions";
    public static final String REMOTE_FUNCTIONS_ENABLED = "remote_functions_enabled";
    public static final String CHECK_ACCESS_CONTROL_ON_UTILIZED_COLUMNS_ONLY = "check_access_control_on_utilized_columns_only";
    public static final String SKIP_REDUNDANT_SORT = "skip_redundant_sort";
    public static final String ALLOW_WINDOW_ORDER_BY_LITERALS = "allow_window_order_by_literals";
    public static final String ENFORCE_FIXED_DISTRIBUTION_FOR_OUTPUT_OPERATOR = "enforce_fixed_distribution_for_output_operator";
    public static final String MAX_UNACKNOWLEDGED_SPLITS_PER_TASK = "max_unacknowledged_splits_per_task";
    public static final String OPTIMIZE_JOINS_WITH_EMPTY_SOURCES = "optimize_joins_with_empty_sources";
    public static final String SPOOLING_OUTPUT_BUFFER_ENABLED = "spooling_output_buffer_enabled";
    public static final String SPARK_ASSIGN_BUCKET_TO_PARTITION_FOR_PARTITIONED_TABLE_WRITE_ENABLED = "spark_assign_bucket_to_partition_for_partitioned_table_write_enabled";
    public static final String LOG_FORMATTED_QUERY_ENABLED = "log_formatted_query_enabled";
    public static final String QUERY_RETRY_LIMIT = "query_retry_limit";
    public static final String QUERY_RETRY_MAX_EXECUTION_TIME = "query_retry_max_execution_time";
    public static final String PARTIAL_RESULTS_ENABLED = "partial_results_enabled";
    public static final String PARTIAL_RESULTS_COMPLETION_RATIO_THRESHOLD = "partial_results_completion_ratio_threshold";
    public static final String PARTIAL_RESULTS_MAX_EXECUTION_TIME_MULTIPLIER = "partial_results_max_execution_time_multiplier";
    public static final String OFFSET_CLAUSE_ENABLED = "offset_clause_enabled";
    public static final String VERBOSE_EXCEEDED_MEMORY_LIMIT_ERRORS_ENABLED = "verbose_exceeded_memory_limit_errors_enabled";
    public static final String MATERIALIZED_VIEW_DATA_CONSISTENCY_ENABLED = "materialized_view_data_consistency_enabled";
    public static final String QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED = "query_optimization_with_materialized_view_enabled";
    public static final String AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY = "aggregation_if_to_filter_rewrite_strategy";
    public static final String RESOURCE_AWARE_SCHEDULING_STRATEGY = "resource_aware_scheduling_strategy";
    public static final String HEAP_DUMP_ON_EXCEEDED_MEMORY_LIMIT_ENABLED = "heap_dump_on_exceeded_memory_limit_enabled";
    public static final String EXCEEDED_MEMORY_LIMIT_HEAP_DUMP_FILE_DIRECTORY = "exceeded_memory_limit_heap_dump_file_directory";
    public static final String ENABLE_DISTRIBUTED_TRACING = "enable_distributed_tracing";
    public static final String VERBOSE_RUNTIME_STATS_ENABLED = "verbose_runtime_stats_enabled";

    //TODO: Prestissimo related session properties that are temporarily put here. They will be relocated in the future
    public static final String PRESTISSIMO_SIMPLIFIED_EXPRESSION_EVALUATION_ENABLED = "simplified_expression_evaluation_enabled";
    public static final String KEY_BASED_SAMPLING_ENABLED = "key_based_sampling_enabled";
    public static final String KEY_BASED_SAMPLING_PERCENTAGE = "key_based_sampling_percentage";
    public static final String KEY_BASED_SAMPLING_FUNCTION = "key_based_sampling_function";

    private final List<PropertyMetadata<?>> sessionProperties;

    public SystemSessionProperties()
    {
        this(
                new QueryManagerConfig(),
                new TaskManagerConfig(),
                new MemoryManagerConfig(),
                new FeaturesConfig(),
                new NodeMemoryConfig(),
                new WarningCollectorConfig(),
                new NodeSchedulerConfig(),
                new NodeSpillConfig(),
                new TracingConfig());
    }

    @Inject
    public SystemSessionProperties(
            QueryManagerConfig queryManagerConfig,
            TaskManagerConfig taskManagerConfig,
            MemoryManagerConfig memoryManagerConfig,
            FeaturesConfig featuresConfig,
            NodeMemoryConfig nodeMemoryConfig,
            WarningCollectorConfig warningCollectorConfig,
            NodeSchedulerConfig nodeSchedulerConfig,
            NodeSpillConfig nodeSpillConfig,
            TracingConfig tracingConfig)
    {
        sessionProperties = ImmutableList.of(
                stringProperty(
                        EXECUTION_POLICY,
                        "Policy used for scheduling query tasks",
                        queryManagerConfig.getQueryExecutionPolicy(),
                        false),
                booleanProperty(
                        OPTIMIZE_HASH_GENERATION,
                        "Compute hash codes for distribution, joins, and aggregations early in query plan",
                        featuresConfig.isOptimizeHashGeneration(),
                        false),
                booleanProperty(
                        DISTRIBUTED_JOIN,
                        "(DEPRECATED) Use a distributed join instead of a broadcast join. If this is set, join_distribution_type is ignored.",
                        null,
                        false),
                new PropertyMetadata<>(
                        JOIN_DISTRIBUTION_TYPE,
                        format("The join method to use. Options are %s",
                                Stream.of(JoinDistributionType.values())
                                        .map(JoinDistributionType::name)
                                        .collect(joining(","))),
                        VARCHAR,
                        JoinDistributionType.class,
                        featuresConfig.getJoinDistributionType(),
                        false,
                        value -> JoinDistributionType.valueOf(((String) value).toUpperCase()),
                        JoinDistributionType::name),
                new PropertyMetadata<>(
                        JOIN_MAX_BROADCAST_TABLE_SIZE,
                        "Maximum estimated size of a table that can be broadcast for JOIN.",
                        VARCHAR,
                        DataSize.class,
                        featuresConfig.getJoinMaxBroadcastTableSize(),
                        true,
                        value -> DataSize.valueOf((String) value),
                        DataSize::toString),
                booleanProperty(
                        DISTRIBUTED_INDEX_JOIN,
                        "Distribute index joins on join keys instead of executing inline",
                        featuresConfig.isDistributedIndexJoinsEnabled(),
                        false),
                integerProperty(
                        HASH_PARTITION_COUNT,
                        "Number of partitions for distributed joins and aggregations",
                        queryManagerConfig.getHashPartitionCount(),
                        false),
                stringProperty(
                        PARTITIONING_PROVIDER_CATALOG,
                        "Name of the catalog providing custom partitioning",
                        queryManagerConfig.getPartitioningProviderCatalog(),
                        false),
                new PropertyMetadata<>(
                        EXCHANGE_MATERIALIZATION_STRATEGY,
                        format("The exchange materialization strategy to use. Options are %s",
                                Stream.of(ExchangeMaterializationStrategy.values())
                                        .map(ExchangeMaterializationStrategy::name)
                                        .collect(joining(","))),
                        VARCHAR,
                        ExchangeMaterializationStrategy.class,
                        queryManagerConfig.getExchangeMaterializationStrategy(),
                        false,
                        value -> ExchangeMaterializationStrategy.valueOf(((String) value).toUpperCase()),
                        ExchangeMaterializationStrategy::name),
                booleanProperty(
                        USE_STREAMING_EXCHANGE_FOR_MARK_DISTINCT,
                        "Use streaming instead of materialization for mark distinct with materialized exchange enabled",
                        queryManagerConfig.getUseStreamingExchangeForMarkDistinct(),
                        false),
                booleanProperty(
                        GROUPED_EXECUTION,
                        "Use grouped execution when possible",
                        featuresConfig.isGroupedExecutionEnabled(),
                        false),
                doubleProperty(
                        MAX_FAILED_TASK_PERCENTAGE,
                        "Max percentage of failed tasks that are retryable for recoverable dynamic scheduling",
                        featuresConfig.getMaxFailedTaskPercentage(),
                        false),
                booleanProperty(
                        RECOVERABLE_GROUPED_EXECUTION,
                        "Experimental: Use recoverable grouped execution when possible",
                        featuresConfig.isRecoverableGroupedExecutionEnabled(),
                        false),
                integerProperty(
                        MAX_STAGE_RETRIES,
                        "Maximum number of times that stages can be retried",
                        featuresConfig.getMaxStageRetries(),
                        false),
                booleanProperty(
                        PREFER_STREAMING_OPERATORS,
                        "Prefer source table layouts that produce streaming operators",
                        false,
                        false),
                new PropertyMetadata<>(
                        TASK_WRITER_COUNT,
                        "Default number of local parallel table writer jobs per worker",
                        BIGINT,
                        Integer.class,
                        taskManagerConfig.getWriterCount(),
                        false,
                        value -> validateValueIsPowerOfTwo(requireNonNull(value, "value is null"), TASK_WRITER_COUNT),
                        value -> value),
                new PropertyMetadata<>(
                        TASK_PARTITIONED_WRITER_COUNT,
                        "Number of writers per task for partitioned writes. If not set, the number set by task.writer-count will be used",
                        BIGINT,
                        Integer.class,
                        taskManagerConfig.getPartitionedWriterCount(),
                        false,
                        value -> validateValueIsPowerOfTwo(value, TASK_PARTITIONED_WRITER_COUNT),
                        value -> value),
                booleanProperty(
                        REDISTRIBUTE_WRITES,
                        "Force parallel distributed writes",
                        featuresConfig.isRedistributeWrites(),
                        false),
                booleanProperty(
                        SCALE_WRITERS,
                        "Scale out writers based on throughput (use minimum necessary)",
                        featuresConfig.isScaleWriters(),
                        false),
                new PropertyMetadata<>(
                        WRITER_MIN_SIZE,
                        "Target minimum size of writer output when scaling writers",
                        VARCHAR,
                        DataSize.class,
                        featuresConfig.getWriterMinSize(),
                        false,
                        value -> DataSize.valueOf((String) value),
                        DataSize::toString),
                booleanProperty(
                        OPTIMIZED_SCALE_WRITER_PRODUCER_BUFFER,
                        "Optimize scale writer creation based on producer buffer",
                        featuresConfig.isOptimizedScaleWriterProducerBuffer(),
                        true),
                booleanProperty(
                        PUSH_TABLE_WRITE_THROUGH_UNION,
                        "Parallelize writes when using UNION ALL in queries that write data",
                        featuresConfig.isPushTableWriteThroughUnion(),
                        false),
                new PropertyMetadata<>(
                        TASK_CONCURRENCY,
                        "Default number of local parallel jobs per worker",
                        BIGINT,
                        Integer.class,
                        taskManagerConfig.getTaskConcurrency(),
                        false,
                        value -> validateValueIsPowerOfTwo(requireNonNull(value, "value is null"), TASK_CONCURRENCY),
                        value -> value),
                booleanProperty(
                        TASK_SHARE_INDEX_LOADING,
                        "Share index join lookups and caching within a task",
                        taskManagerConfig.isShareIndexLoading(),
                        false),
                new PropertyMetadata<>(
                        QUERY_MAX_RUN_TIME,
                        "Maximum run time of a query (includes the queueing time)",
                        VARCHAR,
                        Duration.class,
                        queryManagerConfig.getQueryMaxRunTime(),
                        false,
                        value -> Duration.valueOf((String) value),
                        Duration::toString),
                new PropertyMetadata<>(
                        QUERY_MAX_EXECUTION_TIME,
                        "Maximum execution time of a query",
                        VARCHAR,
                        Duration.class,
                        queryManagerConfig.getQueryMaxExecutionTime(),
                        false,
                        value -> Duration.valueOf((String) value),
                        Duration::toString),
                new PropertyMetadata<>(
                        QUERY_MAX_CPU_TIME,
                        "Maximum CPU time of a query",
                        VARCHAR,
                        Duration.class,
                        queryManagerConfig.getQueryMaxCpuTime(),
                        false,
                        value -> Duration.valueOf((String) value),
                        Duration::toString),
                new PropertyMetadata<>(
                        QUERY_MAX_MEMORY,
                        "Maximum amount of distributed memory a query can use",
                        VARCHAR,
                        DataSize.class,
                        memoryManagerConfig.getSoftMaxQueryMemory(),
                        true,
                        value -> DataSize.valueOf((String) value),
                        DataSize::toString),
                new PropertyMetadata<>(
                        QUERY_MAX_MEMORY_PER_NODE,
                        "Maximum amount of user task memory a query can use",
                        VARCHAR,
                        DataSize.class,
                        nodeMemoryConfig.getSoftMaxQueryMemoryPerNode(),
                        true,
                        value -> DataSize.valueOf((String) value),
                        DataSize::toString),
                new PropertyMetadata<>(
                        QUERY_MAX_BROADCAST_MEMORY,
                        "Maximum amount of memory a query can use for broadcast join",
                        VARCHAR,
                        DataSize.class,
                        nodeMemoryConfig.getMaxQueryBroadcastMemory(),
                        true,
                        value -> DataSize.valueOf((String) value),
                        DataSize::toString),
                new PropertyMetadata<>(
                        QUERY_MAX_TOTAL_MEMORY,
                        "Maximum amount of distributed total memory a query can use",
                        VARCHAR,
                        DataSize.class,
                        memoryManagerConfig.getSoftMaxQueryTotalMemory(),
                        true,
                        value -> DataSize.valueOf((String) value),
                        DataSize::toString),
                new PropertyMetadata<>(
                        QUERY_MAX_TOTAL_MEMORY_PER_NODE,
                        "Maximum amount of total (user + system) task memory a query can use",
                        VARCHAR,
                        DataSize.class,
                        nodeMemoryConfig.getSoftMaxQueryTotalMemoryPerNode(),
                        true,
                        value -> DataSize.valueOf((String) value),
                        DataSize::toString),
                booleanProperty(
                        RESOURCE_OVERCOMMIT,
                        "Use resources which are not guaranteed to be available to the query",
                        false,
                        false),
                dataSizeProperty(
                        QUERY_MAX_SCAN_RAW_INPUT_BYTES,
                        "Maximum scan raw input bytes of a query",
                        queryManagerConfig.getQueryMaxScanRawInputBytes(),
                        false),
                dataSizeProperty(
                        QUERY_MAX_OUTPUT_SIZE,
                        "Maximum data that can be fetched by a query",
                        queryManagerConfig.getQueryMaxOutputSize(),
                        false),
                integerProperty(
                        QUERY_MAX_STAGE_COUNT,
                        "Temporary: Maximum number of stages a query can have",
                        queryManagerConfig.getMaxStageCount(),
                        true),
                booleanProperty(
                        DICTIONARY_AGGREGATION,
                        "Enable optimization for aggregations on dictionaries",
                        featuresConfig.isDictionaryAggregation(),
                        false),
                integerProperty(
                        INITIAL_SPLITS_PER_NODE,
                        "The number of splits each node will run per task, initially",
                        taskManagerConfig.getInitialSplitsPerNode(),
                        false),
                new PropertyMetadata<>(
                        SPLIT_CONCURRENCY_ADJUSTMENT_INTERVAL,
                        "Experimental: Interval between changes to the number of concurrent splits per node",
                        VARCHAR,
                        Duration.class,
                        taskManagerConfig.getSplitConcurrencyAdjustmentInterval(),
                        false,
                        value -> Duration.valueOf((String) value),
                        Duration::toString),
                booleanProperty(
                        OPTIMIZE_METADATA_QUERIES,
                        "Enable optimization for metadata queries if the resulting partitions are not empty according to the partition stats",
                        featuresConfig.isOptimizeMetadataQueries(),
                        false),
                booleanProperty(
                        OPTIMIZE_METADATA_QUERIES_IGNORE_STATS,
                        "Enable optimization for metadata queries. Note if metadata entry has empty data, the result might be different (e.g. empty Hive partition)",
                        featuresConfig.isOptimizeMetadataQueriesIgnoreStats(),
                        false),
                integerProperty(
                        QUERY_PRIORITY,
                        "The priority of queries. Larger numbers are higher priority",
                        1,
                        false),
                booleanProperty(
                        PLAN_WITH_TABLE_NODE_PARTITIONING,
                        "Experimental: Adapt plan to pre-partitioned tables",
                        true,
                        false),
                booleanProperty(
                        REORDER_JOINS,
                        "(DEPRECATED) Reorder joins to remove unnecessary cross joins. If this is set, join_reordering_strategy will be ignored",
                        null,
                        false),
                new PropertyMetadata<>(
                        JOIN_REORDERING_STRATEGY,
                        format("The join reordering strategy to use. Options are %s",
                                Stream.of(JoinReorderingStrategy.values())
                                        .map(JoinReorderingStrategy::name)
                                        .collect(joining(","))),
                        VARCHAR,
                        JoinReorderingStrategy.class,
                        featuresConfig.getJoinReorderingStrategy(),
                        false,
                        value -> JoinReorderingStrategy.valueOf(((String) value).toUpperCase()),
                        JoinReorderingStrategy::name),
                new PropertyMetadata<>(
                        PARTIAL_MERGE_PUSHDOWN_STRATEGY,
                        format("Experimental: Partial merge pushdown strategy to use. Optionas are %s",
                                Stream.of(PartialMergePushdownStrategy.values())
                                        .map(PartialMergePushdownStrategy::name)
                                        .collect(joining(","))),
                        VARCHAR,
                        PartialMergePushdownStrategy.class,
                        featuresConfig.getPartialMergePushdownStrategy(),
                        false,
                        value -> PartialMergePushdownStrategy.valueOf(((String) value).toUpperCase()),
                        PartialMergePushdownStrategy::name),
                new PropertyMetadata<>(
                        MAX_REORDERED_JOINS,
                        "The maximum number of joins to reorder as one group in cost-based join reordering",
                        BIGINT,
                        Integer.class,
                        featuresConfig.getMaxReorderedJoins(),
                        false,
                        value -> {
                            int intValue = ((Number) requireNonNull(value, "value is null")).intValue();
                            if (intValue < 2) {
                                throw new PrestoException(INVALID_SESSION_PROPERTY, format("%s must be greater than or equal to 2: %s", MAX_REORDERED_JOINS, intValue));
                            }
                            return intValue;
                        },
                        value -> value),
                booleanProperty(
                        FAST_INEQUALITY_JOINS,
                        "Use faster handling of inequality join if it is possible",
                        featuresConfig.isFastInequalityJoins(),
                        false),
                booleanProperty(
                        COLOCATED_JOIN,
                        "Experimental: Use a colocated join when possible",
                        featuresConfig.isColocatedJoinsEnabled(),
                        false),
                booleanProperty(
                        SPATIAL_JOIN,
                        "Use spatial index for spatial join when possible",
                        featuresConfig.isSpatialJoinsEnabled(),
                        false),
                stringProperty(
                        SPATIAL_PARTITIONING_TABLE_NAME,
                        "Name of the table containing spatial partitioning scheme",
                        null,
                        false),
                integerProperty(
                        CONCURRENT_LIFESPANS_PER_NODE,
                        "Experimental: Run a fixed number of groups concurrently for eligible JOINs",
                        featuresConfig.getConcurrentLifespansPerTask(),
                        false),
                new PropertyMetadata<>(
                        SPILL_ENABLED,
                        "Experimental: Enable spilling",
                        BOOLEAN,
                        Boolean.class,
                        featuresConfig.isSpillEnabled(),
                        false,
                        value -> {
                            boolean spillEnabled = (Boolean) value;
                            if (spillEnabled
                                    && featuresConfig.getSingleStreamSpillerChoice() == SingleStreamSpillerChoice.LOCAL_FILE
                                    && featuresConfig.getSpillerSpillPaths().isEmpty()) {
                                throw new PrestoException(
                                        INVALID_SESSION_PROPERTY,
                                        format("%s cannot be set to true; no spill paths configured", SPILL_ENABLED));
                            }
                            return spillEnabled;
                        },
                        value -> value),
                booleanProperty(
                        JOIN_SPILL_ENABLED,
                        "Enable join spilling",
                        featuresConfig.isJoinSpillingEnabled(),
                        false),
                booleanProperty(
                        AGGREGATION_SPILL_ENABLED,
                        "Enable aggregate spilling if spill_enabled",
                        featuresConfig.isAggregationSpillEnabled(),
                        false),
                booleanProperty(
                        DISTINCT_AGGREGATION_SPILL_ENABLED,
                        "Enable spill for distinct aggregations if spill_enabled and aggregation_spill_enabled",
                        featuresConfig.isDistinctAggregationSpillEnabled(),
                        false),
                booleanProperty(
                        DEDUP_BASED_DISTINCT_AGGREGATION_SPILL_ENABLED,
                        "Perform deduplication of input data for distinct aggregates before spilling",
                        featuresConfig.isDedupBasedDistinctAggregationSpillEnabled(),
                        false),
                booleanProperty(
                        ORDER_BY_AGGREGATION_SPILL_ENABLED,
                        "Enable spill for order-by aggregations if spill_enabled and aggregation_spill_enabled",
                        featuresConfig.isOrderByAggregationSpillEnabled(),
                        false),
                booleanProperty(
                        WINDOW_SPILL_ENABLED,
                        "Enable window spilling if spill_enabled",
                        featuresConfig.isWindowSpillEnabled(),
                        false),
                booleanProperty(
                        ORDER_BY_SPILL_ENABLED,
                        "Enable order by spilling if spill_enabled",
                        featuresConfig.isOrderBySpillEnabled(),
                        false),
                new PropertyMetadata<>(
                        AGGREGATION_OPERATOR_UNSPILL_MEMORY_LIMIT,
                        "Experimental: How much memory can should be allocated per aggragation operator in unspilling process",
                        VARCHAR,
                        DataSize.class,
                        featuresConfig.getAggregationOperatorUnspillMemoryLimit(),
                        false,
                        value -> DataSize.valueOf((String) value),
                        DataSize::toString),
                new PropertyMetadata<>(
                        QUERY_MAX_REVOCABLE_MEMORY_PER_NODE,
                        "Maximum amount of revocable memory a query can use",
                        VARCHAR,
                        DataSize.class,
                        nodeSpillConfig.getMaxRevocableMemoryPerNode(),
                        true,
                        value -> DataSize.valueOf((String) value),
                        DataSize::toString),
                new PropertyMetadata<>(
                        TEMP_STORAGE_SPILLER_BUFFER_SIZE,
                        "Experimental: Buffer size used by TempStorageSingleStreamSpiller",
                        VARCHAR,
                        DataSize.class,
                        nodeSpillConfig.getTempStorageBufferSize(),
                        false,
                        value -> DataSize.valueOf((String) value),
                        DataSize::toString),
                booleanProperty(
                        OPTIMIZE_DISTINCT_AGGREGATIONS,
                        "Optimize mixed non-distinct and distinct aggregations",
                        featuresConfig.isOptimizeMixedDistinctAggregations(),
                        false),
                booleanProperty(
                        LEGACY_ROW_FIELD_ORDINAL_ACCESS,
                        "Allow accessing anonymous row field with .field0, .field1, ...",
                        featuresConfig.isLegacyRowFieldOrdinalAccess(),
                        false),
                booleanProperty(
                        LEGACY_MAP_SUBSCRIPT,
                        "Do not fail the query if map key is missing",
                        featuresConfig.isLegacyMapSubscript(),
                        true),
                booleanProperty(
                        ITERATIVE_OPTIMIZER,
                        "Experimental: enable iterative optimizer",
                        featuresConfig.isIterativeOptimizerEnabled(),
                        false),
                new PropertyMetadata<>(
                        ITERATIVE_OPTIMIZER_TIMEOUT,
                        "Timeout for plan optimization in iterative optimizer",
                        VARCHAR,
                        Duration.class,
                        featuresConfig.getIterativeOptimizerTimeout(),
                        false,
                        value -> Duration.valueOf((String) value),
                        Duration::toString),
                booleanProperty(
                        RUNTIME_OPTIMIZER_ENABLED,
                        "Experimental: enable runtime optimizer",
                        featuresConfig.isRuntimeOptimizerEnabled(),
                        false),
                booleanProperty(
                        EXCHANGE_COMPRESSION,
                        "Enable compression in exchanges",
                        featuresConfig.isExchangeCompressionEnabled(),
                        false),
                booleanProperty(
                        EXCHANGE_CHECKSUM,
                        "Enable checksum in exchanges",
                        featuresConfig.isExchangeChecksumEnabled(),
                        false),
                booleanProperty(
                        LEGACY_TIMESTAMP,
                        "Use legacy TIME & TIMESTAMP semantics (warning: this will be removed)",
                        featuresConfig.isLegacyTimestamp(),
                        true),
                booleanProperty(
                        ENABLE_INTERMEDIATE_AGGREGATIONS,
                        "Enable the use of intermediate aggregations",
                        featuresConfig.isEnableIntermediateAggregations(),
                        false),
                booleanProperty(
                        PUSH_AGGREGATION_THROUGH_JOIN,
                        "Allow pushing aggregations below joins",
                        featuresConfig.isPushAggregationThroughJoin(),
                        false),
                booleanProperty(
                        PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN,
                        "Push partial aggregations below joins",
                        false,
                        false),
                booleanProperty(
                        PARSE_DECIMAL_LITERALS_AS_DOUBLE,
                        "Parse decimal literals as DOUBLE instead of DECIMAL",
                        featuresConfig.isParseDecimalLiteralsAsDouble(),
                        false),
                booleanProperty(
                        FORCE_SINGLE_NODE_OUTPUT,
                        "Force single node output",
                        featuresConfig.isForceSingleNodeOutput(),
                        true),
                new PropertyMetadata<>(
                        FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_SIZE,
                        "Experimental: Minimum output page size for filter and project operators",
                        VARCHAR,
                        DataSize.class,
                        featuresConfig.getFilterAndProjectMinOutputPageSize(),
                        false,
                        value -> DataSize.valueOf((String) value),
                        DataSize::toString),
                integerProperty(
                        FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_ROW_COUNT,
                        "Experimental: Minimum output page row count for filter and project operators",
                        featuresConfig.getFilterAndProjectMinOutputPageRowCount(),
                        false),
                booleanProperty(
                        DISTRIBUTED_SORT,
                        "Parallelize sort across multiple nodes",
                        featuresConfig.isDistributedSortEnabled(),
                        false),
                booleanProperty(
                        USE_MARK_DISTINCT,
                        "Implement DISTINCT aggregations using MarkDistinct",
                        featuresConfig.isUseMarkDistinct(),
                        false),
                booleanProperty(
                        PREFER_PARTIAL_AGGREGATION,
                        "Prefer splitting aggregations into partial and final stages",
                        null,
                        false),
                new PropertyMetadata<>(
                        PARTIAL_AGGREGATION_STRATEGY,
                        format("Partial aggregation strategy to use. Options are %s",
                                Stream.of(PartialAggregationStrategy.values())
                                        .map(PartialAggregationStrategy::name)
                                        .collect(joining(","))),
                        VARCHAR,
                        PartialAggregationStrategy.class,
                        featuresConfig.getPartialAggregationStrategy(),
                        false,
                        value -> PartialAggregationStrategy.valueOf(((String) value).toUpperCase()),
                        PartialAggregationStrategy::name),
                doubleProperty(
                        PARTIAL_AGGREGATION_BYTE_REDUCTION_THRESHOLD,
                        "Byte reduction ratio threshold at which to disable partial aggregation",
                        featuresConfig.getPartialAggregationByteReductionThreshold(),
                        false),
                booleanProperty(
                        OPTIMIZE_TOP_N_ROW_NUMBER,
                        "Use top N row number optimization",
                        featuresConfig.isOptimizeTopNRowNumber(),
                        false),
                integerProperty(
                        MAX_GROUPING_SETS,
                        "Maximum number of grouping sets in a GROUP BY",
                        featuresConfig.getMaxGroupingSets(),
                        true),
                booleanProperty(
                        LEGACY_UNNEST,
                        "Using legacy unnest semantic, where unnest(array(row)) will create one column of type row",
                        featuresConfig.isLegacyUnnestArrayRows(),
                        false),
                booleanProperty(
                        STATISTICS_CPU_TIMER_ENABLED,
                        "Experimental: Enable cpu time tracking for automatic column statistics collection on write",
                        taskManagerConfig.isStatisticsCpuTimerEnabled(),
                        false),
                booleanProperty(
                        ENABLE_STATS_CALCULATOR,
                        "Experimental: Enable statistics calculator",
                        featuresConfig.isEnableStatsCalculator(),
                        false),
                booleanProperty(
                        ENABLE_STATS_COLLECTION_FOR_TEMPORARY_TABLE,
                        "Experimental: Enable statistics collection of temporary tables created for materialized exchange",
                        featuresConfig.isEnableStatsCollectionForTemporaryTable(),
                        false),
                integerProperty(
                        MAX_TASKS_PER_STAGE,
                        "Maximum number of tasks for a non source distributed stage",
                        taskManagerConfig.getMaxTasksPerStage(),
                        false),
                new PropertyMetadata<>(
                        MAX_DRIVERS_PER_TASK,
                        "Maximum number of drivers per task",
                        INTEGER,
                        Integer.class,
                        null,
                        false,
                        value -> min(taskManagerConfig.getMaxDriversPerTask(), validateNullablePositiveIntegerValue(value, MAX_DRIVERS_PER_TASK)),
                        object -> object),
                booleanProperty(
                        IGNORE_STATS_CALCULATOR_FAILURES,
                        "Ignore statistics calculator failures",
                        featuresConfig.isIgnoreStatsCalculatorFailures(),
                        false),
                booleanProperty(
                        PRINT_STATS_FOR_NON_JOIN_QUERY,
                        "Print stats and cost for non-join-query in plan",
                        featuresConfig.isPrintStatsForNonJoinQuery(),
                        false),
                booleanProperty(
                        DEFAULT_FILTER_FACTOR_ENABLED,
                        "use a default filter factor for unknown filters in a filter node",
                        featuresConfig.isDefaultFilterFactorEnabled(),
                        false),
                booleanProperty(
                        PUSH_LIMIT_THROUGH_OUTER_JOIN,
                        "push limits to the outer side of an outer join",
                        featuresConfig.isPushLimitThroughOuterJoin(),
                        false),
                integerProperty(
                        MAX_CONCURRENT_MATERIALIZATIONS,
                        "Maximum number of materializing plan sections that can run concurrently",
                        featuresConfig.getMaxConcurrentMaterializations(),
                        false),
                booleanProperty(
                        PUSHDOWN_SUBFIELDS_ENABLED,
                        "Experimental: enable subfield pruning",
                        featuresConfig.isPushdownSubfieldsEnabled(),
                        false),
                booleanProperty(
                        PUSHDOWN_DEREFERENCE_ENABLED,
                        "Experimental: enable dereference pushdown",
                        featuresConfig.isPushdownDereferenceEnabled(),
                        false),
                booleanProperty(
                        TABLE_WRITER_MERGE_OPERATOR_ENABLED,
                        "Experimental: enable table writer merge operator",
                        featuresConfig.isTableWriterMergeOperatorEnabled(),
                        false),
                new PropertyMetadata<>(
                        INDEX_LOADER_TIMEOUT,
                        "Timeout for loading indexes for index joins",
                        VARCHAR,
                        Duration.class,
                        featuresConfig.getIndexLoaderTimeout(),
                        false,
                        value -> Duration.valueOf((String) value),
                        Duration::toString),
                booleanProperty(
                        OPTIMIZED_REPARTITIONING_ENABLED,
                        "Experimental: Use optimized repartitioning",
                        featuresConfig.isOptimizedRepartitioningEnabled(),
                        false),
                new PropertyMetadata<>(
                        AGGREGATION_PARTITIONING_MERGING_STRATEGY,
                        format("Strategy to merge partition preference in aggregation node. Options are %s",
                                Stream.of(AggregationPartitioningMergingStrategy.values())
                                        .map(AggregationPartitioningMergingStrategy::name)
                                        .collect(joining(","))),
                        VARCHAR,
                        AggregationPartitioningMergingStrategy.class,
                        featuresConfig.getAggregationPartitioningMergingStrategy(),
                        false,
                        value -> AggregationPartitioningMergingStrategy.valueOf(((String) value).toUpperCase()),
                        AggregationPartitioningMergingStrategy::name),
                booleanProperty(
                        LIST_BUILT_IN_FUNCTIONS_ONLY,
                        "Only List built-in functions in SHOW FUNCTIONS",
                        featuresConfig.isListBuiltInFunctionsOnly(),
                        false),
                new PropertyMetadata<>(
                        PARTITIONING_PRECISION_STRATEGY,
                        format("The strategy to use to pick when to repartition. Options are %s",
                                Stream.of(PartitioningPrecisionStrategy.values())
                                        .map(PartitioningPrecisionStrategy::name)
                                        .collect(joining(","))),
                        VARCHAR,
                        PartitioningPrecisionStrategy.class,
                        featuresConfig.getPartitioningPrecisionStrategy(),
                        false,
                        value -> PartitioningPrecisionStrategy.valueOf(((String) value).toUpperCase()),
                        PartitioningPrecisionStrategy::name),
                booleanProperty(
                        EXPERIMENTAL_FUNCTIONS_ENABLED,
                        "Enable listing of functions marked as experimental",
                        featuresConfig.isExperimentalFunctionsEnabled(),
                        false),
                booleanProperty(
                        USE_LEGACY_SCHEDULER,
                        "Use version of scheduler before refactorings for section retries",
                        featuresConfig.isUseLegacyScheduler(),
                        false),
                booleanProperty(
                        OPTIMIZE_COMMON_SUB_EXPRESSIONS,
                        "Extract and compute common sub-expressions in projection",
                        featuresConfig.isOptimizeCommonSubExpressions(),
                        false),
                booleanProperty(
                        PREFER_DISTRIBUTED_UNION,
                        "Prefer distributed union",
                        featuresConfig.isPreferDistributedUnion(),
                        true),
                new PropertyMetadata<>(
                        WARNING_HANDLING,
                        format("The level of warning handling. Levels are %s",
                                Stream.of(WarningHandlingLevel.values())
                                        .map(WarningHandlingLevel::name)
                                        .collect(joining(","))),
                        VARCHAR,
                        WarningHandlingLevel.class,
                        warningCollectorConfig.getWarningHandlingLevel(),
                        false,
                        value -> WarningHandlingLevel.valueOf(((String) value).toUpperCase()),
                        WarningHandlingLevel::name),
                booleanProperty(
                        OPTIMIZE_NULLS_IN_JOINS,
                        "Filter nulls from inner side of join",
                        featuresConfig.isOptimizeNullsInJoin(),
                        false),
                new PropertyMetadata<>(
                        TARGET_RESULT_SIZE,
                        "Target result size for results being streamed from coordinator",
                        VARCHAR,
                        DataSize.class,
                        null,
                        false,
                        value -> value != null ? DataSize.valueOf((String) value) : null,
                        value -> value != null ? value.toString() : null),
                booleanProperty(
                        ENABLE_DYNAMIC_FILTERING,
                        "Experimental: Enable dynamic filtering",
                        featuresConfig.isEnableDynamicFiltering(),
                        false),
                integerProperty(
                        DYNAMIC_FILTERING_MAX_PER_DRIVER_ROW_COUNT,
                        "Experimental: maximum number of build-side rows to be collected for dynamic filtering per-driver",
                        featuresConfig.getDynamicFilteringMaxPerDriverRowCount(),
                        false),
                new PropertyMetadata<>(
                        DYNAMIC_FILTERING_MAX_PER_DRIVER_SIZE,
                        "Experimental: maximum number of bytes to be collected for dynamic filtering per-driver",
                        VARCHAR,
                        DataSize.class,
                        featuresConfig.getDynamicFilteringMaxPerDriverSize(),
                        false,
                        value -> DataSize.valueOf((String) value),
                        DataSize::toString),
                integerProperty(
                        DYNAMIC_FILTERING_RANGE_ROW_LIMIT_PER_DRIVER,
                        "Maximum number of build-side rows per driver up to which min and max values will be collected for dynamic filtering",
                        featuresConfig.getDynamicFilteringRangeRowLimitPerDriver(),
                        false),
                booleanProperty(
                        FRAGMENT_RESULT_CACHING_ENABLED,
                        "Enable fragment result caching and read/write leaf fragment result pages from/to cache when applicable",
                        featuresConfig.isFragmentResultCachingEnabled(),
                        false),
                booleanProperty(
                        LEGACY_TYPE_COERCION_WARNING_ENABLED,
                        "Enable warning for query relying on legacy type coercion",
                        featuresConfig.isLegacyDateTimestampToVarcharCoercion(),
                        true),
                booleanProperty(
                        SKIP_REDUNDANT_SORT,
                        "Skip redundant sort operations",
                        featuresConfig.isSkipRedundantSort(),
                        false),
                booleanProperty(
                        INLINE_SQL_FUNCTIONS,
                        "Inline SQL function definition at plan time",
                        featuresConfig.isInlineSqlFunctions(),
                        false),
                booleanProperty(
                        REMOTE_FUNCTIONS_ENABLED,
                        "Allow remote functions",
                        false,
                        false),
                booleanProperty(
                        CHECK_ACCESS_CONTROL_ON_UTILIZED_COLUMNS_ONLY,
                        "Apply access control rules on only those columns that are required to produce the query output",
                        featuresConfig.isCheckAccessControlOnUtilizedColumnsOnly(),
                        false),
                booleanProperty(
                        ALLOW_WINDOW_ORDER_BY_LITERALS,
                        "Allow ORDER BY literals in window functions",
                        featuresConfig.isAllowWindowOrderByLiterals(),
                        false),
                booleanProperty(
                        ENFORCE_FIXED_DISTRIBUTION_FOR_OUTPUT_OPERATOR,
                        "Enforce fixed distribution for output operator",
                        featuresConfig.isEnforceFixedDistributionForOutputOperator(),
                        true),
                new PropertyMetadata<>(
                        MAX_UNACKNOWLEDGED_SPLITS_PER_TASK,
                        "Maximum number of leaf splits awaiting delivery to a given task",
                        INTEGER,
                        Integer.class,
                        nodeSchedulerConfig.getMaxUnacknowledgedSplitsPerTask(),
                        false,
                        value -> validateIntegerValue(value, MAX_UNACKNOWLEDGED_SPLITS_PER_TASK, 1, false),
                        object -> object),
                booleanProperty(
                        OPTIMIZE_JOINS_WITH_EMPTY_SOURCES,
                        "Simplify joins with one or more empty sources",
                        featuresConfig.isEmptyJoinOptimization(),
                        false),
                booleanProperty(
                        SPOOLING_OUTPUT_BUFFER_ENABLED,
                        "Enable spooling output buffer for terminal task",
                        featuresConfig.isSpoolingOutputBufferEnabled(),
                        false),
                booleanProperty(
                        SPARK_ASSIGN_BUCKET_TO_PARTITION_FOR_PARTITIONED_TABLE_WRITE_ENABLED,
                        "Assign bucket to partition map for partitioned table write when adding an exchange",
                        featuresConfig.isPrestoSparkAssignBucketToPartitionForPartitionedTableWriteEnabled(),
                        true),
                booleanProperty(
                        LOG_FORMATTED_QUERY_ENABLED,
                        "Log formatted prepared query instead of raw query when enabled",
                        featuresConfig.isLogFormattedQueryEnabled(),
                        false),
                new PropertyMetadata<>(
                        QUERY_RETRY_LIMIT,
                        "Query retry limit due to communication failures",
                        INTEGER,
                        Integer.class,
                        queryManagerConfig.getPerQueryRetryLimit(),
                        true,
                        value -> validateIntegerValue(value, QUERY_RETRY_LIMIT, 0, false),
                        object -> object),
                new PropertyMetadata<>(
                        QUERY_RETRY_MAX_EXECUTION_TIME,
                        "Maximum execution time of a query allowed for retry",
                        VARCHAR,
                        Duration.class,
                        queryManagerConfig.getPerQueryRetryMaxExecutionTime(),
                        true,
                        value -> Duration.valueOf((String) value),
                        Duration::toString),
                booleanProperty(
                        PARTIAL_RESULTS_ENABLED,
                        "Enable returning partial results. Please note that queries might not read all the data when this is enabled",
                        featuresConfig.isPartialResultsEnabled(),
                        false),
                doubleProperty(
                        PARTIAL_RESULTS_COMPLETION_RATIO_THRESHOLD,
                        "Minimum query completion ratio threshold for partial results",
                        featuresConfig.getPartialResultsCompletionRatioThreshold(),
                        false),
                booleanProperty(
                        OFFSET_CLAUSE_ENABLED,
                        "Enable support for OFFSET clause",
                        featuresConfig.isOffsetClauseEnabled(),
                        true),
                doubleProperty(
                        PARTIAL_RESULTS_MAX_EXECUTION_TIME_MULTIPLIER,
                        "This value is multiplied by the time taken to reach the completion ratio threshold and is set as max task end time",
                        featuresConfig.getPartialResultsMaxExecutionTimeMultiplier(),
                        false),
                booleanProperty(
                        VERBOSE_EXCEEDED_MEMORY_LIMIT_ERRORS_ENABLED,
                        "When enabled the error message for exceeded memory limit errors will contain additional operator memory allocation details",
                        nodeMemoryConfig.isVerboseExceededMemoryLimitErrorsEnabled(),
                        false),
                booleanProperty(
                        MATERIALIZED_VIEW_DATA_CONSISTENCY_ENABLED,
                        "When enabled and reading from materialized view, partition stitching is applied to achieve data consistency",
                        featuresConfig.isMaterializedViewDataConsistencyEnabled(),
                        false),
                booleanProperty(
                        QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED,
                        "Enable query optimization with materialized view",
                        featuresConfig.isQueryOptimizationWithMaterializedViewEnabled(),
                        true),
                booleanProperty(
                        ENABLE_DISTRIBUTED_TRACING,
                        "Enable distributed tracing of the query",
                        tracingConfig.getEnableDistributedTracing(),
                        false),
                booleanProperty(
                        VERBOSE_RUNTIME_STATS_ENABLED,
                        "Enable logging all runtime stats",
                        featuresConfig.isVerboseRuntimeStatsEnabled(),
                        false),
                new PropertyMetadata<>(
                        AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY,
                        format("Set the strategy used to rewrite AGG IF to AGG FILTER. Options are %s",
                                Stream.of(AggregationIfToFilterRewriteStrategy.values())
                                        .map(AggregationIfToFilterRewriteStrategy::name)
                                        .collect(joining(","))),
                        VARCHAR,
                        AggregationIfToFilterRewriteStrategy.class,
                        featuresConfig.getAggregationIfToFilterRewriteStrategy(),
                        false,
                        value -> AggregationIfToFilterRewriteStrategy.valueOf(((String) value).toUpperCase()),
                        AggregationIfToFilterRewriteStrategy::name),
                booleanProperty(
                        PRESTISSIMO_SIMPLIFIED_EXPRESSION_EVALUATION_ENABLED,
                        "Enable simplified path in expression evaluation",
                        false,
                        false),
                new PropertyMetadata<>(
                        RESOURCE_AWARE_SCHEDULING_STRATEGY,
                        format("Task assignment strategy to use. Options are %s",
                                Stream.of(ResourceAwareSchedulingStrategy.values())
                                        .map(ResourceAwareSchedulingStrategy::name)
                                        .collect(joining(","))),
                        VARCHAR,
                        ResourceAwareSchedulingStrategy.class,
                        nodeSchedulerConfig.getResourceAwareSchedulingStrategy(),
                        false,
                        value -> ResourceAwareSchedulingStrategy.valueOf(((String) value).toUpperCase()),
                        ResourceAwareSchedulingStrategy::name),
                booleanProperty(
                        HEAP_DUMP_ON_EXCEEDED_MEMORY_LIMIT_ENABLED,
                        "Trigger heap dump to `EXCEEDED_MEMORY_LIMIT_HEAP_DUMP_FILE_PATH` on exceeded memory limit exceptions",
                        false, // This is intended to be used for debugging purposes only and thus we does not need an associated config property
                        true),
                stringProperty(
                        EXCEEDED_MEMORY_LIMIT_HEAP_DUMP_FILE_DIRECTORY,
                        "Directory to which heap snapshot will be dumped, if heap_dump_on_exceeded_memory_limit_enabled",
                        System.getProperty("java.io.tmpdir"),   // This is intended to be used for debugging purposes only and thus we does not need an associated config property
                        true),
                booleanProperty(
                        KEY_BASED_SAMPLING_ENABLED,
                        "Key based sampling of tables enabled",
                        false,
                        false),
                doubleProperty(
                        KEY_BASED_SAMPLING_PERCENTAGE,
                        "Percentage of keys to be sampled",
                        0.01,
                        false),
                stringProperty(
                        KEY_BASED_SAMPLING_FUNCTION,
                        "Sampling function for key based sampling",
                        "key_sampling_percent",
                        false));
    }

    public static boolean isEmptyJoinOptimization(Session session)
    {
        return session.getSystemProperty(OPTIMIZE_JOINS_WITH_EMPTY_SOURCES, Boolean.class);
    }

    public static boolean isSpoolingOutputBufferEnabled(Session session)
    {
        return session.getSystemProperty(SPOOLING_OUTPUT_BUFFER_ENABLED, Boolean.class);
    }

    public static boolean isSkipRedundantSort(Session session)
    {
        return session.getSystemProperty(SKIP_REDUNDANT_SORT, Boolean.class);
    }

    public static boolean isAllowWindowOrderByLiterals(Session session)
    {
        return session.getSystemProperty(ALLOW_WINDOW_ORDER_BY_LITERALS, Boolean.class);
    }

    public static boolean isKeyBasedSamplingEnabled(Session session)
    {
        return session.getSystemProperty(KEY_BASED_SAMPLING_ENABLED, Boolean.class);
    }

    public static double getKeyBasedSamplingPercentage(Session session)
    {
        return session.getSystemProperty(KEY_BASED_SAMPLING_PERCENTAGE, Double.class);
    }

    public static String getKeyBasedSamplingFunction(Session session)
    {
        return session.getSystemProperty(KEY_BASED_SAMPLING_FUNCTION, String.class);
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static String getExecutionPolicy(Session session)
    {
        return session.getSystemProperty(EXECUTION_POLICY, String.class);
    }

    public static boolean isOptimizeHashGenerationEnabled(Session session)
    {
        return session.getSystemProperty(OPTIMIZE_HASH_GENERATION, Boolean.class);
    }

    public static JoinDistributionType getJoinDistributionType(Session session)
    {
        // distributed_join takes precedence until we remove it
        Boolean distributedJoin = session.getSystemProperty(DISTRIBUTED_JOIN, Boolean.class);
        if (distributedJoin != null) {
            if (!distributedJoin) {
                return BROADCAST;
            }
            return PARTITIONED;
        }

        return session.getSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.class);
    }

    public static DataSize getJoinMaxBroadcastTableSize(Session session)
    {
        return session.getSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, DataSize.class);
    }

    public static boolean isDistributedIndexJoinEnabled(Session session)
    {
        return session.getSystemProperty(DISTRIBUTED_INDEX_JOIN, Boolean.class);
    }

    public static int getHashPartitionCount(Session session)
    {
        return session.getSystemProperty(HASH_PARTITION_COUNT, Integer.class);
    }

    public static String getPartitioningProviderCatalog(Session session)
    {
        return session.getSystemProperty(PARTITIONING_PROVIDER_CATALOG, String.class);
    }

    public static ExchangeMaterializationStrategy getExchangeMaterializationStrategy(Session session)
    {
        return session.getSystemProperty(EXCHANGE_MATERIALIZATION_STRATEGY, ExchangeMaterializationStrategy.class);
    }

    public static boolean isUseStreamingExchangeForMarkDistinctEnabled(Session session)
    {
        return session.getSystemProperty(USE_STREAMING_EXCHANGE_FOR_MARK_DISTINCT, Boolean.class);
    }

    public static boolean isGroupedExecutionEnabled(Session session)
    {
        return session.getSystemProperty(GROUPED_EXECUTION, Boolean.class);
    }

    public static boolean isRecoverableGroupedExecutionEnabled(Session session)
    {
        return session.getSystemProperty(RECOVERABLE_GROUPED_EXECUTION, Boolean.class);
    }

    public static double getMaxFailedTaskPercentage(Session session)
    {
        return session.getSystemProperty(MAX_FAILED_TASK_PERCENTAGE, Double.class);
    }

    public static int getMaxStageRetries(Session session)
    {
        return session.getSystemProperty(MAX_STAGE_RETRIES, Integer.class);
    }

    public static boolean preferStreamingOperators(Session session)
    {
        return session.getSystemProperty(PREFER_STREAMING_OPERATORS, Boolean.class);
    }

    public static int getTaskWriterCount(Session session)
    {
        return session.getSystemProperty(TASK_WRITER_COUNT, Integer.class);
    }

    public static int getTaskPartitionedWriterCount(Session session)
    {
        Integer partitionedWriterCount = session.getSystemProperty(TASK_PARTITIONED_WRITER_COUNT, Integer.class);
        if (partitionedWriterCount != null) {
            return partitionedWriterCount;
        }
        return getTaskWriterCount(session);
    }

    public static boolean isRedistributeWrites(Session session)
    {
        return session.getSystemProperty(REDISTRIBUTE_WRITES, Boolean.class);
    }

    public static boolean isScaleWriters(Session session)
    {
        return session.getSystemProperty(SCALE_WRITERS, Boolean.class);
    }

    public static DataSize getWriterMinSize(Session session)
    {
        return session.getSystemProperty(WRITER_MIN_SIZE, DataSize.class);
    }

    public static boolean isOptimizedScaleWriterProducerBuffer(Session session)
    {
        return session.getSystemProperty(OPTIMIZED_SCALE_WRITER_PRODUCER_BUFFER, Boolean.class);
    }

    public static boolean isPushTableWriteThroughUnion(Session session)
    {
        return session.getSystemProperty(PUSH_TABLE_WRITE_THROUGH_UNION, Boolean.class);
    }

    public static int getTaskConcurrency(Session session)
    {
        return session.getSystemProperty(TASK_CONCURRENCY, Integer.class);
    }

    public static boolean isShareIndexLoading(Session session)
    {
        return session.getSystemProperty(TASK_SHARE_INDEX_LOADING, Boolean.class);
    }

    public static boolean isDictionaryAggregationEnabled(Session session)
    {
        return session.getSystemProperty(DICTIONARY_AGGREGATION, Boolean.class);
    }

    public static boolean isOptimizeMetadataQueries(Session session)
    {
        return session.getSystemProperty(OPTIMIZE_METADATA_QUERIES, Boolean.class);
    }

    public static boolean isOptimizeMetadataQueriesIgnoreStats(Session session)
    {
        return session.getSystemProperty(OPTIMIZE_METADATA_QUERIES_IGNORE_STATS, Boolean.class);
    }

    public static DataSize getQueryMaxMemory(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_MEMORY, DataSize.class);
    }

    public static DataSize getQueryMaxMemoryPerNode(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_MEMORY_PER_NODE, DataSize.class);
    }

    public static DataSize getQueryMaxBroadcastMemory(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_BROADCAST_MEMORY, DataSize.class);
    }

    public static DataSize getQueryMaxTotalMemory(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_TOTAL_MEMORY, DataSize.class);
    }

    public static DataSize getQueryMaxTotalMemoryPerNode(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_TOTAL_MEMORY_PER_NODE, DataSize.class);
    }

    public static Duration getQueryMaxRunTime(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_RUN_TIME, Duration.class);
    }

    public static Duration getQueryMaxExecutionTime(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_EXECUTION_TIME, Duration.class);
    }

    public static boolean resourceOvercommit(Session session)
    {
        return session.getSystemProperty(RESOURCE_OVERCOMMIT, Boolean.class);
    }

    public static int getQueryMaxStageCount(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_STAGE_COUNT, Integer.class);
    }

    public static boolean planWithTableNodePartitioning(Session session)
    {
        return session.getSystemProperty(PLAN_WITH_TABLE_NODE_PARTITIONING, Boolean.class);
    }

    public static boolean isFastInequalityJoin(Session session)
    {
        return session.getSystemProperty(FAST_INEQUALITY_JOINS, Boolean.class);
    }

    public static JoinReorderingStrategy getJoinReorderingStrategy(Session session)
    {
        Boolean reorderJoins = session.getSystemProperty(REORDER_JOINS, Boolean.class);
        if (reorderJoins != null) {
            if (!reorderJoins) {
                return NONE;
            }
            return ELIMINATE_CROSS_JOINS;
        }
        return session.getSystemProperty(JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.class);
    }

    public static PartialMergePushdownStrategy getPartialMergePushdownStrategy(Session session)
    {
        return session.getSystemProperty(PARTIAL_MERGE_PUSHDOWN_STRATEGY, PartialMergePushdownStrategy.class);
    }

    public static int getMaxReorderedJoins(Session session)
    {
        return session.getSystemProperty(MAX_REORDERED_JOINS, Integer.class);
    }

    public static boolean isColocatedJoinEnabled(Session session)
    {
        return session.getSystemProperty(COLOCATED_JOIN, Boolean.class);
    }

    public static boolean isSpatialJoinEnabled(Session session)
    {
        return session.getSystemProperty(SPATIAL_JOIN, Boolean.class);
    }

    public static Optional<String> getSpatialPartitioningTableName(Session session)
    {
        return Optional.ofNullable(session.getSystemProperty(SPATIAL_PARTITIONING_TABLE_NAME, String.class));
    }

    public static OptionalInt getConcurrentLifespansPerNode(Session session)
    {
        Integer result = session.getSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, Integer.class);
        if (result == 0) {
            return OptionalInt.empty();
        }
        else {
            checkArgument(result > 0, "Concurrent lifespans per node must be positive if set to non-zero");
            return OptionalInt.of(result);
        }
    }

    public static int getInitialSplitsPerNode(Session session)
    {
        return session.getSystemProperty(INITIAL_SPLITS_PER_NODE, Integer.class);
    }

    public static int getQueryPriority(Session session)
    {
        Integer priority = session.getSystemProperty(QUERY_PRIORITY, Integer.class);
        checkArgument(priority > 0, "Query priority must be positive");
        return priority;
    }

    public static Duration getSplitConcurrencyAdjustmentInterval(Session session)
    {
        return session.getSystemProperty(SPLIT_CONCURRENCY_ADJUSTMENT_INTERVAL, Duration.class);
    }

    public static Duration getQueryMaxCpuTime(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_CPU_TIME, Duration.class);
    }

    public static DataSize getQueryMaxScanRawInputBytes(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_SCAN_RAW_INPUT_BYTES, DataSize.class);
    }

    public static DataSize getQueryMaxOutputSize(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_OUTPUT_SIZE, DataSize.class);
    }

    public static boolean isSpillEnabled(Session session)
    {
        return session.getSystemProperty(SPILL_ENABLED, Boolean.class);
    }

    public static boolean isJoinSpillingEnabled(Session session)
    {
        return session.getSystemProperty(JOIN_SPILL_ENABLED, Boolean.class) && isSpillEnabled(session);
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

    public static DataSize getQueryMaxRevocableMemoryPerNode(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_REVOCABLE_MEMORY_PER_NODE, DataSize.class);
    }

    public static DataSize getTempStorageSpillerBufferSize(Session session)
    {
        DataSize tempStorageSpillerBufferSize = session.getSystemProperty(TEMP_STORAGE_SPILLER_BUFFER_SIZE, DataSize.class);
        checkArgument(tempStorageSpillerBufferSize.toBytes() >= 0, "%s must be positive", TEMP_STORAGE_SPILLER_BUFFER_SIZE);
        return tempStorageSpillerBufferSize;
    }

    public static boolean isOptimizeDistinctAggregationEnabled(Session session)
    {
        return session.getSystemProperty(OPTIMIZE_DISTINCT_AGGREGATIONS, Boolean.class);
    }

    public static boolean isLegacyRowFieldOrdinalAccessEnabled(Session session)
    {
        return session.getSystemProperty(LEGACY_ROW_FIELD_ORDINAL_ACCESS, Boolean.class);
    }

    public static boolean isLegacyMapSubscript(Session session)
    {
        return session.getSystemProperty(LEGACY_MAP_SUBSCRIPT, Boolean.class);
    }

    public static boolean isNewOptimizerEnabled(Session session)
    {
        return session.getSystemProperty(ITERATIVE_OPTIMIZER, Boolean.class);
    }

    public static boolean isRuntimeOptimizerEnabled(Session session)
    {
        return session.getSystemProperty(RUNTIME_OPTIMIZER_ENABLED, Boolean.class);
    }

    @Deprecated
    public static boolean isLegacyTimestamp(Session session)
    {
        return session.getSystemProperty(LEGACY_TIMESTAMP, Boolean.class);
    }

    public static Duration getOptimizerTimeout(Session session)
    {
        return session.getSystemProperty(ITERATIVE_OPTIMIZER_TIMEOUT, Duration.class);
    }

    public static boolean isExchangeCompressionEnabled(Session session)
    {
        return session.getSystemProperty(EXCHANGE_COMPRESSION, Boolean.class);
    }

    public static boolean isExchangeChecksumEnabled(Session session)
    {
        return session.getSystemProperty(EXCHANGE_CHECKSUM, Boolean.class);
    }

    public static boolean isEnableIntermediateAggregations(Session session)
    {
        return session.getSystemProperty(ENABLE_INTERMEDIATE_AGGREGATIONS, Boolean.class);
    }

    public static boolean shouldPushAggregationThroughJoin(Session session)
    {
        return session.getSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, Boolean.class);
    }

    public static boolean isPushAggregationThroughJoin(Session session)
    {
        return session.getSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, Boolean.class);
    }

    public static boolean isParseDecimalLiteralsAsDouble(Session session)
    {
        return session.getSystemProperty(PARSE_DECIMAL_LITERALS_AS_DOUBLE, Boolean.class);
    }

    public static boolean isForceSingleNodeOutput(Session session)
    {
        return session.getSystemProperty(FORCE_SINGLE_NODE_OUTPUT, Boolean.class);
    }

    public static DataSize getFilterAndProjectMinOutputPageSize(Session session)
    {
        return session.getSystemProperty(FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_SIZE, DataSize.class);
    }

    public static int getFilterAndProjectMinOutputPageRowCount(Session session)
    {
        return session.getSystemProperty(FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_ROW_COUNT, Integer.class);
    }

    public static boolean useMarkDistinct(Session session)
    {
        return session.getSystemProperty(USE_MARK_DISTINCT, Boolean.class);
    }

    public static PartialAggregationStrategy getPartialAggregationStrategy(Session session)
    {
        Boolean preferPartialAggregation = session.getSystemProperty(PREFER_PARTIAL_AGGREGATION, Boolean.class);
        if (preferPartialAggregation != null) {
            if (preferPartialAggregation) {
                return ALWAYS;
            }
            return NEVER;
        }
        return session.getSystemProperty(PARTIAL_AGGREGATION_STRATEGY, PartialAggregationStrategy.class);
    }

    public static double getPartialAggregationByteReductionThreshold(Session session)
    {
        return session.getSystemProperty(PARTIAL_AGGREGATION_BYTE_REDUCTION_THRESHOLD, Double.class);
    }

    public static boolean isOptimizeTopNRowNumber(Session session)
    {
        return session.getSystemProperty(OPTIMIZE_TOP_N_ROW_NUMBER, Boolean.class);
    }

    public static boolean isDistributedSortEnabled(Session session)
    {
        return session.getSystemProperty(DISTRIBUTED_SORT, Boolean.class);
    }

    public static int getMaxGroupingSets(Session session)
    {
        return session.getSystemProperty(MAX_GROUPING_SETS, Integer.class);
    }

    public static boolean isLegacyUnnest(Session session)
    {
        return session.getSystemProperty(LEGACY_UNNEST, Boolean.class);
    }

    public static OptionalInt getMaxDriversPerTask(Session session)
    {
        Integer value = session.getSystemProperty(MAX_DRIVERS_PER_TASK, Integer.class);
        if (value == null) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(value);
    }

    public static int getMaxTasksPerStage(Session session)
    {
        return session.getSystemProperty(MAX_TASKS_PER_STAGE, Integer.class);
    }

    private static Integer validateValueIsPowerOfTwo(Object value, String property)
    {
        Number number = (Number) value;
        if (number == null) {
            return null;
        }
        int intValue = number.intValue();
        if (Integer.bitCount(intValue) != 1) {
            throw new PrestoException(
                    INVALID_SESSION_PROPERTY,
                    format("%s must be a power of 2: %s", property, intValue));
        }
        return intValue;
    }

    private static Integer validateNullablePositiveIntegerValue(Object value, String property)
    {
        return validateIntegerValue(value, property, 1, true);
    }

    private static Integer validateIntegerValue(Object value, String property, int lowerBoundIncluded, boolean allowNull)
    {
        if (value == null && !allowNull) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, format("%s must be non-null", property));
        }

        if (value == null) {
            return null;
        }

        int intValue = ((Number) value).intValue();
        if (intValue < lowerBoundIncluded) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, format("%s must be equal or greater than %s", property, lowerBoundIncluded));
        }
        return intValue;
    }

    public static boolean isStatisticsCpuTimerEnabled(Session session)
    {
        return session.getSystemProperty(STATISTICS_CPU_TIMER_ENABLED, Boolean.class);
    }

    public static boolean isEnableStatsCalculator(Session session)
    {
        return session.getSystemProperty(ENABLE_STATS_CALCULATOR, Boolean.class);
    }

    public static boolean isEnableStatsCollectionForTemporaryTable(Session session)
    {
        return session.getSystemProperty(ENABLE_STATS_COLLECTION_FOR_TEMPORARY_TABLE, Boolean.class);
    }

    public static boolean isIgnoreStatsCalculatorFailures(Session session)
    {
        return session.getSystemProperty(IGNORE_STATS_CALCULATOR_FAILURES, Boolean.class);
    }

    public static boolean isPrintStatsForNonJoinQuery(Session session)
    {
        return session.getSystemProperty(PRINT_STATS_FOR_NON_JOIN_QUERY, Boolean.class);
    }

    public static boolean isDefaultFilterFactorEnabled(Session session)
    {
        return session.getSystemProperty(DEFAULT_FILTER_FACTOR_ENABLED, Boolean.class);
    }

    public static boolean isPushLimitThroughOuterJoin(Session session)
    {
        return session.getSystemProperty(PUSH_LIMIT_THROUGH_OUTER_JOIN, Boolean.class);
    }

    public static int getMaxConcurrentMaterializations(Session session)
    {
        return session.getSystemProperty(MAX_CONCURRENT_MATERIALIZATIONS, Integer.class);
    }

    public static boolean isPushdownSubfieldsEnabled(Session session)
    {
        return session.getSystemProperty(PUSHDOWN_SUBFIELDS_ENABLED, Boolean.class);
    }

    public static boolean isPushdownDereferenceEnabled(Session session)
    {
        return session.getSystemProperty(PUSHDOWN_DEREFERENCE_ENABLED, Boolean.class);
    }

    public static boolean isTableWriterMergeOperatorEnabled(Session session)
    {
        return session.getSystemProperty(TABLE_WRITER_MERGE_OPERATOR_ENABLED, Boolean.class);
    }

    public static Duration getIndexLoaderTimeout(Session session)
    {
        return session.getSystemProperty(INDEX_LOADER_TIMEOUT, Duration.class);
    }

    public static boolean isOptimizedRepartitioningEnabled(Session session)
    {
        return session.getSystemProperty(OPTIMIZED_REPARTITIONING_ENABLED, Boolean.class);
    }

    public static AggregationPartitioningMergingStrategy getAggregationPartitioningMergingStrategy(Session session)
    {
        return session.getSystemProperty(AGGREGATION_PARTITIONING_MERGING_STRATEGY, AggregationPartitioningMergingStrategy.class);
    }

    public static boolean isListBuiltInFunctionsOnly(Session session)
    {
        return session.getSystemProperty(LIST_BUILT_IN_FUNCTIONS_ONLY, Boolean.class);
    }

    public static boolean isExactPartitioningPreferred(Session session)
    {
        return session.getSystemProperty(PARTITIONING_PRECISION_STRATEGY, PartitioningPrecisionStrategy.class)
                == PartitioningPrecisionStrategy.PREFER_EXACT_PARTITIONING;
    }

    public static boolean isExperimentalFunctionsEnabled(Session session)
    {
        return session.getSystemProperty(EXPERIMENTAL_FUNCTIONS_ENABLED, Boolean.class);
    }

    public static boolean isUseLegacyScheduler(Session session)
    {
        return session.getSystemProperty(USE_LEGACY_SCHEDULER, Boolean.class);
    }

    public static boolean isOptimizeCommonSubExpressions(Session session)
    {
        return session.getSystemProperty(OPTIMIZE_COMMON_SUB_EXPRESSIONS, Boolean.class);
    }

    public static boolean isPreferDistributedUnion(Session session)
    {
        return session.getSystemProperty(PREFER_DISTRIBUTED_UNION, Boolean.class);
    }

    public static WarningHandlingLevel getWarningHandlingLevel(Session session)
    {
        return session.getSystemProperty(WARNING_HANDLING, WarningHandlingLevel.class);
    }

    public static boolean isOptimizeNullsInJoin(Session session)
    {
        return session.getSystemProperty(OPTIMIZE_NULLS_IN_JOINS, Boolean.class);
    }

    public static Optional<DataSize> getTargetResultSize(Session session)
    {
        return Optional.ofNullable(session.getSystemProperty(TARGET_RESULT_SIZE, DataSize.class));
    }

    public static boolean isEnableDynamicFiltering(Session session)
    {
        return session.getSystemProperty(ENABLE_DYNAMIC_FILTERING, Boolean.class);
    }

    public static int getDynamicFilteringMaxPerDriverRowCount(Session session)
    {
        return session.getSystemProperty(DYNAMIC_FILTERING_MAX_PER_DRIVER_ROW_COUNT, Integer.class);
    }

    public static DataSize getDynamicFilteringMaxPerDriverSize(Session session)
    {
        return session.getSystemProperty(DYNAMIC_FILTERING_MAX_PER_DRIVER_SIZE, DataSize.class);
    }

    public static int getDynamicFilteringRangeRowLimitPerDriver(Session session)
    {
        return session.getSystemProperty(DYNAMIC_FILTERING_RANGE_ROW_LIMIT_PER_DRIVER, Integer.class);
    }

    public static boolean isFragmentResultCachingEnabled(Session session)
    {
        return session.getSystemProperty(FRAGMENT_RESULT_CACHING_ENABLED, Boolean.class);
    }

    public static boolean isLegacyTypeCoercionWarningEnabled(Session session)
    {
        return session.getSystemProperty(LEGACY_TYPE_COERCION_WARNING_ENABLED, Boolean.class);
    }

    public static boolean isInlineSqlFunctions(Session session)
    {
        return session.getSystemProperty(INLINE_SQL_FUNCTIONS, Boolean.class);
    }

    public static boolean isRemoteFunctionsEnabled(Session session)
    {
        return session.getSystemProperty(REMOTE_FUNCTIONS_ENABLED, Boolean.class);
    }

    public static boolean isCheckAccessControlOnUtilizedColumnsOnly(Session session)
    {
        return session.getSystemProperty(CHECK_ACCESS_CONTROL_ON_UTILIZED_COLUMNS_ONLY, Boolean.class);
    }

    public static boolean isEnforceFixedDistributionForOutputOperator(Session session)
    {
        return session.getSystemProperty(ENFORCE_FIXED_DISTRIBUTION_FOR_OUTPUT_OPERATOR, Boolean.class);
    }

    public static int getMaxUnacknowledgedSplitsPerTask(Session session)
    {
        return session.getSystemProperty(MAX_UNACKNOWLEDGED_SPLITS_PER_TASK, Integer.class);
    }

    public static boolean isPrestoSparkAssignBucketToPartitionForPartitionedTableWriteEnabled(Session session)
    {
        return session.getSystemProperty(SPARK_ASSIGN_BUCKET_TO_PARTITION_FOR_PARTITIONED_TABLE_WRITE_ENABLED, Boolean.class);
    }

    public static boolean isLogFormattedQueryEnabled(Session session)
    {
        return session.getSystemProperty(LOG_FORMATTED_QUERY_ENABLED, Boolean.class);
    }

    public static int getQueryRetryLimit(Session session)
    {
        return session.getSystemProperty(QUERY_RETRY_LIMIT, Integer.class);
    }

    public static Duration getQueryRetryMaxExecutionTime(Session session)
    {
        return session.getSystemProperty(QUERY_RETRY_MAX_EXECUTION_TIME, Duration.class);
    }

    public static boolean isPartialResultsEnabled(Session session)
    {
        return session.getSystemProperty(PARTIAL_RESULTS_ENABLED, Boolean.class);
    }

    public static double getPartialResultsCompletionRatioThreshold(Session session)
    {
        return session.getSystemProperty(PARTIAL_RESULTS_COMPLETION_RATIO_THRESHOLD, Double.class);
    }

    public static double getPartialResultsMaxExecutionTimeMultiplier(Session session)
    {
        return session.getSystemProperty(PARTIAL_RESULTS_MAX_EXECUTION_TIME_MULTIPLIER, Double.class);
    }

    public static boolean isOffsetClauseEnabled(Session session)
    {
        return session.getSystemProperty(OFFSET_CLAUSE_ENABLED, Boolean.class);
    }

    public static boolean isVerboseExceededMemoryLimitErrorsEnabled(Session session)
    {
        return session.getSystemProperty(VERBOSE_EXCEEDED_MEMORY_LIMIT_ERRORS_ENABLED, Boolean.class);
    }

    public static boolean isMaterializedViewDataConsistencyEnabled(Session session)
    {
        return session.getSystemProperty(MATERIALIZED_VIEW_DATA_CONSISTENCY_ENABLED, Boolean.class);
    }

    public static boolean isQueryOptimizationWithMaterializedViewEnabled(Session session)
    {
        return session.getSystemProperty(QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED, Boolean.class);
    }

    public static boolean isVerboseRuntimeStatsEnabled(Session session)
    {
        return session.getSystemProperty(VERBOSE_RUNTIME_STATS_ENABLED, Boolean.class);
    }

    public static AggregationIfToFilterRewriteStrategy getAggregationIfToFilterRewriteStrategy(Session session)
    {
        return session.getSystemProperty(AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY, AggregationIfToFilterRewriteStrategy.class);
    }

    public static ResourceAwareSchedulingStrategy getResourceAwareSchedulingStrategy(Session session)
    {
        return session.getSystemProperty(RESOURCE_AWARE_SCHEDULING_STRATEGY, ResourceAwareSchedulingStrategy.class);
    }

    public static Boolean isHeapDumpOnExceededMemoryLimitEnabled(Session session)
    {
        return session.getSystemProperty(HEAP_DUMP_ON_EXCEEDED_MEMORY_LIMIT_ENABLED, Boolean.class);
    }

    public static String getHeapDumpFileDirectory(Session session)
    {
        return session.getSystemProperty(EXCEEDED_MEMORY_LIMIT_HEAP_DUMP_FILE_DIRECTORY, String.class);
    }
}
