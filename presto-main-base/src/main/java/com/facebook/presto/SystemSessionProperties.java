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

import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.common.WarningHandlingLevel;
import com.facebook.presto.common.plan.PlanCanonicalizationStrategy;
import com.facebook.presto.cost.HistoryBasedOptimizationConfig;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.QueryManagerConfig.ExchangeMaterializationStrategy;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig.ResourceAwareSchedulingStrategy;
import com.facebook.presto.execution.warnings.WarningCollectorConfig;
import com.facebook.presto.memory.MemoryManagerConfig;
import com.facebook.presto.memory.NodeMemoryConfig;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.eventlistener.CTEInformation;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spiller.NodeSpillConfig;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.FeaturesConfig.AggregationIfToFilterRewriteStrategy;
import com.facebook.presto.sql.analyzer.FeaturesConfig.AggregationPartitioningMergingStrategy;
import com.facebook.presto.sql.analyzer.FeaturesConfig.CteMaterializationStrategy;
import com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType;
import com.facebook.presto.sql.analyzer.FeaturesConfig.JoinNotNullInferenceStrategy;
import com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy;
import com.facebook.presto.sql.analyzer.FeaturesConfig.LeftJoinArrayContainsToInnerJoinStrategy;
import com.facebook.presto.sql.analyzer.FeaturesConfig.PartialAggregationStrategy;
import com.facebook.presto.sql.analyzer.FeaturesConfig.PartialMergePushdownStrategy;
import com.facebook.presto.sql.analyzer.FeaturesConfig.PartitioningPrecisionStrategy;
import com.facebook.presto.sql.analyzer.FeaturesConfig.PushDownFilterThroughCrossJoinStrategy;
import com.facebook.presto.sql.analyzer.FeaturesConfig.RandomizeOuterJoinNullKeyStrategy;
import com.facebook.presto.sql.analyzer.FeaturesConfig.ShardedJoinStrategy;
import com.facebook.presto.sql.analyzer.FeaturesConfig.SingleStreamSpillerChoice;
import com.facebook.presto.sql.analyzer.FunctionsConfig;
import com.facebook.presto.sql.planner.CompilerConfig;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.tracing.TracingConfig;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import jakarta.inject.Inject;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.dataSizeProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.doubleProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.longProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.BROADCAST;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.PARTITIONED;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.ELIMINATE_CROSS_JOINS;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.PartialAggregationStrategy.ALWAYS;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.PartialAggregationStrategy.NEVER;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Boolean.TRUE;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public final class SystemSessionProperties
{
    public static final String OPTIMIZE_HASH_GENERATION = "optimize_hash_generation";
    public static final String JOIN_DISTRIBUTION_TYPE = "join_distribution_type";
    public static final String JOIN_MAX_BROADCAST_TABLE_SIZE = "join_max_broadcast_table_size";
    public static final String RETRY_QUERY_WITH_HISTORY_BASED_OPTIMIZATION = "retry_query_with_history_based_optimization";
    public static final String SIZE_BASED_JOIN_DISTRIBUTION_TYPE = "size_based_join_distribution_type";
    public static final String DISTRIBUTED_JOIN = "distributed_join";
    public static final String DISTRIBUTED_INDEX_JOIN = "distributed_index_join";
    public static final String HASH_PARTITION_COUNT = "hash_partition_count";
    public static final String CTE_HEURISTIC_REPLICATION_THRESHOLD = "cte_heuristic_replication_threshold";

    public static final String PARTITIONING_PROVIDER_CATALOG = "partitioning_provider_catalog";

    public static final String CTE_PARTITIONING_PROVIDER_CATALOG = "cte_partitioning_provider_catalog";
    public static final String EXCHANGE_MATERIALIZATION_STRATEGY = "exchange_materialization_strategy";
    public static final String USE_STREAMING_EXCHANGE_FOR_MARK_DISTINCT = "use_stream_exchange_for_mark_distinct";
    public static final String GROUPED_EXECUTION = "grouped_execution";
    public static final String RECOVERABLE_GROUPED_EXECUTION = "recoverable_grouped_execution";
    public static final String MAX_FAILED_TASK_PERCENTAGE = "max_failed_task_percentage";
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
    public static final String QUERY_MAX_QUEUED_TIME = "query_max_queued_time";
    public static final String QUERY_MAX_EXECUTION_TIME = "query_max_execution_time";
    public static final String QUERY_MAX_RUN_TIME = "query_max_run_time";
    public static final String RESOURCE_OVERCOMMIT = "resource_overcommit";
    public static final String QUERY_MAX_CPU_TIME = "query_max_cpu_time";
    public static final String QUERY_MAX_SCAN_RAW_INPUT_BYTES = "query_max_scan_raw_input_bytes";

    public static final String QUERY_MAX_WRITTEN_INTERMEDIATE_BYTES = "query_max_written_intermediate_bytes";
    public static final String QUERY_MAX_OUTPUT_POSITIONS = "query_max_output_positions";
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
    public static final String OPTIMIZE_METADATA_QUERIES_CALL_THRESHOLD = "optimize_metadata_queries_call_threshold";
    public static final String FAST_INEQUALITY_JOINS = "fast_inequality_joins";
    public static final String QUERY_PRIORITY = "query_priority";
    public static final String CONFIDENCE_BASED_BROADCAST_ENABLED = "confidence_based_broadcast_enabled";
    public static final String TREAT_LOW_CONFIDENCE_ZERO_ESTIMATION_AS_UNKNOWN_ENABLED = "treat_low_confidence_zero_estimation_unknown_enabled";
    public static final String SPILL_ENABLED = "spill_enabled";
    public static final String JOIN_SPILL_ENABLED = "join_spill_enabled";
    public static final String QUERY_MAX_REVOCABLE_MEMORY_PER_NODE = "query_max_revocable_memory_per_node";
    public static final String OPTIMIZE_DISTINCT_AGGREGATIONS = "optimize_mixed_distinct_aggregations";
    public static final String LEGACY_ROW_FIELD_ORDINAL_ACCESS = "legacy_row_field_ordinal_access";
    public static final String LEGACY_MAP_SUBSCRIPT = "do_not_use_legacy_map_subscript";
    public static final String ITERATIVE_OPTIMIZER = "iterative_optimizer_enabled";
    public static final String ITERATIVE_OPTIMIZER_TIMEOUT = "iterative_optimizer_timeout";
    public static final String QUERY_ANALYZER_TIMEOUT = "query_analyzer_timeout";
    public static final String RUNTIME_OPTIMIZER_ENABLED = "runtime_optimizer_enabled";
    public static final String EXCHANGE_COMPRESSION_CODEC = "exchange_compression_codec";
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
    public static final String EXPLOIT_CONSTRAINTS = "exploit_constraints";
    public static final String PREFER_PARTIAL_AGGREGATION = "prefer_partial_aggregation";
    public static final String PARTIAL_AGGREGATION_STRATEGY = "partial_aggregation_strategy";
    public static final String PARTIAL_AGGREGATION_BYTE_REDUCTION_THRESHOLD = "partial_aggregation_byte_reduction_threshold";
    public static final String ADAPTIVE_PARTIAL_AGGREGATION = "adaptive_partial_aggregation";
    public static final String ADAPTIVE_PARTIAL_AGGREGATION_ROWS_REDUCTION_RATIO_THRESHOLD = "adaptive_partial_aggregation_unique_rows_ratio_threshold";
    public static final String OPTIMIZE_TOP_N_ROW_NUMBER = "optimize_top_n_row_number";
    public static final String OPTIMIZE_CASE_EXPRESSION_PREDICATE = "optimize_case_expression_predicate";
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
    public static final String CTE_MATERIALIZATION_STRATEGY = "cte_materialization_strategy";
    public static final String CTE_FILTER_AND_PROJECTION_PUSHDOWN_ENABLED = "cte_filter_and_projection_pushdown_enabled";
    public static final String DEFAULT_JOIN_SELECTIVITY_COEFFICIENT = "default_join_selectivity_coefficient";
    public static final String DEFAULT_WRITER_REPLICATION_COEFFICIENT = "default_writer_replication_coefficient";
    public static final String PUSH_LIMIT_THROUGH_OUTER_JOIN = "push_limit_through_outer_join";
    public static final String OPTIMIZE_CONSTANT_GROUPING_KEYS = "optimize_constant_grouping_keys";
    public static final String MAX_CONCURRENT_MATERIALIZATIONS = "max_concurrent_materializations";
    public static final String PUSHDOWN_SUBFIELDS_ENABLED = "pushdown_subfields_enabled";
    public static final String PUSHDOWN_SUBFIELDS_FROM_LAMBDA_ENABLED = "pushdown_subfields_from_lambda_enabled";
    public static final String INDEX_LOADER_TIMEOUT = "index_loader_timeout";
    public static final String OPTIMIZED_REPARTITIONING_ENABLED = "optimized_repartitioning";
    public static final String AGGREGATION_PARTITIONING_MERGING_STRATEGY = "aggregation_partitioning_merging_strategy";
    public static final String LIST_BUILT_IN_FUNCTIONS_ONLY = "list_built_in_functions_only";
    public static final String PARTITIONING_PRECISION_STRATEGY = "partitioning_precision_strategy";
    public static final String EXPERIMENTAL_FUNCTIONS_ENABLED = "experimental_functions_enabled";
    public static final String OPTIMIZE_COMMON_SUB_EXPRESSIONS = "optimize_common_sub_expressions";
    public static final String PREFER_DISTRIBUTED_UNION = "prefer_distributed_union";
    public static final String WARNING_HANDLING = "warning_handling";
    public static final String OPTIMIZE_NULLS_IN_JOINS = "optimize_nulls_in_join";
    public static final String OPTIMIZE_PAYLOAD_JOINS = "optimize_payload_joins";
    public static final String TARGET_RESULT_SIZE = "target_result_size";
    public static final String PUSHDOWN_DEREFERENCE_ENABLED = "pushdown_dereference_enabled";
    public static final String ENABLE_DYNAMIC_FILTERING = "enable_dynamic_filtering";
    public static final String DYNAMIC_FILTERING_MAX_PER_DRIVER_ROW_COUNT = "dynamic_filtering_max_per_driver_row_count";
    public static final String DYNAMIC_FILTERING_MAX_PER_DRIVER_SIZE = "dynamic_filtering_max_per_driver_size";
    public static final String DYNAMIC_FILTERING_RANGE_ROW_LIMIT_PER_DRIVER = "dynamic_filtering_range_row_limit_per_driver";
    public static final String FRAGMENT_RESULT_CACHING_ENABLED = "fragment_result_caching_enabled";
    public static final String INLINE_SQL_FUNCTIONS = "inline_sql_functions";
    public static final String REMOTE_FUNCTIONS_ENABLED = "remote_functions_enabled";
    public static final String CHECK_ACCESS_CONTROL_ON_UTILIZED_COLUMNS_ONLY = "check_access_control_on_utilized_columns_only";
    public static final String CHECK_ACCESS_CONTROL_WITH_SUBFIELDS = "check_access_control_with_subfields";
    public static final String SKIP_REDUNDANT_SORT = "skip_redundant_sort";
    public static final String ALLOW_WINDOW_ORDER_BY_LITERALS = "allow_window_order_by_literals";
    public static final String ENFORCE_FIXED_DISTRIBUTION_FOR_OUTPUT_OPERATOR = "enforce_fixed_distribution_for_output_operator";
    public static final String MAX_UNACKNOWLEDGED_SPLITS_PER_TASK = "max_unacknowledged_splits_per_task";
    public static final String OPTIMIZE_JOINS_WITH_EMPTY_SOURCES = "optimize_joins_with_empty_sources";
    public static final String SPOOLING_OUTPUT_BUFFER_ENABLED = "spooling_output_buffer_enabled";
    public static final String SPARK_ASSIGN_BUCKET_TO_PARTITION_FOR_PARTITIONED_TABLE_WRITE_ENABLED = "spark_assign_bucket_to_partition_for_partitioned_table_write_enabled";
    public static final String LOG_FORMATTED_QUERY_ENABLED = "log_formatted_query_enabled";
    public static final String LOG_INVOKED_FUNCTION_NAMES_ENABLED = "log_invoked_function_names_enabled";
    public static final String QUERY_RETRY_LIMIT = "query_retry_limit";
    public static final String QUERY_RETRY_MAX_EXECUTION_TIME = "query_retry_max_execution_time";
    public static final String PARTIAL_RESULTS_ENABLED = "partial_results_enabled";
    public static final String PARTIAL_RESULTS_COMPLETION_RATIO_THRESHOLD = "partial_results_completion_ratio_threshold";
    public static final String ENHANCED_CTE_SCHEDULING_ENABLED = "enhanced_cte_scheduling_enabled";
    public static final String PARTIAL_RESULTS_MAX_EXECUTION_TIME_MULTIPLIER = "partial_results_max_execution_time_multiplier";
    public static final String OFFSET_CLAUSE_ENABLED = "offset_clause_enabled";
    public static final String VERBOSE_EXCEEDED_MEMORY_LIMIT_ERRORS_ENABLED = "verbose_exceeded_memory_limit_errors_enabled";
    public static final String MATERIALIZED_VIEW_DATA_CONSISTENCY_ENABLED = "materialized_view_data_consistency_enabled";
    public static final String CONSIDER_QUERY_FILTERS_FOR_MATERIALIZED_VIEW_PARTITIONS = "consider-query-filters-for-materialized-view-partitions";
    public static final String QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED = "query_optimization_with_materialized_view_enabled";
    public static final String AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY = "aggregation_if_to_filter_rewrite_strategy";
    public static final String JOINS_NOT_NULL_INFERENCE_STRATEGY = "joins_not_null_inference_strategy";
    public static final String RESOURCE_AWARE_SCHEDULING_STRATEGY = "resource_aware_scheduling_strategy";
    public static final String HEAP_DUMP_ON_EXCEEDED_MEMORY_LIMIT_ENABLED = "heap_dump_on_exceeded_memory_limit_enabled";
    public static final String EXCEEDED_MEMORY_LIMIT_HEAP_DUMP_FILE_DIRECTORY = "exceeded_memory_limit_heap_dump_file_directory";
    public static final String DISTRIBUTED_TRACING_MODE = "distributed_tracing_mode";
    public static final String VERBOSE_RUNTIME_STATS_ENABLED = "verbose_runtime_stats_enabled";
    public static final String OPTIMIZERS_TO_ENABLE_VERBOSE_RUNTIME_STATS = "optimizers_to_enable_verbose_runtime_stats";
    public static final String VERBOSE_OPTIMIZER_INFO_ENABLED = "verbose_optimizer_info_enabled";
    public static final String VERBOSE_OPTIMIZER_RESULTS = "verbose_optimizer_results";
    public static final String STREAMING_FOR_PARTIAL_AGGREGATION_ENABLED = "streaming_for_partial_aggregation_enabled";
    public static final String MAX_STAGE_COUNT_FOR_EAGER_SCHEDULING = "max_stage_count_for_eager_scheduling";
    public static final String HYPERLOGLOG_STANDARD_ERROR_WARNING_THRESHOLD = "hyperloglog_standard_error_warning_threshold";
    public static final String PREFER_MERGE_JOIN_FOR_SORTED_INPUTS = "prefer_merge_join_for_sorted_inputs";
    public static final String PREFER_SORT_MERGE_JOIN = "prefer_sort_merge_join";
    public static final String SEGMENTED_AGGREGATION_ENABLED = "segmented_aggregation_enabled";
    public static final String USE_HISTORY_BASED_PLAN_STATISTICS = "use_history_based_plan_statistics";
    public static final String TRACK_HISTORY_BASED_PLAN_STATISTICS = "track_history_based_plan_statistics";
    public static final String TRACK_HISTORY_STATS_FROM_FAILED_QUERIES = "track_history_stats_from_failed_queries";
    public static final String USE_PERFECTLY_CONSISTENT_HISTORIES = "use_perfectly_consistent_histories";
    public static final String HISTORY_CANONICAL_PLAN_NODE_LIMIT = "history_canonical_plan_node_limit";
    public static final String HISTORY_BASED_OPTIMIZER_ESTIMATE_SIZE_USING_VARIABLES = "history_based_optimizer_estimate_size_using_variables";
    public static final String HISTORY_BASED_OPTIMIZER_TIMEOUT_LIMIT = "history_based_optimizer_timeout_limit";
    public static final String RESTRICT_HISTORY_BASED_OPTIMIZATION_TO_COMPLEX_QUERY = "restrict_history_based_optimization_to_complex_query";
    public static final String HISTORY_INPUT_TABLE_STATISTICS_MATCHING_THRESHOLD = "history_input_table_statistics_matching_threshold";
    public static final String HISTORY_BASED_OPTIMIZATION_PLAN_CANONICALIZATION_STRATEGY = "history_based_optimization_plan_canonicalization_strategy";
    public static final String ENABLE_VERBOSE_HISTORY_BASED_OPTIMIZER_RUNTIME_STATS = "enable_verbose_history_based_optimizer_runtime_stats";
    public static final String LOG_QUERY_PLANS_USED_IN_HISTORY_BASED_OPTIMIZER = "log_query_plans_used_in_history_based_optimizer";
    public static final String ENFORCE_HISTORY_BASED_OPTIMIZER_REGISTRATION_TIMEOUT = "enforce_history_based_optimizer_register_timeout";
    public static final String MAX_LEAF_NODES_IN_PLAN = "max_leaf_nodes_in_plan";
    public static final String LEAF_NODE_LIMIT_ENABLED = "leaf_node_limit_enabled";
    public static final String PUSH_REMOTE_EXCHANGE_THROUGH_GROUP_ID = "push_remote_exchange_through_group_id";
    public static final String OPTIMIZE_MULTIPLE_APPROX_PERCENTILE_ON_SAME_FIELD = "optimize_multiple_approx_percentile_on_same_field";
    public static final String RANDOMIZE_OUTER_JOIN_NULL_KEY = "randomize_outer_join_null_key";
    public static final String RANDOMIZE_OUTER_JOIN_NULL_KEY_STRATEGY = "randomize_outer_join_null_key_strategy";
    public static final String RANDOMIZE_OUTER_JOIN_NULL_KEY_NULL_RATIO_THRESHOLD = "randomize_outer_join_null_key_null_ratio_threshold";
    public static final String SHARDED_JOINS_STRATEGY = "sharded_joins_strategy";
    public static final String JOIN_SHARD_COUNT = "join_shard_count";
    public static final String IN_PREDICATES_AS_INNER_JOINS_ENABLED = "in_predicates_as_inner_joins_enabled";
    public static final String PUSH_AGGREGATION_BELOW_JOIN_BYTE_REDUCTION_THRESHOLD = "push_aggregation_below_join_byte_reduction_threshold";
    public static final String KEY_BASED_SAMPLING_ENABLED = "key_based_sampling_enabled";
    public static final String KEY_BASED_SAMPLING_PERCENTAGE = "key_based_sampling_percentage";
    public static final String KEY_BASED_SAMPLING_FUNCTION = "key_based_sampling_function";
    public static final String QUICK_DISTINCT_LIMIT_ENABLED = "quick_distinct_limit_enabled";
    public static final String OPTIMIZE_CONDITIONAL_AGGREGATION_ENABLED = "optimize_conditional_aggregation_enabled";
    public static final String ANALYZER_TYPE = "analyzer_type";
    public static final String PRE_PROCESS_METADATA_CALLS = "pre_process_metadata_calls";
    public static final String REMOVE_REDUNDANT_DISTINCT_AGGREGATION_ENABLED = "remove_redundant_distinct_aggregation_enabled";
    public static final String PREFILTER_FOR_GROUPBY_LIMIT = "prefilter_for_groupby_limit";
    public static final String PREFILTER_FOR_GROUPBY_LIMIT_TIMEOUT_MS = "prefilter_for_groupby_limit_timeout_ms";
    public static final String OPTIMIZE_JOIN_PROBE_FOR_EMPTY_BUILD_RUNTIME = "optimize_join_probe_for_empty_build_runtime";
    public static final String USE_DEFAULTS_FOR_CORRELATED_AGGREGATION_PUSHDOWN_THROUGH_OUTER_JOINS = "use_defaults_for_correlated_aggregation_pushdown_through_outer_joins";
    public static final String MERGE_DUPLICATE_AGGREGATIONS = "merge_duplicate_aggregations";
    public static final String MERGE_AGGREGATIONS_WITH_AND_WITHOUT_FILTER = "merge_aggregations_with_and_without_filter";
    public static final String SIMPLIFY_PLAN_WITH_EMPTY_INPUT = "simplify_plan_with_empty_input";
    public static final String PUSH_DOWN_FILTER_EXPRESSION_EVALUATION_THROUGH_CROSS_JOIN = "push_down_filter_expression_evaluation_through_cross_join";
    public static final String REWRITE_CROSS_JOIN_OR_TO_INNER_JOIN = "rewrite_cross_join_or_to_inner_join";
    public static final String REWRITE_CROSS_JOIN_ARRAY_CONTAINS_TO_INNER_JOIN = "rewrite_cross_join_array_contains_to_inner_join";
    public static final String REWRITE_CROSS_JOIN_ARRAY_NOT_CONTAINS_TO_ANTI_JOIN = "rewrite_cross_join_array_not_contains_to_anti_join";
    public static final String REWRITE_LEFT_JOIN_ARRAY_CONTAINS_TO_EQUI_JOIN = "rewrite_left_join_array_contains_to_equi_join";
    public static final String REWRITE_LEFT_JOIN_NULL_FILTER_TO_SEMI_JOIN = "rewrite_left_join_null_filter_to_semi_join";
    public static final String USE_BROADCAST_WHEN_BUILDSIZE_SMALL_PROBESIDE_UNKNOWN = "use_broadcast_when_buildsize_small_probeside_unknown";
    public static final String ADD_PARTIAL_NODE_FOR_ROW_NUMBER_WITH_LIMIT = "add_partial_node_for_row_number_with_limit";
    public static final String REWRITE_CASE_TO_MAP_ENABLED = "rewrite_case_to_map_enabled";
    public static final String FIELD_NAMES_IN_JSON_CAST_ENABLED = "field_names_in_json_cast_enabled";
    public static final String LEGACY_JSON_CAST = "legacy_json_cast";
    public static final String CANONICALIZED_JSON_EXTRACT = "canonicalized_json_extract";
    public static final String PULL_EXPRESSION_FROM_LAMBDA_ENABLED = "pull_expression_from_lambda_enabled";
    public static final String REWRITE_CONSTANT_ARRAY_CONTAINS_TO_IN_EXPRESSION = "rewrite_constant_array_contains_to_in_expression";
    public static final String INFER_INEQUALITY_PREDICATES = "infer_inequality_predicates";
    public static final String ENABLE_HISTORY_BASED_SCALED_WRITER = "enable_history_based_scaled_writer";
    public static final String USE_PARTIAL_AGGREGATION_HISTORY = "use_partial_aggregation_history";
    public static final String TRACK_PARTIAL_AGGREGATION_HISTORY = "track_partial_aggregation_history";
    public static final String REMOVE_REDUNDANT_CAST_TO_VARCHAR_IN_JOIN = "remove_redundant_cast_to_varchar_in_join";
    public static final String REMOVE_MAP_CAST = "remove_map_cast";
    public static final String HANDLE_COMPLEX_EQUI_JOINS = "handle_complex_equi_joins";
    public static final String SKIP_HASH_GENERATION_FOR_JOIN_WITH_TABLE_SCAN_INPUT = "skip_hash_generation_for_join_with_table_scan_input";
    public static final String GENERATE_DOMAIN_FILTERS = "generate_domain_filters";
    public static final String REWRITE_EXPRESSION_WITH_CONSTANT_EXPRESSION = "rewrite_expression_with_constant_expression";
    public static final String PRINT_ESTIMATED_STATS_FROM_CACHE = "print_estimated_stats_from_cache";
    public static final String REMOVE_CROSS_JOIN_WITH_CONSTANT_SINGLE_ROW_INPUT = "remove_cross_join_with_constant_single_row_input";
    public static final String OPTIMIZE_CONDITIONAL_CONSTANT_APPROXIMATE_DISTINCT = "optimize_conditional_constant_approximate_distinct";
    public static final String EAGER_PLAN_VALIDATION_ENABLED = "eager_plan_validation_enabled";
    public static final String DEFAULT_VIEW_SECURITY_MODE = "default_view_security_mode";
    public static final String JOIN_PREFILTER_BUILD_SIDE = "join_prefilter_build_side";
    public static final String OPTIMIZER_USE_HISTOGRAMS = "optimizer_use_histograms";
    public static final String WARN_ON_COMMON_NAN_PATTERNS = "warn_on_common_nan_patterns";
    public static final String INLINE_PROJECTIONS_ON_VALUES = "inline_projections_on_values";
    public static final String INCLUDE_VALUES_NODE_IN_CONNECTOR_OPTIMIZER = "include_values_node_in_connector_optimizer";
    public static final String SINGLE_NODE_EXECUTION_ENABLED = "single_node_execution_enabled";
    public static final String BROADCAST_SEMI_JOIN_FOR_DELETE = "broadcast_semi_join_for_delete";
    public static final String EXPRESSION_OPTIMIZER_NAME = "expression_optimizer_name";
    public static final String ADD_EXCHANGE_BELOW_PARTIAL_AGGREGATION_OVER_GROUP_ID = "add_exchange_below_partial_aggregation_over_group_id";
    public static final String QUERY_CLIENT_TIMEOUT = "query_client_timeout";
    public static final String REWRITE_MIN_MAX_BY_TO_TOP_N = "rewrite_min_max_by_to_top_n";
    public static final String ADD_DISTINCT_BELOW_SEMI_JOIN_BUILD = "add_distinct_below_semi_join_build";
    public static final String UTILIZE_UNIQUE_PROPERTY_IN_QUERY_PLANNING = "utilize_unique_property_in_query_planning";
    public static final String PUSHDOWN_SUBFIELDS_FOR_MAP_FUNCTIONS = "pushdown_subfields_for_map_functions";
    public static final String MAX_SERIALIZABLE_OBJECT_SIZE = "max_serializable_object_size";

    // TODO: Native execution related session properties that are temporarily put here. They will be relocated in the future.
    public static final String NATIVE_AGGREGATION_SPILL_ALL = "native_aggregation_spill_all";
    public static final String NATIVE_EXECUTION_ENABLED = "native_execution_enabled";
    private static final String NATIVE_EXECUTION_EXECUTABLE_PATH = "native_execution_executable_path";
    private static final String NATIVE_EXECUTION_PROGRAM_ARGUMENTS = "native_execution_program_arguments";
    public static final String NATIVE_EXECUTION_PROCESS_REUSE_ENABLED = "native_execution_process_reuse_enabled";
    public static final String INNER_JOIN_PUSHDOWN_ENABLED = "optimizer_inner_join_pushdown_enabled";
    public static final String INEQUALITY_JOIN_PUSHDOWN_ENABLED = "optimizer_inequality_join_pushdown_enabled";
    public static final String NATIVE_MIN_COLUMNAR_ENCODING_CHANNELS_TO_PREFER_ROW_WISE_ENCODING = "native_min_columnar_encoding_channels_to_prefer_row_wise_encoding";
    public static final String NATIVE_ENFORCE_JOIN_BUILD_INPUT_PARTITION = "native_enforce_join_build_input_partition";
    public static final String NATIVE_EXECUTION_SCALE_WRITER_THREADS_ENABLED = "native_execution_scale_writer_threads_enabled";
    public static final String NATIVE_EXECUTION_TYPE_REWRITE_ENABLED = "native_execution_type_rewrite_enabled";

    private final List<PropertyMetadata<?>> sessionProperties;

    public SystemSessionProperties()
    {
        this(
                new QueryManagerConfig(),
                new TaskManagerConfig(),
                new MemoryManagerConfig(),
                new FeaturesConfig(),
                new FunctionsConfig(),
                new NodeMemoryConfig(),
                new WarningCollectorConfig(),
                new NodeSchedulerConfig(),
                new NodeSpillConfig(),
                new TracingConfig(),
                new CompilerConfig(),
                new HistoryBasedOptimizationConfig());
    }

    @Inject
    public SystemSessionProperties(
            QueryManagerConfig queryManagerConfig,
            TaskManagerConfig taskManagerConfig,
            MemoryManagerConfig memoryManagerConfig,
            FeaturesConfig featuresConfig,
            FunctionsConfig functionsConfig,
            NodeMemoryConfig nodeMemoryConfig,
            WarningCollectorConfig warningCollectorConfig,
            NodeSchedulerConfig nodeSchedulerConfig,
            NodeSpillConfig nodeSpillConfig,
            TracingConfig tracingConfig,
            CompilerConfig compilerConfig,
            HistoryBasedOptimizationConfig historyBasedOptimizationConfig)
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
                        SIZE_BASED_JOIN_DISTRIBUTION_TYPE,
                        "Consider source table size when determining join distribution type when CBO fails",
                        featuresConfig.isSizeBasedJoinDistributionTypeEnabled(),
                        false),
                booleanProperty(
                        CONFIDENCE_BASED_BROADCAST_ENABLED,
                        "Enable confidence based broadcasting when enabled",
                        featuresConfig.isConfidenceBasedBroadcastEnabled(),
                        false),
                booleanProperty(RETRY_QUERY_WITH_HISTORY_BASED_OPTIMIZATION,
                        "Automatically retry a query if it fails and HBO can change the query plan",
                        featuresConfig.isRetryQueryWithHistoryBasedOptimizationEnabled(),
                        false),
                booleanProperty(
                        TREAT_LOW_CONFIDENCE_ZERO_ESTIMATION_AS_UNKNOWN_ENABLED,
                        "Treat low confidence zero estimations as unknowns during joins when enabled",
                        featuresConfig.isTreatLowConfidenceZeroEstimationAsUnknownEnabled(),
                        false),
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
                stringProperty(
                        CTE_PARTITIONING_PROVIDER_CATALOG,
                        "Name of the catalog providing custom partitioning for cte materialization",
                        queryManagerConfig.getCtePartitioningProviderCatalog(),
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
                        featuresConfig.isNativeExecutionEnabled() ? value -> validateNullablePositiveIntegerValue(value, TASK_WRITER_COUNT) : value -> validateValueIsPowerOfTwo(value, TASK_WRITER_COUNT),
                        value -> value),
                new PropertyMetadata<>(
                        TASK_PARTITIONED_WRITER_COUNT,
                        "Number of writers per task for partitioned writes. If not set, the number set by task.writer-count will be used",
                        BIGINT,
                        Integer.class,
                        taskManagerConfig.getPartitionedWriterCount(),
                        false,
                        featuresConfig.isNativeExecutionEnabled() ? value -> validateNullablePositiveIntegerValue(value, TASK_PARTITIONED_WRITER_COUNT) : value -> validateValueIsPowerOfTwo(value, TASK_PARTITIONED_WRITER_COUNT),
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
                        featuresConfig.isNativeExecutionEnabled() ? value -> validateIntegerValue(value, TASK_CONCURRENCY, 1, false) : value -> validateValueIsPowerOfTwo(requireNonNull(value, "value is null"), TASK_CONCURRENCY),
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
                        QUERY_MAX_QUEUED_TIME,
                        "Maximum Queued time of a query",
                        VARCHAR,
                        Duration.class,
                        queryManagerConfig.getQueryMaxQueuedTime(),
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
                        QUERY_MAX_WRITTEN_INTERMEDIATE_BYTES,
                        "Maximum written intermediate bytes of a query",
                        queryManagerConfig.getQueryMaxWrittenIntermediateBytes(),
                        false),
                longProperty(
                        QUERY_MAX_OUTPUT_POSITIONS,
                        "Maximum number of output rows that can be fetched by a query",
                        queryManagerConfig.getQueryMaxOutputPositions(),
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
                        OPTIMIZE_METADATA_QUERIES_CALL_THRESHOLD,
                        "The threshold number of service calls to metastore, used in optimization for metadata queries",
                        featuresConfig.getOptimizeMetadataQueriesCallThreshold(),
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
                        format("Experimental: Partial merge pushdown strategy to use. Options are %s",
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
                new PropertyMetadata<>(
                        QUERY_MAX_REVOCABLE_MEMORY_PER_NODE,
                        "Maximum amount of revocable memory a query can use",
                        VARCHAR,
                        DataSize.class,
                        nodeSpillConfig.getMaxRevocableMemoryPerNode(),
                        true,
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
                        functionsConfig.isLegacyRowFieldOrdinalAccess(),
                        false),
                booleanProperty(
                        LEGACY_MAP_SUBSCRIPT,
                        "Do not fail the query if map key is missing",
                        functionsConfig.isLegacyMapSubscript(),
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
                new PropertyMetadata<>(
                        QUERY_ANALYZER_TIMEOUT,
                        "Maximum processing time for query analyzer",
                        VARCHAR,
                        Duration.class,
                        featuresConfig.getQueryAnalyzerTimeout(),
                        false,
                        value -> Duration.valueOf((String) value),
                        Duration::toString),
                booleanProperty(
                        RUNTIME_OPTIMIZER_ENABLED,
                        "Experimental: enable runtime optimizer",
                        featuresConfig.isRuntimeOptimizerEnabled(),
                        false),
                new PropertyMetadata<>(
                        EXCHANGE_COMPRESSION_CODEC,
                        "Exchange compression codec",
                        VARCHAR,
                        CompressionCodec.class,
                        featuresConfig.getExchangeCompressionCodec(),
                        false,
                        value -> CompressionCodec.valueOf(((String) value).toUpperCase()),
                        CompressionCodec::name),
                booleanProperty(
                        EXCHANGE_CHECKSUM,
                        "Enable checksum in exchanges",
                        featuresConfig.isExchangeChecksumEnabled(),
                        false),
                booleanProperty(
                        LEGACY_TIMESTAMP,
                        "Use legacy TIME & TIMESTAMP semantics (warning: this will be removed)",
                        functionsConfig.isLegacyTimestamp(),
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
                        functionsConfig.isParseDecimalLiteralsAsDouble(),
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
                        EXPLOIT_CONSTRAINTS,
                        "Exploit table constraints.",
                        featuresConfig.isExploitConstraints(),
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
                        ADAPTIVE_PARTIAL_AGGREGATION,
                        "Enable adaptive partial aggregation",
                        featuresConfig.isAdaptivePartialAggregationEnabled(),
                        false),
                doubleProperty(
                        ADAPTIVE_PARTIAL_AGGREGATION_ROWS_REDUCTION_RATIO_THRESHOLD,
                        "Rows reduction ratio threshold at which to adaptively disable partial aggregation",
                        featuresConfig.getAdaptivePartialAggregationRowsReductionRatioThreshold(),
                        false),
                booleanProperty(
                        OPTIMIZE_TOP_N_ROW_NUMBER,
                        "Use top N row number optimization",
                        featuresConfig.isOptimizeTopNRowNumber(),
                        false),
                booleanProperty(
                        OPTIMIZE_CASE_EXPRESSION_PREDICATE,
                        "Optimize case expression predicates",
                        featuresConfig.isOptimizeCaseExpressionPredicate(),
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
                new PropertyMetadata<>(
                        CTE_MATERIALIZATION_STRATEGY,
                        format("The strategy to materialize common table expressions. Options are %s",
                                Stream.of(CteMaterializationStrategy.values())
                                        .map(CteMaterializationStrategy::name)
                                        .collect(joining(","))),
                        VARCHAR,
                        CteMaterializationStrategy.class,
                        featuresConfig.getCteMaterializationStrategy(),
                        false,
                        value -> CteMaterializationStrategy.valueOf(((String) value).toUpperCase()),
                        CteMaterializationStrategy::name),
                booleanProperty(
                        CTE_FILTER_AND_PROJECTION_PUSHDOWN_ENABLED,
                        "Enable pushing of filters and projections inside common table expressions.",
                        featuresConfig.getCteFilterAndProjectionPushdownEnabled(),
                        false),
                integerProperty(
                        CTE_HEURISTIC_REPLICATION_THRESHOLD,
                        "Used with CTE Materialization Strategy = Heuristic. CTES are only materialized if they are used greater than or equal to this number",
                        featuresConfig.getCteHeuristicReplicationThreshold(),
                        false),
                new PropertyMetadata<>(
                        DEFAULT_JOIN_SELECTIVITY_COEFFICIENT,
                        "use a default join selectivity coefficient factor when column statistics are not available in a join node",
                        DOUBLE,
                        Double.class,
                        featuresConfig.getDefaultJoinSelectivityCoefficient(),
                        false,
                        value -> validateDoubleValueWithinSelectivityRange(value, DEFAULT_JOIN_SELECTIVITY_COEFFICIENT),
                        object -> object),
                doubleProperty(
                        DEFAULT_WRITER_REPLICATION_COEFFICIENT,
                        "Replication coefficient for costing write operations",
                        featuresConfig.getDefaultWriterReplicationCoefficient(),
                        false),
                booleanProperty(
                        PUSH_LIMIT_THROUGH_OUTER_JOIN,
                        "push limits to the outer side of an outer join",
                        featuresConfig.isPushLimitThroughOuterJoin(),
                        false),
                booleanProperty(
                        OPTIMIZE_CONSTANT_GROUPING_KEYS,
                        "Pull constant grouping keys above the group by",
                        featuresConfig.isOptimizeConstantGroupingKeys(),
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
                        PUSHDOWN_SUBFIELDS_FROM_LAMBDA_ENABLED,
                        "Enable subfield pruning from lambdas",
                        featuresConfig.isPushdownSubfieldsFromLambdaEnabled(),
                        false),
                booleanProperty(
                        PUSHDOWN_DEREFERENCE_ENABLED,
                        "Experimental: enable dereference pushdown",
                        featuresConfig.isPushdownDereferenceEnabled(),
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
                        "(DEPRECATED) Filter nulls from inner side of join. If this is set, joins_not_null_inference_strategy = 'INFER_FROM_STANDARD_OPERATORS' is assumed",
                        false,
                        false),
                booleanProperty(
                        OPTIMIZE_PAYLOAD_JOINS,
                        "Optimize joins with payload columns",
                        featuresConfig.isOptimizePayloadJoins(),
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
                        "Enable dynamic filtering",
                        featuresConfig.isEnableDynamicFiltering(),
                        false),
                integerProperty(
                        DYNAMIC_FILTERING_MAX_PER_DRIVER_ROW_COUNT,
                        "Maximum number of build-side rows to be collected for dynamic filtering per-driver",
                        featuresConfig.getDynamicFilteringMaxPerDriverRowCount(),
                        false),
                new PropertyMetadata<>(
                        DYNAMIC_FILTERING_MAX_PER_DRIVER_SIZE,
                        "Maximum number of bytes to be collected for dynamic filtering per-driver",
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
                        CHECK_ACCESS_CONTROL_WITH_SUBFIELDS,
                        "Apply access control rules with subfield information from columns containing row types",
                        featuresConfig.isCheckAccessControlWithSubfields(),
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
                        "(Deprecated) Simplify joins with one or more empty sources",
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
                booleanProperty(
                        LOG_INVOKED_FUNCTION_NAMES_ENABLED,
                        "Log the names of the functions invoked by the query when enabled.",
                        featuresConfig.isLogInvokedFunctionNamesEnabled(),
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
                        ENHANCED_CTE_SCHEDULING_ENABLED,
                        "Applicable for CTE Materialization. If enabled, only tablescans of the pending tablewriters are blocked and other stages can continue.",
                        featuresConfig.getEnhancedCTESchedulingEnabled(),
                        true),
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
                        CONSIDER_QUERY_FILTERS_FOR_MATERIALIZED_VIEW_PARTITIONS,
                        "When enabled and counting materialized view partitions, filters partition domains not in base query",
                        featuresConfig.isMaterializedViewPartitionFilteringEnabled(),
                        false),
                booleanProperty(
                        QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED,
                        "Enable query optimization with materialized view",
                        featuresConfig.isQueryOptimizationWithMaterializedViewEnabled(),
                        true),
                stringProperty(
                        DISTRIBUTED_TRACING_MODE,
                        "Mode for distributed tracing. NO_TRACE, ALWAYS_TRACE, or SAMPLE_BASED",
                        tracingConfig.getDistributedTracingMode().name(),
                        false),
                booleanProperty(
                        VERBOSE_RUNTIME_STATS_ENABLED,
                        "Enable logging all runtime stats",
                        featuresConfig.isVerboseRuntimeStatsEnabled(),
                        false),
                stringProperty(
                        OPTIMIZERS_TO_ENABLE_VERBOSE_RUNTIME_STATS,
                        "Optimizers to enable verbose runtime stats",
                        "",
                        false),
                booleanProperty(
                        VERBOSE_OPTIMIZER_INFO_ENABLED,
                        "Enable logging of verbose information about applied optimizations",
                        featuresConfig.isVerboseOptimizerInfoEnabled(),
                        false),
                /**/
                new PropertyMetadata<>(
                        VERBOSE_OPTIMIZER_RESULTS,
                        "Print result of selected optimizer(s), allowed values are ALL | NONE | <OptimizerClassName>[,<OptimizerClassName>...]",
                        VARCHAR,
                        VerboseOptimizerResultsProperty.class,
                        VerboseOptimizerResultsProperty.disabled(),
                        false,
                        value -> VerboseOptimizerResultsProperty.valueOf((String) value),
                        object -> object),
                booleanProperty(
                        STREAMING_FOR_PARTIAL_AGGREGATION_ENABLED,
                        "Enable streaming for partial aggregation",
                        featuresConfig.isStreamingForPartialAggregationEnabled(),
                        false),
                booleanProperty(
                        PREFER_MERGE_JOIN_FOR_SORTED_INPUTS,
                        "Prefer merge join for sorted join inputs, e.g., tables pre-sorted, pre-partitioned by join columns." +
                                "To make it work, the connector needs to guarantee and expose the data properties of the underlying table.",
                        featuresConfig.isPreferMergeJoinForSortedInputs(),
                        true),
                booleanProperty(
                        PREFER_SORT_MERGE_JOIN,
                        "Prefer sort merge join for all joins. A SortNode is added if input is not already sorted.",
                        featuresConfig.isPreferSortMergeJoin(),
                        true),
                booleanProperty(
                        SEGMENTED_AGGREGATION_ENABLED,
                        "Enable segmented aggregation.",
                        featuresConfig.isSegmentedAggregationEnabled(),
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
                stringProperty(
                        ANALYZER_TYPE,
                        "Analyzer type to use.",
                        featuresConfig.getAnalyzerType(),
                        true),
                booleanProperty(
                        PRE_PROCESS_METADATA_CALLS,
                        "Pre-process metadata calls before analyzer invocation.",
                        featuresConfig.isPreProcessMetadataCalls(),
                        false),
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
                        false),
                integerProperty(
                        MAX_STAGE_COUNT_FOR_EAGER_SCHEDULING,
                        "Maximum stage count to use eager scheduling when using the adaptive scheduling policy",
                        featuresConfig.getMaxStageCountForEagerScheduling(),
                        false),
                doubleProperty(
                        HYPERLOGLOG_STANDARD_ERROR_WARNING_THRESHOLD,
                        "Threshold for obtaining precise results from aggregation functions",
                        featuresConfig.getHyperloglogStandardErrorWarningThreshold(),
                        false),
                booleanProperty(
                        QUICK_DISTINCT_LIMIT_ENABLED,
                        "Enable quick distinct limit queries that give results as soon as a new distinct value is found",
                        featuresConfig.isQuickDistinctLimitEnabled(),
                        false),
                booleanProperty(
                        USE_HISTORY_BASED_PLAN_STATISTICS,
                        "Use history based plan statistics service in query optimizer",
                        featuresConfig.isUseHistoryBasedPlanStatistics(),
                        false),
                booleanProperty(
                        TRACK_HISTORY_BASED_PLAN_STATISTICS,
                        "Track history based plan statistics service in query optimizer",
                        featuresConfig.isTrackHistoryBasedPlanStatistics(),
                        false),
                booleanProperty(
                        TRACK_HISTORY_STATS_FROM_FAILED_QUERIES,
                        "Track history based plan statistics from complete plan fragments in failed queries",
                        featuresConfig.isTrackHistoryStatsFromFailedQuery(),
                        false),
                booleanProperty(
                        USE_PERFECTLY_CONSISTENT_HISTORIES,
                        "Use perfectly consistent histories for history based optimizations, even when parts of a query are re-ordered.",
                        featuresConfig.isUsePerfectlyConsistentHistories(),
                        false),
                integerProperty(
                        HISTORY_CANONICAL_PLAN_NODE_LIMIT,
                        "Use history based optimizations only when number of nodes in canonical plan is within this limit. Size of canonical plan can become much larger than original plan leading to increased planning time, particularly in cases when limiting nodes like LimitNode, TopNNode etc. are present.",
                        featuresConfig.getHistoryCanonicalPlanNodeLimit(),
                        false),
                new PropertyMetadata<>(
                        HISTORY_BASED_OPTIMIZER_TIMEOUT_LIMIT,
                        "Timeout in milliseconds for history based optimizer",
                        VARCHAR,
                        Duration.class,
                        featuresConfig.getHistoryBasedOptimizerTimeout(),
                        false,
                        value -> Duration.valueOf((String) value),
                        Duration::toString),
                booleanProperty(
                        RESTRICT_HISTORY_BASED_OPTIMIZATION_TO_COMPLEX_QUERY,
                        "Enable history based optimization only for complex queries, i.e. queries with join and aggregation",
                        true,
                        false),
                booleanProperty(
                        HISTORY_BASED_OPTIMIZER_ESTIMATE_SIZE_USING_VARIABLES,
                        "Estimate the size of the plan node output with variable statistics for HBO",
                        featuresConfig.isHistoryBasedOptimizerEstimateSizeUsingVariables(),
                        false),
                new PropertyMetadata<>(
                        HISTORY_INPUT_TABLE_STATISTICS_MATCHING_THRESHOLD,
                        "When the size difference between current table and history table exceed this threshold, do not match history statistics",
                        DOUBLE,
                        Double.class,
                        historyBasedOptimizationConfig.getHistoryMatchingThreshold(),
                        true,
                        value -> validateDoubleValueWithinSelectivityRange(value, HISTORY_INPUT_TABLE_STATISTICS_MATCHING_THRESHOLD),
                        object -> object),
                stringProperty(
                        HISTORY_BASED_OPTIMIZATION_PLAN_CANONICALIZATION_STRATEGY,
                        format("The plan canonicalization strategies used for history based optimization, the strategies will be applied based on the accuracy of the strategies, from more accurate to less accurate. Options are %s",
                                Stream.of(PlanCanonicalizationStrategy.values())
                                        .map(PlanCanonicalizationStrategy::name)
                                        .collect(joining(","))),
                        featuresConfig.getHistoryBasedOptimizerPlanCanonicalizationStrategies(),
                        false),
                booleanProperty(
                        ENABLE_VERBOSE_HISTORY_BASED_OPTIMIZER_RUNTIME_STATS,
                        "Enable recording of verbose runtime stats for history based optimizer",
                        false,
                        false),
                booleanProperty(
                        LOG_QUERY_PLANS_USED_IN_HISTORY_BASED_OPTIMIZER,
                        "Enable logging of query plans generated and used in history based optimizer",
                        featuresConfig.isLogPlansUsedInHistoryBasedOptimizer(),
                        false),
                booleanProperty(
                        ENFORCE_HISTORY_BASED_OPTIMIZER_REGISTRATION_TIMEOUT,
                        "Enforce timeout for query registration in HBO optimizer",
                        featuresConfig.isEnforceTimeoutForHBOQueryRegistration(),
                        false),
                new PropertyMetadata<>(
                        MAX_LEAF_NODES_IN_PLAN,
                        "Maximum number of leaf nodes in the logical plan of SQL statement",
                        INTEGER,
                        Integer.class,
                        compilerConfig.getLeafNodeLimit(),
                        false,
                        value -> validateIntegerValue(value, MAX_LEAF_NODES_IN_PLAN, 0, false),
                        object -> object),
                booleanProperty(
                        LEAF_NODE_LIMIT_ENABLED,
                        "Throw exception if the number of leaf nodes in logical plan exceeds threshold set in max_leaf_nodes_in_plan",
                        compilerConfig.getLeafNodeLimitEnabled(),
                        false),
                booleanProperty(
                        PUSH_REMOTE_EXCHANGE_THROUGH_GROUP_ID,
                        "Enable optimization rule to push remote exchange through GroupId",
                        featuresConfig.isPushRemoteExchangeThroughGroupId(),
                        false),
                booleanProperty(
                        OPTIMIZE_MULTIPLE_APPROX_PERCENTILE_ON_SAME_FIELD,
                        "Combine individual approx_percentile calls on individual field to evaluation on an array",
                        featuresConfig.isOptimizeMultipleApproxPercentileOnSameFieldEnabled(),
                        false),
                booleanProperty(
                        NATIVE_AGGREGATION_SPILL_ALL,
                        "Native Execution only. If true and spilling has been triggered during the input " +
                                "processing, the spiller will spill all the remaining in-memory state to disk before " +
                                "output processing. This is to simplify the aggregation query OOM prevention in " +
                                "output processing stage.",
                        true,
                        false),
                booleanProperty(
                        NATIVE_EXECUTION_ENABLED,
                        "Enable execution on native engine",
                        featuresConfig.isNativeExecutionEnabled(),
                        true),
                booleanProperty(
                        NATIVE_EXECUTION_PROCESS_REUSE_ENABLED,
                        "Enable reuse the native process within the same JVM",
                        true,
                        false),
                booleanProperty(
                        NATIVE_ENFORCE_JOIN_BUILD_INPUT_PARTITION,
                        "Enforce that the join build input is partitioned on join key",
                        featuresConfig.isNativeEnforceJoinBuildInputPartition(),
                        false),
                booleanProperty(
                        RANDOMIZE_OUTER_JOIN_NULL_KEY,
                        "(Deprecated) Randomize null join key for outer join",
                        false,
                        false),
                new PropertyMetadata<>(
                        RANDOMIZE_OUTER_JOIN_NULL_KEY_STRATEGY,
                        format("When to apply randomization to join keys in outer joins to mitigate null skew. Value must be one of: %s",
                                Stream.of(RandomizeOuterJoinNullKeyStrategy.values())
                                        .map(RandomizeOuterJoinNullKeyStrategy::name)
                                        .collect(joining(","))),
                        VARCHAR,
                        RandomizeOuterJoinNullKeyStrategy.class,
                        featuresConfig.getRandomizeOuterJoinNullKeyStrategy(),
                        false,
                        value -> RandomizeOuterJoinNullKeyStrategy.valueOf(((String) value).toUpperCase()),
                        RandomizeOuterJoinNullKeyStrategy::name),
                doubleProperty(
                        RANDOMIZE_OUTER_JOIN_NULL_KEY_NULL_RATIO_THRESHOLD,
                        "Enable randomizing null join key for outer join when ratio of null join keys exceeds the threshold",
                        0.02,
                        false),
                new PropertyMetadata<>(
                        SHARDED_JOINS_STRATEGY,
                        format("When to shard joins to mitigate skew. Value must be one of: %s",
                                Stream.of(ShardedJoinStrategy.values())
                                        .map(ShardedJoinStrategy::name)
                                        .collect(joining(","))),
                        VARCHAR,
                        ShardedJoinStrategy.class,
                        featuresConfig.getShardedJoinStrategy(),
                        false,
                        value -> ShardedJoinStrategy.valueOf(((String) value).toUpperCase()),
                        ShardedJoinStrategy::name),
                integerProperty(
                        JOIN_SHARD_COUNT,
                        "Number of shards to use in sharded joins optimization",
                        featuresConfig.getJoinShardCount(),
                        true),
                booleanProperty(
                        OPTIMIZE_CONDITIONAL_AGGREGATION_ENABLED,
                        "Enable rewriting IF(condition, AGG(x)) to AGG(x) with condition included in mask",
                        featuresConfig.isOptimizeConditionalAggregationEnabled(),
                        false),
                booleanProperty(
                        REMOVE_REDUNDANT_DISTINCT_AGGREGATION_ENABLED,
                        "Enable removing distinct aggregation node if input is already distinct",
                        featuresConfig.isRemoveRedundantDistinctAggregationEnabled(),
                        false),
                booleanProperty(IN_PREDICATES_AS_INNER_JOINS_ENABLED,
                        "Enable transformation of IN predicates to inner joins",
                        featuresConfig.isInPredicatesAsInnerJoinsEnabled(),
                        false),
                doubleProperty(
                        PUSH_AGGREGATION_BELOW_JOIN_BYTE_REDUCTION_THRESHOLD,
                        "Byte reduction ratio threshold at which to disable pushdown of aggregation below inner join",
                        featuresConfig.getPushAggregationBelowJoinByteReductionThreshold(),
                        false),
                booleanProperty(
                        PREFILTER_FOR_GROUPBY_LIMIT,
                        "Prefilter aggregation source for queries that have aggregations on simple tables with filters",
                        featuresConfig.isPrefilterForGroupbyLimit(),
                        false),
                integerProperty(
                        PREFILTER_FOR_GROUPBY_LIMIT_TIMEOUT_MS,
                        "Timeout for finding the LIMIT number of keys for group by",
                        10000,
                        false),
                booleanProperty(
                        FIELD_NAMES_IN_JSON_CAST_ENABLED,
                        "Include field names in json output when casting rows",
                        functionsConfig.isFieldNamesInJsonCastEnabled(),
                        false),
                booleanProperty(
                        LEGACY_JSON_CAST,
                        "Keep the legacy json cast behavior, do not reserve the case for field names when casting to row type",
                        functionsConfig.isLegacyJsonCast(),
                        true),
                booleanProperty(
                        CANONICALIZED_JSON_EXTRACT,
                        "Extracts json data in a canonicalized manner, and raises a PrestoException when encountering invalid json structures within the input json path",
                        functionsConfig.isCanonicalizedJsonExtract(),
                        true),
                booleanProperty(
                        OPTIMIZE_JOIN_PROBE_FOR_EMPTY_BUILD_RUNTIME,
                        "Optimize join probe at runtime if build side is empty",
                        featuresConfig.isOptimizeJoinProbeForEmptyBuildRuntimeEnabled(),
                        false),
                booleanProperty(
                        USE_DEFAULTS_FOR_CORRELATED_AGGREGATION_PUSHDOWN_THROUGH_OUTER_JOINS,
                        "Coalesce with defaults for correlated aggregations",
                        featuresConfig.isUseDefaultsForCorrelatedAggregationPushdownThroughOuterJoins(),
                        false),
                booleanProperty(
                        MERGE_DUPLICATE_AGGREGATIONS,
                        "Merge identical aggregation functions within the same aggregation node",
                        featuresConfig.isMergeDuplicateAggregationsEnabled(),
                        false),
                booleanProperty(
                        MERGE_AGGREGATIONS_WITH_AND_WITHOUT_FILTER,
                        "Merge aggregations that are same except for filter",
                        featuresConfig.isMergeAggregationsWithAndWithoutFilter(),
                        false),
                booleanProperty(
                        SIMPLIFY_PLAN_WITH_EMPTY_INPUT,
                        "Simplify the query plan with empty input",
                        featuresConfig.isSimplifyPlanWithEmptyInput(),
                        false),
                new PropertyMetadata<>(
                        PUSH_DOWN_FILTER_EXPRESSION_EVALUATION_THROUGH_CROSS_JOIN,
                        format("Push down expression evaluation in filter through cross join %s",
                                Stream.of(PushDownFilterThroughCrossJoinStrategy.values())
                                        .map(PushDownFilterThroughCrossJoinStrategy::name)
                                        .collect(joining(","))),
                        VARCHAR,
                        PushDownFilterThroughCrossJoinStrategy.class,
                        featuresConfig.getPushDownFilterExpressionEvaluationThroughCrossJoin(),
                        false,
                        value -> PushDownFilterThroughCrossJoinStrategy.valueOf(((String) value).toUpperCase()),
                        PushDownFilterThroughCrossJoinStrategy::name),
                booleanProperty(
                        REWRITE_CROSS_JOIN_OR_TO_INNER_JOIN,
                        "Rewrite cross join with or filter to inner join",
                        featuresConfig.isRewriteCrossJoinWithOrFilterToInnerJoin(),
                        false),
                booleanProperty(
                        REWRITE_CROSS_JOIN_ARRAY_CONTAINS_TO_INNER_JOIN,
                        "Rewrite cross join with array contains filter to inner join",
                        featuresConfig.isRewriteCrossJoinWithArrayContainsFilterToInnerJoin(),
                        false),
                booleanProperty(
                        REWRITE_CROSS_JOIN_ARRAY_NOT_CONTAINS_TO_ANTI_JOIN,
                        "Rewrite cross join with array not contains filter to anti join",
                        featuresConfig.isRewriteCrossJoinWithArrayNotContainsFilterToAntiJoin(),
                        false),
                new PropertyMetadata<>(
                        REWRITE_LEFT_JOIN_ARRAY_CONTAINS_TO_EQUI_JOIN,
                        format("Set the strategy used to convert left join with array contains to inner join. Options are: %s",
                                Stream.of(LeftJoinArrayContainsToInnerJoinStrategy.values())
                                        .map(LeftJoinArrayContainsToInnerJoinStrategy::name)
                                        .collect(joining(","))),
                        VARCHAR,
                        LeftJoinArrayContainsToInnerJoinStrategy.class,
                        featuresConfig.getLeftJoinWithArrayContainsToEquiJoinStrategy(),
                        false,
                        value -> LeftJoinArrayContainsToInnerJoinStrategy.valueOf(((String) value).toUpperCase()),
                        LeftJoinArrayContainsToInnerJoinStrategy::name),
                new PropertyMetadata<>(
                        JOINS_NOT_NULL_INFERENCE_STRATEGY,
                        format("Set the strategy used NOT NULL filter inference on Join Nodes. Options are: %s",
                                Stream.of(JoinNotNullInferenceStrategy.values())
                                        .map(JoinNotNullInferenceStrategy::name)
                                        .collect(joining(","))),
                        VARCHAR,
                        JoinNotNullInferenceStrategy.class,
                        featuresConfig.getJoinsNotNullInferenceStrategy(),
                        false,
                        value -> JoinNotNullInferenceStrategy.valueOf(((String) value).toUpperCase()),
                        JoinNotNullInferenceStrategy::name),
                booleanProperty(
                        REWRITE_LEFT_JOIN_NULL_FILTER_TO_SEMI_JOIN,
                        "Rewrite left join with is null check to semi join",
                        featuresConfig.isLeftJoinNullFilterToSemiJoin(),
                        false),
                booleanProperty(
                        USE_BROADCAST_WHEN_BUILDSIZE_SMALL_PROBESIDE_UNKNOWN,
                        "Experimental: When probe side size is unknown but build size is within broadcast limit, choose broadcast join",
                        featuresConfig.isBroadcastJoinWithSmallBuildUnknownProbe(),
                        false),
                booleanProperty(
                        ADD_PARTIAL_NODE_FOR_ROW_NUMBER_WITH_LIMIT,
                        "Add partial row number node for row number node with limit",
                        featuresConfig.isAddPartialNodeForRowNumberWithLimitEnabled(),
                        false),
                booleanProperty(
                        REWRITE_CASE_TO_MAP_ENABLED,
                        "Rewrite case with constant WHEN/THEN/ELSE clauses to use map literals",
                        TRUE,
                        false),
                booleanProperty(
                        PULL_EXPRESSION_FROM_LAMBDA_ENABLED,
                        "Rewrite case with constant WHEN/THEN/ELSE clauses to use map literals",
                        featuresConfig.isPullUpExpressionFromLambdaEnabled(),
                        false),
                booleanProperty(
                        REWRITE_CONSTANT_ARRAY_CONTAINS_TO_IN_EXPRESSION,
                        "Rewrite contsant array contains to IN expression",
                        featuresConfig.isRewriteConstantArrayContainsToInEnabled(),
                        false),
                booleanProperty(
                        INFER_INEQUALITY_PREDICATES,
                        "Infer nonequality predicates for joins",
                        featuresConfig.getInferInequalityPredicates(),
                        false),
                booleanProperty(
                        ENABLE_HISTORY_BASED_SCALED_WRITER,
                        "Enable setting the initial number of tasks for scaled writers with HBO",
                        featuresConfig.isUseHBOForScaledWriters(),
                        false),
                booleanProperty(
                        USE_PARTIAL_AGGREGATION_HISTORY,
                        "Use collected partial aggregation statistics from HBO",
                        featuresConfig.isUsePartialAggregationHistory(),
                        false),
                booleanProperty(
                        TRACK_PARTIAL_AGGREGATION_HISTORY,
                        "Track partial aggregation statistics in HBO",
                        featuresConfig.isTrackPartialAggregationHistory(),
                        false),
                booleanProperty(
                        REMOVE_REDUNDANT_CAST_TO_VARCHAR_IN_JOIN,
                        "If both left and right side of join clause are varchar cast from int/bigint, remove the cast here",
                        featuresConfig.isRemoveRedundantCastToVarcharInJoin(),
                        false),
                booleanProperty(
                        REMOVE_MAP_CAST,
                        "Remove map cast when possible",
                        false,
                        false),
                booleanProperty(
                        HANDLE_COMPLEX_EQUI_JOINS,
                        "Handle complex equi-join conditions to open up join space for join reordering",
                        featuresConfig.getHandleComplexEquiJoins(),
                        false),
                booleanProperty(
                        SKIP_HASH_GENERATION_FOR_JOIN_WITH_TABLE_SCAN_INPUT,
                        "Skip hash generation for join, when input is table scan node",
                        featuresConfig.isSkipHashGenerationForJoinWithTableScanInput(),
                        false),
                booleanProperty(
                        GENERATE_DOMAIN_FILTERS,
                        "Infer predicates from column domains during predicate pushdown",
                        featuresConfig.getGenerateDomainFilters(),
                        false),
                booleanProperty(
                        REWRITE_EXPRESSION_WITH_CONSTANT_EXPRESSION,
                        "Rewrite left join with is null check to semi join",
                        featuresConfig.isRewriteExpressionWithConstantVariable(),
                        false),
                booleanProperty(
                        PRINT_ESTIMATED_STATS_FROM_CACHE,
                        "When printing estimated plan stats after optimization is complete, such as in an EXPLAIN query or for logging in a QueryCompletedEvent, " +
                                "get stats from a cache that was populated during query optimization rather than recalculating the stats on the final plan.",
                        featuresConfig.isPrintEstimatedStatsFromCache(),
                        false),
                booleanProperty(
                        REMOVE_CROSS_JOIN_WITH_CONSTANT_SINGLE_ROW_INPUT,
                        "If one input of the cross join is a single row with constant value, remove this cross join and replace with a project node",
                        featuresConfig.isRemoveCrossJoinWithSingleConstantRow(),
                        false),
                booleanProperty(
                        EAGER_PLAN_VALIDATION_ENABLED,
                        "Enable eager building and validation of logical plan before queueing",
                        featuresConfig.isEagerPlanValidationEnabled(),
                        false),
                new PropertyMetadata<>(
                        DEFAULT_VIEW_SECURITY_MODE,
                        format("Set default view security mode. Options are: %s",
                                Stream.of(CreateView.Security.values())
                                        .map(CreateView.Security::name)
                                        .collect(joining(","))),
                        VARCHAR,
                        CreateView.Security.class,
                        featuresConfig.getDefaultViewSecurityMode(),
                        false,
                        value -> CreateView.Security.valueOf(((String) value).toUpperCase()),
                        CreateView.Security::name),
                booleanProperty(
                        JOIN_PREFILTER_BUILD_SIDE,
                        "Prefiltering the build/inner side of a join with keys from the other side",
                        false,
                        false),
                booleanProperty(OPTIMIZER_USE_HISTOGRAMS,
                        "whether or not to use histograms in the CBO",
                        featuresConfig.isUseHistograms(),
                        false),
                booleanProperty(WARN_ON_COMMON_NAN_PATTERNS,
                        "Whether to give a warning for some common issues relating to NaNs",
                        functionsConfig.getWarnOnCommonNanPatterns(),
                        false),
                booleanProperty(INLINE_PROJECTIONS_ON_VALUES,
                        "Whether to evaluate project node on values node",
                        featuresConfig.getInlineProjectionsOnValues(),
                        false),
                booleanProperty(INCLUDE_VALUES_NODE_IN_CONNECTOR_OPTIMIZER,
                        "Include values node for connector optimizer",
                        featuresConfig.isIncludeValuesNodeInConnectorOptimizer(),
                        false),
                booleanProperty(
                        INNER_JOIN_PUSHDOWN_ENABLED,
                        "Enable Join Predicate Pushdown",
                        featuresConfig.isInnerJoinPushdownEnabled(),
                        false),
                booleanProperty(
                        INEQUALITY_JOIN_PUSHDOWN_ENABLED,
                        "Enable Join Pushdown for Inequality Predicates",
                        featuresConfig.isInEqualityJoinPushdownEnabled(),
                    false),
                integerProperty(
                        NATIVE_MIN_COLUMNAR_ENCODING_CHANNELS_TO_PREFER_ROW_WISE_ENCODING,
                        "Minimum number of columnar encoding channels to consider row wise encoding for partitioned exchange. Native execution only",
                        queryManagerConfig.getMinColumnarEncodingChannelsToPreferRowWiseEncoding(),
                        false),
                booleanProperty(
                        SINGLE_NODE_EXECUTION_ENABLED,
                        "Enable single node execution",
                        featuresConfig.isSingleNodeExecutionEnabled(),
                        false),
                booleanProperty(
                        REWRITE_MIN_MAX_BY_TO_TOP_N,
                        "rewrite min_by/max_by to top n",
                        featuresConfig.isRewriteMinMaxByToTopNEnabled(),
                        false),
                booleanProperty(NATIVE_EXECUTION_SCALE_WRITER_THREADS_ENABLED,
                        "Enable automatic scaling of writer threads",
                        featuresConfig.isNativeExecutionScaleWritersThreadsEnabled(),
                        !featuresConfig.isNativeExecutionEnabled()),
                booleanProperty(NATIVE_EXECUTION_TYPE_REWRITE_ENABLED,
                        "Enable type rewrite for native execution",
                        featuresConfig.isNativeExecutionTypeRewriteEnabled(),
                        !featuresConfig.isNativeExecutionEnabled()),
                stringProperty(
                        EXPRESSION_OPTIMIZER_NAME,
                        "Configure which expression optimizer to use",
                        featuresConfig.getExpressionOptimizerName(),
                        false),
                booleanProperty(BROADCAST_SEMI_JOIN_FOR_DELETE,
                        "Enforce broadcast join for semi join in delete",
                        featuresConfig.isBroadcastSemiJoinForDelete(),
                        false),
                booleanProperty(ADD_EXCHANGE_BELOW_PARTIAL_AGGREGATION_OVER_GROUP_ID,
                        "Enable adding an exchange below partial aggregation over a GroupId node to improve partial aggregation performance",
                        featuresConfig.getAddExchangeBelowPartialAggregationOverGroupId(),
                        false),
                booleanProperty(
                        OPTIMIZE_CONDITIONAL_CONSTANT_APPROXIMATE_DISTINCT,
                        "Optimize out APPROX_DISTINCT operations over constant conditionals",
                        featuresConfig.isOptimizeConditionalApproxDistinct(),
                        false),
                booleanProperty(PUSHDOWN_SUBFIELDS_FOR_MAP_FUNCTIONS,
                        "Enable subfield pruning for map functions, currently include map_subset and map_filter",
                        featuresConfig.isPushdownSubfieldForMapFunctions(),
                        false),
                longProperty(MAX_SERIALIZABLE_OBJECT_SIZE,
                        "Configure the maximum byte size of a serializable object in expression interpreters",
                        featuresConfig.getMaxSerializableObjectSize(),
                        false),
                new PropertyMetadata<>(
                        QUERY_CLIENT_TIMEOUT,
                        "Configures how long the query runs without contact from the client application, such as the CLI, before it's abandoned",
                        VARCHAR,
                        Duration.class,
                        queryManagerConfig.getClientTimeout(),
                        false,
                        value -> Duration.valueOf((String) value),
                        Duration::toString),
                booleanProperty(UTILIZE_UNIQUE_PROPERTY_IN_QUERY_PLANNING,
                        "Utilize the unique property of input columns in query planning",
                        featuresConfig.isUtilizeUniquePropertyInQueryPlanning(),
                        false),
                booleanProperty(ADD_DISTINCT_BELOW_SEMI_JOIN_BUILD,
                        "Add distinct aggregation below semi join build",
                        featuresConfig.isAddDistinctBelowSemiJoinBuild(),
                        false));
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

    public static boolean isSizeBasedJoinDistributionTypeEnabled(Session session)
    {
        return session.getSystemProperty(SIZE_BASED_JOIN_DISTRIBUTION_TYPE, Boolean.class);
    }

    public static boolean isDistributedIndexJoinEnabled(Session session)
    {
        return session.getSystemProperty(DISTRIBUTED_INDEX_JOIN, Boolean.class);
    }

    public static boolean confidenceBasedBroadcastEnabled(Session session)
    {
        return session.getSystemProperty(CONFIDENCE_BASED_BROADCAST_ENABLED, Boolean.class);
    }

    public static boolean treatLowConfidenceZeroEstimationAsUnknownEnabled(Session session)
    {
        return session.getSystemProperty(TREAT_LOW_CONFIDENCE_ZERO_ESTIMATION_AS_UNKNOWN_ENABLED, Boolean.class);
    }

    public static boolean retryQueryWithHistoryBasedOptimizationEnabled(Session session)
    {
        return session.getSystemProperty(RETRY_QUERY_WITH_HISTORY_BASED_OPTIMIZATION, Boolean.class);
    }

    public static int getHashPartitionCount(Session session)
    {
        return session.getSystemProperty(HASH_PARTITION_COUNT, Integer.class);
    }

    public static int getCteHeuristicReplicationThreshold(Session session)
    {
        return session.getSystemProperty(CTE_HEURISTIC_REPLICATION_THRESHOLD, Integer.class);
    }

    public static String getPartitioningProviderCatalog(Session session)
    {
        return session.getSystemProperty(PARTITIONING_PROVIDER_CATALOG, String.class);
    }

    public static String getCtePartitioningProviderCatalog(Session session)
    {
        return session.getSystemProperty(CTE_PARTITIONING_PROVIDER_CATALOG, String.class);
    }

    public static boolean isCteMaterializationApplicable(Session session)
    {
        boolean isStrategyNone = getCteMaterializationStrategy(session) == CteMaterializationStrategy.NONE;
        boolean hasMaterializedCTE = session.getCteInformationCollector().getCTEInformationList()
                .stream()
                .anyMatch(CTEInformation::isMaterialized);
        return !isStrategyNone && hasMaterializedCTE;
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

    public static int getOptimizeMetadataQueriesCallThreshold(Session session)
    {
        return session.getSystemProperty(OPTIMIZE_METADATA_QUERIES_CALL_THRESHOLD, Integer.class);
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

    public static Duration getQueryMaxQueuedTime(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_QUEUED_TIME, Duration.class);
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
                return JoinReorderingStrategy.NONE;
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

    public static DataSize getQueryMaxWrittenIntermediateBytesLimit(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_WRITTEN_INTERMEDIATE_BYTES, DataSize.class);
    }

    public static DataSize getQueryMaxScanRawInputBytes(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_SCAN_RAW_INPUT_BYTES, DataSize.class);
    }

    public static long getQueryMaxOutputPositions(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_OUTPUT_POSITIONS, Long.class);
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

    public static DataSize getQueryMaxRevocableMemoryPerNode(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_REVOCABLE_MEMORY_PER_NODE, DataSize.class);
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

    public static Duration getQueryAnalyzerTimeout(Session session)
    {
        return session.getSystemProperty(QUERY_ANALYZER_TIMEOUT, Duration.class);
    }

    public static CompressionCodec getExchangeCompressionCodec(Session session)
    {
        return session.getSystemProperty(EXCHANGE_COMPRESSION_CODEC, CompressionCodec.class);
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

    public static boolean isNativeExecutionEnabled(Session session)
    {
        return session.getSystemProperty(NATIVE_EXECUTION_ENABLED, Boolean.class);
    }

    public static boolean isSingleNodeExecutionEnabled(Session session)
    {
        return session.getSystemProperty(SINGLE_NODE_EXECUTION_ENABLED, Boolean.class);
    }

    public static boolean isRewriteMinMaxByToTopNEnabled(Session session)
    {
        return session.getSystemProperty(REWRITE_MIN_MAX_BY_TO_TOP_N, Boolean.class);
    }

    public static boolean isPushAggregationThroughJoin(Session session)
    {
        return session.getSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, Boolean.class);
    }

    public static boolean isParseDecimalLiteralsAsDouble(Session session)
    {
        return session.getSystemProperty(PARSE_DECIMAL_LITERALS_AS_DOUBLE, Boolean.class);
    }

    public static boolean isFieldNameInJsonCastEnabled(Session session)
    {
        return session.getSystemProperty(FIELD_NAMES_IN_JSON_CAST_ENABLED, Boolean.class);
    }

    public static boolean isForceSingleNodeOutput(Session session)
    {
        return session.getSystemProperty(FORCE_SINGLE_NODE_OUTPUT, Boolean.class);
    }

    public static DataSize getFilterAndProjectMinOutputPageSize(Session session)
    {
        return session.getSystemProperty(FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_SIZE, DataSize.class);
    }

    public static CteMaterializationStrategy getCteMaterializationStrategy(Session session)
    {
        return session.getSystemProperty(CTE_MATERIALIZATION_STRATEGY, CteMaterializationStrategy.class);
    }

    public static boolean getCteFilterAndProjectionPushdownEnabled(Session session)
    {
        return session.getSystemProperty(CTE_FILTER_AND_PROJECTION_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static double getCteProducerReplicationCoefficient(Session session)
    {
        return session.getSystemProperty(DEFAULT_WRITER_REPLICATION_COEFFICIENT, Double.class);
    }

    public static int getFilterAndProjectMinOutputPageRowCount(Session session)
    {
        return session.getSystemProperty(FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_ROW_COUNT, Integer.class);
    }

    public static boolean useMarkDistinct(Session session)
    {
        return session.getSystemProperty(USE_MARK_DISTINCT, Boolean.class);
    }

    public static boolean isExploitConstraints(Session session)
    {
        return session.getSystemProperty(EXPLOIT_CONSTRAINTS, Boolean.class);
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

    public static boolean isAdaptivePartialAggregationEnabled(Session session)
    {
        return session.getSystemProperty(ADAPTIVE_PARTIAL_AGGREGATION, Boolean.class);
    }

    public static double getAdaptivePartialAggregationRowsReductionRatioThreshold(Session session)
    {
        return session.getSystemProperty(ADAPTIVE_PARTIAL_AGGREGATION_ROWS_REDUCTION_RATIO_THRESHOLD, Double.class);
    }

    public static boolean isOptimizeTopNRowNumber(Session session)
    {
        return session.getSystemProperty(OPTIMIZE_TOP_N_ROW_NUMBER, Boolean.class);
    }

    public static boolean isOptimizeCaseExpressionPredicate(Session session)
    {
        return session.getSystemProperty(OPTIMIZE_CASE_EXPRESSION_PREDICATE, Boolean.class);
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

    private static Double validateDoubleValueWithinSelectivityRange(Object value, String property)
    {
        Double number = (Double) value;
        if (number == null) {
            return null;
        }
        double doubleValue = number.doubleValue();
        if (doubleValue < 0 || doubleValue > 1) {
            throw new PrestoException(
                    INVALID_SESSION_PROPERTY,
                    format("%s must be within the range of 0 and 1.0: %s", property, doubleValue));
        }
        return doubleValue;
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

    public static double getDefaultJoinSelectivityCoefficient(Session session)
    {
        return session.getSystemProperty(DEFAULT_JOIN_SELECTIVITY_COEFFICIENT, Double.class);
    }

    public static boolean isPushLimitThroughOuterJoin(Session session)
    {
        return session.getSystemProperty(PUSH_LIMIT_THROUGH_OUTER_JOIN, Boolean.class);
    }

    public static boolean isOptimizeConstantGroupingKeys(Session session)
    {
        return session.getSystemProperty(OPTIMIZE_CONSTANT_GROUPING_KEYS, Boolean.class);
    }

    public static int getMaxConcurrentMaterializations(Session session)
    {
        return session.getSystemProperty(MAX_CONCURRENT_MATERIALIZATIONS, Integer.class);
    }

    public static boolean isPushdownSubfieldsEnabled(Session session)
    {
        return session.getSystemProperty(PUSHDOWN_SUBFIELDS_ENABLED, Boolean.class);
    }

    public static boolean isPushdownSubfieldsFromArrayLambdasEnabled(Session session)
    {
        return session.getSystemProperty(PUSHDOWN_SUBFIELDS_FROM_LAMBDA_ENABLED, Boolean.class);
    }

    public static boolean isPushdownDereferenceEnabled(Session session)
    {
        return session.getSystemProperty(PUSHDOWN_DEREFERENCE_ENABLED, Boolean.class);
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

    public static boolean isOptimizePayloadJoins(Session session)
    {
        return session.getSystemProperty(OPTIMIZE_PAYLOAD_JOINS, Boolean.class);
    }

    public static JoinNotNullInferenceStrategy getNotNullInferenceStrategy(Session session)
    {
        if (session.getSystemProperty(OPTIMIZE_NULLS_IN_JOINS, Boolean.class)) {
            return JoinNotNullInferenceStrategy.INFER_FROM_STANDARD_OPERATORS;
        }
        return session.getSystemProperty(JOINS_NOT_NULL_INFERENCE_STRATEGY, JoinNotNullInferenceStrategy.class);
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

    public static boolean isCheckAccessControlWithSubfields(Session session)
    {
        return session.getSystemProperty(CHECK_ACCESS_CONTROL_WITH_SUBFIELDS, Boolean.class);
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

    public static boolean isLogInvokedFunctionNamesEnabled(Session session)
    {
        return session.getSystemProperty(LOG_INVOKED_FUNCTION_NAMES_ENABLED, Boolean.class);
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

    public static boolean isEnhancedCTESchedulingEnabled(Session session)
    {
        return isCteMaterializationApplicable(session) & session.getSystemProperty(ENHANCED_CTE_SCHEDULING_ENABLED, Boolean.class);
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

    public static boolean isMaterializedViewPartitionFilteringEnabled(Session session)
    {
        return session.getSystemProperty(CONSIDER_QUERY_FILTERS_FOR_MATERIALIZED_VIEW_PARTITIONS, Boolean.class);
    }

    public static boolean isQueryOptimizationWithMaterializedViewEnabled(Session session)
    {
        return session.getSystemProperty(QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED, Boolean.class);
    }

    public static boolean isVerboseRuntimeStatsEnabled(Session session)
    {
        return session.getSystemProperty(VERBOSE_RUNTIME_STATS_ENABLED, Boolean.class);
    }

    public static String getOptimizersToEnableVerboseRuntimeStats(Session session)
    {
        return session.getSystemProperty(OPTIMIZERS_TO_ENABLE_VERBOSE_RUNTIME_STATS, String.class);
    }

    public static boolean isVerboseOptimizerResults(Session session)
    {
        return session.getSystemProperty(VERBOSE_OPTIMIZER_RESULTS, VerboseOptimizerResultsProperty.class).isEnabled();
    }

    public static boolean isVerboseOptimizerResults(Session session, String optimizer)
    {
        return session.getSystemProperty(VERBOSE_OPTIMIZER_RESULTS, VerboseOptimizerResultsProperty.class).containsOptimizer(optimizer);
    }

    public static boolean isVerboseOptimizerInfoEnabled(Session session)
    {
        return session.getSystemProperty(VERBOSE_OPTIMIZER_INFO_ENABLED, Boolean.class);
    }

    public static boolean isLeafNodeLimitEnabled(Session session)
    {
        return session.getSystemProperty(LEAF_NODE_LIMIT_ENABLED, Boolean.class);
    }

    public static int getMaxLeafNodesInPlan(Session session)
    {
        return session.getSystemProperty(MAX_LEAF_NODES_IN_PLAN, Integer.class);
    }

    public static boolean isStreamingForPartialAggregationEnabled(Session session)
    {
        return session.getSystemProperty(STREAMING_FOR_PARTIAL_AGGREGATION_ENABLED, Boolean.class);
    }

    public static boolean preferMergeJoinForSortedInputs(Session session)
    {
        return session.getSystemProperty(PREFER_MERGE_JOIN_FOR_SORTED_INPUTS, Boolean.class);
    }

    public static boolean preferSortMergeJoin(Session session)
    {
        return session.getSystemProperty(PREFER_SORT_MERGE_JOIN, Boolean.class);
    }

    public static boolean isSegmentedAggregationEnabled(Session session)
    {
        return session.getSystemProperty(SEGMENTED_AGGREGATION_ENABLED, Boolean.class);
    }

    public static boolean isCombineApproxPercentileEnabled(Session session)
    {
        return session.getSystemProperty(OPTIMIZE_MULTIPLE_APPROX_PERCENTILE_ON_SAME_FIELD, Boolean.class);
    }

    public static AggregationIfToFilterRewriteStrategy getAggregationIfToFilterRewriteStrategy(Session session)
    {
        return session.getSystemProperty(AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY, AggregationIfToFilterRewriteStrategy.class);
    }

    public static ResourceAwareSchedulingStrategy getResourceAwareSchedulingStrategy(Session session)
    {
        return session.getSystemProperty(RESOURCE_AWARE_SCHEDULING_STRATEGY, ResourceAwareSchedulingStrategy.class);
    }

    public static String getAnalyzerType(Session session)
    {
        return session.getSystemProperty(ANALYZER_TYPE, String.class);
    }

    public static Boolean isPreProcessMetadataCalls(Session session)
    {
        return session.getSystemProperty(PRE_PROCESS_METADATA_CALLS, Boolean.class);
    }

    public static Boolean isHeapDumpOnExceededMemoryLimitEnabled(Session session)
    {
        return session.getSystemProperty(HEAP_DUMP_ON_EXCEEDED_MEMORY_LIMIT_ENABLED, Boolean.class);
    }

    public static String getHeapDumpFileDirectory(Session session)
    {
        return session.getSystemProperty(EXCEEDED_MEMORY_LIMIT_HEAP_DUMP_FILE_DIRECTORY, String.class);
    }

    public static int getMaxStageCountForEagerScheduling(Session session)
    {
        return session.getSystemProperty(MAX_STAGE_COUNT_FOR_EAGER_SCHEDULING, Integer.class);
    }

    public static double getHyperloglogStandardErrorWarningThreshold(Session session)
    {
        return session.getSystemProperty(HYPERLOGLOG_STANDARD_ERROR_WARNING_THRESHOLD, Double.class);
    }

    public static boolean isQuickDistinctLimitEnabled(Session session)
    {
        return session.getSystemProperty(QUICK_DISTINCT_LIMIT_ENABLED, Boolean.class);
    }

    public static boolean useHistoryBasedPlanStatisticsEnabled(Session session)
    {
        return session.getSystemProperty(USE_HISTORY_BASED_PLAN_STATISTICS, Boolean.class);
    }

    public static boolean trackHistoryBasedPlanStatisticsEnabled(Session session)
    {
        return session.getSystemProperty(TRACK_HISTORY_BASED_PLAN_STATISTICS, Boolean.class);
    }

    public static boolean trackHistoryStatsFromFailedQuery(Session session)
    {
        return session.getSystemProperty(TRACK_HISTORY_STATS_FROM_FAILED_QUERIES, Boolean.class);
    }

    public static boolean usePerfectlyConsistentHistories(Session session)
    {
        return session.getSystemProperty(USE_PERFECTLY_CONSISTENT_HISTORIES, Boolean.class);
    }

    public static int getHistoryCanonicalPlanNodeLimit(Session session)
    {
        return session.getSystemProperty(HISTORY_CANONICAL_PLAN_NODE_LIMIT, Integer.class);
    }

    public static Duration getHistoryBasedOptimizerTimeoutLimit(Session session)
    {
        return session.getSystemProperty(HISTORY_BASED_OPTIMIZER_TIMEOUT_LIMIT, Duration.class);
    }

    public static boolean restrictHistoryBasedOptimizationToComplexQuery(Session session)
    {
        return session.getSystemProperty(RESTRICT_HISTORY_BASED_OPTIMIZATION_TO_COMPLEX_QUERY, Boolean.class);
    }

    public static boolean estimateSizeUsingVariablesForHBO(Session session)
    {
        return session.getSystemProperty(HISTORY_BASED_OPTIMIZER_ESTIMATE_SIZE_USING_VARIABLES, Boolean.class);
    }

    public static double getHistoryInputTableStatisticsMatchingThreshold(Session session)
    {
        return session.getSystemProperty(HISTORY_INPUT_TABLE_STATISTICS_MATCHING_THRESHOLD, Double.class);
    }

    public static List<PlanCanonicalizationStrategy> getHistoryOptimizationPlanCanonicalizationStrategies(Session session)
    {
        List<PlanCanonicalizationStrategy> strategyList;
        try {
            strategyList = Splitter.on(",").trimResults().splitToList(session.getSystemProperty(HISTORY_BASED_OPTIMIZATION_PLAN_CANONICALIZATION_STRATEGY, String.class)).stream()
                    .map(x -> PlanCanonicalizationStrategy.valueOf(x)).sorted(Comparator.comparingInt(PlanCanonicalizationStrategy::getErrorLevel)).collect(toImmutableList());
        }
        catch (Exception e) {
            strategyList = ImmutableList.of();
        }

        return strategyList;
    }

    public static boolean enableVerboseHistoryBasedOptimizerRuntimeStats(Session session)
    {
        return session.getSystemProperty(ENABLE_VERBOSE_HISTORY_BASED_OPTIMIZER_RUNTIME_STATS, Boolean.class);
    }

    public static boolean logQueryPlansUsedInHistoryBasedOptimizer(Session session)
    {
        return session.getSystemProperty(LOG_QUERY_PLANS_USED_IN_HISTORY_BASED_OPTIMIZER, Boolean.class);
    }

    public static boolean enforceHistoryBasedOptimizerRegistrationTimeout(Session session)
    {
        return session.getSystemProperty(ENFORCE_HISTORY_BASED_OPTIMIZER_REGISTRATION_TIMEOUT, Boolean.class);
    }

    public static boolean shouldPushRemoteExchangeThroughGroupId(Session session)
    {
        return session.getSystemProperty(PUSH_REMOTE_EXCHANGE_THROUGH_GROUP_ID, Boolean.class);
    }

    public static boolean isNativeExecutionProcessReuseEnabled(Session session)
    {
        return session.getSystemProperty(NATIVE_EXECUTION_PROCESS_REUSE_ENABLED, Boolean.class);
    }

    public static boolean isNativeJoinBuildPartitionEnforced(Session session)
    {
        return session.getSystemProperty(NATIVE_ENFORCE_JOIN_BUILD_INPUT_PARTITION, Boolean.class);
    }

    public static RandomizeOuterJoinNullKeyStrategy getRandomizeOuterJoinNullKeyStrategy(Session session)
    {
        // If RANDOMIZE_OUTER_JOIN_NULL_KEY is set to true, return always enabled, otherwise get strategy from RANDOMIZE_OUTER_JOIN_NULL_KEY_STRATEGY
        if (session.getSystemProperty(RANDOMIZE_OUTER_JOIN_NULL_KEY, Boolean.class)) {
            return RandomizeOuterJoinNullKeyStrategy.ALWAYS;
        }
        return session.getSystemProperty(RANDOMIZE_OUTER_JOIN_NULL_KEY_STRATEGY, RandomizeOuterJoinNullKeyStrategy.class);
    }

    public static double getRandomizeOuterJoinNullKeyNullRatioThreshold(Session session)
    {
        return session.getSystemProperty(RANDOMIZE_OUTER_JOIN_NULL_KEY_NULL_RATIO_THRESHOLD, Double.class);
    }

    public static ShardedJoinStrategy getShardedJoinStrategy(Session session)
    {
        return session.getSystemProperty(SHARDED_JOINS_STRATEGY, ShardedJoinStrategy.class);
    }

    public static int getJoinShardCount(Session session)
    {
        return session.getSystemProperty(JOIN_SHARD_COUNT, Integer.class);
    }

    public static boolean isOptimizeConditionalAggregationEnabled(Session session)
    {
        return session.getSystemProperty(OPTIMIZE_CONDITIONAL_AGGREGATION_ENABLED, Boolean.class);
    }

    public static boolean isRemoveRedundantDistinctAggregationEnabled(Session session)
    {
        return session.getSystemProperty(REMOVE_REDUNDANT_DISTINCT_AGGREGATION_ENABLED, Boolean.class);
    }

    public static boolean isPrefilterForGroupbyLimit(Session session)
    {
        return session.getSystemProperty(PREFILTER_FOR_GROUPBY_LIMIT, Boolean.class);
    }

    public static boolean isMergeAggregationsWithAndWithoutFilter(Session session)
    {
        return session.getSystemProperty(MERGE_AGGREGATIONS_WITH_AND_WITHOUT_FILTER, Boolean.class);
    }

    public static boolean isInPredicatesAsInnerJoinsEnabled(Session session)
    {
        return session.getSystemProperty(IN_PREDICATES_AS_INNER_JOINS_ENABLED, Boolean.class);
    }

    public static double getPushAggregationBelowJoinByteReductionThreshold(Session session)
    {
        return session.getSystemProperty(PUSH_AGGREGATION_BELOW_JOIN_BYTE_REDUCTION_THRESHOLD, Double.class);
    }

    public static int getPrefilterForGroupbyLimitTimeoutMS(Session session)
    {
        return session.getSystemProperty(PREFILTER_FOR_GROUPBY_LIMIT_TIMEOUT_MS, Integer.class);
    }

    public static boolean isOptimizeJoinProbeForEmptyBuildRuntimeEnabled(Session session)
    {
        return session.getSystemProperty(OPTIMIZE_JOIN_PROBE_FOR_EMPTY_BUILD_RUNTIME, Boolean.class);
    }

    public static boolean useDefaultsForCorrelatedAggregationPushdownThroughOuterJoins(Session session)
    {
        return session.getSystemProperty(USE_DEFAULTS_FOR_CORRELATED_AGGREGATION_PUSHDOWN_THROUGH_OUTER_JOINS, Boolean.class);
    }

    public static boolean isMergeDuplicateAggregationsEnabled(Session session)
    {
        return session.getSystemProperty(MERGE_DUPLICATE_AGGREGATIONS, Boolean.class);
    }

    public static boolean isSimplifyPlanWithEmptyInputEnabled(Session session)
    {
        return session.getSystemProperty(SIMPLIFY_PLAN_WITH_EMPTY_INPUT, Boolean.class) || session.getSystemProperty(OPTIMIZE_JOINS_WITH_EMPTY_SOURCES, Boolean.class);
    }

    public static PushDownFilterThroughCrossJoinStrategy getPushdownFilterExpressionEvaluationThroughCrossJoinStrategy(Session session)
    {
        return session.getSystemProperty(PUSH_DOWN_FILTER_EXPRESSION_EVALUATION_THROUGH_CROSS_JOIN, PushDownFilterThroughCrossJoinStrategy.class);
    }

    public static boolean isRewriteCrossJoinOrToInnerJoinEnabled(Session session)
    {
        return session.getSystemProperty(REWRITE_CROSS_JOIN_OR_TO_INNER_JOIN, Boolean.class);
    }

    public static boolean isRewriteCrossJoinArrayContainsToInnerJoinEnabled(Session session)
    {
        return session.getSystemProperty(REWRITE_CROSS_JOIN_ARRAY_CONTAINS_TO_INNER_JOIN, Boolean.class);
    }

    public static boolean isRewriteCrossJoinArrayNotContainsToAntiJoinEnabled(Session session)
    {
        return session.getSystemProperty(REWRITE_CROSS_JOIN_ARRAY_NOT_CONTAINS_TO_ANTI_JOIN, Boolean.class);
    }

    public static LeftJoinArrayContainsToInnerJoinStrategy getLeftJoinArrayContainsToInnerJoinStrategy(Session session)
    {
        return session.getSystemProperty(REWRITE_LEFT_JOIN_ARRAY_CONTAINS_TO_EQUI_JOIN, LeftJoinArrayContainsToInnerJoinStrategy.class);
    }

    public static boolean isRewriteLeftJoinNullFilterToSemiJoinEnabled(Session session)
    {
        return session.getSystemProperty(REWRITE_LEFT_JOIN_NULL_FILTER_TO_SEMI_JOIN, Boolean.class);
    }

    public static boolean isUseBroadcastJoinWhenBuildSizeSmallProbeSizeUnknownEnabled(Session session)
    {
        return session.getSystemProperty(USE_BROADCAST_WHEN_BUILDSIZE_SMALL_PROBESIDE_UNKNOWN, Boolean.class);
    }

    public static boolean isAddPartialNodeForRowNumberWithLimit(Session session)
    {
        return session.getSystemProperty(ADD_PARTIAL_NODE_FOR_ROW_NUMBER_WITH_LIMIT, Boolean.class);
    }

    public static boolean isRewriteCaseToMapEnabled(Session session)
    {
        return session.getSystemProperty(REWRITE_CASE_TO_MAP_ENABLED, Boolean.class);
    }

    public static boolean isPullExpressionFromLambdaEnabled(Session session)
    {
        return session.getSystemProperty(PULL_EXPRESSION_FROM_LAMBDA_ENABLED, Boolean.class);
    }

    public static boolean isRwriteConstantArrayContainsToInExpressionEnabled(Session session)
    {
        return session.getSystemProperty(REWRITE_CONSTANT_ARRAY_CONTAINS_TO_IN_EXPRESSION, Boolean.class);
    }

    public static boolean shouldInferInequalityPredicates(Session session)
    {
        return session.getSystemProperty(INFER_INEQUALITY_PREDICATES, Boolean.class);
    }

    public static boolean useHistoryBasedScaledWriters(Session session)
    {
        return session.getSystemProperty(ENABLE_HISTORY_BASED_SCALED_WRITER, Boolean.class);
    }

    public static boolean usePartialAggregationHistory(Session session)
    {
        return session.getSystemProperty(USE_PARTIAL_AGGREGATION_HISTORY, Boolean.class);
    }

    public static boolean trackPartialAggregationHistory(Session session)
    {
        return session.getSystemProperty(TRACK_PARTIAL_AGGREGATION_HISTORY, Boolean.class);
    }

    public static boolean isRemoveRedundantCastToVarcharInJoinEnabled(Session session)
    {
        return session.getSystemProperty(REMOVE_REDUNDANT_CAST_TO_VARCHAR_IN_JOIN, Boolean.class);
    }

    public static boolean isRemoveMapCastEnabled(Session session)
    {
        return session.getSystemProperty(REMOVE_MAP_CAST, Boolean.class);
    }

    public static boolean shouldHandleComplexEquiJoins(Session session)
    {
        return session.getSystemProperty(HANDLE_COMPLEX_EQUI_JOINS, Boolean.class);
    }

    public static boolean skipHashGenerationForJoinWithTableScanInput(Session session)
    {
        return session.getSystemProperty(SKIP_HASH_GENERATION_FOR_JOIN_WITH_TABLE_SCAN_INPUT, Boolean.class);
    }

    public static boolean shouldGenerateDomainFilters(Session session)
    {
        return session.getSystemProperty(GENERATE_DOMAIN_FILTERS, Boolean.class);
    }

    public static boolean isRewriteExpressionWithConstantEnabled(Session session)
    {
        return session.getSystemProperty(REWRITE_EXPRESSION_WITH_CONSTANT_EXPRESSION, Boolean.class);
    }

    public static boolean isEagerPlanValidationEnabled(Session session)
    {
        return session.getSystemProperty(EAGER_PLAN_VALIDATION_ENABLED, Boolean.class);
    }

    public static CreateView.Security getDefaultViewSecurityMode(Session session)
    {
        return session.getSystemProperty(DEFAULT_VIEW_SECURITY_MODE, CreateView.Security.class);
    }

    public static boolean isJoinPrefilterEnabled(Session session)
    {
        return session.getSystemProperty(JOIN_PREFILTER_BUILD_SIDE, Boolean.class);
    }

    public static boolean isPrintEstimatedStatsFromCacheEnabled(Session session)
    {
        return session.getSystemProperty(PRINT_ESTIMATED_STATS_FROM_CACHE, Boolean.class);
    }

    public static boolean isRemoveCrossJoinWithConstantSingleRowInputEnabled(Session session)
    {
        return session.getSystemProperty(REMOVE_CROSS_JOIN_WITH_CONSTANT_SINGLE_ROW_INPUT, Boolean.class);
    }

    public static boolean shouldOptimizerUseHistograms(Session session)
    {
        return session.getSystemProperty(OPTIMIZER_USE_HISTOGRAMS, Boolean.class);
    }

    public static boolean warnOnCommonNanPatterns(Session session)
    {
        return session.getSystemProperty(WARN_ON_COMMON_NAN_PATTERNS, Boolean.class);
    }

    public static boolean isInlineProjectionsOnValues(Session session)
    {
        return session.getSystemProperty(INLINE_PROJECTIONS_ON_VALUES, Boolean.class);
    }

    public static boolean isIncludeValuesNodeInConnectorOptimizer(Session session)
    {
        return session.getSystemProperty(INCLUDE_VALUES_NODE_IN_CONNECTOR_OPTIMIZER, Boolean.class);
    }

    public static Boolean isInnerJoinPushdownEnabled(Session session)
    {
        return session.getSystemProperty(INNER_JOIN_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static Boolean isInEqualityPushdownEnabled(Session session)
    {
        return session.getSystemProperty(INEQUALITY_JOIN_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static int getMinColumnarEncodingChannelsToPreferRowWiseEncoding(Session session)
    {
        return session.getSystemProperty(NATIVE_MIN_COLUMNAR_ENCODING_CHANNELS_TO_PREFER_ROW_WISE_ENCODING, Integer.class);
    }

    public static boolean isNativeExecutionScaleWritersThreadsEnabled(Session session)
    {
        return session.getSystemProperty(NATIVE_EXECUTION_SCALE_WRITER_THREADS_ENABLED, Boolean.class);
    }

    public static boolean isNativeExecutionTypeRewriteEnabled(Session session)
    {
        return session.getSystemProperty(NATIVE_EXECUTION_TYPE_REWRITE_ENABLED, Boolean.class);
    }

    public static String getExpressionOptimizerName(Session session)
    {
        return session.getSystemProperty(EXPRESSION_OPTIMIZER_NAME, String.class);
    }

    public static boolean isBroadcastSemiJoinForDeleteEnabled(Session session)
    {
        return session.getSystemProperty(BROADCAST_SEMI_JOIN_FOR_DELETE, Boolean.class);
    }

    public static boolean isEnabledAddExchangeBelowGroupId(Session session)
    {
        return session.getSystemProperty(ADD_EXCHANGE_BELOW_PARTIAL_AGGREGATION_OVER_GROUP_ID, Boolean.class);
    }

    public static boolean isPushSubfieldsForMapFunctionsEnabled(Session session)
    {
        return session.getSystemProperty(PUSHDOWN_SUBFIELDS_FOR_MAP_FUNCTIONS, Boolean.class);
    }

    public static boolean isUtilizeUniquePropertyInQueryPlanningEnabled(Session session)
    {
        return session.getSystemProperty(UTILIZE_UNIQUE_PROPERTY_IN_QUERY_PLANNING, Boolean.class);
    }

    public static boolean isAddDistinctBelowSemiJoinBuildEnabled(Session session)
    {
        return session.getSystemProperty(ADD_DISTINCT_BELOW_SEMI_JOIN_BUILD, Boolean.class);
    }

    public static boolean isCanonicalizedJsonExtract(Session session)
    {
        return session.getSystemProperty(CANONICALIZED_JSON_EXTRACT, Boolean.class);
    }

    public static Duration getQueryClientTimeout(Session session)
    {
        return session.getSystemProperty(QUERY_CLIENT_TIMEOUT, Duration.class);
    }

    public static boolean isOptimizeConditionalApproxDistinctEnabled(Session session)
    {
        return session.getSystemProperty(OPTIMIZE_CONDITIONAL_CONSTANT_APPROXIMATE_DISTINCT, Boolean.class);
    }

    public static long getMaxSerializableObjectSize(Session session)
    {
        return session.getSystemProperty(MAX_SERIALIZABLE_OBJECT_SIZE, Long.class);
    }
}
