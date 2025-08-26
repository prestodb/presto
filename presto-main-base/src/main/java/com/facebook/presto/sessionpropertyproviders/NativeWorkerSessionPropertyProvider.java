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

import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.session.WorkerSessionPropertyProvider;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.doubleProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.longProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static java.util.Objects.requireNonNull;

@Deprecated
public class NativeWorkerSessionPropertyProvider
        implements WorkerSessionPropertyProvider
{
    public static final String NATIVE_SIMPLIFIED_EXPRESSION_EVALUATION_ENABLED = "native_simplified_expression_evaluation_enabled";
    public static final String NATIVE_EXPRESSION_MAX_ARRAY_SIZE_IN_REDUCE = "native_expression_max_array_size_in_reduce";
    public static final String NATIVE_EXPRESSION_MAX_COMPILED_REGEXES = "native_expression_max_compiled_regexes";
    public static final String NATIVE_MAX_SPILL_LEVEL = "native_max_spill_level";
    public static final String NATIVE_MAX_SPILL_FILE_SIZE = "native_max_spill_file_size";
    public static final String NATIVE_SPILL_COMPRESSION_CODEC = "native_spill_compression_codec";
    public static final String NATIVE_SPILL_WRITE_BUFFER_SIZE = "native_spill_write_buffer_size";
    public static final String NATIVE_SPILL_FILE_CREATE_CONFIG = "native_spill_file_create_config";
    public static final String NATIVE_JOIN_SPILL_ENABLED = "native_join_spill_enabled";
    public static final String NATIVE_WINDOW_SPILL_ENABLED = "native_window_spill_enabled";
    public static final String NATIVE_WRITER_SPILL_ENABLED = "native_writer_spill_enabled";
    public static final String NATIVE_WRITER_FLUSH_THRESHOLD_BYTES = "native_writer_flush_threshold_bytes";
    public static final String NATIVE_ROW_NUMBER_SPILL_ENABLED = "native_row_number_spill_enabled";
    public static final String NATIVE_TOPN_ROW_NUMBER_SPILL_ENABLED = "native_topn_row_number_spill_enabled";
    public static final String NATIVE_SPILLER_NUM_PARTITION_BITS = "native_spiller_num_partition_bits";
    public static final String NATIVE_DEBUG_VALIDATE_OUTPUT_FROM_OPERATORS = "native_debug_validate_output_from_operators";
    public static final String NATIVE_DEBUG_DISABLE_EXPRESSION_WITH_PEELING = "native_debug_disable_expression_with_peeling";
    public static final String NATIVE_DEBUG_DISABLE_COMMON_SUB_EXPRESSION = "native_debug_disable_common_sub_expressions";
    public static final String NATIVE_DEBUG_DISABLE_EXPRESSION_WITH_MEMOIZATION = "native_debug_disable_expression_with_memoization";
    public static final String NATIVE_DEBUG_DISABLE_EXPRESSION_WITH_LAZY_INPUTS = "native_debug_disable_expression_with_lazy_inputs";
    public static final String NATIVE_DEBUG_MEMORY_POOL_NAME_REGEX = "native_debug_memory_pool_name_regex";
    public static final String NATIVE_DEBUG_MEMORY_POOL_WARN_THRESHOLD_BYTES = "native_debug_memory_pool_warn_threshold_bytes";
    public static final String NATIVE_SELECTIVE_NIMBLE_READER_ENABLED = "native_selective_nimble_reader_enabled";
    public static final String NATIVE_MAX_PARTIAL_AGGREGATION_MEMORY = "native_max_partial_aggregation_memory";
    public static final String NATIVE_MAX_EXTENDED_PARTIAL_AGGREGATION_MEMORY = "native_max_extended_partial_aggregation_memory";
    public static final String NATIVE_MAX_SPILL_BYTES = "native_max_spill_bytes";
    public static final String NATIVE_MAX_PAGE_PARTITIONING_BUFFER_SIZE = "native_max_page_partitioning_buffer_size";
    public static final String NATIVE_MAX_OUTPUT_BUFFER_SIZE = "native_max_output_buffer_size";
    public static final String NATIVE_QUERY_TRACE_ENABLED = "native_query_trace_enabled";
    public static final String NATIVE_QUERY_TRACE_DIR = "native_query_trace_dir";
    public static final String NATIVE_QUERY_TRACE_NODE_ID = "native_query_trace_node_id";
    public static final String NATIVE_QUERY_TRACE_MAX_BYTES = "native_query_trace_max_bytes";
    public static final String NATIVE_QUERY_TRACE_FRAGMENT_ID = "native_query_trace_fragment_id";
    public static final String NATIVE_QUERY_TRACE_SHARD_ID = "native_query_trace_shard_id";
    public static final String NATIVE_MAX_LOCAL_EXCHANGE_PARTITION_COUNT = "native_max_local_exchange_partition_count";
    public static final String NATIVE_SPILL_PREFIXSORT_ENABLED = "native_spill_prefixsort_enabled";
    public static final String NATIVE_PREFIXSORT_NORMALIZED_KEY_MAX_BYTES = "native_prefixsort_normalized_key_max_bytes";
    public static final String NATIVE_PREFIXSORT_MIN_ROWS = "native_prefixsort_min_rows";
    public static final String NATIVE_OP_TRACE_DIR_CREATE_CONFIG = "native_op_trace_directory_create_config";
    public static final String NATIVE_SCALED_WRITER_REBALANCE_MAX_MEMORY_USAGE_RATIO = "native_scaled_writer_rebalance_max_memory_usage_ratio";
    public static final String NATIVE_SCALED_WRITER_MAX_PARTITIONS_PER_WRITER = "native_scaled_writer_max_partitions_per_writer";
    public static final String NATIVE_SCALED_WRITER_MIN_PARTITION_PROCESSED_BYTES_REBALANCE_THRESHOLD = "native_scaled_writer_min_partition_processed_bytes_rebalance_threshold";
    public static final String NATIVE_SCALED_WRITER_MIN_PROCESSED_BYTES_REBALANCE_THRESHOLD = "native_scaled_writer_min_processed_bytes_rebalance_threshold";
    public static final String NATIVE_TABLE_SCAN_SCALED_PROCESSING_ENABLED = "native_table_scan_scaled_processing_enabled";
    public static final String NATIVE_TABLE_SCAN_SCALE_UP_MEMORY_USAGE_RATIO = "native_table_scan_scale_up_memory_usage_ratio";
    public static final String NATIVE_STREAMING_AGGREGATION_MIN_OUTPUT_BATCH_ROWS = "native_streaming_aggregation_min_output_batch_rows";
    public static final String NATIVE_REQUEST_DATA_SIZES_MAX_WAIT_SEC = "native_request_data_sizes_max_wait_sec";
    public static final String NATIVE_QUERY_MEMORY_RECLAIMER_PRIORITY = "native_query_memory_reclaimer_priority";
    public static final String NATIVE_MAX_NUM_SPLITS_LISTENED_TO = "native_max_num_splits_listened_to";
    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public NativeWorkerSessionPropertyProvider(FeaturesConfig featuresConfig)
    {
        boolean nativeExecution = requireNonNull(featuresConfig, "featuresConfig is null").isNativeExecutionEnabled();
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        NATIVE_SIMPLIFIED_EXPRESSION_EVALUATION_ENABLED,
                        "Native Execution only. Enable simplified path in expression evaluation",
                        false,
                        !nativeExecution),
                integerProperty(
                        NATIVE_EXPRESSION_MAX_ARRAY_SIZE_IN_REDUCE,
                        "Native Execution only. Reduce() function will throw an error if it encounters an array of size greater than this value.",
                        100000,
                        !nativeExecution),
                integerProperty(
                        NATIVE_EXPRESSION_MAX_COMPILED_REGEXES,
                        "Native Execution only. Controls maximum number of compiled regular expression patterns " +
                                "per regular expression function instance per thread of execution.",
                        100,
                        !nativeExecution),
                integerProperty(
                        NATIVE_MAX_SPILL_LEVEL,
                        "Native Execution only. The maximum allowed spilling level for hash join build.\n" +
                                "0 is the initial spilling level, -1 means unlimited.",
                        4,
                        !nativeExecution),
                integerProperty(
                        NATIVE_MAX_SPILL_FILE_SIZE,
                        "The max allowed spill file size. If it is zero, then there is no limit.",
                        0,
                        !nativeExecution),
                stringProperty(
                        NATIVE_SPILL_COMPRESSION_CODEC,
                        "Native Execution only. The compression algorithm type to compress the spilled data.\n " +
                                "Supported compression codecs are: ZLIB, SNAPPY, LZO, ZSTD, LZ4 and GZIP. NONE means no compression.",
                        "zstd",
                        !nativeExecution),
                longProperty(
                        NATIVE_SPILL_WRITE_BUFFER_SIZE,
                        "Native Execution only. The maximum size in bytes to buffer the serialized spill data before writing to disk for IO efficiency.\n" +
                                "If set to zero, buffering is disabled.",
                        1024L * 1024L,
                        !nativeExecution),
                stringProperty(
                        NATIVE_SPILL_FILE_CREATE_CONFIG,
                        "Native Execution only. Config used to create spill files. This config is \n" +
                                "provided to underlying file system and the config is free form. The form should be\n" +
                                "defined by the underlying file system.",
                        "",
                        !nativeExecution),
                booleanProperty(
                        NATIVE_JOIN_SPILL_ENABLED,
                        "Native Execution only. Enable join spilling on native engine",
                        false,
                        !nativeExecution),
                booleanProperty(
                        NATIVE_WINDOW_SPILL_ENABLED,
                        "Native Execution only. Enable window spilling on native engine",
                        false,
                        !nativeExecution),
                booleanProperty(
                        NATIVE_WRITER_SPILL_ENABLED,
                        "Native Execution only. Enable writer spilling on native engine",
                        false,
                        !nativeExecution),
                longProperty(
                        NATIVE_WRITER_FLUSH_THRESHOLD_BYTES,
                        "Native Execution only. Minimum memory footprint size required to reclaim memory from a file " +
                        "writer by flushing its buffered data to disk.",
                        96L << 20,
                        false),
                booleanProperty(
                        NATIVE_ROW_NUMBER_SPILL_ENABLED,
                        "Native Execution only. Enable row number spilling on native engine",
                        false,
                        !nativeExecution),
                booleanProperty(
                        NATIVE_TOPN_ROW_NUMBER_SPILL_ENABLED,
                        "Native Execution only. Enable topN row number spilling on native engine",
                        false,
                        !nativeExecution),
                integerProperty(
                        NATIVE_SPILLER_NUM_PARTITION_BITS,
                        "Native Execution only. The number of bits (N) used to calculate the " +
                                "spilling partition number for hash join and RowNumber: 2 ^ N",
                        3,
                        !nativeExecution),
                booleanProperty(
                        NATIVE_DEBUG_VALIDATE_OUTPUT_FROM_OPERATORS,
                        "If set to true, then during execution of tasks, the output vectors of " +
                                "every operator are validated for consistency. This is an expensive check " +
                                "so should only be used for debugging. It can help debug issues where " +
                                "malformed vector cause failures or crashes by helping identify which " +
                                "operator is generating them.",
                        false,
                        true),
                booleanProperty(
                        NATIVE_DEBUG_DISABLE_EXPRESSION_WITH_PEELING,
                        "If set to true, disables optimization in expression evaluation to peel common " +
                                "dictionary layer from inputs. Should only be used for debugging.",
                        false,
                        true),
                booleanProperty(
                        NATIVE_DEBUG_DISABLE_COMMON_SUB_EXPRESSION,
                        "If set to true, disables optimization in expression evaluation to reuse cached " +
                                "results for common sub-expressions. Should only be used for debugging.",
                        false,
                        true),
                booleanProperty(
                        NATIVE_DEBUG_DISABLE_EXPRESSION_WITH_MEMOIZATION,
                        "If set to true, disables optimization in expression evaluation to reuse cached " +
                                "results between subsequent input batches that are dictionary encoded and " +
                                "have the same alphabet(underlying flat vector). Should only be used for " +
                                "debugging.",
                        false,
                        true),
                booleanProperty(
                        NATIVE_DEBUG_DISABLE_EXPRESSION_WITH_LAZY_INPUTS,
                        "If set to true, disables optimization in expression evaluation to delay loading " +
                                "of lazy inputs unless required. Should only be used for debugging.",
                        false,
                        true),
                stringProperty(
                        NATIVE_DEBUG_MEMORY_POOL_NAME_REGEX,
                        "Regex for filtering on memory pool name if not empty." +
                                " This allows us to only track the callsites of memory allocations for" +
                                " memory pools whose name matches the specified regular expression. Empty" +
                                " string means no match for all.",
                        "",
                        true),
                stringProperty(
                        NATIVE_DEBUG_MEMORY_POOL_WARN_THRESHOLD_BYTES,
                        "Warning threshold in bytes for debug memory pools. When set to a " +
                                "non-zero value, a warning will be logged once per memory pool when " +
                                "allocations cause the pool to exceed this threshold. This is useful for " +
                                "identifying memory usage patterns during debugging. A value of " +
                                "0 means no warning threshold is enforced.",
                        "0B",
                        true),
                booleanProperty(
                        NATIVE_SELECTIVE_NIMBLE_READER_ENABLED,
                        "Temporary flag to control whether selective Nimble reader should be " +
                                "used in this query or not.  Will be removed after the selective Nimble " +
                                "reader is fully rolled out.",
                        false,
                        !nativeExecution),
                longProperty(
                        NATIVE_MAX_PARTIAL_AGGREGATION_MEMORY,
                        "The max partial aggregation memory when data reduction is not optimal.",
                        1L << 24,
                        !nativeExecution),
                longProperty(
                        NATIVE_MAX_EXTENDED_PARTIAL_AGGREGATION_MEMORY,
                        "The max partial aggregation memory when data reduction is optimal.",
                        1L << 26,
                        !nativeExecution),
                longProperty(
                        NATIVE_MAX_SPILL_BYTES,
                        "The max allowed spill bytes",
                        100L << 30,
                        !nativeExecution),
                booleanProperty(NATIVE_QUERY_TRACE_ENABLED,
                        "Enables query tracing.",
                        false,
                        !nativeExecution),
                stringProperty(NATIVE_QUERY_TRACE_DIR,
                        "Base dir of a query to store tracing data.",
                        "",
                        !nativeExecution),
                stringProperty(NATIVE_QUERY_TRACE_NODE_ID,
                        "The plan node id whose input data will be traced.",
                        "",
                        !nativeExecution),
                longProperty(NATIVE_QUERY_TRACE_MAX_BYTES,
                        "The max trace bytes limit. Tracing is disabled if zero.",
                        0L,
                        !nativeExecution),
                stringProperty(NATIVE_OP_TRACE_DIR_CREATE_CONFIG,
                        "Config used to create operator trace directory. This config is provided to underlying file system and the config is free form. The form should be defined by the underlying file system.",
                        "",
                        !nativeExecution),
                stringProperty(NATIVE_QUERY_TRACE_FRAGMENT_ID,
                            "The fragment id of the traced task.",
                        "",
                        !nativeExecution),
                stringProperty(NATIVE_QUERY_TRACE_SHARD_ID,
                        "The shard id of the traced task.",
                        "",
                        !nativeExecution),
                longProperty(NATIVE_MAX_OUTPUT_BUFFER_SIZE,
                        "The maximum size in bytes for the task's buffered output. The buffer is shared among all drivers.",
                        200L << 20,
                        !nativeExecution),
                longProperty(NATIVE_MAX_PAGE_PARTITIONING_BUFFER_SIZE,
                        "The maximum bytes to buffer per PartitionedOutput operator to avoid creating tiny " +
                                "SerializedPages. For PartitionedOutputNode::Kind::kPartitioned, PartitionedOutput operator " +
                                "would buffer up to that number of bytes / number of destinations for each destination before " +
                                "producing a SerializedPage.",
                        24L << 20,
                        !nativeExecution),
                integerProperty(
                        NATIVE_MAX_LOCAL_EXCHANGE_PARTITION_COUNT,
                        "Maximum number of partitions created by a local exchange. " +
                                "Affects concurrency for pipelines containing LocalPartitionNode",
                        null,
                        !nativeExecution),
                booleanProperty(
                        NATIVE_SPILL_PREFIXSORT_ENABLED,
                        "Enable the prefix sort or fallback to std::sort in spill. " +
                                "The prefix sort is faster than std::sort but requires the memory to build normalized " +
                                "prefix keys, which might have potential risk of running out of server memory.",
                        false,
                        !nativeExecution),
                integerProperty(
                        NATIVE_PREFIXSORT_NORMALIZED_KEY_MAX_BYTES,
                        "Maximum number of bytes to use for the normalized key in prefix-sort. " +
                                "Use 0 to disable prefix-sort.",
                        128,
                        !nativeExecution),
                integerProperty(
                        NATIVE_PREFIXSORT_MIN_ROWS,
                        "Minimum number of rows to use prefix-sort. " +
                                "The default value (130) has been derived using micro-benchmarking.",
                        130,
                        !nativeExecution),
                doubleProperty(
                        NATIVE_SCALED_WRITER_REBALANCE_MAX_MEMORY_USAGE_RATIO,
                        "The max ratio of a query used memory to its max capacity, " +
                                "and the scale writer exchange stops scaling writer processing if the query's current " +
                                "memory usage exceeds this ratio. The value is in the range of (0, 1].",
                        0.7,
                        !nativeExecution),
                integerProperty(
                        NATIVE_SCALED_WRITER_MAX_PARTITIONS_PER_WRITER,
                        "The max number of logical table partitions that can be assigned to a " +
                                "single table writer thread. The logical table partition is used by local " +
                                "exchange writer for writer scaling, and multiple physical table " +
                                "partitions can be mapped to the same logical table partition based on the " +
                                "hash value of calculated partitioned ids",
                        128,
                        !nativeExecution),
                longProperty(
                        NATIVE_SCALED_WRITER_MIN_PARTITION_PROCESSED_BYTES_REBALANCE_THRESHOLD,
                        "Minimum amount of data processed by a logical table partition " +
                                "to trigger writer scaling if it is detected as overloaded by scale writer exchange.",
                        128L << 20,
                        !nativeExecution),
                longProperty(
                        NATIVE_SCALED_WRITER_MIN_PROCESSED_BYTES_REBALANCE_THRESHOLD,
                        "Minimum amount of data processed by all the logical table partitions " +
                                "to trigger skewed partition rebalancing by scale writer exchange.",
                        256L << 20,
                        !nativeExecution),
                booleanProperty(
                        NATIVE_TABLE_SCAN_SCALED_PROCESSING_ENABLED,
                        "If set to true, enables scaling the table scan concurrency on each worker.",
                        false,
                        !nativeExecution),
                doubleProperty(
                        NATIVE_TABLE_SCAN_SCALE_UP_MEMORY_USAGE_RATIO,
                        "The query memory usage ratio used by scan controller to decide if it can " +
                                "increase the number of running scan threads. When the query memory usage " +
                                "is below this ratio, the scan controller keeps increasing the running scan " +
                                "thread for scale up, and stop once exceeds this ratio. The value is in the " +
                                "range of [0, 1].",
                        0.7,
                        !nativeExecution),
                integerProperty(
                        NATIVE_STREAMING_AGGREGATION_MIN_OUTPUT_BATCH_ROWS,
                        "In streaming aggregation, wait until we have enough number of output rows " +
                                "to produce a batch of size specified by this. If set to 0, then " +
                                "Operator::outputBatchRows will be used as the min output batch rows.",
                        0,
                        !nativeExecution),
                integerProperty(
                        NATIVE_REQUEST_DATA_SIZES_MAX_WAIT_SEC,
                        "Maximum wait time for exchange long poll requests in seconds.",
                        10,
                        !nativeExecution),
                integerProperty(
                        NATIVE_QUERY_MEMORY_RECLAIMER_PRIORITY,
                        "Native Execution only. Priority of memory recliamer when deciding on memory pool to abort." +
                        "Lower value has higher priority and less likely to be choosen for memory pool abort",
                        2147483647,
                        !nativeExecution),
                integerProperty(
                        NATIVE_MAX_NUM_SPLITS_LISTENED_TO,
                        "Maximum number of splits to listen to per table scan node per worker.",
                        0,
                        !nativeExecution));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }
}
