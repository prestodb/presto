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
    public static final String NATIVE_SELECTIVE_NIMBLE_READER_ENABLED = "native_selective_nimble_reader_enabled";
    public static final String NATIVE_MAX_PARTIAL_AGGREGATION_MEMORY = "native_max_partial_aggregation_memory";
    public static final String NATIVE_MAX_EXTENDED_PARTIAL_AGGREGATION_MEMORY = "native_max_extended_partial_aggregation_memory";
    public static final String NATIVE_MAX_SPILL_BYTES = "native_max_spill_bytes";
    public static final String NATIVE_MAX_PAGE_PARTITIONING_BUFFER_SIZE = "native_max_page_partitioning_buffer_size";
    public static final String NATIVE_MAX_OUTPUT_BUFFER_SIZE = "native_max_output_buffer_size";
    public static final String NATIVE_QUERY_TRACE_ENABLED = "native_query_trace_enabled";
    public static final String NATIVE_QUERY_TRACE_DIR = "native_query_trace_dir";
    public static final String NATIVE_QUERY_TRACE_NODE_IDS = "native_query_trace_node_ids";
    public static final String NATIVE_QUERY_TRACE_MAX_BYTES = "native_query_trace_max_bytes";
    public static final String NATIVE_QUERY_TRACE_REG_EXP = "native_query_trace_task_reg_exp";
    public static final String NATIVE_MAX_LOCAL_EXCHANGE_PARTITION_COUNT = "native_max_local_exchange_partition_count";
    public static final String NATIVE_SPILL_PREFIXSORT_ENABLED = "native_spill_prefixsort_enabled";
    public static final String NATIVE_PREFIXSORT_NORMALIZED_KEY_MAX_BYTES = "native_prefixsort_normalized_key_max_bytes";
    public static final String NATIVE_PREFIXSORT_MIN_ROWS = "native_prefixsort_min_rows";
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
                stringProperty(NATIVE_QUERY_TRACE_NODE_IDS,
                        "A comma-separated list of plan node ids whose input data will be traced. Empty string if only want to trace the query metadata.",
                        "",
                        !nativeExecution),
                longProperty(NATIVE_QUERY_TRACE_MAX_BYTES,
                        "The max trace bytes limit. Tracing is disabled if zero.",
                        0L,
                        !nativeExecution),
                stringProperty(NATIVE_QUERY_TRACE_REG_EXP,
                        "The regexp of traced task id. We only enable trace on a task if its id matches.",
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
                        !nativeExecution));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }
}
