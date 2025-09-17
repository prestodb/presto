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
#pragma once

#include "presto_cpp/external/json/nlohmann/json.hpp"
#include "velox/type/Type.h"

using json = nlohmann::json;

namespace facebook::presto {

/// This is the interface of the session property.
/// Note: This interface should align with java coordinator.
class SessionProperty {
 public:
  SessionProperty(
      const std::string& name,
      const std::string& description,
      const std::string& type,
      bool hidden,
      const std::string& veloxConfigName,
      const std::string& defaultValue)
      : name_(name),
        description_(description),
        type_(type),
        hidden_(hidden),
        veloxConfigName_(veloxConfigName),
        defaultValue_(defaultValue),
        value_(defaultValue) {}

  const std::string getVeloxConfigName() {
    return veloxConfigName_;
  }

  void updateValue(const std::string& value) {
    value_ = value;
  }

  bool operator==(const SessionProperty& other) const {
    return name_ == other.name_ && description_ == other.description_ &&
        type_ == other.type_ && hidden_ == other.hidden_ &&
        veloxConfigName_ == other.veloxConfigName_ &&
        defaultValue_ == other.defaultValue_;
  }

  json serialize();

 private:
  const std::string name_;
  const std::string description_;

  // Datatype of presto native property.
  const std::string type_;
  const bool hidden_;
  const std::string veloxConfigName_;
  const std::string defaultValue_;
  std::string value_;
};

/// Defines all system session properties supported by native worker to ensure
/// that they are the source of truth and to differentiate them from Java based
/// session properties. Also maps the native session properties to velox.
class SessionProperties {
 public:
  /// Enable simplified path in expression evaluation.
  static constexpr const char* kExprEvalSimplified =
      "native_simplified_expression_evaluation_enabled";

  /// Reduce() function will throw an error if it encounters an array of size
  /// greater than this value.
  static constexpr const char* kExprMaxArraySizeInReduce =
      "native_expression_max_array_size_in_reduce";

  /// Controls maximum number of compiled regular expression patterns per
  /// regular expression function instance per thread of execution.
  static constexpr const char* kExprMaxCompiledRegexes =
      "native_expression_max_compiled_regexes";

  /// The maximum memory used by partial aggregation when data reduction is not
  /// optimal.
  static constexpr const char* kMaxPartialAggregationMemory =
      "native_max_partial_aggregation_memory";

  /// The max partial aggregation memory when data reduction is optimal.
  /// When good data reduction is achieved through partial aggregation, more
  /// memory would be given even when we reach limit of
  /// kMaxPartialAggregationMemory.
  static constexpr const char* kMaxExtendedPartialAggregationMemory =
      "native_max_extended_partial_aggregation_memory";

  /// Enable join spilling on native engine.
  static constexpr const char* kJoinSpillEnabled = "native_join_spill_enabled";

  /// The maximum allowed spilling level for hash join build.
  static constexpr const char* kMaxSpillLevel = "native_max_spill_level";

  /// The maximum allowed spill file size.
  static constexpr const char* kMaxSpillFileSize = "native_max_spill_file_size";

  static constexpr const char* kMaxSpillBytes = "native_max_spill_bytes";

  /// Enable row number spilling on native engine.
  static constexpr const char* kRowNumberSpillEnabled =
      "native_row_number_spill_enabled";

  /// The compression algorithm type to compress the spilled data.
  static constexpr const char* kSpillCompressionCodec =
      "native_spill_compression_codec";

  /// The maximum size in bytes to buffer the serialized spill data before
  /// writing to disk for IO efficiency.
  static constexpr const char* kSpillWriteBufferSize =
      "native_spill_write_buffer_size";

  /// Config used to create spill files. This config is provided to underlying
  /// file system and the config is free form.
  static constexpr const char* kSpillFileCreateConfig =
      "native_spill_file_create_config";

  /// Enable window spilling on native engine.
  static constexpr const char* kWindowSpillEnabled =
      "native_window_spill_enabled";

  /// Enable writer spilling on native engine.
  static constexpr const char* kWriterSpillEnabled =
      "native_writer_spill_enabled";

  /// Minimum memory footprint size required to reclaim memory from a file
  /// writer by flushing its buffered data to disk.
  static constexpr const char* kWriterFlushThresholdBytes =
      "native_writer_flush_threshold_bytes";

  /// The number of bits (N) used to calculate the spilling partition number for
  /// hash join and RowNumber: 2 ^ N
  static constexpr const char* kSpillerNumPartitionBits =
      "native_spiller_num_partition_bits";

  /// Enable topN row number spilling on native engine.
  static constexpr const char* kTopNRowNumberSpillEnabled =
      "native_topn_row_number_spill_enabled";

  /// If set to true, then during execution of tasks, the output vectors of
  /// every operator are validated for consistency. This is an expensive check
  /// so should only be used for debugging.
  static constexpr const char* kValidateOutputFromOperators =
      "native_debug_validate_output_from_operators";

  /// Disable optimization in expression evaluation to peel common dictionary
  /// layer from inputs.
  static constexpr const char* kDebugDisableExpressionWithPeeling =
      "native_debug_disable_expression_with_peeling";

  /// Disable optimization in expression evaluation to reuse cached results for
  /// common sub-expressions.
  static constexpr const char* kDebugDisableCommonSubExpressions =
      "native_debug_disable_common_sub_expressions";

  /// Disable optimization in expression evaluation to reuse cached results
  /// between subsequent input batches that are dictionary encoded and have the
  /// same alphabet(underlying flat vector).
  static constexpr const char* kDebugDisableExpressionWithMemoization =
      "native_debug_disable_expression_with_memoization";

  /// Disable optimization in expression evaluation to delay loading of lazy
  /// inputs unless required.
  static constexpr const char* kDebugDisableExpressionWithLazyInputs =
      "native_debug_disable_expression_with_lazy_inputs";

  /// Regex for filtering on memory pool name if not empty. This allows us to
  /// only track the callsites of memory allocations for memory pools whose name
  /// matches the specified regular expression. Empty string means no match for
  /// all.
  static constexpr const char* kDebugMemoryPoolNameRegex =
      "native_debug_memory_pool_name_regex";

  /// Warning threshold in bytes for memory pool allocations. Logs callsites 
  /// when exceeded. Requires allocation tracking to be enabled with 
  /// `native_debug_memory_pool_name_regex` property for the pool.
  static constexpr const char* kDebugMemoryPoolWarnThresholdBytes =
      "native_debug_memory_pool_warn_threshold_bytes";

  /// Temporary flag to control whether selective Nimble reader should be used
  /// in this query or not.  Will be removed after the selective Nimble reader
  /// is fully rolled out.
  static constexpr const char* kSelectiveNimbleReaderEnabled =
      "native_selective_nimble_reader_enabled";

  /// The max ratio of a query used memory to its max capacity, and the scale
  /// writer exchange stops scaling writer processing if the query's current
  /// memory usage exceeds this ratio. The value is in the range of (0, 1].
  static constexpr const char* kScaleWriterRebalanceMaxMemoryUsageRatio =
      "native_scaled_writer_rebalance_max_memory_usage_ratio";

  /// The max number of logical table partitions that can be assigned to a
  /// single table writer thread. The logical table partition is used by local
  /// exchange writer for writer scaling, and multiple physical table
  /// partitions can be mapped to the same logical table partition based on the
  /// hash value of calculated partitioned ids.
  static constexpr const char* kScaleWriterMaxPartitionsPerWriter =
      "native_scaled_writer_max_partitions_per_writer";

  /// Minimum amount of data processed by a logical table partition to trigger
  /// writer scaling if it is detected as overloaded by scale writer exchange.
  static constexpr const char*
      kScaleWriterMinPartitionProcessedBytesRebalanceThreshold =
          "native_scaled_writer_min_partition_processed_bytes_rebalance_threshold";

  /// Minimum amount of data processed by all the logical table partitions to
  /// trigger skewed partition rebalancing by scale writer exchange.
  static constexpr const char* kScaleWriterMinProcessedBytesRebalanceThreshold =
      "native_scaled_writer_min_processed_bytes_rebalance_threshold";

  /// Enable timezone-less timestamp conversions.
  static constexpr const char* kLegacyTimestamp = "legacy_timestamp";

  /// Specifies the cpu time slice limit in ms that a driver thread
  /// can continuously run without yielding.
  static constexpr const char* kDriverCpuTimeSliceLimitMs =
      "driver_cpu_time_slice_limit_ms";

  /// Enables query tracing.
  static constexpr const char* kQueryTraceEnabled =
      "native_query_trace_enabled";

  /// Base dir of a query to store tracing data.
  static constexpr const char* kQueryTraceDir = "native_query_trace_dir";

  /// The plan node id whose input data will be traced.
  static constexpr const char* kQueryTraceNodeId = "native_query_trace_node_id";

  /// The max trace bytes limit. Tracing is disabled if zero.
  static constexpr const char* kQueryTraceMaxBytes =
      "native_query_trace_max_bytes";

  /// Config used to create operator trace directory. This config is provided to
  /// underlying file system and the config is free form. The form should be
  /// defined by the underlying file system.
  static constexpr const char* kOpTraceDirectoryCreateConfig =
      "native_op_trace_directory_create_config";

  /// The fragment id of the traced task. Used to construct
  /// the regular expression for matching
  static constexpr const char* kQueryTraceFragmentId =
      "native_query_trace_fragment_id";

  /// The shard id of the traced task. Used to construct
  /// the regular expression for matching
  static constexpr const char* kQueryTraceShardId =
      "native_query_trace_shard_id";

  /// The maximum size in bytes for the task's buffered output. The buffer is
  /// shared among all drivers.
  static constexpr const char* kMaxOutputBufferSize =
      "native_max_output_buffer_size";

  /// The maximum bytes to buffer per PartitionedOutput operator to avoid
  /// creating tiny SerializedPages. For
  /// PartitionedOutputNode::Kind::kPartitioned, PartitionedOutput operator
  /// would buffer up to that number of bytes / number of destinations for each
  /// destination before producing a SerializedPage.
  static constexpr const char* kMaxPartitionedOutputBufferSize =
      "native_max_page_partitioning_buffer_size";

  /// Maximum number of partitions created by a local exchange.
  /// Affects concurrency for pipelines containing LocalPartitionNode.
  static constexpr const char* kMaxLocalExchangePartitionCount =
      "native_max_local_exchange_partition_count";

  /// Enable the prefix sort or fallback to std::sort in spill. The prefix sort
  /// is faster than std::sort but requires the memory to build normalized
  /// prefix keys, which might have potential risk of running out of server
  /// memory.
  static constexpr const char* kSpillPrefixSortEnabled =
      "native_spill_prefixsort_enabled";

  /// Maximum number of bytes to use for the normalized key in prefix-sort. Use
  /// 0 to disable prefix-sort.
  static constexpr const char* kPrefixSortNormalizedKeyMaxBytes =
      "native_prefixsort_normalized_key_max_bytes";

  /// Minimum number of rows to use prefix-sort. The default value (130) has
  /// been derived using micro-benchmarking.
  static constexpr const char* kPrefixSortMinRows =
      "native_prefixsort_min_rows";

  /// The compression algorithm type to compress the shuffle.
  static constexpr const char* kShuffleCompressionCodec =
      "exchange_compression_codec";

  /// If set to true, enables scaled processing for table scans.
  static constexpr const char* kTableScanScaledProcessingEnabled =
      "native_table_scan_scaled_processing_enabled";

  /// Controls the ratio of available memory that can be used for scaling up
  /// table scans. The value is in the range of [0, 1].
  static constexpr const char* kTableScanScaleUpMemoryUsageRatio =
      "native_table_scan_scale_up_memory_usage_ratio";

  /// In streaming aggregation, wait until we have enough number of output rows
  /// to produce a batch of size specified by this. If set to 0, then
  /// Operator::outputBatchRows will be used as the min output batch rows.
  static constexpr const char* kStreamingAggregationMinOutputBatchRows =
      "native_streaming_aggregation_min_output_batch_rows";

  /// Maximum wait time for exchange long poll requests in seconds.
  static constexpr const char* kRequestDataSizesMaxWaitSec =
      "native_request_data_sizes_max_wait_sec";

  /// Priority of memory pool reclaimer when deciding on memory pool to abort.
  /// Lower value has higher priority and less likely to be chosen as candidate
  /// for memory pool abort.
  static constexpr const char* kNativeQueryMemoryReclaimerPriority =
      "native_query_memory_reclaimer_priority";

  /// Maximum number of splits to listen to by SplitListener on native workers.
  static constexpr const char* kMaxNumSplitsListenedTo =
      "native_max_num_splits_listened_to";

  /// Specifies the max number of input batches to prefetch to do index lookup
  /// ahead. If it is zero, then process one input batch at a time.
  static constexpr const char* kIndexLookupJoinMaxPrefetchBatches =
      "native_index_lookup_join_max_prefetch_batches";

  /// If this is true, then the index join operator might split output for each
  /// input batch based on the output batch size control. Otherwise, it tries to
  /// produce a single output for each input batch.
  static constexpr const char* kIndexLookupJoinSplitOutput =
      "native_index_lookup_join_split_output";

  /// If this is true, then the unnest operator might split output for each
  /// input batch based on the output batch size control. Otherwise, it produces
  /// a single output for each input batch.
  static constexpr const char* kUnnestSplitOutput =
      "native_unnest_split_output";

  static SessionProperties* instance();

  SessionProperties();

  /// Utility function to translate a config name in Presto to its equivalent in
  /// Velox. Returns 'name' as is if there is no mapping.
  const std::string toVeloxConfig(const std::string& name) const;

  json serialize() const;

  const std::unordered_map<std::string, std::shared_ptr<SessionProperty>>&
  testingSessionProperties() const;

 private:
  void addSessionProperty(
      const std::string& name,
      const std::string& description,
      const velox::TypePtr& type,
      bool isHidden,
      const std::string& veloxConfigName,
      const std::string& veloxDefault);

  std::unordered_map<std::string, std::shared_ptr<SessionProperty>>
      sessionProperties_;
};

} // namespace facebook::presto
