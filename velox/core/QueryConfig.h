/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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

#include "velox/core/Config.h"

namespace facebook::velox::core {
enum class CapacityUnit {
  BYTE,
  KILOBYTE,
  MEGABYTE,
  GIGABYTE,
  TERABYTE,
  PETABYTE
};

double toBytesPerCapacityUnit(CapacityUnit unit);

CapacityUnit valueOfCapacityUnit(const std::string& unitStr);

/// Convert capacity string with unit to the capacity number in the specified
/// units
uint64_t toCapacity(const std::string& from, CapacityUnit to);

std::chrono::duration<double> toDuration(const std::string& str);

/// A simple wrapper around velox::Config. Defines constants for query
/// config properties and accessor methods.
/// Create per query context. Does not have a singleton instance.
/// Does not allow altering properties on the fly. Only at creation time.
class QueryConfig {
 public:
  explicit QueryConfig(
      const std::unordered_map<std::string, std::string>& values);

  explicit QueryConfig(std::unordered_map<std::string, std::string>&& values);

  static constexpr const char* kCodegenEnabled = "codegen.enabled";

  /// Maximum memory that a query can use on a single host.
  static constexpr const char* kQueryMaxMemoryPerNode =
      "query_max_memory_per_node";

  static constexpr const char* kCodegenConfigurationFilePath =
      "codegen.configuration_file_path";

  static constexpr const char* kCodegenLazyLoading = "codegen.lazy_loading";

  /// User provided session timezone. Stores a string with the actual timezone
  /// name, e.g: "America/Los_Angeles".
  static constexpr const char* kSessionTimezone = "session_timezone";

  /// If true, timezone-less timestamp conversions (e.g. string to timestamp,
  /// when the string does not specify a timezone) will be adjusted to the user
  /// provided session timezone (if any).
  ///
  /// For instance:
  ///
  ///  if this option is true and user supplied "America/Los_Angeles",
  ///  "1970-01-01" will be converted to -28800 instead of 0.
  ///
  /// False by default.
  static constexpr const char* kAdjustTimestampToTimezone =
      "adjust_timestamp_to_session_timezone";

  /// Whether to use the simplified expression evaluation path. False by
  /// default.
  static constexpr const char* kExprEvalSimplified =
      "expression.eval_simplified";

  /// Whether to track CPU usage for individual expressions (supported by call
  /// and cast expressions). False by default. Can be expensive when processing
  /// small batches, e.g. < 10K rows.
  static constexpr const char* kExprTrackCpuUsage =
      "expression.track_cpu_usage";

  /// Whether to track CPU usage for stages of individual operators. True by
  /// default. Can be expensive when processing small batches, e.g. < 10K rows.
  static constexpr const char* kOperatorTrackCpuUsage =
      "track_operator_cpu_usage";

  /// Flags used to configure the CAST operator:

  static constexpr const char* kLegacyCast = "legacy_cast";

  /// This flag makes the Row conversion to by applied in a way that the casting
  /// row field are matched by name instead of position.
  static constexpr const char* kCastMatchStructByName =
      "cast_match_struct_by_name";

  /// Used for backpressure to block local exchange producers when the local
  /// exchange buffer reaches or exceeds this size.
  static constexpr const char* kMaxLocalExchangeBufferSize =
      "max_local_exchange_buffer_size";

  /// Maximum size in bytes to accumulate in ExchangeQueue. Enforced
  /// approximately, not strictly.
  static constexpr const char* kMaxExchangeBufferSize =
      "exchange.max_buffer_size";

  /// Maximum size in bytes to accumulate among all sources of the merge
  /// exchange. Enforced approximately, not strictly.
  static constexpr const char* kMaxMergeExchangeBufferSize =
      "merge_exchange.max_buffer_size";

  static constexpr const char* kMaxPartialAggregationMemory =
      "max_partial_aggregation_memory";

  static constexpr const char* kMaxExtendedPartialAggregationMemory =
      "max_extended_partial_aggregation_memory";

  static constexpr const char* kAbandonPartialAggregationMinRows =
      "abandon_partial_aggregation_min_rows";

  static constexpr const char* kAbandonPartialAggregationMinPct =
      "abandon_partial_aggregation_min_pct";

  static constexpr const char* kAbandonPartialTopNRowNumberMinRows =
      "abandon_partial_topn_row_number_min_rows";

  static constexpr const char* kAbandonPartialTopNRowNumberMinPct =
      "abandon_partial_topn_row_number_min_pct";

  static constexpr const char* kMaxPartitionedOutputBufferSize =
      "max_page_partitioning_buffer_size";

  static constexpr const char* kMaxOutputBufferSize = "max_output_buffer_size";

  /// Preferred size of batches in bytes to be returned by operators from
  /// Operator::getOutput. It is used when an estimate of average row size is
  /// known. Otherwise kPreferredOutputBatchRows is used.
  static constexpr const char* kPreferredOutputBatchBytes =
      "preferred_output_batch_bytes";

  /// Preferred number of rows to be returned by operators from
  /// Operator::getOutput. It is used when an estimate of average row size is
  /// not known. When the estimate of average row size is known,
  /// kPreferredOutputBatchBytes is used.
  static constexpr const char* kPreferredOutputBatchRows =
      "preferred_output_batch_rows";

  /// Max number of rows that could be return by operators from
  /// Operator::getOutput. It is used when an estimate of average row size is
  /// known and kPreferredOutputBatchBytes is used to compute the number of
  /// output rows.
  static constexpr const char* kMaxOutputBatchRows = "max_output_batch_rows";

  /// TableScan operator will exit getOutput() method after this many
  /// milliseconds even if it has no data to return yet. Zero means 'no time
  /// limit'.
  static constexpr const char* kTableScanGetOutputTimeLimitMs =
      "table_scan_getoutput_time_limit_ms";

  /// If false, the 'group by' code is forced to use generic hash mode
  /// hashtable.
  static constexpr const char* kHashAdaptivityEnabled =
      "hash_adaptivity_enabled";

  /// If true, the conjunction expression can reorder inputs based on the time
  /// taken to calculate them.
  static constexpr const char* kAdaptiveFilterReorderingEnabled =
      "adaptive_filter_reordering_enabled";

  /// Global enable spilling flag.
  static constexpr const char* kSpillEnabled = "spill_enabled";

  /// Aggregation spilling flag, only applies if "spill_enabled" flag is set.
  static constexpr const char* kAggregationSpillEnabled =
      "aggregation_spill_enabled";

  /// Join spilling flag, only applies if "spill_enabled" flag is set.
  static constexpr const char* kJoinSpillEnabled = "join_spill_enabled";

  /// OrderBy spilling flag, only applies if "spill_enabled" flag is set.
  static constexpr const char* kOrderBySpillEnabled = "order_by_spill_enabled";

  /// Window spilling flag, only applies if "spill_enabled" flag is set.
  static constexpr const char* kWindowSpillEnabled = "window_spill_enabled";

  /// If true, the memory arbitrator will reclaim memory from table writer by
  /// flushing its buffered data to disk.
  static constexpr const char* kWriterSpillEnabled = "writer_spill_enabled";

  /// RowNumber spilling flag, only applies if "spill_enabled" flag is set.
  static constexpr const char* kRowNumberSpillEnabled =
      "row_number_spill_enabled";

  /// TopNRowNumber spilling flag, only applies if "spill_enabled" flag is set.
  static constexpr const char* kTopNRowNumberSpillEnabled =
      "topn_row_number_spill_enabled";

  /// The max memory that a final aggregation can use before spilling. If it 0,
  /// then there is no limit.
  static constexpr const char* kAggregationSpillMemoryThreshold =
      "aggregation_spill_memory_threshold";

  /// The max memory that a hash join can use before spilling. If it 0, then
  /// there is no limit.
  static constexpr const char* kJoinSpillMemoryThreshold =
      "join_spill_memory_threshold";

  /// The max memory that an order by can use before spilling. If it 0, then
  /// there is no limit.
  static constexpr const char* kOrderBySpillMemoryThreshold =
      "order_by_spill_memory_threshold";

  /// The max row numbers to fill and spill for each spill run. This is used to
  /// cap the memory used for spilling. If it is zero, then there is no limit
  /// and spilling might run out of memory.
  /// Based on offline test results, the default value is set to 12 million rows
  /// which uses ~128MB memory when to fill a spill run.
  static constexpr const char* kMaxSpillRunRows = "max_spill_run_rows";

  /// The max spill bytes limit set for each query. This is used to cap the
  /// storage used for spilling. If it is zero, then there is no limit and
  /// spilling might exhaust the storage or takes too long to run. The default
  /// value is set to 100 GB.
  static constexpr const char* kMaxSpillBytes = "max_spill_bytes";

  static constexpr const char* kTestingSpillPct = "testing.spill_pct";

  /// The max allowed spilling level with zero being the initial spilling level.
  /// This only applies for hash build spilling which might trigger recursive
  /// spilling when the build table is too big. If it is set to -1, then there
  /// is no limit and then some extreme large query might run out of spilling
  /// partition bits (see kSpillPartitionBits) at the end. The max spill level
  /// is used in production to prevent some bad user queries from using too much
  /// io and cpu resources.
  static constexpr const char* kMaxSpillLevel = "max_spill_level";

  /// The max allowed spill file size. If it is zero, then there is no limit.
  static constexpr const char* kMaxSpillFileSize = "max_spill_file_size";

  /// The min spill run size limit used to select partitions for spilling. The
  /// spiller tries to spill a previously spilled partitions if its data size
  /// exceeds this limit, otherwise it spills the partition with most data.
  /// If the limit is zero, then the spiller always spill a previously spilled
  /// partition if it has any data. This is to avoid spill from a partition with
  /// a small amount of data which might result in generating too many small
  /// spilled files.
  static constexpr const char* kMinSpillRunSize = "min_spill_run_size";

  static constexpr const char* kSpillCompressionKind =
      "spill_compression_codec";

  /// Specifies spill write buffer size in bytes. The spiller tries to buffer
  /// serialized spill data up to the specified size before write to storage
  /// underneath for io efficiency. If it is set to zero, then spill write
  /// buffering is disabled.
  static constexpr const char* kSpillWriteBufferSize =
      "spill_write_buffer_size";

  /// Config used to create spill files. This config is provided to underlying
  /// file system and the config is free form. The form should be defined by the
  /// underlying file system.
  static constexpr const char* kSpillFileCreateConfig =
      "spill_file_create_config";

  static constexpr const char* kSpillStartPartitionBit =
      "spiller_start_partition_bit";

  static constexpr const char* kJoinSpillPartitionBits =
      "join_spiller_partition_bits";

  static constexpr const char* kMinSpillableReservationPct =
      "min_spillable_reservation_pct";

  static constexpr const char* kSpillableReservationGrowthPct =
      "spillable_reservation_growth_pct";

  /// Minimum memory footprint size required to reclaim memory from a file
  /// writer by flushing its buffered data to disk.
  static constexpr const char* kWriterFlushThresholdBytes =
      "writer_flush_threshold_bytes";

  /// If true, array_agg() aggregation function will ignore nulls in the input.
  static constexpr const char* kPrestoArrayAggIgnoreNulls =
      "presto.array_agg.ignore_nulls";

  /// If false, size function returns null for null input.
  static constexpr const char* kSparkLegacySizeOfNull =
      "spark.legacy_size_of_null";

  // The default number of expected items for the bloomfilter.
  static constexpr const char* kSparkBloomFilterExpectedNumItems =
      "spark.bloom_filter.expected_num_items";

  /// The default number of bits to use for the bloom filter.
  static constexpr const char* kSparkBloomFilterNumBits =
      "spark.bloom_filter.num_bits";

  /// The max number of bits to use for the bloom filter.
  static constexpr const char* kSparkBloomFilterMaxNumBits =
      "spark.bloom_filter.max_num_bits";

  /// The number of local parallel table writer operators per task.
  static constexpr const char* kTaskWriterCount = "task_writer_count";

  /// The number of local parallel table writer operators per task for
  /// partitioned writes. If not set, use "task_writer_count".
  static constexpr const char* kTaskPartitionedWriterCount =
      "task_partitioned_writer_count";

  /// If true, finish the hash probe on an empty build table for a specific set
  /// of hash joins.
  static constexpr const char* kHashProbeFinishEarlyOnEmptyBuild =
      "hash_probe_finish_early_on_empty_build";

  /// The minimum number of table rows that can trigger the parallel hash join
  /// table build.
  static constexpr const char* kMinTableRowsForParallelJoinBuild =
      "min_table_rows_for_parallel_join_build";

  /// If set to true, then during execution of tasks, the output vectors of
  /// every operator are validated for consistency. This is an expensive check
  /// so should only be used for debugging. It can help debug issues where
  /// malformed vector cause failures or crashes by helping identify which
  /// operator is generating them.
  static constexpr const char* kValidateOutputFromOperators =
      "debug.validate_output_from_operators";

  /// If true, enable caches in expression evaluation for performance, including
  /// ExecCtx::vectorPool_, ExecCtx::decodedVectorPool_,
  /// ExecCtx::selectivityVectorPool_, Expr::baseDictionary_,
  /// Expr::dictionaryCache_, and Expr::cachedDictionaryIndices_. Otherwise,
  /// disable the caches.
  static constexpr const char* kEnableExpressionEvaluationCache =
      "enable_expression_evaluation_cache";

  // For a given shared subexpression, the maximum distinct sets of inputs we
  // cache results for. Lambdas can call the same expression with different
  // inputs many times, causing the results we cache to explode in size. Putting
  // a limit contains the memory usage.
  static constexpr const char* kMaxSharedSubexprResultsCached =
      "max_shared_subexpr_results_cached";

  /// Maximum number of splits to preload. Set to 0 to disable preloading.
  static constexpr const char* kMaxSplitPreloadPerDriver =
      "max_split_preload_per_driver";

  /// If not zero, specifies the cpu time slice limit in ms that a driver thread
  /// can continuously run without yielding. If it is zero, then there is no
  /// limit.
  static constexpr const char* kDriverCpuTimeSliceLimitMs =
      "driver_cpu_time_slice_limit_ms";

  uint64_t queryMaxMemoryPerNode() const {
    return toCapacity(
        get<std::string>(kQueryMaxMemoryPerNode, "0B"), CapacityUnit::BYTE);
  }

  uint64_t maxPartialAggregationMemoryUsage() const {
    static constexpr uint64_t kDefault = 1L << 24;
    return get<uint64_t>(kMaxPartialAggregationMemory, kDefault);
  }

  uint64_t maxExtendedPartialAggregationMemoryUsage() const {
    static constexpr uint64_t kDefault = 1L << 26;
    return get<uint64_t>(kMaxExtendedPartialAggregationMemory, kDefault);
  }

  int32_t abandonPartialAggregationMinRows() const {
    return get<int32_t>(kAbandonPartialAggregationMinRows, 100'000);
  }

  int32_t abandonPartialAggregationMinPct() const {
    return get<int32_t>(kAbandonPartialAggregationMinPct, 80);
  }

  int32_t abandonPartialTopNRowNumberMinRows() const {
    return get<int32_t>(kAbandonPartialTopNRowNumberMinRows, 100'000);
  }

  int32_t abandonPartialTopNRowNumberMinPct() const {
    return get<int32_t>(kAbandonPartialTopNRowNumberMinPct, 80);
  }

  uint64_t aggregationSpillMemoryThreshold() const {
    static constexpr uint64_t kDefault = 0;
    return get<uint64_t>(kAggregationSpillMemoryThreshold, kDefault);
  }

  uint64_t joinSpillMemoryThreshold() const {
    static constexpr uint64_t kDefault = 0;
    return get<uint64_t>(kJoinSpillMemoryThreshold, kDefault);
  }

  uint64_t orderBySpillMemoryThreshold() const {
    static constexpr uint64_t kDefault = 0;
    return get<uint64_t>(kOrderBySpillMemoryThreshold, kDefault);
  }

  uint64_t maxSpillRunRows() const {
    static constexpr uint64_t kDefault = 12UL << 20;
    return get<uint64_t>(kMaxSpillRunRows, kDefault);
  }

  uint64_t maxSpillBytes() const {
    static constexpr uint64_t kDefault = 100UL << 30;
    return get<uint64_t>(kMaxSpillBytes, kDefault);
  }

  /// Returns the maximum number of bytes to buffer in PartitionedOutput
  /// operator to avoid creating tiny SerializedPages.
  ///
  /// For PartitionedOutputNode::Kind::kPartitioned, PartitionedOutput operator
  /// would buffer up to that number of bytes / number of destinations for each
  /// destination before producing a SerializedPage.
  uint64_t maxPartitionedOutputBufferSize() const {
    static constexpr uint64_t kDefault = 32UL << 20;
    return get<uint64_t>(kMaxPartitionedOutputBufferSize, kDefault);
  }

  /// Returns the maximum size in bytes for the task's buffered output.
  ///
  /// The producer Drivers are blocked when the buffered size exceeds
  /// this. The Drivers are resumed when the buffered size goes below
  /// OutputBufferManager::kContinuePct % of this.
  uint64_t maxOutputBufferSize() const {
    static constexpr uint64_t kDefault = 32UL << 20;
    return get<uint64_t>(kMaxOutputBufferSize, kDefault);
  }

  uint64_t maxLocalExchangeBufferSize() const {
    static constexpr uint64_t kDefault = 32UL << 20;
    return get<uint64_t>(kMaxLocalExchangeBufferSize, kDefault);
  }

  uint64_t maxExchangeBufferSize() const {
    static constexpr uint64_t kDefault = 32UL << 20;
    return get<uint64_t>(kMaxExchangeBufferSize, kDefault);
  }

  uint64_t maxMergeExchangeBufferSize() const {
    static constexpr uint64_t kDefault = 128UL << 20;
    return get<uint64_t>(kMaxMergeExchangeBufferSize, kDefault);
  }

  uint64_t preferredOutputBatchBytes() const {
    static constexpr uint64_t kDefault = 10UL << 20;
    return get<uint64_t>(kPreferredOutputBatchBytes, kDefault);
  }

  uint32_t preferredOutputBatchRows() const {
    return get<uint32_t>(kPreferredOutputBatchRows, 1024);
  }

  uint32_t maxOutputBatchRows() const {
    return get<uint32_t>(kMaxOutputBatchRows, 10'000);
  }

  uint32_t tableScanGetOutputTimeLimitMs() const {
    return get<uint64_t>(kTableScanGetOutputTimeLimitMs, 5'000);
  }

  bool hashAdaptivityEnabled() const {
    return get<bool>(kHashAdaptivityEnabled, true);
  }

  uint32_t writeStrideSize() const {
    static constexpr uint32_t kDefault = 100'000;
    return kDefault;
  }

  bool flushPerBatch() const {
    static constexpr bool kDefault = true;
    return kDefault;
  }

  bool adaptiveFilterReorderingEnabled() const {
    return get<bool>(kAdaptiveFilterReorderingEnabled, true);
  }

  bool isLegacyCast() const {
    return get<bool>(kLegacyCast, false);
  }

  bool isMatchStructByName() const {
    return get<bool>(kCastMatchStructByName, false);
  }

  bool codegenEnabled() const {
    return get<bool>(kCodegenEnabled, false);
  }

  std::string codegenConfigurationFilePath() const {
    return get<std::string>(kCodegenConfigurationFilePath, "");
  }

  bool codegenLazyLoading() const {
    return get<bool>(kCodegenLazyLoading, true);
  }

  bool adjustTimestampToTimezone() const {
    return get<bool>(kAdjustTimestampToTimezone, false);
  }

  std::string sessionTimezone() const {
    return get<std::string>(kSessionTimezone, "");
  }

  bool exprEvalSimplified() const {
    return get<bool>(kExprEvalSimplified, false);
  }

  /// Returns true if spilling is enabled.
  bool spillEnabled() const {
    return get<bool>(kSpillEnabled, false);
  }

  /// Returns 'is aggregation spilling enabled' flag. Must also check the
  /// spillEnabled()!g
  bool aggregationSpillEnabled() const {
    return get<bool>(kAggregationSpillEnabled, true);
  }

  /// Returns 'is join spilling enabled' flag. Must also check the
  /// spillEnabled()!
  bool joinSpillEnabled() const {
    return get<bool>(kJoinSpillEnabled, true);
  }

  /// Returns 'is orderby spilling enabled' flag. Must also check the
  /// spillEnabled()!
  bool orderBySpillEnabled() const {
    return get<bool>(kOrderBySpillEnabled, true);
  }

  /// Returns true if spilling is enabled for Window operator. Must also
  /// check the spillEnabled()!
  bool windowSpillEnabled() const {
    return get<bool>(kWindowSpillEnabled, true);
  }

  /// Returns 'is writer spilling enabled' flag. Must also check the
  /// spillEnabled()!
  bool writerSpillEnabled() const {
    return get<bool>(kWriterSpillEnabled, true);
  }

  /// Returns true if spilling is enabled for RowNumber operator. Must also
  /// check the spillEnabled()!
  bool rowNumberSpillEnabled() const {
    return get<bool>(kRowNumberSpillEnabled, true);
  }

  /// Returns true if spilling is enabled for TopNRowNumber operator. Must also
  /// check the spillEnabled()!
  bool topNRowNumberSpillEnabled() const {
    return get<bool>(kTopNRowNumberSpillEnabled, true);
  }

  /// Returns a percentage of aggregation or join input batches that will be
  /// forced to spill for testing. 0 means no extra spilling.
  int32_t testingSpillPct() const {
    return get<int32_t>(kTestingSpillPct, 0);
  }

  int32_t maxSpillLevel() const {
    return get<int32_t>(kMaxSpillLevel, 4);
  }

  /// Returns the start partition bit which is used with
  /// 'kJoinSpillPartitionBits' or 'kAggregationSpillPartitionBits' together to
  /// calculate the spilling partition number for join spill or aggregation
  /// spill.
  uint8_t spillStartPartitionBit() const {
    constexpr uint8_t kDefaultStartBit = 48;
    return get<uint8_t>(kSpillStartPartitionBit, kDefaultStartBit);
  }

  /// Returns the number of bits used to calculate the spilling partition
  /// number for hash join. The number of spilling partitions will be power of
  /// two.
  ///
  /// NOTE: as for now, we only support up to 8-way spill partitioning.
  uint8_t joinSpillPartitionBits() const {
    constexpr uint8_t kDefaultBits = 2;
    constexpr uint8_t kMaxBits = 3;
    return std::min(
        kMaxBits, get<uint8_t>(kJoinSpillPartitionBits, kDefaultBits));
  }

  uint64_t writerFlushThresholdBytes() const {
    return get<uint64_t>(kWriterFlushThresholdBytes, 96L << 20);
  }

  uint64_t maxSpillFileSize() const {
    constexpr uint64_t kDefaultMaxFileSize = 0;
    return get<uint64_t>(kMaxSpillFileSize, kDefaultMaxFileSize);
  }

  uint64_t minSpillRunSize() const {
    constexpr uint64_t kDefaultMinSpillRunSize = 256 << 20; // 256MB.
    return get<uint64_t>(kMinSpillRunSize, kDefaultMinSpillRunSize);
  }

  std::string spillCompressionKind() const {
    return get<std::string>(kSpillCompressionKind, "none");
  }

  uint64_t spillWriteBufferSize() const {
    // The default write buffer size set to 1MB.
    return get<uint64_t>(kSpillWriteBufferSize, 1L << 20);
  }

  std::string spillFileCreateConfig() const {
    return get<std::string>(kSpillFileCreateConfig, "");
  }

  /// Returns the minimal available spillable memory reservation in percentage
  /// of the current memory usage. Suppose the current memory usage size of M,
  /// available memory reservation size of N and min reservation percentage of
  /// P, if M * P / 100 > N, then spiller operator needs to grow the memory
  /// reservation with percentage of spillableReservationGrowthPct(). This
  /// ensures we have sufficient amount of memory reservation to process the
  /// large input outlier.
  int32_t minSpillableReservationPct() const {
    constexpr int32_t kDefaultPct = 5;
    return get<int32_t>(kMinSpillableReservationPct, kDefaultPct);
  }

  /// Returns the spillable memory reservation growth percentage of the previous
  /// memory reservation size. 10 means exponential growth along a series of
  /// integer powers of 11/10. The reservation grows by this much until it no
  /// longer can, after which it starts spilling.
  int32_t spillableReservationGrowthPct() const {
    constexpr int32_t kDefaultPct = 10;
    return get<int32_t>(kSpillableReservationGrowthPct, kDefaultPct);
  }

  bool sparkLegacySizeOfNull() const {
    constexpr bool kDefault{true};
    return get<bool>(kSparkLegacySizeOfNull, kDefault);
  }

  bool prestoArrayAggIgnoreNulls() const {
    return get<bool>(kPrestoArrayAggIgnoreNulls, false);
  }

  int64_t sparkBloomFilterExpectedNumItems() const {
    constexpr int64_t kDefault = 1'000'000L;
    return get<int64_t>(kSparkBloomFilterExpectedNumItems, kDefault);
  }

  int64_t sparkBloomFilterNumBits() const {
    constexpr int64_t kDefault = 8'388'608L;
    return get<int64_t>(kSparkBloomFilterNumBits, kDefault);
  }

  // Spark kMaxNumBits is 67'108'864, but velox has memory limit sizeClassSizes
  // 256, so decrease it to not over memory limit.
  int64_t sparkBloomFilterMaxNumBits() const {
    constexpr int64_t kDefault = 4'096 * 1024;
    auto value = get<int64_t>(kSparkBloomFilterMaxNumBits, kDefault);
    VELOX_USER_CHECK_LE(
        value,
        kDefault,
        "{} cannot exceed the default value",
        kSparkBloomFilterMaxNumBits);
    return value;
  }

  bool exprTrackCpuUsage() const {
    return get<bool>(kExprTrackCpuUsage, false);
  }

  bool operatorTrackCpuUsage() const {
    return get<bool>(kOperatorTrackCpuUsage, true);
  }

  uint32_t taskWriterCount() const {
    return get<uint32_t>(kTaskWriterCount, 4);
  }

  uint32_t taskPartitionedWriterCount() const {
    return get<uint32_t>(kTaskPartitionedWriterCount)
        .value_or(taskWriterCount());
  }

  bool hashProbeFinishEarlyOnEmptyBuild() const {
    return get<bool>(kHashProbeFinishEarlyOnEmptyBuild, true);
  }

  uint32_t minTableRowsForParallelJoinBuild() const {
    return get<uint32_t>(kMinTableRowsForParallelJoinBuild, 1'000);
  }

  bool validateOutputFromOperators() const {
    return get<bool>(kValidateOutputFromOperators, false);
  }

  bool isExpressionEvaluationCacheEnabled() const {
    return get<bool>(kEnableExpressionEvaluationCache, true);
  }

  uint32_t maxSharedSubexprResultsCached() const {
    // 10 was chosen as a default as there are cases where a shared
    // subexpression can be called in 2 different places and a particular
    // argument may be peeled in one and not peeled in another. 10 is large
    // enough to handle this happening for a few arguments in different
    // combinations.
    //
    // For example, when the UDF at the root of a shared subexpression does not
    // have default null behavior and takes an input that is dictionary encoded
    // with nulls set in the DictionaryVector. That dictionary
    // encoding may be peeled depending on whether or not there is a UDF above
    // it in the expression tree that has default null behavior and takes the
    // same input as an argument.
    return get<uint32_t>(kMaxSharedSubexprResultsCached, 10);
  }

  int32_t maxSplitPreloadPerDriver() const {
    return get<int32_t>(kMaxSplitPreloadPerDriver, 2);
  }

  uint32_t driverCpuTimeSliceLimitMs() const {
    return get<uint32_t>(kDriverCpuTimeSliceLimitMs, 0);
  }

  template <typename T>
  T get(const std::string& key, const T& defaultValue) const {
    return config_->get<T>(key, defaultValue);
  }
  template <typename T>
  std::optional<T> get(const std::string& key) const {
    return std::optional<T>(config_->get<T>(key));
  }

  /// Test-only method to override the current query config properties.
  /// It is not thread safe.
  void testingOverrideConfigUnsafe(
      std::unordered_map<std::string, std::string>&& values);

 private:
  std::unique_ptr<velox::Config> config_;
};
} // namespace facebook::velox::core
