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
#include "presto_cpp/main/SessionProperties.h"
#include "velox/core/QueryConfig.h"

using namespace facebook::velox;

namespace facebook::presto {

namespace {
const std::string boolToString(bool value) {
  return value ? "true" : "false";
}
} // namespace

json SessionProperty::serialize() {
  json j;
  j["name"] = name_;
  j["description"] = description_;
  j["typeSignature"] = type_;
  j["defaultValue"] = defaultValue_;
  j["hidden"] = hidden_;
  return j;
}

SessionProperties* SessionProperties::instance() {
  static std::unique_ptr<SessionProperties> instance =
      std::make_unique<SessionProperties>();
  return instance.get();
}

void SessionProperties::addSessionProperty(
    const std::string& name,
    const std::string& description,
    const TypePtr& type,
    bool isHidden,
    const std::string& veloxConfigName,
    const std::string& veloxDefault) {
  sessionProperties_[name] = std::make_shared<SessionProperty>(
      name,
      description,
      type->toString(),
      isHidden,
      veloxConfigName,
      veloxDefault);
}

// List of native session properties is kept as the source of truth here.
SessionProperties::SessionProperties() {
  using velox::core::QueryConfig;
  // Use empty instance to get default property values.
  QueryConfig c{{}};

  addSessionProperty(
      kExprEvalSimplified,
      "Native Execution only. Enable simplified path in expression evaluation",
      BOOLEAN(),
      false,
      QueryConfig::kExprEvalSimplified,
      boolToString(c.exprEvalSimplified()));

  addSessionProperty(
      kExprMaxArraySizeInReduce,
      "Reduce() function will throw an error if it encounters an array of size greater than this value.",
      BIGINT(),
      false,
      QueryConfig::kExprMaxArraySizeInReduce,
      std::to_string(c.exprMaxArraySizeInReduce()));

  addSessionProperty(
      kExprMaxCompiledRegexes,
      "Controls maximum number of compiled regular expression patterns per regular expression function instance "
      "per thread of execution.",
      BIGINT(),
      false,
      QueryConfig::kExprMaxCompiledRegexes,
      std::to_string(c.exprMaxCompiledRegexes()));

  addSessionProperty(
      kMaxPartialAggregationMemory,
      "The max partial aggregation memory when data reduction is not optimal.",
      BIGINT(),
      false,
      QueryConfig::kMaxPartialAggregationMemory,
      std::to_string(c.maxPartialAggregationMemoryUsage()));

  addSessionProperty(
      kMaxExtendedPartialAggregationMemory,
      "The max partial aggregation memory when data reduction is optimal.",
      BIGINT(),
      false,
      QueryConfig::kMaxExtendedPartialAggregationMemory,
      std::to_string(c.maxExtendedPartialAggregationMemoryUsage()));

  addSessionProperty(
      kMaxSpillLevel,
      "Native Execution only. The maximum allowed spilling level for hash join "
      "build. 0 is the initial spilling level, -1 means unlimited.",
      INTEGER(),
      false,
      QueryConfig::kMaxSpillLevel,
      std::to_string(c.maxSpillLevel()));

  addSessionProperty(
      kMaxSpillFileSize,
      "The max allowed spill file size. If it is zero, then there is no limit.",
      INTEGER(),
      false,
      QueryConfig::kMaxSpillFileSize,
      std::to_string(c.maxSpillFileSize()));

  addSessionProperty(
      kMaxSpillBytes,
      "The max allowed spill bytes.",
      BIGINT(),
      false,
      QueryConfig::kMaxSpillBytes,
      std::to_string(c.maxSpillBytes()));

  addSessionProperty(
      kSpillCompressionCodec,
      "Native Execution only. The compression algorithm type to compress the "
      "spilled data.\n Supported compression codecs are: ZLIB, SNAPPY, LZO, "
      "ZSTD, LZ4 and GZIP. NONE means no compression.",
      VARCHAR(),
      false,
      QueryConfig::kSpillCompressionKind,
      c.spillCompressionKind());

  addSessionProperty(
      kSpillWriteBufferSize,
      "Native Execution only. The maximum size in bytes to buffer the serialized "
      "spill data before writing to disk for IO efficiency. If set to zero, "
      "buffering is disabled.",
      BIGINT(),
      false,
      QueryConfig::kSpillWriteBufferSize,
      std::to_string(c.spillWriteBufferSize()));

  addSessionProperty(
      kSpillFileCreateConfig,
      "Native Execution only. Config used to create spill files. This config is "
      "provided to underlying file system and the config is free form. The form should be "
      "defined by the underlying file system.",
      VARCHAR(),
      false,
      QueryConfig::kSpillFileCreateConfig,
      c.spillFileCreateConfig());

  addSessionProperty(
      kJoinSpillEnabled,
      "Native Execution only. Enable join spilling on native engine",
      BOOLEAN(),
      false,
      QueryConfig::kJoinSpillEnabled,
      boolToString(c.joinSpillEnabled()));

  addSessionProperty(
      kWindowSpillEnabled,
      "Native Execution only. Enable window spilling on native engine",
      BOOLEAN(),
      false,
      QueryConfig::kWindowSpillEnabled,
      boolToString(c.windowSpillEnabled()));

  addSessionProperty(
      kWriterSpillEnabled,
      "Native Execution only. Enable writer spilling on native engine",
      BOOLEAN(),
      false,
      QueryConfig::kWriterSpillEnabled,
      boolToString(c.writerSpillEnabled()));

  addSessionProperty(
      kWriterFlushThresholdBytes,
      "Native Execution only. Minimum memory footprint size required "
      "to reclaim memory from a file writer by flushing its buffered data to "
      "disk.",
      BIGINT(),
      false,
      QueryConfig::kWriterFlushThresholdBytes,
      std::to_string(c.writerFlushThresholdBytes()));

  addSessionProperty(
      kRowNumberSpillEnabled,
      "Native Execution only. Enable row number spilling on native engine",
      BOOLEAN(),
      false,
      QueryConfig::kRowNumberSpillEnabled,
      boolToString(c.rowNumberSpillEnabled()));

  addSessionProperty(
      kSpillerNumPartitionBits,
      "The number of bits (N) used to calculate the spilling "
      "partition number for hash join and RowNumber: 2 ^ N",
      TINYINT(),
      false,
      QueryConfig::kSpillNumPartitionBits,
      std::to_string(c.spillNumPartitionBits()));

  addSessionProperty(
      kTopNRowNumberSpillEnabled,
      "Native Execution only. Enable topN row number spilling on native engine",
      BOOLEAN(),
      false,
      QueryConfig::kTopNRowNumberSpillEnabled,
      boolToString(c.topNRowNumberSpillEnabled()));

  addSessionProperty(
      kValidateOutputFromOperators,
      "If set to true, then during execution of tasks, the output vectors of "
      "every operator are validated for consistency. This is an expensive check "
      "so should only be used for debugging. It can help debug issues where "
      "malformed vector cause failures or crashes by helping identify which "
      "operator is generating them.",
      BOOLEAN(),
      false,
      QueryConfig::kValidateOutputFromOperators,
      boolToString(c.validateOutputFromOperators()));

  addSessionProperty(
      kDebugDisableExpressionWithPeeling,
      "If set to true, disables optimization in expression evaluation to peel "
      "common dictionary layer from inputs. Should only be used for debugging.",
      BOOLEAN(),
      false,
      QueryConfig::kDebugDisableExpressionWithPeeling,
      boolToString(c.debugDisableExpressionsWithPeeling()));

  addSessionProperty(
      kDebugDisableCommonSubExpressions,
      "If set to true, disables optimization in expression evaluation to "
      "re-use cached results for common sub-expressions. Should only be "
      "used for debugging.",
      BOOLEAN(),
      false,
      QueryConfig::kDebugDisableCommonSubExpressions,
      boolToString(c.debugDisableCommonSubExpressions()));

  addSessionProperty(
      kDebugDisableExpressionWithMemoization,
      "If set to true, disables optimization in expression evaluation to "
      "re-use cached results between subsequent input batches that are "
      "dictionary encoded and have the same alphabet(underlying flat vector). "
      "Should only be used for debugging.",
      BOOLEAN(),
      false,
      QueryConfig::kDebugDisableExpressionWithMemoization,
      boolToString(c.debugDisableExpressionsWithMemoization()));

  addSessionProperty(
      kDebugDisableExpressionWithLazyInputs,
      "If set to true, disables optimization in expression evaluation to delay "
      "loading of lazy inputs unless required. Should only be used for "
      "debugging.",
      BOOLEAN(),
      false,
      QueryConfig::kDebugDisableExpressionWithLazyInputs,
      boolToString(c.debugDisableExpressionsWithLazyInputs()));

  addSessionProperty(
      kDebugMemoryPoolNameRegex,
      "Regex for filtering on memory pool name if not empty. This allows us to "
      "only track the callsites of memory allocations for memory pools whose "
      "name matches the specified regular expression. Empty string means no "
      "match for all.",
      VARCHAR(),
      false,
      QueryConfig::kDebugMemoryPoolNameRegex,
      c.debugMemoryPoolNameRegex());

  addSessionProperty(
      kDebugMemoryPoolWarnThresholdBytes,
      "Warning threshold in bytes for debug memory pools. When set to a "
      "non-zero value, a warning will be logged once per memory pool when "
      "allocations cause the pool to exceed this threshold. This is useful for "
      "identifying memory usage patterns during debugging. Requires allocation "
      "tracking to be enabled with `native_debug_memory_pool_name_regex` "
      "for the pool. A value of 0 means no warning threshold is enforced.",
      BIGINT(),
      false,
      QueryConfig::kDebugMemoryPoolWarnThresholdBytes,
      std::to_string(c.debugMemoryPoolWarnThresholdBytes()));

  addSessionProperty(
      kSelectiveNimbleReaderEnabled,
      "Temporary flag to control whether selective Nimble reader should be "
      "used in this query or not.  Will be removed after the selective Nimble "
      "reader is fully rolled out.",
      BOOLEAN(),
      false,
      QueryConfig::kSelectiveNimbleReaderEnabled,
      boolToString(c.selectiveNimbleReaderEnabled()));

  addSessionProperty(
      kQueryTraceEnabled,
      "Enables query tracing.",
      BOOLEAN(),
      false,
      QueryConfig::kQueryTraceEnabled,
      boolToString(c.queryTraceEnabled()));

  addSessionProperty(
      kQueryTraceDir,
      "Base dir of a query to store tracing data.",
      VARCHAR(),
      false,
      QueryConfig::kQueryTraceDir,
      c.queryTraceDir());

  addSessionProperty(
      kQueryTraceNodeId,
      "The plan node id whose input data will be traced.",
      VARCHAR(),
      false,
      QueryConfig::kQueryTraceNodeId,
      c.queryTraceNodeId());

  addSessionProperty(
      kQueryTraceMaxBytes,
      "The max trace bytes limit. Tracing is disabled if zero.",
      BIGINT(),
      false,
      QueryConfig::kQueryTraceMaxBytes,
      std::to_string(c.queryTraceMaxBytes()));

  addSessionProperty(
      kOpTraceDirectoryCreateConfig,
      "Config used to create operator trace directory. This config is provided to"
      " underlying file system and the config is free form. The form should be defined "
      "by the underlying file system.",
      VARCHAR(),
      false,
      QueryConfig::kOpTraceDirectoryCreateConfig,
      c.opTraceDirectoryCreateConfig());

  addSessionProperty(
      kMaxOutputBufferSize,
      "The maximum size in bytes for the task's buffered output. The buffer is"
      " shared among all drivers.",
      BIGINT(),
      false,
      QueryConfig::kMaxOutputBufferSize,
      std::to_string(c.maxOutputBufferSize()));

  addSessionProperty(
      kMaxPartitionedOutputBufferSize,
      "The maximum bytes to buffer per PartitionedOutput operator to avoid"
      "creating tiny SerializedPages. For "
      "PartitionedOutputNode::Kind::kPartitioned, PartitionedOutput operator"
      "would buffer up to that number of bytes / number of destinations for "
      "each destination before producing a SerializedPage.",
      BIGINT(),
      false,
      QueryConfig::kMaxPartitionedOutputBufferSize,
      std::to_string(c.maxPartitionedOutputBufferSize()));

  // If `legacy_timestamp` is true, the coordinator expects timestamp
  // conversions without a timezone to be converted to the user's
  // session_timezone.
  addSessionProperty(
      kLegacyTimestamp,
      "Native Execution only. Use legacy TIME & TIMESTAMP semantics. Warning: "
      "this will be removed",
      BOOLEAN(),
      false,
      QueryConfig::kAdjustTimestampToTimezone,
      // Overrides velox default value. legacy_timestamp default value is true
      // in the coordinator.
      "true");

  // TODO: remove this once cpu driver slicing config is turned on by default in
  // Velox.
  addSessionProperty(
      kDriverCpuTimeSliceLimitMs,
      "Native Execution only. The cpu time slice limit in ms that a driver thread. "
      "If not zero, can continuously run without yielding. If it is zero, then "
      "there is no limit.",
      INTEGER(),
      false,
      QueryConfig::kDriverCpuTimeSliceLimitMs,
      // Overrides velox default value. Set it to 1 second to be aligned with
      // Presto Java.
      std::to_string(1000));

  addSessionProperty(
      kMaxLocalExchangePartitionCount,
      "Maximum number of partitions created by a local exchange."
      "Affects concurrency for pipelines containing LocalPartitionNode",
      BIGINT(),
      false,
      QueryConfig::kMaxLocalExchangePartitionCount,
      std::to_string(c.maxLocalExchangePartitionCount()));

  addSessionProperty(
      kSpillPrefixSortEnabled,
      "Enable the prefix sort or fallback to std::sort in spill. The prefix sort is "
      "faster than std::sort but requires the memory to build normalized prefix "
      "keys, which might have potential risk of running out of server memory.",
      BOOLEAN(),
      false,
      QueryConfig::kSpillPrefixSortEnabled,
      std::to_string(c.spillPrefixSortEnabled()));

  addSessionProperty(
      kPrefixSortNormalizedKeyMaxBytes,
      "Maximum number of bytes to use for the normalized key in prefix-sort. "
      "Use 0 to disable prefix-sort.",
      INTEGER(),
      false,
      QueryConfig::kPrefixSortNormalizedKeyMaxBytes,
      std::to_string(c.prefixSortNormalizedKeyMaxBytes()));

  addSessionProperty(
      kPrefixSortMinRows,
      "Minimum number of rows to use prefix-sort. The default value (130) has been "
      "derived using micro-benchmarking.",
      INTEGER(),
      false,
      QueryConfig::kPrefixSortMinRows,
      std::to_string(c.prefixSortMinRows()));

  addSessionProperty(
      kScaleWriterRebalanceMaxMemoryUsageRatio,
      "The max ratio of a query used memory to its max capacity, "
      "and the scale writer exchange stops scaling writer processing if the query's current "
      "memory usage exceeds this ratio. The value is in the range of (0, 1].",
      DOUBLE(),
      false,
      QueryConfig::kScaleWriterRebalanceMaxMemoryUsageRatio,
      std::to_string(c.scaleWriterRebalanceMaxMemoryUsageRatio()));

  addSessionProperty(
      kScaleWriterMaxPartitionsPerWriter,
      "The max number of logical table partitions that can be assigned to a "
      "single table writer thread. The logical table partition is used by local "
      "exchange writer for writer scaling, and multiple physical table "
      "partitions can be mapped to the same logical table partition based on the "
      "hash value of calculated partitioned ids.",
      INTEGER(),
      false,
      QueryConfig::kScaleWriterMaxPartitionsPerWriter,
      std::to_string(c.scaleWriterMaxPartitionsPerWriter()));

  addSessionProperty(
      kScaleWriterMinPartitionProcessedBytesRebalanceThreshold,
      "Minimum amount of data processed by a logical table partition "
      "to trigger writer scaling if it is detected as overloaded by scale writer exchange.",
      BIGINT(),
      false,
      QueryConfig::kScaleWriterMinPartitionProcessedBytesRebalanceThreshold,
      std::to_string(
          c.scaleWriterMinPartitionProcessedBytesRebalanceThreshold()));

  addSessionProperty(
      kScaleWriterMinProcessedBytesRebalanceThreshold,
      "Minimum amount of data processed by all the logical table partitions "
      "to trigger skewed partition rebalancing by scale writer exchange.",
      BIGINT(),
      false,
      QueryConfig::kScaleWriterMinProcessedBytesRebalanceThreshold,
      std::to_string(c.scaleWriterMinProcessedBytesRebalanceThreshold()));

  addSessionProperty(
      kTableScanScaledProcessingEnabled,
      "If set to true, enables scaled processing for table scans.",
      BOOLEAN(),
      false,
      QueryConfig::kTableScanScaledProcessingEnabled,
      std::to_string(c.tableScanScaledProcessingEnabled()));

  addSessionProperty(
      kTableScanScaleUpMemoryUsageRatio,
      "Controls the ratio of available memory that can be used for scaling up table scans. "
      "The value is in the range of [0, 1].",
      DOUBLE(),
      false,
      QueryConfig::kTableScanScaleUpMemoryUsageRatio,
      std::to_string(c.tableScanScaleUpMemoryUsageRatio()));

  addSessionProperty(
      kStreamingAggregationMinOutputBatchRows,
      "In streaming aggregation, wait until we have enough number of output rows"
      "to produce a batch of size specified by this. If set to 0, then"
      "Operator::outputBatchRows will be used as the min output batch rows.",
      INTEGER(),
      false,
      QueryConfig::kStreamingAggregationMinOutputBatchRows,
      std::to_string(c.streamingAggregationMinOutputBatchRows()));

  addSessionProperty(
      kRequestDataSizesMaxWaitSec,
      "Maximum wait time for exchange long poll requests in seconds.",
      INTEGER(),
      10,
      QueryConfig::kRequestDataSizesMaxWaitSec,
      std::to_string(c.requestDataSizesMaxWaitSec()));

  addSessionProperty(
      kNativeQueryMemoryReclaimerPriority,
      "Memory pool reclaimer priority.",
      INTEGER(),
      2147483647,
      QueryConfig::kQueryMemoryReclaimerPriority,
      std::to_string(c.queryMemoryReclaimerPriority()));

  addSessionProperty(
      kMaxNumSplitsListenedTo,
      "Maximum number of splits to listen to by SplitListener on native workers.",
      INTEGER(),
      0,
      QueryConfig::kMaxNumSplitsListenedTo,
      std::to_string(c.maxNumSplitsListenedTo()));

  addSessionProperty(
      kIndexLookupJoinMaxPrefetchBatches,
      "Specifies the max number of input batches to prefetch to do index"
      "lookup ahead. If it is zero, then process one input batch at a time.",
      INTEGER(),
      false,
      QueryConfig::kIndexLookupJoinMaxPrefetchBatches,
      std::to_string(c.indexLookupJoinMaxPrefetchBatches()));

  addSessionProperty(
      kIndexLookupJoinSplitOutput,
      "If this is true, then the index join operator might split output for"
      "each input batch based on the output batch size control. Otherwise, it tries to"
      "produce a single output for each input batch.",
      BOOLEAN(),
      false,
      QueryConfig::kIndexLookupJoinSplitOutput,
      std::to_string(c.indexLookupJoinSplitOutput()));

  addSessionProperty(
      kUnnestSplitOutput,
      "In streaming aggregation, wait until we have enough number of output"
      "rows to produce a batch of size specified by this. If set to 0, then"
      "Operator::outputBatchRows will be used as the min output batch rows.",
      BOOLEAN(),
      false,
      QueryConfig::kUnnestSplitOutput,
      std::to_string(c.unnestSplitOutput()));
}

const std::unordered_map<std::string, std::shared_ptr<SessionProperty>>&
SessionProperties::testingSessionProperties() const {
  return sessionProperties_;
}

const std::string SessionProperties::toVeloxConfig(
    const std::string& name) const {
  auto it = sessionProperties_.find(name);
  return it == sessionProperties_.end() ? name
                                        : it->second->getVeloxConfigName();
}

json SessionProperties::serialize() const {
  json j = json::array();
  for (const auto& entry : sessionProperties_) {
    j.push_back(entry.second->serialize());
  }
  return j;
}

} // namespace facebook::presto
