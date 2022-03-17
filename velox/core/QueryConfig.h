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

#include "velox/core/Context.h"

namespace facebook::velox::core {

class QueryConfig {
 public:
  explicit QueryConfig(BaseConfigManager* config) : config_{config} {}

  static constexpr const char* kCodegenEnabled = "driver.codegen.enabled";

  static constexpr const char* kCodegenConfigurationFilePath =
      "driver.codegen.configuration_file_path";

  static constexpr const char* kCodegenLazyLoading =
      "driver.codegen.lazy_loading";

  // User provided session timezone. Stores a string with the actual timezone
  // name, e.g: "America/Los_Angeles".
  static constexpr const char* kSessionTimezone = "driver.session.timezone";

  // If true, timezone-less timestamp conversions (e.g. string to timestamp,
  // when the string does not specify a timezone) will be adjusted to the user
  // provided session timezone (if any).
  //
  // For instance:
  //
  //  if this option is true and user supplied "America/Los_Angeles",
  //  "1970-01-01" will be converted to -28800 instead of 0.
  //
  // False by default.
  static constexpr const char* kAdjustTimestampToTimezone =
      "driver.session.adjust_timestamp_to_timezone";

  // Whether to use the simplified expression evaluation path. False by default.
  static constexpr const char* kExprEvalSimplified =
      "driver.expr_eval.simplified";

  // Flags used to configure the CAST operator:

  // This flag makes the Row conversion to by applied
  // in a way that the casting row field are matched by
  // name instead of position
  static constexpr const char* kCastMatchStructByName =
      "driver.cast.match_struct_by_name";

  // This flags forces the cast from float/double to integer to be performed by
  // truncating the decimal part instead of rounding.
  static constexpr const char* kCastIntByTruncate =
      "driver.cast.int_by_truncate";

  static constexpr const char* kMaxLocalExchangeBufferSize =
      "max_local_exchange_buffer_size";

  static constexpr const char* kMaxPartialAggregationMemory =
      "max_partial_aggregation_memory";

  static constexpr const char* kMaxPartitionedOutputBufferSize =
      "driver.max-page-partitioning-buffer-size";

  /// Preffered number of rows to be returned by operators from
  /// Operator::getOutput.
  static constexpr const char* kPreferredOutputBatchSize =
      "preferred_output_batch_size";

  static constexpr const char* kHashAdaptivityEnabled =
      "driver.hash_adaptivity_enabled";

  static constexpr const char* kAdaptiveFilterReorderingEnabled =
      "driver.adaptive_filter_reordering_enabled";

  static constexpr const char* kCreateEmptyFiles = "driver.create_empty_files";

  uint64_t maxPartialAggregationMemoryUsage() const {
    static constexpr uint64_t kDefault = 1L << 24;
    return get<uint64_t>(kMaxPartialAggregationMemory, kDefault);
  }

  // Returns the target size for a Task's buffered output. The
  // producer Drivers are blocked when the buffered size exceeds
  // this. The Drivers are resumed when the buffered size goes below
  // PartitionedOutputBufferManager::kContinuePct % of this.
  uint64_t maxPartitionedOutputBufferSize() const {
    static constexpr uint64_t kDefault = 32UL << 20;
    return get<uint64_t>(kMaxPartitionedOutputBufferSize, kDefault);
  }

  uint64_t maxLocalExchangeBufferSize() const {
    static constexpr uint64_t kDefault = 32UL << 20;
    return get<uint64_t>(kMaxLocalExchangeBufferSize, kDefault);
  }

  uint32_t preferredOutputBatchSize() const {
    return get<uint32_t>(kPreferredOutputBatchSize, 1024);
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

  bool isMatchStructByName() const {
    return get<bool>(kCastMatchStructByName, false);
  }

  bool isCastIntByTruncate() const {
    return get<bool>(kCastIntByTruncate, false);
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

  bool createEmptyFiles() const {
    return get<bool>(kCreateEmptyFiles, false);
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

 private:
  template <typename T>
  T get(const std::string& key, const T& defaultValue) const {
    return config_->get<T>(key, defaultValue);
  }

  BaseConfigManager* config_;
};
} // namespace facebook::velox::core
