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

#include <string>

namespace facebook::velox::exec::trace {
/// Defines the shared constants used by query trace implementation.
struct TraceTraits {
  static inline const std::string kPlanNodeKey = "planNode";
  static inline const std::string kQueryConfigKey = "queryConfig";
  static inline const std::string kDataTypeKey = "rowType";
  static inline const std::string kConnectorPropertiesKey =
      "connectorProperties";

  static inline const std::string kTaskMetaFileName = "task_trace_meta.json";
};

struct OperatorTraceTraits {
  static inline const std::string kSummaryFileName = "op_trace_summary.json";
  static inline const std::string kInputFileName = "op_input_trace.data";
  static inline const std::string kSplitFileName = "op_split_trace.split";

  /// Keys for operator trace summary file.
  static inline const std::string kOpTypeKey = "opType";
  static inline const std::string kPeakMemoryKey = "peakMemory";
  static inline const std::string kInputRowsKey = "inputRows";
  static inline const std::string kNumSplits = "numSplits";
};

/// Contains the summary of an operator trace.
struct OperatorTraceSummary {
  std::string opType;
  uint64_t inputRows{0};
  uint64_t peakMemory{0};

  std::string toString() const;
};

#define VELOX_TRACE_LIMIT_EXCEEDED(errorMessage)                    \
  _VELOX_THROW(                                                     \
      ::facebook::velox::VeloxRuntimeError,                         \
      ::facebook::velox::error_source::kErrorSourceRuntime.c_str(), \
      ::facebook::velox::error_code::kTraceLimitExceeded.c_str(),   \
      /* isRetriable */ true,                                       \
      "{}",                                                         \
      errorMessage);
} // namespace facebook::velox::exec::trace
