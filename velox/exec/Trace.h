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

#include <cstdint>
#include <optional>
#include <string>

namespace facebook::velox::exec::trace {
/// Defines the shared constants used by query trace implementation.
struct TraceTraits {
  static inline const std::string kPlanNodeKey = "planNode";
  static inline const std::string kQueryConfigKey = "queryConfig";
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
  static inline const std::string kInputBytesKey = "inputBytes";
  static inline const std::string kRawInputRowsKey = "rawInputRows";
  static inline const std::string kRawInputBytesKey = "rawInputBytes";
  static inline const std::string kNumSplitsKey = "numSplits";
};

/// Contains the summary of an operator trace.
struct OperatorTraceSummary {
  std::string opType;
  /// The number of splits processed by a table scan operator, nullopt for the
  /// other operator types.
  std::optional<uint32_t> numSplits{std::nullopt};

  uint64_t inputRows{0};
  uint64_t inputBytes{0};
  uint64_t rawInputRows{0};
  uint64_t rawInputBytes{0};
  uint64_t peakMemory{0};

  std::string toString() const;
};

} // namespace facebook::velox::exec::trace
