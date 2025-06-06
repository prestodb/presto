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
#include <functional>
#include <string>
#include <unordered_set>

namespace facebook::velox {

#define VELOX_TRACE_LIMIT_EXCEEDED(errorMessage)                    \
  _VELOX_THROW(                                                     \
      ::facebook::velox::VeloxRuntimeError,                         \
      ::facebook::velox::error_source::kErrorSourceRuntime.c_str(), \
      ::facebook::velox::error_code::kTraceLimitExceeded.c_str(),   \
      /* isRetriable */ true,                                       \
      "{}",                                                         \
      errorMessage);

/// The callback used to update and aggregate the trace bytes of a query. If the
/// query trace limit is set, the callback return true if the aggregate traced
/// bytes exceed the set limit otherwise return false.
using UpdateAndCheckTraceLimitCB = std::function<void(uint64_t)>;

struct TraceConfig {
  /// Target query trace nodes.
  std::string queryNodeId;
  /// Base dir of query trace.
  std::string queryTraceDir;
  UpdateAndCheckTraceLimitCB updateAndCheckTraceLimitCB;
  /// The trace task regexp.
  std::string taskRegExp;
  /// If true, we only collect operator input trace without the actual
  /// execution. This is used by crash debugging so that we can collect the
  /// input that triggers the crash.
  bool dryRun{false};

  TraceConfig(
      std::string queryNodeIds,
      std::string queryTraceDir,
      UpdateAndCheckTraceLimitCB updateAndCheckTraceLimitCB,
      std::string taskRegExp,
      bool dryRun);
};
} // namespace facebook::velox
