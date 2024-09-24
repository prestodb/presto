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

#include <functional>
#include <string>
#include <unordered_set>

namespace facebook::velox::exec::trace {

/// The callback used to update and aggregate the trace bytes of a query. If the
/// query trace limit is set, the callback return true if the aggregate traced
/// bytes exceed the set limit otherwise return false.
using UpdateAndCheckTraceLimitCB = std::function<bool(uint64_t)>;

struct QueryTraceConfig {
  /// Target query trace nodes.
  std::unordered_set<std::string> queryNodes;
  /// Base dir of query trace.
  std::string queryTraceDir;
  UpdateAndCheckTraceLimitCB updateAndCheckTraceLimitCB;
  /// The trace task regexp.
  std::string taskRegExp;

  QueryTraceConfig(
      std::unordered_set<std::string> _queryNodeIds,
      std::string _queryTraceDir,
      UpdateAndCheckTraceLimitCB _updateAndCheckTraceLimitCB,
      std::string _taskRegExp);

  QueryTraceConfig(std::string _queryTraceDir);

  QueryTraceConfig() = default;
};
} // namespace facebook::velox::exec::trace
