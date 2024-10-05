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

#include "velox/exec/QueryTraceConfig.h"

#include <utility>

namespace facebook::velox::exec::trace {

QueryTraceConfig::QueryTraceConfig(
    std::unordered_set<std::string> _queryNodeIds,
    std::string _queryTraceDir,
    UpdateAndCheckTraceLimitCB _updateAndCheckTraceLimitCB,
    std::string _taskRegExp)
    : queryNodes(std::move(_queryNodeIds)),
      queryTraceDir(std::move(_queryTraceDir)),
      updateAndCheckTraceLimitCB(std::move(_updateAndCheckTraceLimitCB)),
      taskRegExp(std::move(_taskRegExp)) {}

QueryTraceConfig::QueryTraceConfig(std::string _queryTraceDir)
    : QueryTraceConfig(
          std::unordered_set<std::string>{},
          std::move(_queryTraceDir),
          [](uint64_t) { return false; },
          ".*") {}

} // namespace facebook::velox::exec::trace
