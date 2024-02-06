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

#include "velox/common/process/TraceHistory.h"

#include <folly/system/ThreadId.h>

#include <algorithm>

namespace facebook::velox::process {

namespace {
auto registry = std::make_shared<ThreadLocalRegistry<TraceHistory>>();
}

namespace detail {
thread_local ThreadLocalRegistry<TraceHistory>::Reference traceHistory(
    registry);
}

TraceHistory::TraceHistory()
    : threadId_(std::this_thread::get_id()), osTid_(folly::getOSThreadID()) {}

std::vector<TraceHistory::EntriesWithThreadInfo> TraceHistory::listAll() {
  std::vector<EntriesWithThreadInfo> results;
  registry->forAllValues([&](auto& history) {
    EntriesWithThreadInfo result;
    result.threadId = history.threadId_;
    result.osTid = history.osTid_;
    for (int i = 0; i < kCapacity; ++i) {
      const int j = (history.index_ + kCapacity - 1 - i) % kCapacity;
      if (!populated(history.data_[j])) {
        break;
      }
      result.entries.push_back(history.data_[j]);
    }
    std::reverse(result.entries.begin(), result.entries.end());
    results.push_back(std::move(result));
  });
  return results;
}

} // namespace facebook::velox::process
