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

#include "velox/common/process/TraceContext.h"

#include <sstream>

namespace facebook::velox::process {

namespace {
folly::Synchronized<std::unordered_map<std::string, TraceData>>& traceMap() {
  static folly::Synchronized<std::unordered_map<std::string, TraceData>>
      staticTraceMap;
  return staticTraceMap;
}
} // namespace

TraceContext::TraceContext(std::string label, bool isTemporary)
    : label_(std::move(label)),
      enterTime_(std::chrono::steady_clock::now()),
      isTemporary_(isTemporary) {
  traceMap().withWLock([&](auto& counts) {
    auto& data = counts[label_];
    ++data.numThreads;
    if (data.numThreads == 1) {
      data.startTime = enterTime_;
    }
    ++data.numEnters;
  });
}

TraceContext::~TraceContext() {
  traceMap().withWLock([&](auto& counts) {
    auto& data = counts[label_];
    --data.numThreads;
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::steady_clock::now() - enterTime_)
                  .count();
    data.totalMs += ms;
    data.maxMs = std::max<uint64_t>(data.maxMs, ms);
    if (!data.numThreads && isTemporary_) {
      counts.erase(label_);
    }
  });
}

// static
std::string TraceContext::statusLine() {
  std::stringstream out;
  auto now = std::chrono::steady_clock::now();
  traceMap().withRLock([&](auto& counts) {
    for (auto& pair : counts) {
      if (pair.second.numThreads) {
        auto continued = std::chrono::duration_cast<std::chrono::milliseconds>(
                             now - pair.second.startTime)
                             .count();

        out << pair.first << "=" << pair.second.numThreads << " entered "
            << pair.second.numEnters << " avg ms "
            << (pair.second.totalMs / pair.second.numEnters) << " max ms "
            << pair.second.maxMs << " continuous for " << continued
            << std::endl;
      }
    }
  });
  return out.str();
}

// static
std::unordered_map<std::string, TraceData> TraceContext::status() {
  return traceMap().withRLock([&](auto& map) { return map; });
}

} // namespace facebook::velox::process
