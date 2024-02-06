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

#include "velox/common/process/TraceHistory.h"

#include <sstream>

namespace facebook::velox::process {

namespace {

// We use thread local instead lock here since the critical path is on write
// side.
auto registry = std::make_shared<TraceContext::Registry>();
thread_local auto threadLocalTraceData =
    std::make_shared<TraceContext::Registry::Reference>(registry);

} // namespace

TraceContext::TraceContext(std::string label, bool isTemporary)
    : label_(std::move(label)),
      enterTime_(std::chrono::steady_clock::now()),
      isTemporary_(isTemporary),
      traceData_(threadLocalTraceData) {
  TraceHistory::push([&](auto& entry) {
    entry.time = enterTime_;
    entry.file = __FILE__;
    entry.line = __LINE__;
    snprintf(entry.label, entry.kLabelCapacity, "%s", label_.c_str());
  });
  traceData_->withValue([&](auto& counts) {
    auto& data = counts[label_];
    ++data.numThreads;
    if (data.numThreads == 1) {
      data.startTime = enterTime_;
    }
    ++data.numEnters;
  });
}

TraceContext::~TraceContext() {
  traceData_->withValue([&](auto& counts) {
    auto it = counts.find(label_);
    auto& data = it->second;
    if (--data.numThreads == 0 && isTemporary_) {
      counts.erase(it);
      return;
    }
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::steady_clock::now() - enterTime_)
                  .count();
    data.totalMs += ms;
    data.maxMs = std::max<uint64_t>(data.maxMs, ms);
  });
}

// static
std::string TraceContext::statusLine() {
  std::stringstream out;
  auto now = std::chrono::steady_clock::now();
  auto counts = status();
  for (auto& [label, data] : counts) {
    if (data.numThreads > 0) {
      auto continued = std::chrono::duration_cast<std::chrono::milliseconds>(
                           now - data.startTime)
                           .count();
      out << label << ": numThreads=" << data.numThreads
          << " numEnters=" << data.numEnters
          << " avgMs=" << (data.totalMs / data.numEnters)
          << " maxMs=" << data.maxMs << " continued=" << continued << std::endl;
    }
  }
  return out.str();
}

// static
folly::F14FastMap<std::string, TraceData> TraceContext::status() {
  folly::F14FastMap<std::string, TraceData> total;
  registry->forAllValues([&](auto& counts) {
    for (auto& [k, v] : counts) {
      auto& sofar = total[k];
      if (sofar.numEnters == 0) {
        sofar.startTime = v.startTime;
      } else if (v.numEnters > 0) {
        sofar.startTime = std::min(sofar.startTime, v.startTime);
      }
      sofar.numThreads += v.numThreads;
      sofar.numEnters += v.numEnters;
      sofar.totalMs += v.totalMs;
      sofar.maxMs = std::max(sofar.maxMs, v.maxMs);
    }
  });
  return total;
}

} // namespace facebook::velox::process
