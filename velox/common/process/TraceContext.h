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

#include "velox/common/process/ThreadLocalRegistry.h"

#include <mutex>
#include <string>
#include <unordered_map>

#include <folly/container/F14Map.h>

namespace facebook::velox::process {

// Aggregates data for a trace context with a given label.
struct TraceData {
  // Number of threads currently in a context covered by 'this'
  int32_t numThreads{0};
  // Cumulative number of times a thread has entered this context.
  int32_t numEnters{0};
  // The total time threads have spent in this context. This is updated when a
  // thread leaves the context.
  uint64_t totalMs{0};
  // The longest time a thread has spent in this context. Updated when a thread
  // leaves.
  uint64_t maxMs{0};
  // Start time of continuous occupancy. This starts when the first
  // thread enters and ends when the last thread leaves.
  std::chrono::steady_clock::time_point startTime;
};

// Records that a thread has entered a section described by a
// label. Possible labels are operations like 'opening a file'
// waiting for read from file, processing file xx and so on. This
// produces a concise report of what the system is doing at any one
// time. This is good for diagnosing crashes or hangs which are
// difficult to figure out from stacks in a core dump.
//
// NOTE: TraceContext is not sharable between different threads.
class TraceContext {
 public:
  // Starts a trace context. isTemporary is false if this is a generic
  // operation for which records should be kept for the lifetime of
  // the process, like opening a file. This is true if we are
  // recording a one off event like 'reading file <filename>' for
  // which the record should be dropped once the last thread finishes.
  explicit TraceContext(std::string label, bool isTemporary = false);

  TraceContext(const TraceContext&) = delete;
  TraceContext& operator=(const TraceContext&) = delete;

  ~TraceContext();

  // Produces a human readable report of all TraceContexts in existence at the
  // time.
  static std::string statusLine();

  // Returns a copy of the trace status.
  static folly::F14FastMap<std::string, TraceData> status();

  // Implementation detail type.  Made public to be available with
  // std::make_shared.  Do not use outside this class.
  using Registry =
      ThreadLocalRegistry<folly::F14FastMap<std::string, TraceData>>;

 private:
  const std::string label_;
  const std::chrono::steady_clock::time_point enterTime_;
  const bool isTemporary_;
  std::shared_ptr<Registry::Reference> traceData_;
};

} // namespace facebook::velox::process
