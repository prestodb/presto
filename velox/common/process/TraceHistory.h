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

#include <cassert>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

/// Push an entry to the history ring buffer with a label from format string
/// (same as printf) and optional arguments.
#define VELOX_TRACE_HISTORY_PUSH(_format, ...)                             \
  ::facebook::velox::process::TraceHistory::push([&](auto& entry) {        \
    entry.time = ::std::chrono::steady_clock::now();                       \
    entry.file = __FILE__;                                                 \
    entry.line = __LINE__;                                                 \
    ::snprintf(entry.label, entry.kLabelCapacity, _format, ##__VA_ARGS__); \
  })

namespace facebook::velox::process {

class TraceHistory;

namespace detail {
extern thread_local ThreadLocalRegistry<TraceHistory>::Reference traceHistory;
}

/// Keep list of labels in a ring buffer that is fixed sized and thread local.
class TraceHistory {
 public:
  TraceHistory();

  /// An entry with tracing information and custom label.
  struct Entry {
    std::chrono::steady_clock::time_point time;
    const char* file;
    int32_t line;

    static constexpr int kLabelCapacity =
        64 - sizeof(time) - sizeof(file) - sizeof(line);
    char label[kLabelCapacity];
  };

  /// NOTE: usually VELOX_TRACE_HISTORY_PUSH should be used instead of calling
  /// this function directly.
  ///
  /// Add a new entry to the thread local instance.  If there are more than
  /// `kCapacity' entries, overwrite the oldest ones.  All the mutation on the
  /// new entry should be done in the functor `init'.
  template <typename F>
  static void push(F&& init) {
    detail::traceHistory.withValue(
        [init = std::forward<F>(init)](auto& history) {
          auto& entry = history.data_[history.index_];
          init(entry);
          assert(populated(entry));
          history.index_ = (history.index_ + 1) % kCapacity;
        });
  }

  /// All entries in a specific thread.
  struct EntriesWithThreadInfo {
    std::thread::id threadId;
    uint64_t osTid;
    std::vector<Entry> entries;
  };

  /// List all entries from all threads.
  static std::vector<EntriesWithThreadInfo> listAll();

  /// Keep the last `kCapacity' entries per thread.  Must be a power of 2.
  static constexpr int kCapacity = 16;

 private:
  static_assert((kCapacity & (kCapacity - 1)) == 0);
  static_assert(sizeof(Entry) == 64);

  static bool populated(const Entry& entry) {
    return entry.file != nullptr;
  }

  alignas(64) Entry data_[kCapacity]{};
  const std::thread::id threadId_;
  const uint64_t osTid_;
  int index_ = 0;
};

} // namespace facebook::velox::process
