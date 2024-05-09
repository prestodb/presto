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

#include <folly/experimental/ThreadedRepeatingFunctionRunner.h>
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/SsdFile.h"
#include "velox/common/memory/MemoryArbitrator.h"

namespace folly {
class CPUThreadPoolExecutor;
}

namespace facebook::velox {

namespace memory {
class MemoryAllocator;
}

namespace cache {
class AsyncDataCache;
}

/// Manages a background daemon thread to report stats through 'StatsReporter'.
class PeriodicStatsReporter {
 public:
  struct Options {
    Options() {}

    const velox::memory::MemoryAllocator* allocator{nullptr};
    uint64_t allocatorStatsIntervalMs{2'000};

    const velox::cache::AsyncDataCache* cache{nullptr};
    uint64_t cacheStatsIntervalMs{60'000};

    const memory::MemoryArbitrator* arbitrator{nullptr};
    uint64_t arbitratorStatsIntervalMs{60'000};

    std::string toString() const {
      return fmt::format(
          "allocatorStatsIntervalMs:{}, cacheStatsIntervalMs:{}, "
          "arbitratorStatsIntervalMs:{}",
          allocatorStatsIntervalMs,
          cacheStatsIntervalMs,
          arbitratorStatsIntervalMs);
    }
  };

  PeriodicStatsReporter(const Options& options = Options());

  /// Invoked to start the report daemon in background.
  void start();

  /// Invoked to stop the report daemon in background.
  void stop();

 private:
  // Add a task to run periodically.
  template <typename TFunc>
  void addTask(const std::string& taskName, TFunc&& func, size_t intervalMs) {
    scheduler_.add(
        taskName,
        [taskName,
         intervalMs,
         func = std::forward<TFunc>(func)]() mutable noexcept {
          try {
            func();
          } catch (const std::exception& e) {
            LOG(ERROR) << "Error running periodic task " << taskName << ": "
                       << e.what();
          }
          return std::chrono::milliseconds(intervalMs);
        });
  }

  void reportCacheStats();
  void reportAllocatorStats();
  void reportArbitratorStats();

  const velox::memory::MemoryAllocator* const allocator_{nullptr};
  const velox::cache::AsyncDataCache* const cache_{nullptr};
  const velox::memory::MemoryArbitrator* const arbitrator_{nullptr};
  const Options options_;

  cache::CacheStats lastCacheStats_;

  folly::ThreadedRepeatingFunctionRunner scheduler_;
};

/// Initializes and starts the process-wide periodic stats reporter. Before
/// 'stopPeriodicStatsReporter()' is called, this method can only be called once
/// process-wide, and additional calls to this method will throw.
void startPeriodicStatsReporter(const PeriodicStatsReporter::Options& options);

/// Stops the process-wide periodic stats reporter.
void stopPeriodicStatsReporter();

} // namespace facebook::velox
