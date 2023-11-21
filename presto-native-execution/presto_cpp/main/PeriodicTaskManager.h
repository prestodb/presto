/*
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

#include <folly/experimental/FunctionScheduler.h>
#include "velox/common/memory/Memory.h"
#include "velox/exec/Spill.h"

namespace folly {
class CPUThreadPoolExecutor;
class IOThreadPoolExecutor;
} // namespace folly

namespace facebook::velox::connector {
class Connector;
}

namespace facebook::velox::cache {
class AsyncDataCache;
}

namespace facebook::presto {

class TaskManager;

/// Manages a set of periodic tasks via folly::FunctionScheduler.
/// This is a place to add a new task or add more functionality to an existing
/// one.
class PeriodicTaskManager {
 public:
  explicit PeriodicTaskManager(
      folly::CPUThreadPoolExecutor* const driverCPUExecutor,
      folly::IOThreadPoolExecutor* const httpExecutor,
      TaskManager* const taskManager,
      const velox::memory::MemoryAllocator* const memoryAllocator,
      const velox::cache::AsyncDataCache* const asyncDataCache,
      const std::unordered_map<
          std::string,
          std::shared_ptr<velox::connector::Connector>>& connectors);

  ~PeriodicTaskManager() {
    stop();
  }

  /// Invoked to start all registered, and fundamental periodic tasks running at
  /// the background.
  ///
  /// NOTE: start() shall be called after everything in PrestoServer is
  /// initialized because PeriodicTaskManager relies on proper initializations
  /// of various entities in the system to work as expected.
  void start();

  /// Add a task to run periodically.
  template <typename TFunc>
  void addTask(TFunc&& func, size_t periodMicros, const std::string& taskName) {
    scheduler_.addFunction(
        func, std::chrono::microseconds{periodMicros}, taskName);
  }

  /// Add a task to run once. Before adding, cancels the any task that has same
  /// name.
  template <typename TFunc>
  void
  addTaskOnce(TFunc&& func, size_t periodMicros, const std::string& taskName) {
    scheduler_.cancelFunction(taskName);
    scheduler_.addFunctionOnce(
        func, taskName, std::chrono::microseconds{periodMicros});
  }

  /// Stops all periodic tasks. Returns only when everything is stopped.
  void stop();

 private:
  void addExecutorStatsTask();
  void updateExecutorStats();

  void addTaskStatsTask();
  void updateTaskStats();

  void addOldTaskCleanupTask();
  void cleanupOldTask();

  void addMemoryAllocatorStatsTask();
  void updateMemoryAllocatorStats();

  void addPrestoExchangeSourceMemoryStatsTask();
  void updatePrestoExchangeSourceMemoryStats();

  void addCacheStatsUpdateTask();
  void updateCacheStats();

  void addConnectorStatsTask();

  void addOperatingSystemStatsUpdateTask();
  void updateOperatingSystemStats();

  void addSpillStatsUpdateTask();
  void updateSpillStatsTask();

  // Adds task that periodically prints http endpoint latency metrics.
  void addHttpEndpointLatencyStatsTask();
  void printHttpEndpointLatencyStats();

  void addArbitratorStatsTask();
  void updateArbitratorStatsTask();

  folly::FunctionScheduler scheduler_;
  folly::CPUThreadPoolExecutor* const driverCPUExecutor_;
  folly::IOThreadPoolExecutor* const httpExecutor_;
  TaskManager* const taskManager_;
  const velox::memory::MemoryAllocator* const memoryAllocator_;
  const velox::cache::AsyncDataCache* const asyncDataCache_;
  const velox::memory::MemoryArbitrator* const arbitrator_;
  const std::unordered_map<
      std::string,
      std::shared_ptr<velox::connector::Connector>>& connectors_;

  // Cache related stats
  int64_t lastMemoryCacheHits_{0};
  int64_t lastMemoryCacheHitsBytes_{0};
  int64_t lastMemoryCacheInserts_{0};
  int64_t lastMemoryCacheEvictions_{0};
  int64_t lastMemoryCacheEvictionChecks_{0};
  int64_t lastMemoryCacheStalls_{0};
  int64_t lastMemoryCacheAllocClocks_{0};

  // Operating system related stats.
  int64_t lastUserCpuTimeUs_{0};
  int64_t lastSystemCpuTimeUs_{0};
  int64_t lastSoftPageFaults_{0};
  int64_t lastHardPageFaults_{0};
  int64_t lastVoluntaryContextSwitches_{0};
  int64_t lastForcedContextSwitches_{0};
  // Renabled this after update velox.
#if 0
  velox::exec::SpillStats lastSpillStats_;
#endif
  velox::memory::MemoryArbitrator::Stats lastArbitratorStats_;
};

} // namespace facebook::presto
