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
#include <folly/experimental/ThreadedRepeatingFunctionRunner.h>
#include "velox/common/memory/Memory.h"
#include "velox/exec/Task.h"

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
class PrestoServer;

/// Manages a set of periodic tasks via folly::FunctionScheduler.
/// This is a place to add a new task or add more functionality to an existing
/// one.
class PeriodicTaskManager {
 public:
  explicit PeriodicTaskManager(
      folly::CPUThreadPoolExecutor* driverCPUExecutor,
      folly::CPUThreadPoolExecutor* spillerExecutor,
      folly::IOThreadPoolExecutor* httpExecutor,
      TaskManager* taskManager,
      const velox::memory::MemoryAllocator* memoryAllocator,
      const velox::cache::AsyncDataCache* asyncDataCache,
      const std::unordered_map<
          std::string,
          std::shared_ptr<velox::connector::Connector>>& connectors,
      PrestoServer* server);

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
    repeatedRunner_.add(
        taskName,
        [taskName,
         periodMicros,
         func = std::forward<TFunc>(func)]() mutable noexcept {
          try {
            func();
          } catch (const std::exception& e) {
            LOG(ERROR) << "Error running periodic task " << taskName << ": "
                       << e.what();
          }
          return std::chrono::milliseconds(periodMicros / 1000);
        });
  }

  /// Add a task to run once. Before adding, cancels the any task that has same
  /// name.
  template <typename TFunc>
  void
  addTaskOnce(TFunc&& func, size_t periodMicros, const std::string& taskName) {
    oneTimeRunner_.cancelFunction(taskName);
    oneTimeRunner_.addFunctionOnce(
        std::forward<TFunc>(func),
        taskName,
        std::chrono::microseconds{periodMicros});
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

  void addPrestoExchangeSourceMemoryStatsTask();
  void updatePrestoExchangeSourceMemoryStats();

  void addConnectorStatsTask();

  void addOperatingSystemStatsUpdateTask();
  void updateOperatingSystemStats();

  // Adds task that periodically prints http endpoint latency metrics.
  void addHttpEndpointLatencyStatsTask();
  void printHttpEndpointLatencyStats();

  void addWatchdogTask();

  void detachWorker(const char* reason);
  void maybeAttachWorker();

  folly::CPUThreadPoolExecutor* driverCPUExecutor_;
  folly::CPUThreadPoolExecutor* spillerExecutor_;
  folly::IOThreadPoolExecutor* httpExecutor_;
  TaskManager* taskManager_;
  const velox::memory::MemoryAllocator* memoryAllocator_;
  const velox::cache::AsyncDataCache* asyncDataCache_;
  const velox::memory::MemoryArbitrator* arbitrator_;
  const std::unordered_map<
      std::string,
      std::shared_ptr<velox::connector::Connector>>& connectors_;
  PrestoServer* server_;

  // Operating system related stats.
  int64_t lastUserCpuTimeUs_{0};
  int64_t lastSystemCpuTimeUs_{0};
  int64_t lastSoftPageFaults_{0};
  int64_t lastHardPageFaults_{0};
  int64_t lastVoluntaryContextSwitches_{0};
  int64_t lastForcedContextSwitches_{0};

  // NOTE: declare last since the threads access other members of `this`.
  folly::FunctionScheduler oneTimeRunner_;
  folly::ThreadedRepeatingFunctionRunner repeatedRunner_;
  size_t numDriverThreads_{0};
};

} // namespace facebook::presto
