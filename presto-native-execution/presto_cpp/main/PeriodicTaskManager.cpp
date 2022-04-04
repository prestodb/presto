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

#include "presto_cpp/main/PeriodicTaskManager.h"
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/stop_watch.h>
#include "presto_cpp/main/TaskManager.h"
#include "presto_cpp/main/common/Counters.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/memory/MappedMemory.h"

namespace facebook::presto {

// Every two seconds we export server counters.
static constexpr size_t kTaskPeriodGlobalCounters{2'000'000}; // 2 seconds.
// Every two seconds we export memory counters.
static constexpr size_t kMemoryPeriodGlobalCounters{2'000'000}; // 2 seconds.
// Every 1 minute we clean old tasks.
static constexpr size_t kTaskPeriodCleanOldTasks{60'000'000}; // 60 seconds.

PeriodicTaskManager::PeriodicTaskManager(
    folly::CPUThreadPoolExecutor* driverCPUExecutor,
    folly::IOThreadPoolExecutor* httpExecutor,
    TaskManager* taskManager)
    : driverCPUExecutor_(driverCPUExecutor),
      httpExecutor_(httpExecutor),
      taskManager_(taskManager) {}

void PeriodicTaskManager::start() {
  // Add new functions here.

  // If executors are null, don't bother starting this task.
  if (driverCPUExecutor_ or httpExecutor_) {
    scheduler_.addFunction(
        [driverCPUExecutor = driverCPUExecutor_,
         httpExecutor = httpExecutor_]() {
          if (driverCPUExecutor) {
            // Report the current queue size of the thread pool.
            REPORT_ADD_STAT_VALUE(
                kCounterDriverCPUExecutorQueueSize,
                driverCPUExecutor->getTaskQueueSize());

            // Report the latency between scheduling the task and its execution.
            folly::stop_watch<std::chrono::milliseconds> timer;
            driverCPUExecutor->add([timer = timer]() {
              REPORT_ADD_STAT_VALUE(
                  kCounterDriverCPUExecutorLatencyMs, timer.elapsed().count());
            });
          }

          if (httpExecutor) {
            // Report the latency between scheduling the task and its execution.
            folly::stop_watch<std::chrono::milliseconds> timer;
            httpExecutor->add([timer = timer]() {
              REPORT_ADD_STAT_VALUE(
                  kCounterHTTPExecutorLatencyMs, timer.elapsed().count());
            });
          }
        },
        std::chrono::microseconds{kTaskPeriodGlobalCounters},
        "executor_counters");
  }

  if (taskManager_) {
    scheduler_.addFunction(
        [taskManager = taskManager_]() {
          // Report the number of tasks and drivers in the system.
          size_t numTasks{0};
          auto taskNumbers = taskManager->getTaskNumbers(numTasks);
          REPORT_ADD_STAT_VALUE(kCounterNumTasks, taskManager->getNumTasks());
          REPORT_ADD_STAT_VALUE(
              kCounterNumTasksRunning,
              taskNumbers[velox::exec::TaskState::kRunning]);
          REPORT_ADD_STAT_VALUE(
              kCounterNumTasksFinished,
              taskNumbers[velox::exec::TaskState::kFinished]);
          REPORT_ADD_STAT_VALUE(
              kCounterNumTasksCancelled,
              taskNumbers[velox::exec::TaskState::kCanceled]);
          REPORT_ADD_STAT_VALUE(
              kCounterNumTasksAborted,
              taskNumbers[velox::exec::TaskState::kAborted]);
          REPORT_ADD_STAT_VALUE(
              kCounterNumTasksFailed,
              taskNumbers[velox::exec::TaskState::kFailed]);
          REPORT_ADD_STAT_VALUE(
              kCounterNumDrivers, taskManager->getNumRunningDrivers());
        },
        std::chrono::microseconds{kTaskPeriodGlobalCounters},
        "task_counters");

    scheduler_.addFunction(
        [taskManager = taskManager_]() {
          // Report the number of tasks and drivers in the system.
          taskManager->cleanOldTasks();
        },
        std::chrono::microseconds{kTaskPeriodCleanOldTasks},
        "clean_old_tasks");
  }

  if (auto* mappedMemory = velox::memory::MappedMemory::getInstance()) {
    scheduler_.addFunction(
        [mappedMemory]() {
          REPORT_ADD_STAT_VALUE(
              kCounterMappedMemoryBytes, (mappedMemory->numMapped() * 4096l));
          REPORT_ADD_STAT_VALUE(
              kCounterAllocatedMemoryBytes,
              (mappedMemory->numAllocated() * 4096l));
        },
        std::chrono::microseconds{kMemoryPeriodGlobalCounters},
        "memory_counters");
  }

  // This should be the last call in this method.
  scheduler_.start();
}

void PeriodicTaskManager::stop() {
  scheduler_.cancelAllFunctionsAndWait();
  scheduler_.shutdown();
}

} // namespace facebook::presto
