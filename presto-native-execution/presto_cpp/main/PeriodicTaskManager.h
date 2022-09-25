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

namespace folly {
class CPUThreadPoolExecutor;
class IOThreadPoolExecutor;
} // namespace folly

namespace facebook::presto {

class TaskManager;

// Manages a set of periodic tasks via folly::FunctionScheduler.
// This is a place to add a new task or add more functionality to an existing
// one.
class PeriodicTaskManager {
 public:
  explicit PeriodicTaskManager(
      folly::CPUThreadPoolExecutor* driverCPUExecutor,
      folly::IOThreadPoolExecutor* httpExecutor,
      TaskManager* taskManager);
  ~PeriodicTaskManager() {
    stop();
  }

  // All the tasks will start here.
  void start();

  // Add a task to run periodically.
  template <typename TFunc>
  void addTask(TFunc&& func, size_t periodMicros, const std::string& taskName) {
    scheduler_.addFunction(
        func, std::chrono::microseconds{periodMicros}, taskName);
  }

  // Stops all periodic tasks. Returns only when everything is stopped.
  void stop();

 private:
  folly::FunctionScheduler scheduler_;
  folly::CPUThreadPoolExecutor* driverCPUExecutor_;
  folly::IOThreadPoolExecutor* httpExecutor_;
  TaskManager* taskManager_;
  velox::memory::MemoryManager<velox::memory::MmapMemoryAllocator>&
      memoryManager_;
};

} // namespace facebook::presto
