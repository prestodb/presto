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

#include <pybind11/embed.h>

#include "velox/core/PlanNode.h"
#include "velox/exec/Cursor.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/python/plan_builder/PyPlanBuilder.h"
#include "velox/python/type/PyType.h"

namespace facebook::velox::py {

class PyTaskIterator;

/// A C++ wrapper to allow Python clients to execute plans using TaskCursor.
///
/// @param pyPlanNode The plan to be executed (created using
/// pyvelox.plan_builder).
/// @param pool The memory pool to pass to the task.
/// @param executor The executor that will be used by drivers.
class PyLocalRunner {
 public:
  explicit PyLocalRunner(
      const PyPlanNode& pyPlanNode,
      const std::shared_ptr<memory::MemoryPool>& pool,
      const std::shared_ptr<folly::CPUThreadPoolExecutor>& executor);

  /// Add a split to scan an entire file.
  ///
  /// @param pyFile The Python File object describin the file path and format.
  /// @param planId The plan node ID of the scan.
  /// @param connectorId The connector used by the scan.
  void addFileSplit(
      const PyFile& pyFile,
      const std::string& planId,
      const std::string& connectorId);

  /// Add a query configuration parameter. These values are passed to the Velox
  /// Task through a query context object.
  ///
  /// @param configName The name (key) of the configuration parameter.
  /// @param configValue The configuration value.
  void addQueryConfig(
      const std::string& configName,
      const std::string& configValue);

  /// Execute the task and returns an iterable to the output vectors.
  ///
  /// @param maxDrivers Maximum number of drivers to use when executing the
  /// plan.
  pybind11::iterator execute(int32_t maxDrivers = 1);

  /// Prints a descriptive debug message containing plan and execution stats.
  /// If the task hasn't finished, will print the plan with the current stats.
  std::string printPlanWithStats() const;

 private:
  friend class PyTaskIterator;

  // Memory pools and thread pool to be used by queryCtx.
  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> outputPool_;
  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;

  // The plan node to be executed (created using pyvelox.plan_builder).
  core::PlanNodePtr planNode_;

  // The task cursor that executed the Velox Task.
  std::shared_ptr<exec::TaskCursor> cursor_;

  // The Python iterator that exposes output vectors.
  std::shared_ptr<PyTaskIterator> pyIterator_;

  // Pointer to the list of splits to be added to the task.
  TScanFiles scanFiles_;

  // Query configs to be passed to the task.
  TQueryConfigs queryConfigs_;
};

// Iterator class that wraps around a PyLocalRunner and provides an iterable API
// for Python. It needs to provide a .begin() and .end() methods, and the object
// returned by them needs to be comparable and incrementable.
class PyTaskIterator {
 public:
  explicit PyTaskIterator(
      const std::shared_ptr<exec::TaskCursor>& cursor,
      const std::shared_ptr<memory::MemoryPool>& pool)
      : outputPool_(pool), cursor_(cursor) {}

  class Iterator {
   public:
    Iterator() {}

    explicit Iterator(
        const std::shared_ptr<exec::TaskCursor>& cursor,
        const std::shared_ptr<memory::MemoryPool>& pool)
        : outputPool_(pool), cursor_(cursor) {
      // Advance to the first batch.
      advance();
    }

    PyVector operator*() const;

    void advance();

    Iterator& operator++() {
      advance();
      return *this;
    }

    bool operator!=(const Iterator& other) const {
      return vector_ != other.vector_;
    }

    bool operator==(const Iterator& other) const {
      return vector_ == other.vector_;
    }

   private:
    std::shared_ptr<memory::MemoryPool> outputPool_;
    std::shared_ptr<exec::TaskCursor> cursor_;
    RowVectorPtr vector_{nullptr};
  };

  Iterator begin() const {
    return Iterator(cursor_, outputPool_);
  }

  Iterator end() const {
    return Iterator();
  }

 private:
  std::shared_ptr<memory::MemoryPool> outputPool_;
  std::shared_ptr<exec::TaskCursor> cursor_;
};

/// To avoid desctruction order issues during shutdown, this function will
/// iterate over any pending tasks created by this module and wait for their
/// task and drivers to finish.
void drainAllTasks();

} // namespace facebook::velox::py
