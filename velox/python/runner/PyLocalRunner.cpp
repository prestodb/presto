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

#include "velox/python/runner/PyLocalRunner.h"

#include <pybind11/stl.h>
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/core/PlanNode.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/Spill.h"
#include "velox/python/vector/PyVector.h"

namespace facebook::velox::py {
namespace {

std::list<std::weak_ptr<exec::Task>>& taskRegistry() {
  static std::list<std::weak_ptr<exec::Task>> registry;
  return registry;
}

std::mutex& taskRegistryLock() {
  static std::mutex lock;
  return lock;
}

} // namespace

namespace py = pybind11;

PyVector PyTaskIterator::Iterator::operator*() const {
  return PyVector{vector_, outputPool_};
}

void PyTaskIterator::Iterator::advance() {
  if (cursor_ && cursor_->moveNext()) {
    vector_ = cursor_->current();
  } else {
    vector_ = nullptr;
  }
}

PyLocalRunner::PyLocalRunner(
    const PyPlanNode& pyPlanNode,
    const std::shared_ptr<memory::MemoryPool>& pool,
    const std::shared_ptr<folly::CPUThreadPoolExecutor>& executor)
    : rootPool_(pool),
      outputPool_(memory::memoryManager()->addLeafPool()),
      executor_(executor),
      planNode_(pyPlanNode.planNode()),
      scanFiles_(pyPlanNode.scanFiles()),
      queryConfigs_(pyPlanNode.queryConfigs()) {}

void PyLocalRunner::addFileSplit(
    const PyFile& pyFile,
    const std::string& planId,
    const std::string& connectorId) {
  scanFiles_[planId].emplace_back(
      std::make_shared<connector::hive::HiveConnectorSplit>(
          connectorId, pyFile.filePath(), pyFile.fileFormat()));
}

void PyLocalRunner::addQueryConfig(
    const std::string& configName,
    const std::string& configValue) {
  queryConfigs_[configName] = configValue;
}

py::iterator PyLocalRunner::execute(int32_t maxDrivers) {
  if (pyIterator_) {
    throw std::runtime_error("PyLocalRunner can only be executed once.");
  }

  // Create query context.
  auto queryCtx = core::QueryCtx::create(
      executor_.get(),
      core::QueryConfig(queryConfigs_),
      {},
      cache::AsyncDataCache::getInstance(),
      rootPool_);

  // Intialize task cursor and task.
  cursor_ = exec::TaskCursor::create({
      .planNode = planNode_,
      .maxDrivers = maxDrivers,
      .queryCtx = queryCtx,
      .outputPool = outputPool_,
  });

  // Add any files passed by the client during plan building.
  for (auto& [scanId, splits] : scanFiles_) {
    for (auto& split : splits) {
      cursor_->task()->addSplit(scanId, exec::Split(std::move(split)));
    }
    cursor_->task()->noMoreSplits(scanId);
  }

  {
    std::lock_guard<std::mutex> guard(taskRegistryLock());
    taskRegistry().push_back(cursor_->task());
  }

  pyIterator_ = std::make_shared<PyTaskIterator>(cursor_, outputPool_);
  return py::make_iterator(pyIterator_->begin(), pyIterator_->end());
}

std::string PyLocalRunner::printPlanWithStats() const {
  return exec::printPlanWithStats(
      *planNode_, cursor_->task()->taskStats(), true);
}

void drainAllTasks() {
  auto& executor = folly::QueuedImmediateExecutor::instance();
  std::lock_guard<std::mutex> guard(taskRegistryLock());

  auto it = taskRegistry().begin();
  while (it != taskRegistry().end()) {
    // Try to acquire a shared_ptr from the weak_ptr (in case the task has
    // already finished).
    if (auto task = it->lock()) {
      if (!task->isFinished()) {
        task->requestAbort();
      }
      auto future = task->taskCompletionFuture()
                        .within(std::chrono::seconds(1))
                        .via(&executor);
      future.wait();
    }
    it = taskRegistry().erase(it);
  }
}

} // namespace facebook::velox::py
