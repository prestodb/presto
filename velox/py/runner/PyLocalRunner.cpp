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

#include "velox/py/runner/PyLocalRunner.h"

#include <pybind11/stl.h>
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/core/PlanNode.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/py/vector/PyVector.h"

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

std::unordered_set<std::string>& connectorRegistry() {
  static std::unordered_set<std::string> registry;
  return registry;
}

} // namespace

namespace py = pybind11;

void registerHive(const std::string& connectorId) {
  connector::registerConnectorFactory(
      std::make_shared<connector::hive::HiveConnectorFactory>());

  // TODO: Allow Python users to specify connector configs.
  std::unordered_map<std::string, std::string> configValues = {};
  const auto configs =
      std::make_shared<velox::config::ConfigBase>(std::move(configValues));

  auto hiveConnector =
      connector::getConnectorFactory(connectorId)
          ->newConnector(
              connectorId, configs, folly::getGlobalCPUExecutor().get());
  connector::registerConnector(hiveConnector);
  connectorRegistry().insert(connectorId);
}

// Is it ok to unregister connectors that were not registered.
void unregisterHive(const std::string& connectorId) {
  if (!facebook::velox::connector::unregisterConnector(connectorId) ||
      !facebook::velox::connector::unregisterConnectorFactory(connectorId)) {
    throw std::runtime_error(
        fmt::format("Unable to unregister connector '{}'", connectorId));
  }
  connectorRegistry().erase(connectorId);
}

void unregisterAll() {
  while (!connectorRegistry().empty()) {
    unregisterHive(*connectorRegistry().begin());
  }
}

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
      scanFiles_(pyPlanNode.scanFiles()) {
  // TODO: Make these configurable.
  std::unordered_map<std::string, std::string> configs = {
      {"selective_nimble_reader_enabled", "true"}};

  auto queryCtx = core::QueryCtx::create(
      executor_.get(),
      core::QueryConfig(configs),
      {},
      cache::AsyncDataCache::getInstance(),
      rootPool_);

  cursor_ = exec::TaskCursor::create({
      .planNode = planNode_,
      .queryCtx = queryCtx,
      .outputPool = outputPool_,
  });
}

void PyLocalRunner::addFileSplit(
    const PyFile& pyFile,
    const std::string& planId,
    const std::string& connectorId) {
  auto split =
      velox::exec::Split(std::make_shared<connector::hive::HiveConnectorSplit>(
          connectorId, pyFile.filePath(), pyFile.fileFormat()));
  cursor_->task()->addSplit(planId, std::move(split));
}

py::iterator PyLocalRunner::execute() {
  if (pyIterator_) {
    throw std::runtime_error("PyLocalRunner can only be executed once.");
  }

  // Add any files passed by the client during plan building.
  for (const auto& [scanId, scanPair] : *scanFiles_) {
    for (const auto& inputFile : scanPair.second) {
      addFileSplit(inputFile, scanId, scanPair.first);
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
