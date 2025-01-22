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

#include "velox/tool/trace/TraceReplayTaskRunner.h"

namespace facebook::velox::tool::trace {

std::pair<std::shared_ptr<exec::Task>, RowVectorPtr> TraceReplayTaskRunner::run(
    bool copyResults) {
  auto cursor = exec::TaskCursor::create(cursorParams_);
  std::vector<RowVectorPtr> results;
  auto* task = cursor->task().get();
  addSplits(task);

  while (cursor->moveNext()) {
    results.push_back(cursor->current());
  }

  task->taskCompletionFuture().wait();

  if (copyResults) {
    return {cursor->task(), copy(results)};
  }

  return {cursor->task(), nullptr};
}

std::shared_ptr<RowVector> TraceReplayTaskRunner::copy(
    const std::vector<RowVectorPtr>& results) {
  auto totalRows = 0;
  for (const auto& result : results) {
    totalRows += result->size();
  }
  auto copyResult = BaseVector::create<RowVector>(
      cursorParams_.planNode->outputType(),
      totalRows,
      memory::traceMemoryPool());
  auto resultRowOffset = 0;
  for (const auto& result : results) {
    copyResult->copy(result.get(), resultRowOffset, 0, result->size());
    resultRowOffset += result->size();
  }
  return copyResult;
}

TraceReplayTaskRunner& TraceReplayTaskRunner::maxDrivers(int32_t maxDrivers) {
  cursorParams_.maxDrivers = maxDrivers;
  return *this;
}

TraceReplayTaskRunner& TraceReplayTaskRunner::spillDirectory(
    const std::string& dir) {
  cursorParams_.spillDirectory = dir;
  return *this;
}

TraceReplayTaskRunner& TraceReplayTaskRunner::splits(
    const core::PlanNodeId& planNodeId,
    std::vector<exec::Split> splits) {
  splits_[planNodeId] = std::move(splits);
  return *this;
}

void TraceReplayTaskRunner::addSplits(exec::Task* task) {
  for (auto& [nodeId, nodeSplits] : splits_) {
    for (auto& split : nodeSplits) {
      task->addSplit(nodeId, std::move(split));
    }
    task->noMoreSplits(nodeId);
  }
}

} // namespace facebook::velox::tool::trace
