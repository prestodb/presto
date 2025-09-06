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

#include "velox/exec/TaskStructs.h"

namespace facebook::velox::exec {

void SplitsStore::addSplit(
    Split split,
    std::vector<ContinuePromise>& promises) {
  VELOX_CHECK(!noMoreSplits_);
  VELOX_CHECK(!(remoteSplit_ && split.isBarrier()));
  splits_.push_back(std::move(split));
  if (promises_.empty()) {
    return;
  }
  promises.push_back(std::move(promises_.back()));
  promises_.pop_back();
}

ContinueFuture SplitsStore::makeFuture() {
  auto [promise, future] =
      makeVeloxContinuePromiseContract("SplitsStore::makeFuture");
  promises_.push_back(std::move(promise));
  return std::move(future);
}

Split SplitsStore::getSplit(
    int maxPreloadSplits,
    const ConnectorSplitPreloadFunc& preload) {
  int readySplitIndex = -1;
  if (maxPreloadSplits > 0) {
    for (int i = 0, end = std::min<size_t>(maxPreloadSplits, splits_.size());
         i < end;
         ++i) {
      if (splits_[i].isBarrier()) {
        VELOX_CHECK(!remoteSplit_);
        continue;
      }
      auto& connectorSplit = splits_[i].connectorSplit;
      if (!connectorSplit->dataSource) {
        // Initializes split->dataSource.
        preload(connectorSplit);
        preloadingSplits_->insert(connectorSplit);
      } else if (
          readySplitIndex == -1 && connectorSplit->dataSource->hasValue()) {
        readySplitIndex = i;
        preloadingSplits_->erase(connectorSplit);
      }
    }
  }
  if (readySplitIndex == -1) {
    readySplitIndex = 0;
  }
  VELOX_CHECK(!splits_.empty());
  auto split = std::move(splits_[readySplitIndex]);
  splits_.erase(splits_.begin() + readySplitIndex);
  --taskStats_->numQueuedSplits;
  ++taskStats_->numRunningSplits;
  if (!remoteSplit_ && split.connectorSplit) {
    --taskStats_->numQueuedTableScanSplits;
    ++taskStats_->numRunningTableScanSplits;
    taskStats_->queuedTableScanSplitWeights -=
        split.connectorSplit->splitWeight;
    taskStats_->runningTableScanSplitWeights +=
        split.connectorSplit->splitWeight;
  }
  taskStats_->lastSplitStartTimeMs = getCurrentTimeMs();
  if (taskStats_->firstSplitStartTimeMs == 0) {
    taskStats_->firstSplitStartTimeMs = taskStats_->lastSplitStartTimeMs;
  }
  return split;
}

} // namespace facebook::velox::exec
