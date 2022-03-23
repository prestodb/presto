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

#include <boost/circular_buffer.hpp>

#include "velox/exec/ContainerRowSerde.h"
#include "velox/exec/Merge.h"
#include "velox/exec/Task.h"

namespace facebook::velox::exec {

Merge::Merge(
    int32_t operatorId,
    DriverCtx* ctx,
    RowTypePtr outputType,
    const std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>&
        sortingKeys,
    const std::vector<core::SortOrder>& sortingOrders,
    const std::string& planNodeId,
    const std::string& operatorType)
    : SourceOperator(
          ctx,
          std::move(outputType),
          operatorId,
          planNodeId,
          operatorType),
      rowContainer_(std::make_unique<RowContainer>(
          outputType_->children(),
          operatorCtx_->mappedMemory())),
      comparator_(Comparator(
          outputType_,
          sortingKeys,
          sortingOrders,
          rowContainer_.get())),
      future_(false) {}

BlockingReason Merge::isBlocked(ContinueFuture* future) {
  BlockingReason reason = blockingReason_;
  if (blockingReason_ != BlockingReason::kNotBlocked) {
    *future = std::move(future_);
  } else {
    reason = addMergeSources(future);
  }
  blockingReason_ = BlockingReason::kNotBlocked;
  return reason;
}

BlockingReason Merge::pushSource(ContinueFuture* future, size_t sourceId) {
  if (sourceCursors_.size() <= sourceId) {
    sourceCursors_.resize(sourceId + 1);
  }

  auto& cursor = sourceCursors_[sourceId];
  if (cursor.hasNext()) {
    candidates_.push_back({sourceId, cursor.nextUnchecked()});
    return BlockingReason::kNotBlocked;
  }

  if (cursor.atEnd) {
    return BlockingReason::kNotBlocked;
  }

  RowVectorPtr data;
  auto reason = sources_[sourceId]->next(data, future);
  if (reason != BlockingReason::kNotBlocked) {
    return reason;
  }

  if (data) {
    VELOX_CHECK_LT(0, data->size());
    cursor.reset(data, rowContainer_.get());
    candidates_.push_back({sourceId, cursor.nextUnchecked()});
  } else {
    cursor.atEnd = true;
  }

  return BlockingReason::kNotBlocked;
}

void Merge::SourceCursor::reset(
    const RowVectorPtr& data,
    RowContainer* rowContainer) {
  rows.clear();
  index = 0;

  SelectivityVector allRows(data->size());
  rows.reserve(data->size());
  for (int i = 0; i < data->size(); ++i) {
    rows.emplace_back(rowContainer->newRow());
  }

  DecodedVector decoded;
  for (int col = 0; col < data->childrenSize(); ++col) {
    decoded.decode(*data->childAt(col), allRows);
    for (int i = 0; i < data->size(); ++i) {
      rowContainer->store(decoded, i, rows[i], col);
    }
  }
}

// Returns kNotBlocked if all sources ready and the priority queue has
// their top rows.
BlockingReason Merge::ensureSourcesReady(ContinueFuture* future) {
  auto reason = addMergeSources(future);
  if (reason != BlockingReason::kNotBlocked) {
    return reason;
  }

  if (numSourcesAdded_ < sources_.size()) {
    // We have not considered some of the sources yet. Push top rows of
    // sources into the priority queue. If they are not available yet, then
    // block.
    while (currentSourcePos_ < sources_.size()) {
      reason = pushSource(future, currentSourcePos_);
      if (reason != BlockingReason::kNotBlocked) {
        return reason;
      }
      ++currentSourcePos_;
      ++numSourcesAdded_;
    }
  }

  // Finally, push any outstanding rows into the priority queue.
  if (currentSourcePos_ < sources_.size()) {
    reason = pushSource(future, currentSourcePos_);
    if (reason != BlockingReason::kNotBlocked) {
      return reason;
    }
    currentSourcePos_ = sources_.size();
  }

  return BlockingReason::kNotBlocked;
}

bool Merge::isFinished() {
  return blockingReason_ == BlockingReason::kNotBlocked && candidates_.empty();
}

Merge::SourceRow Merge::nextOutputRow() {
  size_t maxSource = 0;
  for (auto i = 1; i < candidates_.size(); i++) {
    if (comparator_(candidates_[maxSource], candidates_[i])) {
      maxSource = i;
    }
  }

  auto entry = candidates_[maxSource];
  if (maxSource < candidates_.size() - 1) {
    candidates_[maxSource] = candidates_.back();
  }
  candidates_.pop_back();
  return entry;
}

RowVectorPtr Merge::getOutput() {
  blockingReason_ = ensureSourcesReady(&future_);
  if (blockingReason_ != BlockingReason::kNotBlocked) {
    return nullptr;
  }

  const auto numRowsPerBatch =
      rowContainer_->estimatedNumRowsPerBatch(kBatchSizeInBytes);
  rows_.reserve(numRowsPerBatch);

  while (!candidates_.empty()) {
    auto entry = nextOutputRow();
    rows_.push_back(entry.second);
    blockingReason_ = pushSource(&future_, entry.first);
    if (blockingReason_ != BlockingReason::kNotBlocked) {
      currentSourcePos_ = entry.first;
      break;
    }
    if (rows_.size() >= numRowsPerBatch) {
      break;
    }
  }

  if (rows_.size() >= numRowsPerBatch ||
      (!rows_.empty() && candidates_.empty())) {
    auto result = std::dynamic_pointer_cast<RowVector>(
        BaseVector::create(outputType_, rows_.size(), operatorCtx_->pool()));

    rowContainer_->extractRows(rows_, result);
    rowContainer_->eraseRows(folly::Range(rows_.data(), rows_.size()));
    rows_.clear();
    return result;
  }
  return nullptr;
}

Merge::Comparator::Comparator(
    const RowTypePtr& type,
    const std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>&
        sortingKeys,
    const std::vector<core::SortOrder>& sortingOrders,
    RowContainer* rowContainer)
    : rowContainer_(rowContainer) {
  auto numKeys = sortingKeys.size();
  for (int i = 0; i < numKeys; ++i) {
    auto channel = exprToChannel(sortingKeys[i].get(), type);
    VELOX_CHECK_NE(
        channel,
        kConstantChannel,
        "Merge doesn't allow constant grouping keys");
    keyInfo_.emplace_back(
        channel,
        CompareFlags{
            sortingOrders[i].isNullsFirst(),
            sortingOrders[i].isAscending(),
            false});
  }
}

LocalMerge::LocalMerge(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::LocalMergeNode>& localMergeNode)
    : Merge(
          operatorId,
          driverCtx,
          localMergeNode->outputType(),
          localMergeNode->sortingKeys(),
          localMergeNode->sortingOrders(),
          localMergeNode->id(),
          "LocalMerge") {
  VELOX_CHECK_EQ(
      operatorCtx_->driverCtx()->driverId,
      0,
      "LocalMerge needs to run single-threaded");
}

BlockingReason LocalMerge::addMergeSources(ContinueFuture* /* future */) {
  if (sources_.empty()) {
    sources_ = operatorCtx_->task()->getLocalMergeSources(
        operatorCtx_->driverCtx()->splitGroupId, planNodeId());
  }
  return BlockingReason::kNotBlocked;
}

MergeExchange::MergeExchange(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::MergeExchangeNode>& mergeExchangeNode)
    : Merge(
          operatorId,
          driverCtx,
          mergeExchangeNode->outputType(),
          mergeExchangeNode->sortingKeys(),
          mergeExchangeNode->sortingOrders(),
          mergeExchangeNode->id(),
          "MergeExchange") {}

BlockingReason MergeExchange::addMergeSources(ContinueFuture* future) {
  if (operatorCtx_->driverCtx()->driverId != 0) {
    // When there are multiple pipelines, a single operator, the one from
    // pipeline 0, is responsible for merging pages.
    return BlockingReason::kNotBlocked;
  }
  if (noMoreSplits_) {
    return BlockingReason::kNotBlocked;
  }
  for (;;) {
    exec::Split split;
    auto reason = operatorCtx_->task()->getSplitOrFuture(
        operatorCtx_->driverCtx()->splitGroupId, planNodeId(), split, *future);
    if (reason == BlockingReason::kNotBlocked) {
      if (split.hasConnectorSplit()) {
        auto remoteSplit = std::dynamic_pointer_cast<RemoteConnectorSplit>(
            split.connectorSplit);
        VELOX_CHECK(remoteSplit, "Wrong type of split");

        sources_.emplace_back(
            MergeSource::createMergeExchangeSource(this, remoteSplit->taskId));
        ++numSplits_;
      } else {
        noMoreSplits_ = true;
        // TODO Delay this call until all input data has been processed.
        operatorCtx_->task()->multipleSplitsFinished(numSplits_);
        return BlockingReason::kNotBlocked;
      }
    } else {
      return reason;
    }
  }
}

} // namespace facebook::velox::exec
