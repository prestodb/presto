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

#include "velox/exec/Merge.h"
#include "velox/exec/Task.h"

namespace facebook::velox::exec {

Merge::Merge(
    int32_t operatorId,
    DriverCtx* driverCtx,
    RowTypePtr outputType,
    const std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>&
        sortingKeys,
    const std::vector<core::SortOrder>& sortingOrders,
    const std::string& planNodeId,
    const std::string& operatorType)
    : SourceOperator(
          driverCtx,
          std::move(outputType),
          operatorId,
          planNodeId,
          operatorType),
      outputBatchSize_{driverCtx->queryConfig().preferredOutputBatchSize()} {
  auto numKeys = sortingKeys.size();
  sortingKeys_.reserve(numKeys);
  for (int i = 0; i < numKeys; ++i) {
    auto channel = exprToChannel(sortingKeys[i].get(), outputType_);
    VELOX_CHECK_NE(
        channel,
        kConstantChannel,
        "Merge doesn't allow constant grouping keys");
    sortingKeys_.emplace_back(
        channel,
        CompareFlags{
            sortingOrders[i].isNullsFirst(),
            sortingOrders[i].isAscending(),
            false});
  }
}

void Merge::initializeTreeOfLosers() {
  std::vector<std::unique_ptr<SourceStream>> sourceCursors;
  sourceCursors.reserve(sources_.size());
  for (auto& source : sources_) {
    sourceCursors.push_back(std::make_unique<SourceStream>(
        source.get(), sortingKeys_, outputBatchSize_));
  }

  // Save the pointers to cursors before moving these into the TreeOfLosers.
  streams_.reserve(sources_.size());
  for (auto& cursor : sourceCursors) {
    streams_.push_back(cursor.get());
  }

  treeOfLosers_ =
      std::make_unique<TreeOfLosers<SourceStream>>(std::move(sourceCursors));
}

BlockingReason Merge::isBlocked(ContinueFuture* future) {
  auto reason = addMergeSources(future);
  if (reason != BlockingReason::kNotBlocked) {
    return reason;
  }

  // No merging is needed if there is only one source.
  if (streams_.empty() && sources_.size() > 1) {
    initializeTreeOfLosers();
  }

  if (sourceBlockingFutures_.empty()) {
    for (auto& cursor : streams_) {
      cursor->isBlocked(sourceBlockingFutures_);
    }
  }

  if (!sourceBlockingFutures_.empty()) {
    *future = std::move(sourceBlockingFutures_.back());
    sourceBlockingFutures_.pop_back();
    return BlockingReason::kWaitForExchange;
  }

  return BlockingReason::kNotBlocked;
}

bool Merge::isFinished() {
  return finished_;
}

RowVectorPtr Merge::getOutput() {
  if (finished_) {
    return nullptr;
  }

  // No merging is needed if there is only one source.
  if (sources_.size() == 1) {
    ContinueFuture future{false};
    RowVectorPtr data;
    auto reason = sources_[0]->next(data, &future);
    if (reason != BlockingReason::kNotBlocked) {
      sourceBlockingFutures_.emplace_back(std::move(future));
      return nullptr;
    }

    finished_ = data == nullptr;
    return data;
  }

  if (!output_) {
    output_ = std::dynamic_pointer_cast<RowVector>(BaseVector::create(
        outputType_, outputBatchSize_, operatorCtx_->pool()));
    for (auto& child : output_->children()) {
      child->resize(outputBatchSize_);
    }
  }

  for (;;) {
    auto stream = treeOfLosers_->next();

    if (!stream) {
      output_->resize(outputSize_);
      finished_ = true;
      return std::move(output_);
    }

    if (stream->setOutputRow(outputSize_)) {
      // The stream is at end of input batch. Need to copy out the rows before
      // fetching next batch in 'pop'.
      stream->copyToOutput(output_);
    }

    ++outputSize_;

    // Advance the stream.
    stream->pop(sourceBlockingFutures_);

    if (outputSize_ == outputBatchSize_) {
      // Copy out data from all sources.
      for (auto& s : streams_) {
        s->copyToOutput(output_);
      }

      outputSize_ = 0;
      return std::move(output_);
    }

    if (!sourceBlockingFutures_.empty()) {
      return nullptr;
    }
  }
}

bool SourceStream::operator<(const MergeStream& other) const {
  const auto& otherCursor = static_cast<const SourceStream&>(other);
  for (auto i = 0; i < sortingKeys_.size(); ++i) {
    const auto& key = sortingKeys_[i];
    if (auto result = keyColumns_[i]->compare(
            otherCursor.keyColumns_[i],
            currentSourceRow_,
            otherCursor.currentSourceRow_,
            key.second)) {
      return result < 0;
    }
  }
  return false;
}

bool SourceStream::pop(std::vector<ContinueFuture>& futures) {
  ++currentSourceRow_;
  if (currentSourceRow_ == data_->size()) {
    // Make sure all current data has been copied out.
    VELOX_CHECK(!outputRows_.hasSelections());
    return fetchMoreData(futures);
  }

  return false;
}

void SourceStream::copyToOutput(RowVectorPtr& output) {
  outputRows_.updateBounds();

  if (!outputRows_.hasSelections()) {
    return;
  }

  vector_size_t sourceRow = firstSourceRow_;
  outputRows_.applyToSelected(
      [&](auto row) { sourceRows_[row] = sourceRow++; });

  for (auto i = 0; i < output->type()->size(); ++i) {
    output->childAt(i)->copy(
        data_->childAt(i).get(), outputRows_, sourceRows_.data());
  }

  outputRows_.clearAll();

  if (currentSourceRow_ == data_->size() - 1) {
    firstSourceRow_ = 0;
  } else {
    firstSourceRow_ = currentSourceRow_;
  }
}

bool SourceStream::fetchMoreData(std::vector<ContinueFuture>& futures) {
  ContinueFuture future{false};
  auto reason = source_->next(data_, &future);
  if (reason != BlockingReason::kNotBlocked) {
    needData_ = true;
    futures.emplace_back(std::move(future));
    return true;
  }

  atEnd_ = !data_ || data_->size() == 0;
  needData_ = false;
  currentSourceRow_ = 0;

  if (!atEnd_) {
    for (auto& child : data_->children()) {
      child = BaseVector::loadedVectorShared(child);
    }
    keyColumns_.clear();
    for (const auto& key : sortingKeys_) {
      keyColumns_.push_back(data_->childAt(key.first).get());
    }
  }
  return false;
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
