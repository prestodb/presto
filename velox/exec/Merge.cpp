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

#include "velox/exec/Merge.h"

#include "velox/common/testutil/TestValue.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/Task.h"

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::exec {
namespace {
std::unique_ptr<VectorSerde::Options> getVectorSerdeOptions(
    const core::QueryConfig& queryConfig,
    VectorSerde::Kind kind) {
  std::unique_ptr<VectorSerde::Options> options =
      kind == VectorSerde::Kind::kPresto
      ? std::make_unique<serializer::presto::PrestoVectorSerde::PrestoOptions>()
      : std::make_unique<VectorSerde::Options>();
  options->compressionKind =
      common::stringToCompressionKind(queryConfig.shuffleCompressionKind());
  return options;
}
} // namespace

Merge::Merge(
    int32_t operatorId,
    DriverCtx* driverCtx,
    RowTypePtr outputType,
    const std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>&
        sortingKeys,
    const std::vector<core::SortOrder>& sortingOrders,
    const std::string& planNodeId,
    const std::string& operatorType,
    const std::optional<common::SpillConfig>& spillConfig)
    : SourceOperator(
          driverCtx,
          std::move(outputType),
          operatorId,
          planNodeId,
          operatorType,
          spillConfig),
      outputBatchSize_{outputBatchRows()},
      sortingKeys_([&]() {
        auto numKeys = sortingKeys.size();
        std::vector<SpillSortKey> keys;
        keys.reserve(numKeys);
        for (int i = 0; i < numKeys; ++i) {
          auto channel = exprToChannel(sortingKeys[i].get(), outputType_);
          VELOX_CHECK_NE(
              channel,
              kConstantChannel,
              "Merge doesn't allow constant grouping keys");
          keys.emplace_back(
              channel,
              CompareFlags{
                  sortingOrders[i].isNullsFirst(),
                  sortingOrders[i].isAscending(),
                  false});
        }
        return keys;
      }()) {}

BlockingReason Merge::isBlocked(ContinueFuture* future) {
  TestValue::adjust("facebook::velox::exec::Merge::isBlocked", this);

  const auto reason = addMergeSources(future);
  if (reason != BlockingReason::kNotBlocked) {
    return reason;
  }

  // NOTE: the task might terminate early which leaves empty sources. Once it
  // happens, we shall simply mark the merge operator as finished.
  if (sources_.empty()) {
    finished_ = true;
    return BlockingReason::kNotBlocked;
  }

  maybeStartNextMergeSourceGroup();

  if (sourceBlockingFutures_.empty()) {
    return BlockingReason::kNotBlocked;
  }

  *future = std::move(sourceBlockingFutures_.back());
  sourceBlockingFutures_.pop_back();
  return BlockingReason::kWaitForProducer;
}

bool Merge::isFinished() {
  return finished_;
}

void Merge::maybeSetupOutputSpiller() {
  VELOX_CHECK(canSpill());
  if (mergeOutputSpiller_ != nullptr) {
    return;
  }

  mergeOutputSpiller_ = std::make_unique<MergeSpiller>(
      outputType_,
      HashBitRange{},
      sortingKeys_,
      &spillConfig_.value(),
      &spillStats_);
}

void Merge::spill() {
  if (output_ == nullptr) {
    return;
  }
  maybeSetupOutputSpiller();
  numSpilledRows_ += output_->size();
  mergeOutputSpiller_->spill(SpillPartitionId{0}, output_);
  output_ = nullptr;
}

void Merge::finishMergeSourceGroup() {
  sourceMerger_ = nullptr;
  if (mergeOutputSpiller_ == nullptr) {
    return;
  }
  VELOX_CHECK(needSpill());
  VELOX_CHECK_GT(numSpilledRows_, 0);
  // Finishes spill if it has happened and setup spill merger if no more source
  // to merge.
  SpillPartitionSet spillPartitionSet;
  mergeOutputSpiller_->finishSpill(spillPartitionSet);
  mergeOutputSpiller_ = nullptr;
  VELOX_CHECK_EQ(spillPartitionSet.size(), 1);
  auto spillFiles = spillPartitionSet.begin()->second->files();
  VELOX_CHECK(!spillFiles.empty());
  spillFileGroups_.push_back(std::move(spillFiles));
}

void Merge::setupSpillMerger() {
  VELOX_CHECK(!spillFileGroups_.empty());
  VELOX_CHECK_NULL(spillMerger_);
  std::vector<std::vector<std::unique_ptr<SpillReadFile>>> spillReadFilesGroups;
  spillReadFilesGroups.reserve(spillFileGroups_.size());
  for (const auto& spillFiles : spillFileGroups_) {
    std::vector<std::unique_ptr<SpillReadFile>> spillReadFiles;
    spillReadFiles.reserve(spillFiles.size());
    for (const auto& spillFile : spillFiles) {
      spillReadFiles.emplace_back(SpillReadFile::create(
          spillFile, spillConfig_->readBufferSize, pool(), &spillStats_));
    }
    spillReadFilesGroups.push_back(std::move(spillReadFiles));
  }
  spillFileGroups_.clear();
  spillMerger_ = std::make_unique<SpillMerger>(
      outputType_, numSpilledRows_, std::move(spillReadFilesGroups), pool());
}

void Merge::maybeStartNextMergeSourceGroup() {
  SCOPE_EXIT {
    if (sourceMerger_ != nullptr) {
      sourceMerger_->isBlocked(sourceBlockingFutures_);
    }
  };

  if (sourceMerger_ != nullptr || numStartedSources_ >= sources_.size()) {
    return;
  }

  // Gets the merge sources for the next partial merge run.
  std::vector<MergeSource*> sources;
  for (auto i = numStartedSources_; i <
       (std::min(sources_.size(), numStartedSources_ + maxNumMergeSources_));
       ++i) {
    sources.push_back(sources_[i].get());
  }

  // Initializes the source merger.
  std::vector<std::unique_ptr<SourceStream>> cursors;
  cursors.reserve(sources.size());
  for (auto* source : sources) {
    cursors.push_back(
        std::make_unique<SourceStream>(source, sortingKeys_, outputBatchSize_));
  }

  sourceMerger_ =
      std::make_unique<SourceMerger>(outputType_, std::move(cursors), pool());
  // Start sources.
  for (const auto& source : sources) {
    source->start();
  }
  numStartedSources_ += sources.size();
}

RowVectorPtr Merge::getOutputFromSpill() {
  VELOX_CHECK_NOT_NULL(spillMerger_);
  VELOX_CHECK_NULL(sourceMerger_);
  output_ = spillMerger_->getOutput(outputBatchSize_);
  if (output_ != nullptr) {
    return std::move(output_);
  }
  spillMerger_.reset();
  finished_ = true;
  return nullptr;
}

RowVectorPtr Merge::getOutputFromSource() {
  VELOX_CHECK_NULL(spillMerger_);
  bool atEnd = false;
  output_ =
      sourceMerger_->getOutput(outputBatchSize_, sourceBlockingFutures_, atEnd);
  SCOPE_EXIT {
    if (!atEnd) {
      return;
    }

    finishMergeSourceGroup();
    if (numStartedSources_ < sources_.size()) {
      return;
    }

    if (numSpilledRows_ > 0) {
      setupSpillMerger();
      return;
    }
    finished_ = true;
  };

  if (needSpill()) {
    spill();
    return nullptr;
  }

  VELOX_CHECK_EQ(numSpilledRows_, 0);
  return std::move(output_);
}

RowVectorPtr Merge::getOutput() {
  if (finished_) {
    return nullptr;
  }

  // Read from spill.
  if (spillMerger_ != nullptr) {
    return getOutputFromSpill();
  }

  return getOutputFromSource();
}

void Merge::close() {
  for (auto& source : sources_) {
    source->close();
  }
  Operator::close();
}

SourceMerger::SourceMerger(
    const RowTypePtr& type,
    std::vector<std::unique_ptr<SourceStream>> sourceStreams,
    velox::memory::MemoryPool* pool)
    : type_(type),
      streams_([&sourceStreams]() {
        std::vector<SourceStream*> streams;
        streams.reserve(sourceStreams.size());
        for (auto& cursor : sourceStreams) {
          streams.push_back(cursor.get());
        }
        return streams;
      }()),
      merger_(std::make_unique<TreeOfLosers<SourceStream>>(
          std::move(sourceStreams))),
      pool_(pool) {}

void SourceMerger::isBlocked(
    std::vector<ContinueFuture>& sourceBlockingFutures) const {
  if (sourceBlockingFutures.empty()) {
    for (auto* stream : streams_) {
      stream->isBlocked(sourceBlockingFutures);
    }
  }
}

RowVectorPtr SourceMerger::getOutput(
    vector_size_t maxOutputRows,
    std::vector<ContinueFuture>& sourceBlockingFutures,
    bool& atEnd) {
  VELOX_CHECK_NOT_NULL(merger_);
  atEnd = false;
  if (!output_) {
    output_ = BaseVector::create<RowVector>(type_, maxOutputRows, pool_);
    for (auto& child : output_->children()) {
      child->resize(maxOutputRows);
    }
  }

  uint64_t outputSize = 0;
  for (;;) {
    const auto stream = merger_->next();
    if (stream == nullptr) {
      if (outputSize > 0) {
        output_->resize(outputSize);
        return std::move(output_);
      }
      atEnd = true;
      return nullptr;
    }

    if (stream->setOutputRow(outputSize)) {
      // The stream is at end of input batch. Need to copy out the rows
      // before fetching next batch in 'pop'.
      stream->copyToOutput(output_);
    }

    ++outputSize;

    // Advance the stream.
    stream->pop(sourceBlockingFutures);

    if (outputSize == maxOutputRows) {
      // Copy out data from all sources.
      for (const auto& s : streams_) {
        s->copyToOutput(output_);
      }
      return std::move(output_);
    }

    if (!sourceBlockingFutures.empty()) {
      return nullptr;
    }
  }
}

std::unique_ptr<TreeOfLosers<SpillMergeStream>> SpillMerger::createSpillMerger(
    std::vector<std::vector<std::unique_ptr<SpillReadFile>>>&&
        spillReadFilesGroups) {
  std::vector<std::unique_ptr<SpillMergeStream>> streams;
  streams.reserve(spillReadFilesGroups.size());
  for (auto i = 0; i < spillReadFilesGroups.size(); ++i) {
    streams.push_back(ConcatFilesSpillMergeStream::create(
        i, std::move(spillReadFilesGroups[i])));
  }
  return std::make_unique<TreeOfLosers<SpillMergeStream>>(std::move(streams));
}

SpillMerger::SpillMerger(
    const RowTypePtr& type,
    uint64_t numSpilledRows,
    std::vector<std::vector<std::unique_ptr<SpillReadFile>>>&&
        spillReadFilesGroup,
    velox::memory::MemoryPool* pool)
    : type_(type),
      numSpilledRows_(numSpilledRows),
      merger_([&spillReadFilesGroup]() {
        std::vector<std::unique_ptr<SpillMergeStream>> streams;
        streams.reserve(spillReadFilesGroup.size());
        for (auto i = 0; i < spillReadFilesGroup.size(); ++i) {
          streams.push_back(ConcatFilesSpillMergeStream::create(
              i, std::move(spillReadFilesGroup[i])));
        }
        return std::make_unique<TreeOfLosers<SpillMergeStream>>(
            std::move(streams));
      }()),
      pool_(pool) {
  VELOX_CHECK_NOT_NULL(merger_);
}

RowVectorPtr SpillMerger::getOutput(vector_size_t maxOutputRows) {
  // Check if the spill merger has finished.
  if (numOutputRows_ == numSpilledRows_) {
    return nullptr;
  }

  VELOX_CHECK_GT(maxOutputRows, 0);
  VELOX_CHECK_GT(numSpilledRows_, numOutputRows_);
  const vector_size_t batchSize =
      std::min<uint64_t>(numSpilledRows_ - numOutputRows_, maxOutputRows);
  if (output_ != nullptr) {
    VectorPtr output = std::move(output_);
    BaseVector::prepareForReuse(output, batchSize);
    output_ = std::static_pointer_cast<RowVector>(output);
  } else {
    output_ = std::static_pointer_cast<RowVector>(
        BaseVector::create(type_, batchSize, pool_));
  }

  for (const auto& child : output_->children()) {
    child->resize(batchSize);
  }

  spillSources_.resize(batchSize);
  spillSourceRows_.resize(batchSize);

  VELOX_CHECK_GT(output_->size(), 0);
  VELOX_CHECK_LE(output_->size() + numOutputRows_, numSpilledRows_);

  int32_t outputRow = 0;
  int32_t outputSize = 0;
  bool isEndOfBatch = false;
  while (outputRow + outputSize < output_->size()) {
    SpillMergeStream* stream = merger_->next();
    VELOX_CHECK_NOT_NULL(stream);

    spillSources_[outputSize] = &stream->current();
    spillSourceRows_[outputSize] = stream->currentIndex(&isEndOfBatch);
    ++outputSize;
    if (FOLLY_UNLIKELY(isEndOfBatch)) {
      // The stream is at end of input batch. Need to copy out the rows before
      // fetching next batch in 'pop'.
      gatherCopy(
          output_.get(),
          outputRow,
          outputSize,
          spillSources_,
          spillSourceRows_,
          {});
      outputRow += outputSize;
      outputSize = 0;
    }
    // Advance the stream.
    stream->pop();
  }
  VELOX_CHECK_EQ(outputRow + outputSize, output_->size());

  if (FOLLY_LIKELY(outputSize != 0)) {
    gatherCopy(
        output_.get(),
        outputRow,
        outputSize,
        spillSources_,
        spillSourceRows_,
        {});
  }

  numOutputRows_ += output_->size();
  return std::move(output_);
}

bool SourceStream::operator<(const MergeStream& other) const {
  const auto& otherCursor = static_cast<const SourceStream&>(other);
  for (auto i = 0; i < sortingKeys_.size(); ++i) {
    const auto& [_, compareFlags] = sortingKeys_[i];
    VELOX_DCHECK(
        compareFlags.nullAsValue(), "not supported null handling mode");
    if (auto result = keyColumns_[i]
                          ->compare(
                              otherCursor.keyColumns_[i],
                              currentSourceRow_,
                              otherCursor.currentSourceRow_,
                              compareFlags)
                          .value()) {
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

  if (sourceRow == data_->size()) {
    firstSourceRow_ = 0;
  } else {
    firstSourceRow_ = sourceRow;
  }
}

bool SourceStream::fetchMoreData(std::vector<ContinueFuture>& futures) {
  ContinueFuture future;
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
          "LocalMerge",
          localMergeNode->canSpill(driverCtx->queryConfig())
              ? driverCtx->makeSpillConfig(operatorId)
              : std::nullopt) {
  VELOX_CHECK_EQ(
      operatorCtx_->driverCtx()->driverId,
      0,
      "LocalMerge needs to run single-threaded");
  maxNumMergeSources_ = operatorCtx_->task()
                            ->queryCtx()
                            ->queryConfig()
                            .localMergeMaxNumMergeSources();
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
          "MergeExchange"),
      serde_(getNamedVectorSerde(mergeExchangeNode->serdeKind())),
      serdeOptions_(getVectorSerdeOptions(
          driverCtx->queryConfig(),
          mergeExchangeNode->serdeKind())) {}

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
    if (reason != BlockingReason::kNotBlocked) {
      return reason;
    }

    if (split.hasConnectorSplit()) {
      auto remoteSplit =
          std::dynamic_pointer_cast<RemoteConnectorSplit>(split.connectorSplit);
      VELOX_CHECK_NOT_NULL(remoteSplit, "Wrong type of split");
      remoteSourceTaskIds_.push_back(remoteSplit->taskId);
      continue;
    }

    noMoreSplits_ = true;
    if (!remoteSourceTaskIds_.empty()) {
      const auto maxMergeExchangeBufferSize =
          operatorCtx_->driverCtx()->queryConfig().maxMergeExchangeBufferSize();
      const auto maxQueuedBytesPerSource = std::min<int64_t>(
          std::max<int64_t>(
              maxMergeExchangeBufferSize / remoteSourceTaskIds_.size(),
              MergeSource::kMaxQueuedBytesLowerLimit),
          MergeSource::kMaxQueuedBytesUpperLimit);
      for (uint32_t remoteSourceIndex = 0;
           remoteSourceIndex < remoteSourceTaskIds_.size();
           ++remoteSourceIndex) {
        auto* pool = operatorCtx_->task()->addMergeSourcePool(
            operatorCtx_->planNodeId(),
            operatorCtx_->driverCtx()->pipelineId,
            remoteSourceIndex);
        sources_.emplace_back(MergeSource::createMergeExchangeSource(
            this,
            remoteSourceTaskIds_[remoteSourceIndex],
            operatorCtx_->task()->destination(),
            maxQueuedBytesPerSource,
            pool,
            operatorCtx_->task()->queryCtx()->executor()));
      }
    }
    // TODO Delay this call until all input data has been processed.
    operatorCtx_->task()->multipleSplitsFinished(
        false, remoteSourceTaskIds_.size(), 0);
    return BlockingReason::kNotBlocked;
  }
}

void MergeExchange::close() {
  for (auto& source : sources_) {
    source->close();
  }
  Operator::close();
  {
    auto lockedStats = stats_.wlock();
    lockedStats->addRuntimeStat(
        Operator::kShuffleSerdeKind,
        RuntimeCounter(static_cast<int64_t>(serde_->kind())));
    lockedStats->addRuntimeStat(
        Operator::kShuffleCompressionKind,
        RuntimeCounter(static_cast<int64_t>(serdeOptions_->compressionKind)));
  }
}
} // namespace facebook::velox::exec
