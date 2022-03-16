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

#include "velox/exec/LocalPartition.h"
#include "velox/exec/Task.h"

namespace facebook::velox::exec {
namespace {
void notify(std::vector<VeloxPromise<bool>>& promises) {
  for (auto& promise : promises) {
    promise.setValue(true);
  }
}
} // namespace

bool LocalExchangeMemoryManager::increaseMemoryUsage(
    ContinueFuture* future,
    int64_t added) {
  std::lock_guard<std::mutex> l(mutex_);
  bufferedBytes_ += added;

  if (bufferedBytes_ >= maxBufferSize_) {
    promises_.emplace_back("LocalExchangeMemoryManager::updateMemoryUsage");
    *future = promises_.back().getSemiFuture();
    return true;
  }

  return false;
}

void LocalExchangeMemoryManager::decreaseMemoryUsage(int64_t removed) {
  std::vector<VeloxPromise<bool>> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    bufferedBytes_ -= removed;

    if (bufferedBytes_ < maxBufferSize_ &&
        bufferedBytes_ + removed >= maxBufferSize_) {
      promises = std::move(promises_);
    }
  }

  notify(promises);
}

void LocalExchangeSource::addProducer() {
  queue_.withWLock([&](auto& /*queue*/) {
    VELOX_CHECK(!noMoreProducers_, "addProducer called after noMoreProducers");
    ++pendingProducers_;
  });
}

void LocalExchangeSource::noMoreProducers() {
  std::vector<VeloxPromise<bool>> consumerPromises;
  std::vector<VeloxPromise<bool>> producerPromises;
  queue_.withWLock([&](auto& queue) {
    VELOX_CHECK(!noMoreProducers_, "noMoreProducers can be called only once");
    noMoreProducers_ = true;
    if (pendingProducers_ == 0) {
      // No more data will be produced.
      consumerPromises = std::move(consumerPromises_);

      if (queue.empty()) {
        // All data has been consumed.
        producerPromises = std::move(producerPromises_);
      }
    }
  });
  notify(consumerPromises);
  notify(producerPromises);
}

BlockingReason LocalExchangeSource::enqueue(
    RowVectorPtr input,
    ContinueFuture* future) {
  auto inputBytes = input->retainedSize();

  std::vector<VeloxPromise<bool>> consumerPromises;
  queue_.withWLock([&](auto& queue) {
    queue.push(std::move(input));
    consumerPromises = std::move(consumerPromises_);
  });
  notify(consumerPromises);

  if (memoryManager_->increaseMemoryUsage(future, inputBytes)) {
    return BlockingReason::kWaitForConsumer;
  }

  return BlockingReason::kNotBlocked;
}

void LocalExchangeSource::noMoreData() {
  std::vector<VeloxPromise<bool>> consumerPromises;
  std::vector<VeloxPromise<bool>> producerPromises;
  queue_.withWLock([&](auto queue) {
    VELOX_CHECK_GT(pendingProducers_, 0);
    --pendingProducers_;
    if (noMoreProducers_ && pendingProducers_ == 0) {
      consumerPromises = std::move(consumerPromises_);
      if (queue.empty()) {
        producerPromises = std::move(producerPromises_);
      }
    }
  });
  notify(consumerPromises);
  notify(producerPromises);
}

BlockingReason LocalExchangeSource::next(
    ContinueFuture* future,
    memory::MemoryPool* pool,
    RowVectorPtr* data) {
  std::vector<VeloxPromise<bool>> producerPromises;
  auto blockingReason = queue_.withWLock([&](auto& queue) {
    *data = nullptr;
    if (queue.empty()) {
      if (noMoreProducers_ && pendingProducers_ == 0) {
        return BlockingReason::kNotBlocked;
      }

      consumerPromises_.emplace_back("LocalExchangeSource::next");
      *future = consumerPromises_.back().getSemiFuture();

      return BlockingReason::kWaitForExchange;
    }

    *data = queue.front();
    queue.pop();

    memoryManager_->decreaseMemoryUsage((*data)->retainedSize());

    if (noMoreProducers_ && pendingProducers_ == 0 && queue.empty()) {
      producerPromises = std::move(producerPromises_);
    }

    return BlockingReason::kNotBlocked;
  });
  notify(producerPromises);
  return blockingReason;
}

BlockingReason LocalExchangeSource::isFinished(ContinueFuture* future) {
  return queue_.withWLock([&](auto& queue) {
    if (noMoreProducers_ && pendingProducers_ == 0 && queue.empty()) {
      return BlockingReason::kNotBlocked;
    }

    producerPromises_.emplace_back("LocalExchangeSource::isFinished");
    *future = producerPromises_.back().getSemiFuture();

    return BlockingReason::kWaitForConsumer;
  });
}

bool LocalExchangeSource::isFinished() {
  return queue_.withWLock([&](auto& queue) {
    if (noMoreProducers_ && pendingProducers_ == 0 && queue.empty()) {
      return true;
    }

    return false;
  });
}

LocalExchangeSourceOperator::LocalExchangeSourceOperator(
    int32_t operatorId,
    DriverCtx* ctx,
    RowTypePtr outputType,
    const std::string& planNodeId,
    int partition)
    : SourceOperator(
          ctx,
          std::move(outputType),
          operatorId,
          planNodeId,
          "LocalExchangeSource"),
      partition_{partition},
      source_{operatorCtx_->task()->getLocalExchangeSource(
          ctx->splitGroupId,
          planNodeId,
          partition)} {}

BlockingReason LocalExchangeSourceOperator::isBlocked(ContinueFuture* future) {
  if (blockingReason_ != BlockingReason::kNotBlocked) {
    *future = std::move(future_);
    auto reason = blockingReason_;
    blockingReason_ = BlockingReason::kNotBlocked;
    return reason;
  }

  return BlockingReason::kNotBlocked;
}

RowVectorPtr LocalExchangeSourceOperator::getOutput() {
  RowVectorPtr data;
  blockingReason_ = source_->next(&future_, pool(), &data);
  if (blockingReason_ != BlockingReason::kNotBlocked) {
    return nullptr;
  }
  if (data != nullptr) {
    stats().inputPositions += data->size();
    stats().inputBytes += data->estimateFlatSize();
  }
  return data;
}

bool LocalExchangeSourceOperator::isFinished() {
  return source_->isFinished();
}

LocalPartition::LocalPartition(
    int32_t operatorId,
    DriverCtx* ctx,
    const std::shared_ptr<const core::LocalPartitionNode>& planNode)
    : Operator(
          ctx,
          planNode->outputType(),
          operatorId,
          planNode->id(),
          "LocalPartition"),
      localExchangeSources_{ctx->task->getLocalExchangeSources(
          ctx->splitGroupId,
          planNode->id())},
      numPartitions_{localExchangeSources_.size()},
      partitionFunction_(
          numPartitions_ == 1
              ? nullptr
              : planNode->partitionFunctionFactory()(numPartitions_)),
      outputChannels_{calculateOutputChannels(
          planNode->inputType(),
          planNode->outputType())},
      blockingReasons_{numPartitions_} {
  VELOX_CHECK(numPartitions_ == 1 || partitionFunction_ != nullptr);

  for (auto& source : localExchangeSources_) {
    source->addProducer();
  }

  futures_.reserve(numPartitions_);
  for (auto i = 0; i < numPartitions_; i++) {
    futures_.emplace_back(false);
  }
}

namespace {
std::vector<BufferPtr> allocateIndexBuffers(
    int numBuffers,
    vector_size_t size,
    memory::MemoryPool* pool) {
  std::vector<BufferPtr> indexBuffers;
  indexBuffers.reserve(numBuffers);
  for (auto i = 0; i < numBuffers; i++) {
    indexBuffers.emplace_back(allocateIndices(size, pool));
  }
  return indexBuffers;
}

std::vector<vector_size_t*> getRawIndices(
    const std::vector<BufferPtr>& indexBuffers) {
  std::vector<vector_size_t*> rawIndices;
  rawIndices.reserve(indexBuffers.size());
  for (auto& buffer : indexBuffers) {
    rawIndices.emplace_back(buffer->asMutable<vector_size_t>());
  }
  return rawIndices;
}

RowVectorPtr
wrapChildren(const RowVectorPtr& input, vector_size_t size, BufferPtr indices) {
  std::vector<VectorPtr> wrappedChildren;
  wrappedChildren.reserve(input->type()->size());
  for (auto i = 0; i < input->type()->size(); i++) {
    wrappedChildren.emplace_back(BaseVector::wrapInDictionary(
        BufferPtr(nullptr), indices, size, input->childAt(i)));
  }

  return std::make_shared<RowVector>(
      input->pool(), input->type(), BufferPtr(nullptr), size, wrappedChildren);
}
} // namespace

BlockingReason LocalPartition::enqueue(
    int32_t source,
    RowVectorPtr data,
    ContinueFuture* future) {
  RowVectorPtr projectedData;
  if (outputChannels_.empty() && outputType_->size() > 0) {
    projectedData = std::move(data);
  } else {
    std::vector<VectorPtr> outputColumns;
    outputColumns.reserve(outputChannels_.size());
    for (const auto& i : outputChannels_) {
      outputColumns.push_back(data->childAt(i));
    }

    projectedData = std::make_shared<RowVector>(
        data->pool(), outputType_, nullptr, data->size(), outputColumns);
  }

  return localExchangeSources_[source]->enqueue(projectedData, future);
}

void LocalPartition::addInput(RowVectorPtr input) {
  stats_.outputBytes += input->estimateFlatSize();
  stats_.outputPositions += input->size();

  // Lazy vectors must be loaded or processed.
  for (auto& child : input->children()) {
    child->loadedVector();
  }

  input_ = std::move(input);

  if (numPartitions_ == 1) {
    blockingReasons_[0] = enqueue(0, input_, &futures_[0]);
    if (blockingReasons_[0] != BlockingReason::kNotBlocked) {
      numBlockedPartitions_ = 1;
    }
  } else {
    partitionFunction_->partition(*input_, partitions_);

    auto numInput = input_->size();
    auto indexBuffers = allocateIndexBuffers(numPartitions_, numInput, pool());
    auto rawIndices = getRawIndices(indexBuffers);

    std::vector<vector_size_t> maxIndex(numPartitions_, 0);
    for (auto i = 0; i < numInput; ++i) {
      auto partition = partitions_[i];
      rawIndices[partition][maxIndex[partition]] = i;
      ++maxIndex[partition];
    }

    for (auto i = 0; i < numPartitions_; i++) {
      auto partitionSize = maxIndex[i];
      if (partitionSize == 0) {
        // Do not enqueue empty partitions.
        continue;
      }
      indexBuffers[i]->setSize(partitionSize * sizeof(vector_size_t));
      auto partitionData =
          wrapChildren(input_, partitionSize, std::move(indexBuffers[i]));

      ContinueFuture future{false};
      auto reason = enqueue(i, partitionData, &future);
      if (reason != BlockingReason::kNotBlocked) {
        blockingReasons_[numBlockedPartitions_] = reason;
        futures_[numBlockedPartitions_] = std::move(future);
        ++numBlockedPartitions_;
      }
    }
  }
}

BlockingReason LocalPartition::isBlocked(ContinueFuture* future) {
  if (numBlockedPartitions_) {
    --numBlockedPartitions_;
    *future = std::move(futures_[numBlockedPartitions_]);
    return blockingReasons_[numBlockedPartitions_];
  }

  if (noMoreInput_) {
    for (const auto& source : localExchangeSources_) {
      auto reason = source->isFinished(future);
      if (reason != BlockingReason::kNotBlocked) {
        return reason;
      }
    }
  }

  return BlockingReason::kNotBlocked;
}

void LocalPartition::noMoreInput() {
  Operator::noMoreInput();
  for (const auto& source : localExchangeSources_) {
    source->noMoreData();
  }
}

bool LocalPartition::isFinished() {
  if (numBlockedPartitions_ || !noMoreInput_) {
    return false;
  }

  for (const auto& source : localExchangeSources_) {
    if (!source->isFinished()) {
      return false;
    }
  }

  return true;
}
} // namespace facebook::velox::exec
