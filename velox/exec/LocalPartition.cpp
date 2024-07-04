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
void notify(std::vector<ContinuePromise>& promises) {
  for (auto& promise : promises) {
    promise.setValue();
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

std::vector<ContinuePromise> LocalExchangeMemoryManager::decreaseMemoryUsage(
    int64_t removed) {
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    bufferedBytes_ -= removed;

    if (bufferedBytes_ < maxBufferSize_) {
      promises = std::move(promises_);
    }
  }
  return promises;
}

void LocalExchangeQueue::addProducer() {
  queue_.withWLock([&](auto& /*queue*/) {
    VELOX_CHECK(!noMoreProducers_, "addProducer called after noMoreProducers");
    ++pendingProducers_;
  });
}

void LocalExchangeQueue::noMoreProducers() {
  std::vector<ContinuePromise> consumerPromises;
  queue_.withWLock([&](auto& queue) {
    VELOX_CHECK(!noMoreProducers_, "noMoreProducers can be called only once");
    noMoreProducers_ = true;
    if (pendingProducers_ == 0) {
      // No more data will be produced.
      consumerPromises = std::move(consumerPromises_);
    }
  });
  notify(consumerPromises);
}

BlockingReason LocalExchangeQueue::enqueue(
    RowVectorPtr input,
    int64_t inputBytes,
    ContinueFuture* future) {
  std::vector<ContinuePromise> consumerPromises;
  bool blockedOnConsumer = false;
  bool isClosed = queue_.withWLock([&](auto& queue) {
    if (closed_) {
      return true;
    }
    queue.emplace(std::move(input), inputBytes);
    consumerPromises = std::move(consumerPromises_);

    if (memoryManager_->increaseMemoryUsage(future, inputBytes)) {
      blockedOnConsumer = true;
    }

    return false;
  });

  if (isClosed) {
    return BlockingReason::kNotBlocked;
  }

  notify(consumerPromises);

  if (blockedOnConsumer) {
    return BlockingReason::kWaitForConsumer;
  }

  return BlockingReason::kNotBlocked;
}

void LocalExchangeQueue::noMoreData() {
  std::vector<ContinuePromise> consumerPromises;
  queue_.withWLock([&](auto& queue) {
    VELOX_CHECK_GT(pendingProducers_, 0);
    --pendingProducers_;
    if (noMoreProducers_ && pendingProducers_ == 0) {
      consumerPromises = std::move(consumerPromises_);
    }
  });
  notify(consumerPromises);
}

BlockingReason LocalExchangeQueue::next(
    ContinueFuture* future,
    memory::MemoryPool* pool,
    RowVectorPtr* data) {
  std::vector<ContinuePromise> memoryPromises;
  auto blockingReason = queue_.withWLock([&](auto& queue) {
    *data = nullptr;
    if (queue.empty()) {
      if (isFinishedLocked(queue)) {
        return BlockingReason::kNotBlocked;
      }

      consumerPromises_.emplace_back("LocalExchangeQueue::next");
      *future = consumerPromises_.back().getSemiFuture();

      return BlockingReason::kWaitForProducer;
    }

    int64_t size;
    std::tie(*data, size) = queue.front();
    queue.pop();

    memoryPromises = memoryManager_->decreaseMemoryUsage(size);

    return BlockingReason::kNotBlocked;
  });
  notify(memoryPromises);
  return blockingReason;
}

bool LocalExchangeQueue::isFinishedLocked(const Queue& queue) const {
  if (closed_) {
    return true;
  }

  if (noMoreProducers_ && pendingProducers_ == 0 && queue.empty()) {
    return true;
  }

  return false;
}

bool LocalExchangeQueue::isFinished() {
  return queue_.withWLock([&](auto& queue) { return isFinishedLocked(queue); });
}

void LocalExchangeQueue::close() {
  std::vector<ContinuePromise> consumerPromises;
  std::vector<ContinuePromise> memoryPromises;
  queue_.withWLock([&](auto& queue) {
    uint64_t freedBytes = 0;
    while (!queue.empty()) {
      freedBytes += queue.front().second;
      queue.pop();
    }

    if (freedBytes) {
      memoryPromises = memoryManager_->decreaseMemoryUsage(freedBytes);
    }

    consumerPromises = std::move(consumerPromises_);
    closed_ = true;
  });
  notify(consumerPromises);
  notify(memoryPromises);
}

LocalExchange::LocalExchange(
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
          "LocalExchange"),
      partition_{partition},
      queue_{operatorCtx_->task()->getLocalExchangeQueue(
          ctx->splitGroupId,
          planNodeId,
          partition)} {}

BlockingReason LocalExchange::isBlocked(ContinueFuture* future) {
  if (blockingReason_ != BlockingReason::kNotBlocked) {
    *future = std::move(future_);
    auto reason = blockingReason_;
    blockingReason_ = BlockingReason::kNotBlocked;
    return reason;
  }

  return BlockingReason::kNotBlocked;
}

RowVectorPtr LocalExchange::getOutput() {
  RowVectorPtr data;
  blockingReason_ = queue_->next(&future_, pool(), &data);
  if (blockingReason_ != BlockingReason::kNotBlocked) {
    return nullptr;
  }
  if (data != nullptr) {
    auto lockedStats = stats_.wlock();
    lockedStats->addInputVector(data->estimateFlatSize(), data->size());
  }
  return data;
}

bool LocalExchange::isFinished() {
  return queue_->isFinished();
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
      queues_{
          ctx->task->getLocalExchangeQueues(ctx->splitGroupId, planNode->id())},
      numPartitions_{queues_.size()},
      partitionFunction_(
          numPartitions_ == 1
              ? nullptr
              : planNode->partitionFunctionSpec().create(numPartitions_)) {
  VELOX_CHECK(numPartitions_ == 1 || partitionFunction_ != nullptr);

  for (auto& queue : queues_) {
    queue->addProducer();
  }
}

namespace {
std::vector<BufferPtr> allocateIndexBuffers(
    const std::vector<vector_size_t>& sizes,
    memory::MemoryPool* pool) {
  std::vector<BufferPtr> indexBuffers;
  indexBuffers.reserve(sizes.size());
  for (auto size : sizes) {
    indexBuffers.push_back(allocateIndices(size, pool));
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

void LocalPartition::addInput(RowVectorPtr input) {
  {
    auto lockedStats = stats_.wlock();
    lockedStats->addOutputVector(input->estimateFlatSize(), input->size());
  }

  // Lazy vectors must be loaded or processed.
  for (auto& child : input->children()) {
    child->loadedVector();
  }

  if (numPartitions_ == 1) {
    ContinueFuture future;
    auto blockingReason =
        queues_[0]->enqueue(input, input->retainedSize(), &future);
    if (blockingReason != BlockingReason::kNotBlocked) {
      blockingReasons_.push_back(blockingReason);
      futures_.push_back(std::move(future));
    }
    return;
  }

  const auto singlePartition =
      partitionFunction_->partition(*input, partitions_);
  if (singlePartition.has_value()) {
    ContinueFuture future;
    auto blockingReason = queues_[singlePartition.value()]->enqueue(
        input, input->retainedSize(), &future);
    if (blockingReason != BlockingReason::kNotBlocked) {
      blockingReasons_.push_back(blockingReason);
      futures_.push_back(std::move(future));
    }
    return;
  }

  const auto numInput = input->size();
  std::vector<vector_size_t> maxIndex(numPartitions_, 0);
  for (auto i = 0; i < numInput; ++i) {
    ++maxIndex[partitions_[i]];
  }
  auto indexBuffers = allocateIndexBuffers(maxIndex, pool());
  auto rawIndices = getRawIndices(indexBuffers);

  std::fill(maxIndex.begin(), maxIndex.end(), 0);
  for (auto i = 0; i < numInput; ++i) {
    auto partition = partitions_[i];
    rawIndices[partition][maxIndex[partition]] = i;
    ++maxIndex[partition];
  }

  const int64_t totalSize = input->retainedSize();
  for (auto i = 0; i < numPartitions_; i++) {
    auto partitionSize = maxIndex[i];
    if (partitionSize == 0) {
      // Do not enqueue empty partitions.
      continue;
    }
    auto partitionData =
        wrapChildren(input, partitionSize, std::move(indexBuffers[i]));
    ContinueFuture future;
    auto reason = queues_[i]->enqueue(
        partitionData, totalSize * partitionSize / numInput, &future);
    if (reason != BlockingReason::kNotBlocked) {
      blockingReasons_.push_back(reason);
      futures_.push_back(std::move(future));
    }
  }
}

BlockingReason LocalPartition::isBlocked(ContinueFuture* future) {
  if (!futures_.empty()) {
    auto blockingReason = blockingReasons_.front();
    *future = folly::collectAll(futures_.begin(), futures_.end()).unit();
    futures_.clear();
    blockingReasons_.clear();
    return blockingReason;
  }

  return BlockingReason::kNotBlocked;
}

void LocalPartition::noMoreInput() {
  Operator::noMoreInput();
  for (const auto& queue : queues_) {
    queue->noMoreData();
  }
}

bool LocalPartition::isFinished() {
  if (!futures_.empty() || !noMoreInput_) {
    return false;
  }

  return true;
}
} // namespace facebook::velox::exec
