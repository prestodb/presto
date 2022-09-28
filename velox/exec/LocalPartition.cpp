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
  std::vector<ContinuePromise> producerPromises;
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

BlockingReason LocalExchangeQueue::enqueue(
    RowVectorPtr input,
    ContinueFuture* future) {
  auto inputBytes = input->retainedSize();

  std::vector<ContinuePromise> consumerPromises;
  bool isClosed = queue_.withWLock([&](auto& queue) {
    if (closed_) {
      return true;
    }
    queue.push(std::move(input));
    consumerPromises = std::move(consumerPromises_);
    return false;
  });

  if (isClosed) {
    return BlockingReason::kNotBlocked;
  }

  notify(consumerPromises);

  if (memoryManager_->increaseMemoryUsage(future, inputBytes)) {
    return BlockingReason::kWaitForConsumer;
  }

  return BlockingReason::kNotBlocked;
}

void LocalExchangeQueue::noMoreData() {
  std::vector<ContinuePromise> consumerPromises;
  std::vector<ContinuePromise> producerPromises;
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

BlockingReason LocalExchangeQueue::next(
    ContinueFuture* future,
    memory::MemoryPool* pool,
    RowVectorPtr* data) {
  std::vector<ContinuePromise> producerPromises;
  std::vector<ContinuePromise> memoryPromises;
  auto blockingReason = queue_.withWLock([&](auto& queue) {
    *data = nullptr;
    if (queue.empty()) {
      if (isFinishedLocked(queue)) {
        return BlockingReason::kNotBlocked;
      }

      consumerPromises_.emplace_back("LocalExchangeQueue::next");
      *future = consumerPromises_.back().getSemiFuture();

      return BlockingReason::kWaitForExchange;
    }

    *data = queue.front();
    queue.pop();

    memoryPromises =
        memoryManager_->decreaseMemoryUsage((*data)->retainedSize());

    if (noMoreProducers_ && pendingProducers_ == 0 && queue.empty()) {
      producerPromises = std::move(producerPromises_);
    }

    return BlockingReason::kNotBlocked;
  });
  notify(memoryPromises);
  notify(producerPromises);
  return blockingReason;
}

bool LocalExchangeQueue::isFinishedLocked(
    const std::queue<RowVectorPtr>& queue) const {
  if (closed_) {
    return true;
  }

  if (noMoreProducers_ && pendingProducers_ == 0 && queue.empty()) {
    return true;
  }

  return false;
}

BlockingReason LocalExchangeQueue::isFinished(ContinueFuture* future) {
  return queue_.withWLock([&](auto& queue) {
    if (isFinishedLocked(queue)) {
      return BlockingReason::kNotBlocked;
    }

    producerPromises_.emplace_back("LocalExchangeQueue::isFinished");
    *future = producerPromises_.back().getSemiFuture();

    return BlockingReason::kWaitForConsumer;
  });
}

bool LocalExchangeQueue::isFinished() {
  return queue_.withWLock([&](auto& queue) { return isFinishedLocked(queue); });
}

void LocalExchangeQueue::close() {
  std::vector<ContinuePromise> producerPromises;
  std::vector<ContinuePromise> consumerPromises;
  std::vector<ContinuePromise> memoryPromises;
  queue_.withWLock([&](auto& queue) {
    uint64_t freedBytes = 0;
    while (!queue.empty()) {
      freedBytes += queue.front()->retainedSize();
      queue.pop();
    }

    if (freedBytes) {
      memoryPromises = memoryManager_->decreaseMemoryUsage(freedBytes);
    }

    producerPromises = std::move(producerPromises_);
    consumerPromises = std::move(consumerPromises_);
    closed_ = true;
  });
  notify(producerPromises);
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
    stats().inputPositions += data->size();
    stats().inputBytes += data->estimateFlatSize();
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
              : planNode->partitionFunctionFactory()(numPartitions_)),
      blockingReasons_{numPartitions_} {
  VELOX_CHECK(numPartitions_ == 1 || partitionFunction_ != nullptr);

  for (auto& queue : queues_) {
    queue->addProducer();
  }

  futures_.reserve(numPartitions_);
  for (auto i = 0; i < numPartitions_; i++) {
    futures_.emplace_back();
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

void LocalPartition::addInput(RowVectorPtr input) {
  stats_.outputBytes += input->estimateFlatSize();
  stats_.outputPositions += input->size();

  // Lazy vectors must be loaded or processed.
  for (auto& child : input->children()) {
    child->loadedVector();
  }

  input_ = std::move(input);

  if (numPartitions_ == 1) {
    blockingReasons_[0] = queues_[0]->enqueue(input_, &futures_[0]);
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

      ContinueFuture future;
      auto reason = queues_[i]->enqueue(partitionData, &future);
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
    for (const auto& queue : queues_) {
      auto reason = queue->isFinished(future);
      if (reason != BlockingReason::kNotBlocked) {
        return reason;
      }
    }
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
  if (numBlockedPartitions_ || !noMoreInput_) {
    return false;
  }

  for (const auto& queue : queues_) {
    if (!queue->isFinished()) {
      return false;
    }
  }

  return true;
}
} // namespace facebook::velox::exec
