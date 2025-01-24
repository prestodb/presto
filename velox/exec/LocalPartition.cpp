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

void LocalExchangeVectorPool::push(const RowVectorPtr& vector, int64_t size) {
  pool_.withWLock([&](auto& pool) {
    if (totalSize_ + size <= capacity_) {
      pool.emplace(vector, size);
      totalSize_ += size;
    }
  });
}

RowVectorPtr LocalExchangeVectorPool::pop() {
  return pool_.withWLock([&](auto& pool) -> RowVectorPtr {
    while (!pool.empty()) {
      auto [vector, size] = std::move(pool.front());
      pool.pop();
      totalSize_ -= size;
      VELOX_CHECK_GE(totalSize_, 0);
      if (vector.use_count() == 1) {
        return vector;
      }
    }
    VELOX_CHECK_EQ(totalSize_, 0);
    return nullptr;
  });
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
  int64_t size;
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

    std::tie(*data, size) = std::move(queue.front());
    queue.pop();

    memoryPromises = memoryManager_->decreaseMemoryUsage(size);

    return BlockingReason::kNotBlocked;
  });
  notify(memoryPromises);
  if (*data != nullptr) {
    vectorPool_->push(*data, size);
  }
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

bool LocalExchangeQueue::testingProducersDone() const {
  return queue_.withRLock(
      [&](auto& queue) { return noMoreProducers_ && pendingProducers_ == 0; });
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
          numPartitions_ == 1 ? nullptr
                              : planNode->partitionFunctionSpec().create(
                                    numPartitions_,
                                    /*localExchange=*/true)) {
  VELOX_CHECK(numPartitions_ == 1 || partitionFunction_ != nullptr);

  for (auto& queue : queues_) {
    queue->addProducer();
  }
  if (numPartitions_ > 0) {
    indexBuffers_.resize(numPartitions_);
    rawIndices_.resize(numPartitions_);
  }
}

void LocalPartition::allocateIndexBuffers(
    const std::vector<vector_size_t>& sizes) {
  VELOX_CHECK_EQ(indexBuffers_.size(), sizes.size());
  VELOX_CHECK_EQ(rawIndices_.size(), sizes.size());

  for (auto i = 0; i < sizes.size(); ++i) {
    const auto indicesBufferBytes = sizes[i] * sizeof(vector_size_t);
    if ((indexBuffers_[i] == nullptr) ||
        (indexBuffers_[i]->capacity() < indicesBufferBytes) ||
        !indexBuffers_[i]->unique()) {
      indexBuffers_[i] = allocateIndices(sizes[i], pool());
    } else {
      const auto indicesBufferBytes = sizes[i] * sizeof(vector_size_t);
      indexBuffers_[i]->setSize(indicesBufferBytes);
    }
    rawIndices_[i] = indexBuffers_[i]->asMutable<vector_size_t>();
  }
}

RowVectorPtr LocalPartition::wrapChildren(
    const RowVectorPtr& input,
    vector_size_t size,
    const BufferPtr& indices,
    RowVectorPtr reusable) {
  RowVectorPtr result;
  if (!reusable) {
    result = std::make_shared<RowVector>(
        pool(),
        input->type(),
        nullptr,
        size,
        std::vector<VectorPtr>(input->childrenSize()));
  } else {
    VELOX_CHECK(!reusable->mayHaveNulls());
    VELOX_CHECK_EQ(reusable.use_count(), 1);
    reusable->unsafeResize(size);
    result = std::move(reusable);
  }
  VELOX_CHECK_NOT_NULL(result);

  for (auto i = 0; i < input->childrenSize(); ++i) {
    auto& child = result->childAt(i);
    if (child && child->encoding() == VectorEncoding::Simple::DICTIONARY &&
        child.use_count() == 1) {
      child->BaseVector::resize(size);
      child->setWrapInfo(indices);
      child->setValueVector(input->childAt(i));
    } else {
      child = BaseVector::wrapInDictionary(
          nullptr, indices, size, input->childAt(i));
    }
  }

  result->updateContainsLazyNotLoaded();
  return result;
}

void LocalPartition::addInput(RowVectorPtr input) {
  prepareForInput(input);

  const auto singlePartition = numPartitions_ == 1
      ? 0
      : partitionFunction_->partition(*input, partitions_);
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
  allocateIndexBuffers(maxIndex);

  std::fill(maxIndex.begin(), maxIndex.end(), 0);
  for (auto i = 0; i < numInput; ++i) {
    auto partition = partitions_[i];
    rawIndices_[partition][maxIndex[partition]] = i;
    ++maxIndex[partition];
  }

  const int64_t totalSize = input->retainedSize();
  for (auto i = 0; i < numPartitions_; i++) {
    auto partitionSize = maxIndex[i];
    if (partitionSize == 0) {
      // Do not enqueue empty partitions.
      continue;
    }
    auto partitionData = wrapChildren(
        input, partitionSize, indexBuffers_[i], queues_[i]->getVector());
    ContinueFuture future;
    auto reason = queues_[i]->enqueue(
        partitionData, totalSize * partitionSize / numInput, &future);
    if (reason != BlockingReason::kNotBlocked) {
      blockingReasons_.push_back(reason);
      futures_.push_back(std::move(future));
    }
  }
}

void LocalPartition::prepareForInput(RowVectorPtr& input) {
  {
    auto lockedStats = stats_.wlock();
    lockedStats->addOutputVector(input->estimateFlatSize(), input->size());
  }

  // Lazy vectors must be loaded or processed to ensure the late materialized in
  // order.
  for (auto& child : input->children()) {
    child->loadedVector();
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
