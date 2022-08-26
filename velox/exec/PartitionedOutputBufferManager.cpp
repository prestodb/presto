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
#include "velox/exec/PartitionedOutputBufferManager.h"
#include <velox/exec/Exchange.h>

namespace facebook::velox::exec {

std::vector<std::unique_ptr<folly::IOBuf>> DestinationBuffer::getData(
    uint64_t maxBytes,
    int64_t sequence,
    DataAvailableCallback notify) {
  VELOX_CHECK_GE(
      sequence, sequence_, "Get received for an already acknowledged item");

  if (sequence - sequence_ > data_.size()) {
    VLOG(0) << this << " Out of order get: " << sequence << " over "
            << sequence_ << " Setting second notify " << notifySequence_
            << " / " << sequence;
    notify_ = notify;
    notifySequence_ = std::min(notifySequence_, sequence);
    notifyMaxBytes_ = maxBytes;
    return {};
  }
  if (sequence - sequence_ == data_.size()) {
    notify_ = notify;
    notifySequence_ = sequence;
    notifyMaxBytes_ = maxBytes;
    return {};
  }

  std::vector<std::unique_ptr<folly::IOBuf>> result;
  uint64_t resultBytes = 0;
  for (auto i = sequence - sequence_; i < data_.size(); i++) {
    // nullptr is used as end marker
    if (!data_[i]) {
      VELOX_CHECK_EQ(i, data_.size() - 1, "null marker found in the middle");
      result.push_back(nullptr);
      break;
    }
    result.push_back(data_[i]->getIOBuf());
    resultBytes += data_[i]->size();
    if (resultBytes >= maxBytes) {
      break;
    }
  }
  return result;
}

DataAvailable DestinationBuffer::getAndClearNotify() {
  if (!notify_) {
    return DataAvailable();
  }
  DataAvailable result;
  result.callback = notify_;
  result.sequence = notifySequence_;
  result.data = getData(notifyMaxBytes_, notifySequence_, nullptr);
  notify_ = nullptr;
  notifySequence_ = 0;
  notifyMaxBytes_ = 0;
  return result;
}

std::vector<std::shared_ptr<SerializedPage>> DestinationBuffer::acknowledge(
    int64_t sequence,
    bool fromGetData) {
  int64_t numDeleted = sequence - sequence_;
  if (numDeleted == 0 && fromGetData) {
    // If called from getData, it is expected that there will be
    // nothing to delete because a previous acknowledge has been
    // received before the getData. This is not guaranteed though
    // because the messages may arrive out of order. Note that getData
    // implicitly acknowledges all messages with a lower sequence
    // number than the one in getData.
    return {};
  }
  if (numDeleted <= 0) {
    // Acknowledges come out of order, e.g. ack of 10 and 9 have
    // swapped places in flight.
    VLOG(0) << this << " Out of order ack: " << sequence << " over "
            << sequence_;
    return {};
  }

  VELOX_CHECK_LE(
      numDeleted, data_.size(), "Ack received for a not yet produced item");
  std::vector<std::shared_ptr<SerializedPage>> freed;
  for (auto i = 0; i < numDeleted; ++i) {
    if (!data_[i]) {
      VELOX_CHECK_EQ(i, data_.size() - 1, "null marker found in the middle");
      break;
    }
    freed.push_back(std::move(data_[i]));
  }
  data_.erase(data_.begin(), data_.begin() + numDeleted);
  sequence_ += numDeleted;
  return freed;
}

std::vector<std::shared_ptr<SerializedPage>>
DestinationBuffer::deleteResults() {
  std::vector<std::shared_ptr<SerializedPage>> freed;
  for (auto i = 0; i < data_.size(); ++i) {
    if (!data_[i]) {
      VELOX_CHECK_EQ(i, data_.size() - 1, "null marker found in the middle");
      break;
    }
    freed.push_back(std::move(data_[i]));
  }
  data_.clear();
  return freed;
}

std::string DestinationBuffer::toString() {
  std::stringstream out;
  out << "[available: " << data_.size() << ", "
      << "sequence: " << sequence_ << ", "
      << (notify_ ? "notify registered, " : "") << this << "]";
  return out.str();
}

namespace {
// Frees 'freed' and realizes 'promises'. Used after
// updateAfterAcknowledgeLocked. This runs outside of the mutex, so
// that we do the expensive free outside and only then continue the
// producers which will allocate more memory.
void releaseAfterAcknowledge(
    std::vector<std::shared_ptr<SerializedPage>>& freed,
    std::vector<ContinuePromise>& promises) {
  freed.clear();
  for (auto& promise : promises) {
    promise.setValue();
  }
}
} // namespace

PartitionedOutputBuffer::PartitionedOutputBuffer(
    std::shared_ptr<Task> task,
    bool broadcast,
    int numDestinations,
    uint32_t numDrivers)
    : task_(std::move(task)),
      broadcast_(broadcast),
      numDrivers_(numDrivers),
      maxSize_(task_->queryCtx()->config().maxPartitionedOutputBufferSize()),
      continueSize_((maxSize_ * kContinuePct) / 100) {
  buffers_.reserve(numDestinations);
  for (int i = 0; i < numDestinations; i++) {
    buffers_.push_back(std::make_unique<DestinationBuffer>());
  }
}

void PartitionedOutputBuffer::updateBroadcastOutputBuffers(
    int numBuffers,
    bool noMoreBuffers) {
  VELOX_CHECK(broadcast_);

  std::vector<ContinuePromise> promises;
  bool isFinished;
  {
    std::lock_guard<std::mutex> l(mutex_);

    if (numBuffers > buffers_.size()) {
      addBroadcastOutputBuffersLocked(numBuffers);
    }

    if (!noMoreBuffers) {
      return;
    }

    noMoreBroadcastBuffers_ = true;
    isFinished = isFinishedLocked();
    updateAfterAcknowledgeLocked(dataToBroadcast_, promises);
  }

  releaseAfterAcknowledge(dataToBroadcast_, promises);
  if (isFinished) {
    task_->setAllOutputConsumed();
  }
}

void PartitionedOutputBuffer::updateNumDrivers(uint32_t newNumDrivers) {
  bool isNoMoreDrivers{false};
  {
    std::lock_guard<std::mutex> l(mutex_);
    numDrivers_ = newNumDrivers;
    // If we finished all drivers, ensure we register that we are 'done'.
    if (numDrivers_ == numFinished_) {
      isNoMoreDrivers = true;
    }
  }
  if (isNoMoreDrivers) {
    noMoreDrivers();
  }
}

void PartitionedOutputBuffer::addBroadcastOutputBuffersLocked(int numBuffers) {
  VELOX_CHECK(!noMoreBroadcastBuffers_)
  buffers_.reserve(numBuffers);
  for (auto i = buffers_.size(); i < numBuffers; i++) {
    auto buffer = std::make_unique<DestinationBuffer>();
    for (const auto& data : dataToBroadcast_) {
      buffer->enqueue(data);
    }
    if (atEnd_) {
      buffer->enqueue(nullptr);
    }
    buffers_.emplace_back(std::move(buffer));
  }
}

BlockingReason PartitionedOutputBuffer::enqueue(
    int destination,
    std::unique_ptr<SerializedPage> data,
    ContinueFuture* future) {
  VELOX_CHECK(data);
  std::vector<DataAvailable> dataAvailableCallbacks;
  bool blocked = false;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK_LT(destination, buffers_.size());
    VELOX_CHECK(
        task_->isRunning(), "Task is terminated, cannot add data to output.");

    totalSize_ += data->size();
    if (broadcast_) {
      std::shared_ptr<SerializedPage> sharedData(data.release());
      for (auto& buffer : buffers_) {
        if (buffer) {
          buffer->enqueue(sharedData);
          dataAvailableCallbacks.emplace_back(buffer->getAndClearNotify());
        }
      }

      if (!noMoreBroadcastBuffers_) {
        dataToBroadcast_.emplace_back(sharedData);
      }
    } else {
      if (auto buffer = buffers_[destination].get()) {
        buffer->enqueue(std::move(data));
        dataAvailableCallbacks.emplace_back(buffer->getAndClearNotify());
      } else {
        // Some downstream tasks may finish early and delete the
        // corresponding buffers. Further data for these buffers is dropped.
        totalSize_ -= data->size();
      }
    }

    if (totalSize_ > maxSize_ && future) {
      promises_.emplace_back("PartitionedOutputBuffer::enqueue");
      *future = promises_.back().getSemiFuture();
      blocked = true;
    }
  }

  // Outside mutex_.
  for (auto& callback : dataAvailableCallbacks) {
    callback.notify();
  }

  return blocked ? BlockingReason::kWaitForConsumer
                 : BlockingReason::kNotBlocked;
}

void PartitionedOutputBuffer::noMoreData() {
  checkIfDone(true); // Increment number of finished drivers.
}

void PartitionedOutputBuffer::noMoreDrivers() {
  checkIfDone(false); // Do not increment number of finished drivers.
}

void PartitionedOutputBuffer::checkIfDone(bool oneDriverFinished) {
  std::vector<DataAvailable> finished;
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (oneDriverFinished) {
      ++numFinished_;
    }
    VELOX_CHECK_LE(
        numFinished_,
        numDrivers_,
        "Each driver should call noMoreData exactly once");
    atEnd_ = numFinished_ == numDrivers_;
    if (atEnd_) {
      for (auto& buffer : buffers_) {
        if (buffer) {
          buffer->enqueue(nullptr);
          finished.push_back(buffer->getAndClearNotify());
        }
      }
    }
  }

  // Notify outside of mutex.
  for (auto& notification : finished) {
    notification.notify();
  }
}

bool PartitionedOutputBuffer::isFinished() {
  std::lock_guard<std::mutex> l(mutex_);
  return isFinishedLocked();
}

bool PartitionedOutputBuffer::isFinishedLocked() {
  if (broadcast_ && !noMoreBroadcastBuffers_) {
    return false;
  }
  for (auto& buffer : buffers_) {
    if (buffer) {
      return false;
    }
  }
  return true;
}

void PartitionedOutputBuffer::acknowledge(int destination, int64_t sequence) {
  std::vector<std::shared_ptr<SerializedPage>> freed;
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK_LT(destination, buffers_.size());
    auto* buffer = buffers_[destination].get();
    if (!buffer) {
      VLOG(1) << "Ack received after final ack for destination " << destination
              << " and sequence " << sequence;
      return;
    }
    freed = buffer->acknowledge(sequence, false);
    updateAfterAcknowledgeLocked(freed, promises);
  }
  releaseAfterAcknowledge(freed, promises);
}

void PartitionedOutputBuffer::updateAfterAcknowledgeLocked(
    const std::vector<std::shared_ptr<SerializedPage>>& freed,
    std::vector<ContinuePromise>& promises) {
  uint64_t totalFreed = 0;
  for (const auto& free : freed) {
    if (free.unique()) {
      totalFreed += free->size();
    }
  }
  if (totalFreed == 0) {
    return;
  }

  VELOX_CHECK_LE(
      totalFreed,
      totalSize_,
      "Output buffer size goes negative: released {} over {}",
      totalFreed,
      totalSize_);
  totalSize_ -= totalFreed;
  if (totalSize_ < continueSize_) {
    promises = std::move(promises_);
  }
}

bool PartitionedOutputBuffer::deleteResults(int destination) {
  std::vector<std::shared_ptr<SerializedPage>> freed;
  std::vector<ContinuePromise> promises;
  bool isFinished;
  DataAvailable dataAvailable;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK_LT(destination, buffers_.size());
    auto* buffer = buffers_[destination].get();
    if (!buffer) {
      VLOG(1) << "Extra delete  received  for destination " << destination;
      return false;
    }
    freed = buffer->deleteResults();
    dataAvailable = buffer->getAndClearNotify();
    buffers_[destination] = nullptr;
    ++numFinalAcknowledges_;
    isFinished = isFinishedLocked();
    updateAfterAcknowledgeLocked(freed, promises);
  }

  // Outside of mutex.
  dataAvailable.notify();

  if (!promises.empty()) {
    VLOG(1) << "Delete of results unblocks producers. Can happen in early end "
            << "due to error or limit";
  }
  releaseAfterAcknowledge(freed, promises);
  if (isFinished) {
    task_->setAllOutputConsumed();
  }
  return isFinished;
}

void PartitionedOutputBuffer::getData(
    int destination,
    uint64_t maxBytes,
    int64_t sequence,
    DataAvailableCallback notify) {
  std::vector<std::unique_ptr<folly::IOBuf>> data;
  std::vector<std::shared_ptr<SerializedPage>> freed;
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);

    if (broadcast_ && destination >= buffers_.size()) {
      addBroadcastOutputBuffersLocked(destination + 1);
    }

    VELOX_CHECK_LT(destination, buffers_.size());
    auto destinationBuffer = buffers_[destination].get();
    VELOX_CHECK(
        destinationBuffer,
        "getData received after its buffer is deleted. Destination: {}, sequence: {}",
        destination,
        sequence);
    freed = destinationBuffer->acknowledge(sequence, true);
    updateAfterAcknowledgeLocked(freed, promises);
    data = destinationBuffer->getData(maxBytes, sequence, notify);
  }
  releaseAfterAcknowledge(freed, promises);
  if (!data.empty()) {
    notify(std::move(data), sequence);
  }
}

void PartitionedOutputBuffer::terminate() {
  std::vector<ContinuePromise> outstandingPromises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK(not task_->isRunning());
    outstandingPromises.swap(promises_);
  }
  for (auto& promise : outstandingPromises) {
    promise.setValue();
  }
}

std::string PartitionedOutputBuffer::toString() {
  std::lock_guard<std::mutex> l(mutex_);
  std::stringstream out;
  out << "[PartitionedOutputBuffer totalSize_=" << totalSize_
      << "b, num producers blocked=" << promises_.size()
      << ", completed=" << numFinished_ << "/" << numDrivers_ << ", "
      << (atEnd_ ? "at end, " : "") << "destinations: " << std::endl;
  for (auto i = 0; i < buffers_.size(); ++i) {
    auto buffer = buffers_[i].get();
    out << i << ": " << (buffer ? buffer->toString() : "none") << std::endl;
  }
  out << "]" << std::endl;
  return out.str();
}

// static
std::weak_ptr<PartitionedOutputBufferManager>
PartitionedOutputBufferManager::getInstance() {
  static auto kInstance = std::make_shared<PartitionedOutputBufferManager>();
  return kInstance;
}

std::shared_ptr<PartitionedOutputBuffer>
PartitionedOutputBufferManager::getBuffer(const std::string& taskId) {
  return buffers_.withLock([&](auto& buffers) {
    auto it = buffers.find(taskId);
    VELOX_CHECK(
        it != buffers.end(), "Output buffers for task not found: {}", taskId);
    return it->second;
  });
}

uint64_t PartitionedOutputBufferManager::numBuffers() const {
  return buffers_.lock()->size();
}

BlockingReason PartitionedOutputBufferManager::enqueue(
    const std::string& taskId,
    int destination,
    std::unique_ptr<SerializedPage> data,
    ContinueFuture* future) {
  return getBuffer(taskId)->enqueue(destination, std::move(data), future);
}

void PartitionedOutputBufferManager::noMoreData(const std::string& taskId) {
  getBuffer(taskId)->noMoreData();
}

bool PartitionedOutputBufferManager::isFinished(const std::string& taskId) {
  return getBuffer(taskId)->isFinished();
}

void PartitionedOutputBufferManager::acknowledge(
    const std::string& taskId,
    int destination,
    int64_t sequence) {
  auto buffer = buffers_.withLock(
      [&](auto& buffers) -> std::shared_ptr<PartitionedOutputBuffer> {
        auto it = buffers.find(taskId);
        if (it == buffers.end()) {
          VLOG(1) << "Receiving ack for non-existent task " << taskId
                  << " destination " << destination << " sequence " << sequence;
          return nullptr;
        }
        return it->second;
      });
  if (buffer) {
    buffer->acknowledge(destination, sequence);
  }
}

void PartitionedOutputBufferManager::deleteResults(
    const std::string& taskId,
    int destination) {
  auto buffer = buffers_.withLock(
      [&](auto& buffers) -> std::shared_ptr<PartitionedOutputBuffer> {
        auto it = buffers.find(taskId);
        if (it == buffers.end()) {
          return nullptr;
        }
        return it->second;
      });
  if (buffer && buffer->deleteResults(destination)) {
    buffers_.withLock([&](auto& buffers) { buffers.erase(taskId); });
  }
}

void PartitionedOutputBufferManager::getData(
    const std::string& taskId,
    int destination,
    uint64_t maxBytes,
    int64_t sequence,
    DataAvailableCallback notify) {
  getBuffer(taskId)->getData(destination, maxBytes, sequence, notify);
}

void PartitionedOutputBufferManager::initializeTask(
    std::shared_ptr<Task> task,
    bool broadcast,
    int numDestinations,
    int numDrivers) {
  const auto& taskId = task->taskId();

  buffers_.withLock([&](auto& buffers) {
    auto it = buffers.find(taskId);
    if (it == buffers.end()) {
      buffers[taskId] = std::make_shared<PartitionedOutputBuffer>(
          std::move(task), broadcast, numDestinations, numDrivers);
    } else {
      VELOX_FAIL(
          "Registering an output buffer for pre-existing taskId {}", taskId);
    }
  });
}

void PartitionedOutputBufferManager::updateBroadcastOutputBuffers(
    const std::string& taskId,
    int numBuffers,
    bool noMoreBuffers) {
  getBuffer(taskId)->updateBroadcastOutputBuffers(numBuffers, noMoreBuffers);
}

void PartitionedOutputBufferManager::updateNumDrivers(
    const std::string& taskId,
    uint32_t newNumDrivers) {
  getBuffer(taskId)->updateNumDrivers(newNumDrivers);
}

void PartitionedOutputBufferManager::removeTask(const std::string& taskId) {
  auto buffer = buffers_.withLock(
      [&](auto& buffers) -> std::shared_ptr<PartitionedOutputBuffer> {
        auto it = buffers.find(taskId);
        if (it == buffers.end()) {
          // Already removed.
          return nullptr;
        }
        auto taskBuffer = it->second;
        buffers.erase(taskId);
        return taskBuffer;
      });
  if (buffer) {
    buffer->terminate();
  }
}

std::string PartitionedOutputBufferManager::toString() {
  return buffers_.withLock([](const auto& buffers) {
    std::stringstream out;
    out << "[BufferManager:" << std::endl;
    for (const auto& pair : buffers) {
      out << pair.first << ": " << pair.second->toString() << std::endl;
    }
    out << "]";
    return out.str();
  });
}

} // namespace facebook::velox::exec
