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

namespace facebook::velox::exec {

void DestinationBuffer::getData(
    uint64_t maxBytes,
    int64_t sequence,
    DataAvailableCallback notify,
    std::vector<std::shared_ptr<VectorStreamGroup>>& result) {
  VELOX_CHECK_GE(
      sequence, sequence_, "Get received for an already acknowledged item");

  if (sequence - sequence_ > data_.size()) {
    VLOG(0) << this << " Out of order get: " << sequence << " over "
            << sequence_ << " Setting second notify " << notifySequence_
            << " / " << sequence;
    notify_ = notify;
    notifySequence_ = std::min(notifySequence_, sequence);
    notifyMaxBytes_ = maxBytes;
    return;
  }
  if (sequence - sequence_ == data_.size()) {
    notify_ = notify;
    notifySequence_ = sequence;
    notifyMaxBytes_ = maxBytes;
    return;
  }

  uint64_t resultBytes = 0;
  for (auto i = sequence - sequence_; i < data_.size(); i++) {
    // nullptr is used as end marker
    if (!data_[i]) {
      VELOX_CHECK_EQ(i, data_.size() - 1, "null marker found in the middle");
      result.push_back(nullptr);
      break;
    }
    result.push_back(data_[i]);
    resultBytes += data_[i]->size();
    if (resultBytes >= maxBytes) {
      break;
    }
  }
}

DataAvailable DestinationBuffer::getAndClearNotify() {
  if (!notify_) {
    return DataAvailable();
  }
  DataAvailable result;
  result.callback = notify_;
  result.sequence = notifySequence_;
  getData(notifyMaxBytes_, notifySequence_, nullptr, result.data);
  notify_ = nullptr;
  notifySequence_ = 0;
  notifyMaxBytes_ = 0;
  return result;
}

void DestinationBuffer::acknowledge(
    int64_t sequence,
    bool fromGetData,
    std::vector<std::shared_ptr<VectorStreamGroup>>& freed,
    uint64_t* totalFreed) {
  int64_t numDeleted = sequence - sequence_;
  if (numDeleted == 0 && fromGetData) {
    // If called from getData, it is expected that there will be
    // nothing to delete because a previous acknowledge has been
    // received before the getData. This is not guaranteed though
    // because the messages may arrive out of order. Note that getData
    // implicitly acknowledges all messages with a lower sequence
    // number than the one in getData.
    return;
  }
  if (numDeleted <= 0) {
    // Acknowledges come out of order, e.g. ack of 10 and 9 have
    // swapped places in flight.
    *totalFreed = 0;
    VLOG(0) << this << " Out of order ack: " << sequence << " over "
            << sequence_;
    return;
  }
  VELOX_CHECK_LE(
      numDeleted, data_.size(), "Ack received for a not yet produced item");
  uint64_t freedBytes = 0;
  for (auto i = 0; i < numDeleted; ++i) {
    if (!data_[i]) {
      VELOX_CHECK_EQ(i, data_.size() - 1, "null marker found in the middle");
      break;
    }
    freedBytes += data_[i]->size();
    freed.push_back(std::move(data_[i]));
  }
  data_.erase(data_.begin(), data_.begin() + numDeleted);
  sequence_ += numDeleted;
  *totalFreed = freedBytes;
}

void DestinationBuffer::deleteResults(
    std::vector<std::shared_ptr<VectorStreamGroup>>& freed,
    uint64_t* totalFreed) {
  uint64_t freedBytes = 0;
  for (auto i = 0; i < data_.size(); ++i) {
    if (!data_[i]) {
      VELOX_CHECK_EQ(i, data_.size() - 1, "null marker found in the middle");
      break;
    }
    freedBytes += data_[i]->size();
    freed.push_back(std::move(data_[i]));
  }
  *totalFreed = freedBytes;
  data_.clear();
  // Notify any consumers that are still waiting.
  getAndClearNotify().notify();
}

std::string DestinationBuffer::toString() {
  std::stringstream out;
  out << "[available: " << data_.size() << ", "
      << "sequence: " << sequence_ << ", "
      << (notify_ ? "notify registered, " : "") << this << "]";
  return out.str();
}

BlockingReason PartitionedOutputBuffer::enqueue(
    int destination,
    std::unique_ptr<VectorStreamGroup>&& data,
    ContinueFuture* future) {
  VELOX_CHECK(data);
  DataAvailable dataAvailable;
  bool blocked = false;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK_LT(destination, buffers_.size());
    if (task_->state() != kRunning) {
      throw std::runtime_error(
          "Task is terminated, cannot add data to output.");
    }
    totalSize_ += data->size();
    auto buffer = buffers_[destination].get();
    buffer->enqueue(std::move(data));
    if (totalSize_ > maxSize_ && future) {
      promises_.emplace_back("PartitionedOutputBuffer::enqueue");
      *future = promises_.back().getSemiFuture();
      blocked = true;
    }
    dataAvailable = buffer->getAndClearNotify();
  }

  // outside of mutex_
  dataAvailable.notify();
  return blocked ? BlockingReason::kWaitForConsumer
                 : BlockingReason::kNotBlocked;
}

void PartitionedOutputBuffer::noMoreData() {
  std::vector<DataAvailable> finished;
  {
    std::lock_guard<std::mutex> l(mutex_);
    ++numFinished_;
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
  // outside of mutex_
  if (atEnd_) {
    for (auto& notification : finished) {
      notification.notify();
    }
  }
}

bool PartitionedOutputBuffer::isFinished() {
  std::lock_guard<std::mutex> l(mutex_);
  return isFinishedLocked();
}

bool PartitionedOutputBuffer::isFinishedLocked() {
  for (auto& buffer : buffers_) {
    if (buffer) {
      return false;
    }
  }
  return true;
}

// Frees 'freed' and realizes 'promises'. Used after
// updateAfterAcknowledgeLocked. This runs outside of the mutex, so
// that we do the expensive free outside and only then continue the
// producers which will allocate more memory.
namespace {
void releaseAfterAcknowledge(
    std::vector<std::shared_ptr<VectorStreamGroup>>& freed,
    std::vector<VeloxPromise<bool>>& promises) {
  freed.clear();
  for (auto& promise : promises) {
    promise.setValue(true);
  }
}
} // namespace

void PartitionedOutputBuffer::acknowledge(int destination, int64_t sequence) {
  std::vector<std::shared_ptr<VectorStreamGroup>> freed;
  std::vector<VeloxPromise<bool>> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK_LT(destination, buffers_.size());
    auto* buffer = buffers_[destination].get();
    if (!buffer) {
      VLOG(1) << "Ack received after final ack for destination " << destination
              << " and sequence " << sequence;
      return;
    }
    uint64_t totalFreed = 0;
    buffer->acknowledge(sequence, false, freed, &totalFreed);
    updateAfterAcknowledgeLocked(totalFreed, promises);
  }
  releaseAfterAcknowledge(freed, promises);
}

void PartitionedOutputBuffer::updateAfterAcknowledgeLocked(
    int64_t totalFreed,
    std::vector<VeloxPromise<bool>>& promises) {
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
  std::vector<std::shared_ptr<VectorStreamGroup>> freed;
  std::vector<VeloxPromise<bool>> promises;
  bool isFinished;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK(destination < buffers_.size());
    auto* buffer = buffers_[destination].get();
    if (!buffer) {
      VLOG(1) << "Extra delete  received  for destination " << destination;
      return false;
    }
    uint64_t totalFreed = 0;
    buffer->deleteResults(freed, &totalFreed);
    buffers_[destination] = nullptr;
    ++numFinalAcknowledges_;
    isFinished = isFinishedLocked();
    updateAfterAcknowledgeLocked(totalFreed, promises);
  }
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
  std::vector<std::shared_ptr<VectorStreamGroup>> data;
  std::vector<std::shared_ptr<VectorStreamGroup>> freed;
  std::vector<VeloxPromise<bool>> promises;
  uint64_t totalFreed = 0;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK_LT(destination, buffers_.size());
    auto destinationBuffer = buffers_[destination].get();
    VELOX_CHECK(
        destinationBuffer,
        "getData received after its buffer is deleted. Destination: {}, sequence: {}",
        destination,
        sequence);
    destinationBuffer->acknowledge(sequence, true, freed, &totalFreed);
    updateAfterAcknowledgeLocked(totalFreed, promises);
    destinationBuffer->getData(maxBytes, sequence, notify, data);
  }
  releaseAfterAcknowledge(freed, promises);
  if (!data.empty()) {
    notify(data, sequence);
  }
}

void PartitionedOutputBuffer::terminate() {
  VELOX_CHECK(task_->state() != kRunning);
  std::lock_guard<std::mutex> l(mutex_);
  for (auto& promise : promises_) {
    promise.setValue(true);
  }
}

std::string PartitionedOutputBuffer::toString() {
  std::stringstream out;
  out << "[PartitionedOutputBuffer totalSize_=" << totalSize_
      << "b, num producers blocked=" << promises_.size()
      << ", completed=" << numFinished_ << "/" << numDrivers_ << ", "
      << (atEnd_ ? "at end, " : "") << "destinations: " << std::endl;
  for (int i = 0; i < buffers_.size(); ++i) {
    auto buffer = buffers_[i].get();
    out << i << ": " << (buffer ? buffer->toString() : "none") << std::endl;
  }
  out << "]" << std::endl;
  return out.str();
}

// static
std::weak_ptr<PartitionedOutputBufferManager>
PartitionedOutputBufferManager::getInstance(const std::string& host) {
  static std::mutex mutex;
  static std::unordered_map<
      std::string,
      std::shared_ptr<PartitionedOutputBufferManager>>
      instances;
  std::lock_guard<std::mutex> l(mutex);
  auto it = instances.find(host);
  if (it != instances.end()) {
    return it->second;
  }
  auto instance = std::make_shared<PartitionedOutputBufferManager>();
  instances[host] = instance;
  return instance;
}

std::shared_ptr<PartitionedOutputBuffer>
PartitionedOutputBufferManager::getBuffer(const std::string& taskId) {
  std::lock_guard<std::mutex> l(mutex_);
  auto it = buffers_.find(taskId);
  VELOX_CHECK(
      it != buffers_.end(), "Output buffers for task not found: {}", taskId);
  return it->second;
}

BlockingReason PartitionedOutputBufferManager::enqueue(
    const std::string& taskId,
    int destination,
    std::unique_ptr<VectorStreamGroup>&& data,
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
  std::shared_ptr<PartitionedOutputBuffer> buffer;
  {
    std::lock_guard<std::mutex> l(mutex_);
    auto it = buffers_.find(taskId);
    if (it == buffers_.end()) {
      VLOG(1) << "Receiving ack for non-existent task " << taskId
              << " destination " << destination << " sequence " << sequence;
      return;
    }
    buffer = it->second;
  }
  buffer->acknowledge(destination, sequence);
}

void PartitionedOutputBufferManager::deleteResults(
    const std::string& taskId,
    int destination) {
  std::shared_ptr<PartitionedOutputBuffer> buffer;
  {
    std::lock_guard<std::mutex> l(mutex_);
    auto it = buffers_.find(taskId);
    if (it == buffers_.end()) {
      return;
    }
    buffer = it->second;
  }
  if (buffer->deleteResults(destination)) {
    std::lock_guard<std::mutex> l(mutex_);
    buffers_.erase(taskId);
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
    int numDestinations,
    int numDrivers) {
  std::lock_guard<std::mutex> l(mutex_);
  auto it = buffers_.find(task->taskId());
  if (it == buffers_.end()) {
    buffers_[task->taskId()] = std::make_shared<PartitionedOutputBuffer>(
        task, numDestinations, numDrivers);
  } else {
    VELOX_FAIL(
        "Registering an output buffer for pre-existing taskId {}",
        task->taskId());
  }
}

void PartitionedOutputBufferManager::removeTask(const std::string& taskId) {
  std::shared_ptr<PartitionedOutputBuffer> buffer;
  {
    std::lock_guard<std::mutex> l(mutex_);
    auto iter = buffers_.find(taskId);
    if (iter == buffers_.end()) {
      // Already removed.
      return;
    }
    buffer = iter->second;
    buffers_.erase(taskId);
  }
  buffer->terminate();
}

std::string PartitionedOutputBufferManager::toString() {
  std::stringstream out;
  out << "[BufferManager:" << std::endl;
  for (auto& pair : buffers_) {
    out << pair.first << ": " << pair.second->toString() << std::endl;
  }
  out << "]";
  return out.str();
}

} // namespace facebook::velox::exec
