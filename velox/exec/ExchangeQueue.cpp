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
#include "velox/exec/ExchangeQueue.h"
#include <algorithm>

namespace facebook::velox::exec {

SerializedPage::SerializedPage(
    std::unique_ptr<folly::IOBuf> iobuf,
    std::function<void(folly::IOBuf&)> onDestructionCb,
    std::optional<int64_t> numRows)
    : iobuf_(std::move(iobuf)),
      iobufBytes_(chainBytes(*iobuf_.get())),
      numRows_(numRows),
      onDestructionCb_(onDestructionCb) {
  VELOX_CHECK_NOT_NULL(iobuf_);
  for (auto& buf : *iobuf_) {
    int32_t bufSize = buf.size();
    ranges_.push_back(ByteRange{
        const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(buf.data())),
        bufSize,
        0});
  }
}

SerializedPage::~SerializedPage() {
  if (onDestructionCb_) {
    onDestructionCb_(*iobuf_.get());
  }
}

std::unique_ptr<ByteInputStream> SerializedPage::prepareStreamForDeserialize() {
  return std::make_unique<BufferInputStream>(std::move(ranges_));
}

void ExchangeQueue::noMoreSources() {
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    noMoreSources_ = true;
    promises = checkCompleteLocked();
  }
  clearPromises(promises);
}

void ExchangeQueue::close() {
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    promises = closeLocked();
  }
  clearPromises(promises);
}

int64_t ExchangeQueue::minOutputBatchBytesLocked() const {
  // always allow to unblock when at end
  if (atEnd_) {
    return 0;
  }
  // At most 1% of received bytes so far to minimize latency for small exchanges
  return std::min<int64_t>(minOutputBatchBytes_, receivedBytes_ / 100);
}

void ExchangeQueue::enqueueLocked(
    std::unique_ptr<SerializedPage>&& page,
    std::vector<ContinuePromise>& promises) {
  if (page == nullptr) {
    ++numCompleted_;
    auto completedPromises = checkCompleteLocked();
    promises.reserve(promises.size() + completedPromises.size());
    for (auto& promise : completedPromises) {
      promises.push_back(std::move(promise));
    }
    return;
  }

  totalBytes_ += page->size();
  if (peakBytes_ < totalBytes_) {
    peakBytes_ = totalBytes_;
  }

  ++receivedPages_;
  receivedBytes_ += page->size();

  queue_.push_back(std::move(page));
  const auto minBatchSize = minOutputBatchBytesLocked();
  while (!promises_.empty()) {
    VELOX_CHECK_LE(promises_.size(), numberOfConsumers_);
    const int32_t unblockedConsumers = numberOfConsumers_ - promises_.size();
    const int64_t unasignedBytes =
        totalBytes_ - unblockedConsumers * minBatchSize;
    if (unasignedBytes < minBatchSize) {
      break;
    }
    // Resume one of the waiting drivers.
    auto it = promises_.begin();
    promises.push_back(std::move(it->second));
    promises_.erase(it);
  }
}

void ExchangeQueue::addPromiseLocked(
    int consumerId,
    ContinueFuture* future,
    ContinuePromise* stalePromise) {
  ContinuePromise promise{"ExchangeQueue::dequeue"};
  *future = promise.getSemiFuture();
  auto it = promises_.find(consumerId);
  if (it != promises_.end()) {
    // resolve stale promises outside the lock to avoid broken promises
    *stalePromise = std::move(it->second);
    it->second = std::move(promise);
  } else {
    promises_[consumerId] = std::move(promise);
  }
  VELOX_CHECK_LE(promises_.size(), numberOfConsumers_);
}

std::vector<std::unique_ptr<SerializedPage>> ExchangeQueue::dequeueLocked(
    int consumerId,
    uint32_t maxBytes,
    bool* atEnd,
    ContinueFuture* future,
    ContinuePromise* stalePromise) {
  VELOX_CHECK_NOT_NULL(future);
  if (!error_.empty()) {
    *atEnd = true;
    VELOX_FAIL(error_);
  }

  *atEnd = false;

  // If we don't have enough bytes to return, we wait for more data to be
  // available
  if (totalBytes_ < minOutputBatchBytesLocked()) {
    addPromiseLocked(consumerId, future, stalePromise);
    return {};
  }

  std::vector<std::unique_ptr<SerializedPage>> pages;
  uint32_t pageBytes = 0;
  for (;;) {
    if (queue_.empty()) {
      if (atEnd_) {
        *atEnd = true;
      } else if (pages.empty()) {
        addPromiseLocked(consumerId, future, stalePromise);
      }
      return pages;
    }

    if (pageBytes > 0 && pageBytes + queue_.front()->size() > maxBytes) {
      return pages;
    }

    pages.emplace_back(std::move(queue_.front()));
    queue_.pop_front();
    pageBytes += pages.back()->size();
    totalBytes_ -= pages.back()->size();
  }

  VELOX_UNREACHABLE();
}

void ExchangeQueue::setError(const std::string& error) {
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (!error_.empty()) {
      return;
    }
    error_ = error;
    atEnd_ = true;
    // NOTE: clear the serialized page queue as we won't consume from an
    // errored queue.
    queue_.clear();
    promises = clearAllPromisesLocked();
  }
  clearPromises(promises);
}

} // namespace facebook::velox::exec
