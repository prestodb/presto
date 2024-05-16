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

ByteInputStream SerializedPage::prepareStreamForDeserialize() {
  return ByteInputStream(std::move(ranges_));
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
  if (!promises_.empty()) {
    // Resume one of the waiting drivers.
    promises.push_back(std::move(promises_.back()));
    promises_.pop_back();
  }
}

std::vector<std::unique_ptr<SerializedPage>> ExchangeQueue::dequeueLocked(
    uint32_t maxBytes,
    bool* atEnd,
    ContinueFuture* future) {
  VELOX_CHECK_NOT_NULL(future);
  if (!error_.empty()) {
    *atEnd = true;
    VELOX_FAIL(error_);
  }

  *atEnd = false;

  std::vector<std::unique_ptr<SerializedPage>> pages;
  uint32_t pageBytes = 0;
  for (;;) {
    if (queue_.empty()) {
      if (atEnd_) {
        *atEnd = true;
      } else if (pages.empty()) {
        promises_.emplace_back("ExchangeQueue::dequeue");
        *future = promises_.back().getSemiFuture();
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
