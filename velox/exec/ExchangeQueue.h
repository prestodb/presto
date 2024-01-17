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
#pragma once

#include "velox/common/memory/ByteStream.h"

namespace facebook::velox::exec {

// Corresponds to Presto SerializedPage, i.e. a container for
// serialize vectors in Presto wire format.
class SerializedPage {
 public:
  // Construct from IOBuf chain.
  explicit SerializedPage(
      std::unique_ptr<folly::IOBuf> iobuf,
      std::function<void(folly::IOBuf&)> onDestructionCb = nullptr,
      std::optional<int64_t> numRows = std::nullopt);

  ~SerializedPage();

  // Returns the size of the serialized data in bytes.
  uint64_t size() const {
    return iobufBytes_;
  }

  std::optional<int64_t> numRows() const {
    return numRows_;
  }

  // Makes 'input' ready for deserializing 'this' with
  // VectorStreamGroup::read().
  ByteInputStream prepareStreamForDeserialize();

  std::unique_ptr<folly::IOBuf> getIOBuf() const {
    return iobuf_->clone();
  }

 private:
  static int64_t chainBytes(folly::IOBuf& iobuf) {
    int64_t size = 0;
    for (auto& range : iobuf) {
      size += range.size();
    }
    return size;
  }

  // Buffers containing the serialized data. The memory is owned by 'iobuf_'.
  std::vector<ByteRange> ranges_;

  // IOBuf holding the data in 'ranges_.
  std::unique_ptr<folly::IOBuf> iobuf_;

  // Number of payload bytes in 'iobuf_'.
  const int64_t iobufBytes_;

  // Number of payload rows, if provided.
  const std::optional<int64_t> numRows_;

  // Callback that will be called on destruction of the SerializedPage,
  // primarily used to free externally allocated memory backing folly::IOBuf
  // from caller. Caller is responsible to pass in proper cleanup logic to
  // prevent any memory leak.
  std::function<void(folly::IOBuf&)> onDestructionCb_;
};

/// Queue of results retrieved from source. Owned by shared_ptr by
/// Exchange and client threads and registered callbacks waiting
/// for input.
class ExchangeQueue {
 public:
  ~ExchangeQueue() {
    clearAllPromises();
  }

  std::mutex& mutex() {
    return mutex_;
  }

  bool empty() const {
    return queue_.empty();
  }

  /// Enqueues 'page' to the queue. One random promise(top of promise queue)
  /// associated with the future that is waiting for the data from the queue is
  /// returned in 'promises' if 'page' is not nullptr. When 'page' is nullptr
  /// and the queue is completed serving data, all left over promises will be
  /// returned in 'promises'. When 'page' is nullptr and the queue is not
  /// completed serving data, no 'promises' will be added and returned.
  void enqueueLocked(
      std::unique_ptr<SerializedPage>&& page,
      std::vector<ContinuePromise>& promises);

  /// If data is permanently not available, e.g. the source cannot be
  /// contacted, this registers an error message and causes the reading
  /// Exchanges to throw with the message.
  void setError(const std::string& error);

  /// Returns pages of data.
  ///
  /// Returns empty list if no data is available. If data is still expected,
  /// sets 'atEnd' to false and 'future' to a Future that will complete when
  /// data arrives. If no more data is expected, sets 'atEnd' to true. Returns
  /// at least one page if data is available. If multiple pages are available,
  /// returns as many pages as fit within 'maxBytes', but no fewer than onc.
  /// Calling this method with 'maxBytes' of 1 returns at most one page.
  ///
  /// The data may be compressed, in which case 'maxBytes' applies to compressed
  /// size.
  std::vector<std::unique_ptr<SerializedPage>>
  dequeueLocked(uint32_t maxBytes, bool* atEnd, ContinueFuture* future);

  /// Returns the total bytes held by SerializedPages in 'this'.
  uint64_t totalBytes() const {
    return totalBytes_;
  }

  /// Returns the maximum value of total bytes.
  uint64_t peakBytes() const {
    return peakBytes_;
  }

  /// Returns total number of pages received from all sources.
  uint64_t receivedPages() const {
    return receivedPages_;
  }

  /// Returns an average size of received pages. Returns 0 if hasn't received
  /// any pages yet.
  uint64_t averageReceivedPageBytes() const {
    return receivedPages_ > 0 ? receivedBytes_ / receivedPages_ : 0;
  }

  void addSourceLocked() {
    VELOX_CHECK(!noMoreSources_, "addSource called after noMoreSources");
    numSources_++;
  }

  void noMoreSources();

  void close();

 private:
  std::vector<ContinuePromise> closeLocked() {
    queue_.clear();
    return clearAllPromisesLocked();
  }

  std::vector<ContinuePromise> checkCompleteLocked() {
    if (noMoreSources_ && numCompleted_ == numSources_) {
      atEnd_ = true;
      return clearAllPromisesLocked();
    }
    return {};
  }

  void clearAllPromises() {
    std::vector<ContinuePromise> promises;
    {
      std::lock_guard<std::mutex> l(mutex_);
      promises = clearAllPromisesLocked();
    }
    clearPromises(promises);
  }

  std::vector<ContinuePromise> clearAllPromisesLocked() {
    return std::move(promises_);
  }

  static void clearPromises(std::vector<ContinuePromise>& promises) {
    for (auto& promise : promises) {
      promise.setValue();
    }
  }

  int numCompleted_{0};
  int numSources_{0};
  bool noMoreSources_{false};
  bool atEnd_{false};

  std::mutex mutex_;
  std::deque<std::unique_ptr<SerializedPage>> queue_;
  std::vector<ContinuePromise> promises_;
  // When set, all promises will be realized and the next dequeue will
  // throw an exception with this message.
  std::string error_;
  // Total size of SerializedPages in queue.
  uint64_t totalBytes_{0};
  // Number of SerializedPages received.
  int64_t receivedPages_{0};
  // Total size of SerializedPages received. Used to calculate an average
  // expected size.
  int64_t receivedBytes_{0};
  // Maximum value of totalBytes_.
  int64_t peakBytes_{0};
};
} // namespace facebook::velox::exec
