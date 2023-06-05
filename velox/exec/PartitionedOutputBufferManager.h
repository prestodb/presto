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

#include "velox/exec/Exchange.h"
#include "velox/exec/Operator.h"
#include "velox/exec/Task.h"

namespace facebook::velox::exec {

/// nullptr in pages indicates that there is no more data.
/// sequence is the same as specified in BufferManager::getData call. The
/// caller is expected to advance sequence by the number of entries in groups
/// and call BufferManager::acknowledge.
using DataAvailableCallback = std::function<
    void(std::vector<std::unique_ptr<folly::IOBuf>> pages, int64_t sequence)>;

struct DataAvailable {
  DataAvailableCallback callback;
  int64_t sequence;
  std::vector<std::unique_ptr<folly::IOBuf>> data;

  void notify() {
    if (callback) {
      callback(std::move(data), sequence);
    }
  }
};

/// The class is used to buffer the output pages which haven't been fetched by
/// any destination for arbitrary output.
///
/// NOTE: there is only one arbitrary buffer setup for arbitrary output to share
/// among destinations. Also, this class is not thread-safe.
class ArbitraryBuffer {
 public:
  /// Returns true if this arbitrary buffer has no buffered pages.
  bool empty() const {
    return pages_.empty() || hasNoMoreData();
  }

  /// Returns true if this arbitrary buffer will not receive any new pages from
  /// enqueue() but it can still has buffered pages waiting to dispatch to
  /// destination on data fetch.
  bool hasNoMoreData() const {
    return !pages_.empty() && (pages_.back() == nullptr);
  }

  /// Marks this arbitrary buffer will not receive any new incoming pages. It
  /// appends a null page at the end of 'pages_' as end marker.
  void noMoreData();

  void enqueue(std::unique_ptr<SerializedPage> page);

  /// Returns a number of pages with total bytes no less than 'maxBytes' if
  /// there are sufficient buffered pages.
  std::vector<std::shared_ptr<SerializedPage>> getPages(uint64_t maxBytes);

  std::string toString() const;

 private:
  std::deque<std::shared_ptr<SerializedPage>> pages_;
};

class DestinationBuffer {
 public:
  void enqueue(std::shared_ptr<SerializedPage> data);

  /// Invoked to load data with up to 'notifyMaxBytes_' bytes from arbitrary
  /// 'buffer' if there is pending fetch from this destination in which case
  /// 'notify_' is not null. Otherwise, it does nothing. This only used by
  /// arbitrary output type when enqueue new data.
  void maybeLoadData(ArbitraryBuffer* buffer);

  /// Invoked to load data with up to 'maxBytes' from arbitrary 'buffer' when
  /// fetch data from this destination. This only used by arbitrary output type
  /// which doesn't expect to buffer any data and is always load data from the
  /// arbitrary buffer on demand.
  void loadData(ArbitraryBuffer* buffer, uint64_t maxBytes);

  // Returns a shallow copy (folly::IOBuf::clone) of the data starting at
  // 'sequence', stopping after exceeding 'maxBytes'. If there is no data,
  // 'notify' is installed so that this gets called when data is added.
  std::vector<std::unique_ptr<folly::IOBuf>> getData(
      uint64_t maxBytes,
      int64_t sequence,
      DataAvailableCallback notify,
      ArbitraryBuffer* arbitraryBuffer = nullptr);

  // Removes data from the queue and returns removed data. If 'fromGetData' we
  // do not give a warning for the case where no data is removed, otherwise we
  // expect that data does get freed. We cannot assert that data gets
  // deleted because acknowledge messages can arrive out of order.
  std::vector<std::shared_ptr<SerializedPage>> acknowledge(
      int64_t sequence,
      bool fromGetData);

  // Removes all remaining data from the queue and returns the removed data.
  std::vector<std::shared_ptr<SerializedPage>> deleteResults();

  // Returns and clears the notify callback, if any, along with arguments for
  // the callback.
  DataAvailable getAndClearNotify();

  std::string toString();

 private:
  std::vector<std::shared_ptr<SerializedPage>> data_;
  // The sequence number of the first in 'data_'.
  int64_t sequence_ = 0;
  DataAvailableCallback notify_ = nullptr;
  // The sequence number of the first item to pass to 'notify'.
  int64_t notifySequence_{0};
  uint64_t notifyMaxBytes_{0};
};

class PartitionedOutputBuffer {
 public:
  enum class Kind {
    kPartitioned,
    kBroadcast,
    kArbitrary,
  };
  static std::string kindString(Kind kind);

  PartitionedOutputBuffer(
      std::shared_ptr<Task> task,
      Kind kind,
      int numDestinations,
      uint32_t numDrivers);

  Kind kind() const {
    return kind_;
  }

  /// The total number of output buffers may not be known at the task start
  /// time for broadcast and arbitrary output buffer type. This method can be
  /// called to update the total number of broadcast or arbitrary destinations
  /// while the task is running. The function throws if this is partitioned
  /// output buffer type.
  void updateOutputBuffers(int numBuffers, bool noMoreBuffers);

  /// When we understand the final number of split groups (for grouped
  /// execution only), we need to update the number of producing drivers here.
  void updateNumDrivers(uint32_t newNumDrivers);

  BlockingReason enqueue(
      int destination,
      std::unique_ptr<SerializedPage> data,
      ContinueFuture* future);

  void noMoreData();

  void noMoreDrivers();

  bool isFinished();

  bool isFinishedLocked();

  void acknowledge(int destination, int64_t sequence);

  // Deletes all data for 'destination'. Returns true if all
  // destinations are deleted, meaning that the buffer is fully
  // consumed and the producer can be marked finished and the buffers
  // freed.
  bool deleteResults(int destination);

  void getData(
      int destination,
      uint64_t maxSize,
      int64_t sequence,
      DataAvailableCallback notify);

  // Continues any possibly waiting producers. Called when the
  // producer task has an error or cancellation.
  void terminate();

  std::string toString();

 private:
  // Percentage of maxSize below which a blocked producer should
  // be unblocked.
  static constexpr int32_t kContinuePct = 90;

  /// If this is called due to a driver processed all its data (no more data),
  /// we increment the number of finished drivers. If it is called due to us
  /// updating the total number of drivers, we don't.
  void checkIfDone(bool oneDriverFinished);

  // Updates buffered size and returns possibly continuable producer promises
  // in 'promises'.
  void updateAfterAcknowledgeLocked(
      const std::vector<std::shared_ptr<SerializedPage>>& freed,
      std::vector<ContinuePromise>& promises);

  /// Given an updated total number of broadcast buffers, add any missing ones
  /// and enqueue data that has been produced so far (e.g. dataToBroadcast_).
  void addOutputBuffersLocked(int numBuffers);

  void enqueueBroadcastOutputLocked(
      std::unique_ptr<SerializedPage> data,
      std::vector<DataAvailable>& dataAvailableCbs);

  void enqueueArbitraryOutputLocked(
      std::unique_ptr<SerializedPage> data,
      std::vector<DataAvailable>& dataAvailableCbs);

  void enqueuePartitionedOutputLocked(
      int destination,
      std::unique_ptr<SerializedPage> data,
      std::vector<DataAvailable>& dataAvailableCbs);

  std::string toStringLocked() const;

  FOLLY_ALWAYS_INLINE bool isBroadcast() const {
    return kind_ == Kind::kBroadcast;
  }

  FOLLY_ALWAYS_INLINE bool isPartitioned() const {
    return kind_ == Kind::kPartitioned;
  }

  FOLLY_ALWAYS_INLINE bool isArbitrary() const {
    return kind_ == Kind::kArbitrary;
  }

  const std::shared_ptr<Task> task_;
  const Kind kind_;
  /// If 'totalSize_' > 'maxSize_', each producer is blocked after adding
  /// data.
  const uint64_t maxSize_;
  // When 'totalSize_' goes below 'continueSize_', blocked producers are
  // resumed.
  const uint64_t continueSize_;
  const std::unique_ptr<ArbitraryBuffer> arbitraryBuffer_;

  // Total number of drivers expected to produce results. This number will
  // decrease in the end of grouped execution, when we understand the real
  // number of producer drivers (depending on the number of split groups).
  uint32_t numDrivers_{0};

  // If true, then we don't allow to add new destination buffers. This only
  // applies for non-partitioned output buffer type.
  bool noMoreBuffers_{false};

  // While noMoreBuffers_ is false, stores the enqueued data to
  // broadcast to destinations that have not yet been initialized. Cleared
  // after receiving no-more-broadcast-buffers signal.
  std::vector<std::shared_ptr<SerializedPage>> dataToBroadcast_;

  std::mutex mutex_;
  // Actual data size in 'buffers_'.
  uint64_t totalSize_ = 0;
  std::vector<ContinuePromise> promises_;
  // The next buffer index in 'buffers_' to load data from arbitrary buffer
  // which is only used by arbitrary output type.
  int32_t nextArbitraryLoadBufferIndex_{0};
  // One buffer per destination.
  std::vector<std::unique_ptr<DestinationBuffer>> buffers_;
  uint32_t numFinished_{0};
  // When this reaches buffers_.size(), 'this' can be freed.
  int numFinalAcknowledges_ = 0;
  bool atEnd_ = false;
};

FOLLY_ALWAYS_INLINE std::ostream& operator<<(
    std::ostream& out,
    const PartitionedOutputBuffer::Kind kind) {
  return out << PartitionedOutputBuffer::kindString(kind);
}

class PartitionedOutputBufferManager {
 public:
  void initializeTask(
      std::shared_ptr<Task> task,
      PartitionedOutputBuffer::Kind kind,
      int numDestinations,
      int numDrivers);

  /// Updates the number of buffers to broadcast. Return true if the buffer
  /// exists for a given taskId, else returns false.
  bool updateBroadcastOutputBuffers(
      const std::string& taskId,
      int numBuffers,
      bool noMoreBuffers);

  /// Updates the number of buffers. Return true if the buffer exists for a
  /// given taskId, else returns false.
  bool updateOutputBuffers(
      const std::string& taskId,
      int numBuffers,
      bool noMoreBuffers);

  /// When we understand the final number of split groups (for grouped
  /// execution only), we need to update the number of producing drivers here.
  void updateNumDrivers(const std::string& taskId, uint32_t newNumDrivers);

  // Adds data to the outgoing queue for 'destination'. 'data' must not be
  // nullptr. 'data' is always added but if the buffers are full the future is
  // set to a ContinueFuture that will be realized when there is space.
  BlockingReason enqueue(
      const std::string& taskId,
      int destination,
      std::unique_ptr<SerializedPage> data,
      ContinueFuture* future);

  void noMoreData(const std::string& taskId);

  // Returns true if noMoreData has been called and all the accumulated data
  // have been fetched and acknowledged.
  bool isFinished(const std::string& taskId);

  // Removes data with sequence number < 'sequence' from the queue for
  // 'destination_'.
  void
  acknowledge(const std::string& taskId, int destination, int64_t sequence);

  void deleteResults(const std::string& taskId, int destination);

  // Adds up to 'maxBytes' bytes worth of data for 'destination' from
  // 'taskId'. The sequence number of the data must be >= 'sequence'.
  // If there is no buffer associated with the given taskId, returns false.
  // If there is no data, 'notify' will be registered and
  // called when there is data or the source is at end, the function returns
  // true.
  // Existing data with a sequence number < sequence is deleted. The caller is
  // expected to increment the sequence number between calls by the
  // number of items received. In this way the next call implicitly
  // acknowledges receipt of the results from the previous. The
  // acknowledge method is offered for an early ack, so that the
  // producer can continue before the consumer is done processing the
  // received data.
  bool getData(
      const std::string& taskId,
      int destination,
      uint64_t maxBytes,
      int64_t sequence,
      DataAvailableCallback notify);

  void removeTask(const std::string& taskId);

  static std::weak_ptr<PartitionedOutputBufferManager> getInstance();

  uint64_t numBuffers() const;

  // Returns a new stream listener if a listener factory has been set.
  std::unique_ptr<OutputStreamListener> newListener() const {
    return listenerFactory_ ? listenerFactory_() : nullptr;
  }

  // Sets the stream listener factory. This allows custom processing of data
  // for repartitioning, e.g. computing checksums.
  void setListenerFactory(
      std::function<std::unique_ptr<OutputStreamListener>()> factory) {
    listenerFactory_ = factory;
  }

  std::string toString();

 private:
  // Retrieves the set of buffers for a query.
  // Throws an exception if buffer doesn't exist.
  std::shared_ptr<PartitionedOutputBuffer> getBuffer(const std::string& taskId);

  // Retrieves the set of buffers for a query if exists.
  // Returns NULL if task not found.
  std::shared_ptr<PartitionedOutputBuffer> getBufferIfExists(
      const std::string& taskId);

  folly::Synchronized<
      std::unordered_map<std::string, std::shared_ptr<PartitionedOutputBuffer>>,
      std::mutex>
      buffers_;

  std::function<std::unique_ptr<OutputStreamListener>()> listenerFactory_{
      nullptr};
};
} // namespace facebook::velox::exec
