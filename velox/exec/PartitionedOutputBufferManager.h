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

#include "velox/exec/Operator.h"
#include "velox/exec/Task.h"

namespace facebook::velox::exec {

// nullptr in groups indicates that there is no more data.
// sequence is the same as specified in BufferManager::getData call. The caller
// is expected to advance sequence by the number of entries in groups and call
// BufferManager::acknowledge.
using DataAvailableCallback = std::function<void(
    std::vector<std::shared_ptr<VectorStreamGroup>>& groups,
    int64_t sequence)>;

struct DataAvailable {
  DataAvailableCallback callback;
  int64_t sequence;
  std::vector<std::shared_ptr<VectorStreamGroup>> data;

  void notify() {
    if (callback) {
      callback(data, sequence);
    }
  }
};

class DestinationBuffer {
 public:
  void enqueue(std::unique_ptr<VectorStreamGroup>&& data) {
    // drop duplicate end markers
    if (data == nullptr && !data_.empty() && data_.back() == nullptr) {
      return;
    }

    // The ownership of 'data' moves from unique to shared. This
    // allows for multiple re-fetches and acknowledging of one sequence number
    // without any ordering guarantees, the data does not get deleted while some
    // thread holds a reference.
    std::shared_ptr<VectorStreamGroup> shared(std::move(data));
    data_.push_back(std::move(shared));
  }

  // Copies data starting at 'sequence' into 'result', stopping after
  // exceeding 'maxBytes'. If there is no data, 'notify' is installed
  // so that this gets called when data is added.
  void getData(
      uint64_t maxBytes,
      int64_t sequence,
      DataAvailableCallback notify,
      std::vector<std::shared_ptr<VectorStreamGroup>>& result);

  // Removes data from the queue. If 'fromGetData' we do not give a
  // warning for the case where no data is removed, otherwise we
  // expect that data does get freed. We cannot assert that data gets
  // deleted because acknowledge messages can arrive out of order.
  std::vector<std::shared_ptr<VectorStreamGroup>> acknowledge(
      int64_t sequence,
      bool fromGetData);

  // Returns all data to be freed in 'freed' and their size in
  // 'totalFreed'. 'this' can be destroyed after this.
  std::vector<std::shared_ptr<VectorStreamGroup>> deleteResults();

  // Returns and clears the notify callback, if any, along with arguments for
  // the callback.
  DataAvailable getAndClearNotify();

  std::string toString();

 private:
  std::vector<std::shared_ptr<VectorStreamGroup>> data_;
  // The sequence number of the first in 'data_'.
  int64_t sequence_ = 0;
  DataAvailableCallback notify_ = nullptr;
  // The sequence number of the first item to pass to 'notify'.
  int64_t notifySequence_;
  uint64_t notifyMaxBytes_;
};

class PartitionedOutputBuffer {
 public:
  PartitionedOutputBuffer(
      std::shared_ptr<Task> task,
      int numDestinations,
      int numDrivers)
      : task_(task),
        numDrivers_(numDrivers),
        maxSize_(task->queryCtx()->maxPartitionedOutputBufferSize()),
        continueSize_(maxSize_ / 2) {
    buffers_.reserve(numDestinations);
    for (int i = 0; i < numDestinations; i++) {
      buffers_.push_back(std::make_unique<DestinationBuffer>());
    }
  }

  BlockingReason enqueue(
      int destination,
      std::unique_ptr<VectorStreamGroup> data,
      ContinueFuture* future);

  void noMoreData();

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
  // Updates buffered size and returns possibly continuable producer promises in
  // 'promises'.
  void updateAfterAcknowledgeLocked(
      const std::vector<std::shared_ptr<VectorStreamGroup>>& freed,
      std::vector<VeloxPromise<bool>>& promises);

  std::shared_ptr<Task> task_;
  const int numDrivers_ = 0;
  // If 'totalSize_' > 'maxSize_', each producer is blocked after adding data.
  const uint64_t maxSize_;
  // When 'totalSize_' goes below 'continueSize_', blocked producers are
  // resumed.
  const uint64_t continueSize_;

  std::mutex mutex_;
  // Actual data size in 'buffers_'.
  uint64_t totalSize_ = 0;
  std::vector<VeloxPromise<bool>> promises_;
  // One buffer per destination
  std::vector<std::unique_ptr<DestinationBuffer>> buffers_;
  int numFinished_ = 0;
  // When this reaches buffers_.size(), 'this' can be freed.
  int numFinalAcknowledges_ = 0;
  bool atEnd_ = false;
};

class PartitionedOutputBufferManager {
 public:
  void initializeTask(
      std::shared_ptr<Task> task,
      int numDestinations,
      int numDrivers);

  // Adds data to the outgoing queue for 'destination'. 'data' must not be
  // nullptr. 'data' is always added but if the buffers are full the future is
  // set to a continue future that will be realized when there is space.
  BlockingReason enqueue(
      const std::string& taskId,
      int destination,
      std::unique_ptr<VectorStreamGroup> data,
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
  // 'taskId'. The sequence number of the data must be >=
  // 'sequence'. If there is no data, 'notify' will be registered and
  // called when there is data or the source is at end. Existing data
  // with a sequence number < sequence is deleted. The caller is
  // expected to increment the sequence number between calls by the
  // number of items received. In this way the next call implicitly
  // acknowledges receipt of the results from the previous. The
  // acknowledge method is offered for an early ack, so that the
  // producer can continue before the consumer is done processing the
  // received data.
  void getData(
      const std::string& taskId,
      int destination,
      uint64_t maxBytes,
      int64_t sequence,
      DataAvailableCallback notify);

  void removeTask(const std::string& taskId);

  // Returns the instance corresponding to the logical host
  // 'host'. Multiple logical servers can be collocated in one process
  // for testing, for example with Presto, these are differentiated by
  // the Host header in the messages coming to each.
  static std::weak_ptr<PartitionedOutputBufferManager> getInstance(
      const std::string& host = "local");

  std::string toString();

 private:
  // Retrieves the set of buffers for a query.
  std::shared_ptr<PartitionedOutputBuffer> getBuffer(const std::string& taskId);

  std::mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<PartitionedOutputBuffer>>
      buffers_;
};
} // namespace facebook::velox::exec
