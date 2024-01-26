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

#include "velox/exec/OutputBuffer.h"

namespace facebook::velox::exec {

class OutputBufferManager {
 public:
  void initializeTask(
      std::shared_ptr<Task> task,
      core::PartitionedOutputNode::Kind kind,
      int numDestinations,
      int numDrivers);

  /// Updates the number of buffers. Returns true if the buffer exists for a
  /// given taskId, else returns false.
  bool updateOutputBuffers(
      const std::string& taskId,
      int numBuffers,
      bool noMoreBuffers);

  /// When we understand the final number of split groups (for grouped
  /// execution only), we need to update the number of producing drivers here.
  /// Returns true if the buffer exists for a given taskId, else returns false.
  bool updateNumDrivers(const std::string& taskId, uint32_t newNumDrivers);

  // Adds data to the outgoing queue for 'destination'. 'data' must not be
  // nullptr. 'data' is always added but if the buffers are full the future is
  // set to a ContinueFuture that will be realized when there is space.
  bool enqueue(
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

  /// Adds up to 'maxBytes' bytes worth of data for 'destination' from 'taskId'.
  /// The sequence number of the data must be >= 'sequence'. If there is no
  /// buffer associated with the given taskId, returns false. If there is no
  /// data, 'notify' will be registered and called when there is data or the
  /// source is at end, the function returns true. Existing data with a sequence
  /// number < sequence is deleted. The caller is expected to increment the
  /// sequence number between calls by the number of items received. In this way
  /// the next call implicitly acknowledges receipt of the results from the
  /// previous. The acknowledge method is offered for an early ack, so that the
  /// producer can continue before the consumer is done processing the received
  /// data. If not null, 'activeCheck' is used to check if data consumer is
  /// currently active or not. This only applies for arbitrary output buffer for
  /// now.
  bool getData(
      const std::string& taskId,
      int destination,
      uint64_t maxBytes,
      int64_t sequence,
      DataAvailableCallback notify,
      DataConsumerActiveCheckCallback activeCheck = nullptr);

  void removeTask(const std::string& taskId);

  static std::weak_ptr<OutputBufferManager> getInstance();

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

  // Gets the memory utilization ratio for the output buffer from a task of
  // taskId, if the task of this taskId is not found, return 0.
  double getUtilization(const std::string& taskId);

  // If the output buffer from a task of taskId is over-utilized and blocks its
  // producers. When the task of this taskId is not found, return false.
  bool isOverutilized(const std::string& taskId);

  // Returns nullopt when the specified output buffer doesn't exist.
  std::optional<OutputBuffer::Stats> stats(const std::string& taskId);

  // Retrieves the set of buffers for a query if exists.
  // Returns NULL if task not found.
  std::shared_ptr<OutputBuffer> getBufferIfExists(const std::string& taskId);

 private:
  // Retrieves the set of buffers for a query.
  // Throws an exception if buffer doesn't exist.
  std::shared_ptr<OutputBuffer> getBuffer(const std::string& taskId);

  folly::Synchronized<
      std::unordered_map<std::string, std::shared_ptr<OutputBuffer>>,
      std::mutex>
      buffers_;

  std::function<std::unique_ptr<OutputStreamListener>()> listenerFactory_{
      nullptr};
};
} // namespace facebook::velox::exec
