/*
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

#include <atomic>
#include <deque>
#include <mutex>
#include <vector>

#include <folly/io/IOBuf.h>
#include "presto_cpp/main/operators/ShuffleInterface.h"
#include "velox/common/future/VeloxPromise.h"
#include "velox/common/memory/MemoryPool.h"

namespace facebook::presto::operators {

/// Shared buffer between ExchangeWrite operators and a ShuffleWriter.
///
/// Accepts serialized RowGroups per partition from multiple drivers, buffers
/// them, and drains to the ShuffleWriter when the per-partition threshold is
/// hit. Supports ContinueFuture-based cooperative backpressure.
///
/// Memory for buffered RowGroups is tracked through bufferPool_ (a system
/// pool visible for accounting). The writer uses a separate system pool.
class ExchangeOutputBuffer {
 public:
  static constexpr int64_t kDefaultDrainThreshold = 130L * 1024;

  ExchangeOutputBuffer(
      int32_t numPartitions,
      std::shared_ptr<ShuffleWriter> writer,
      std::shared_ptr<velox::memory::MemoryPool> bufferPool,
      int64_t maxBufferedBytes,
      int64_t partitionDrainThreshold = 0);

  ~ExchangeOutputBuffer();

  /// Enqueue a serialized RowGroup for a partition. If total buffered
  /// bytes exceeds maxBufferedBytes, populates *future and returns true.
  bool enqueue(
      int32_t partition,
      std::unique_ptr<folly::IOBuf> rowGroup,
      velox::ContinueFuture* future);

  /// Drain a specific partition — coalesces buffered RowGroups and calls
  /// writer->collect(). Exceptions propagate to the caller.
  int64_t drainPartition(int32_t partition);

  /// Drain all partitions.
  uint64_t drainAll();

  /// Signal that no more data will be enqueued. Drains remaining data
  /// and calls writer->noMoreData(true).
  void noMoreData();

  /// Abort — clears buffers and calls writer->noMoreData(false).
  void abort();

  int64_t bufferedBytes() const {
    return bufferedBytes_;
  }

  int64_t currentDrainThreshold() const {
    return partitionDrainThreshold_;
  }

  /// Record the number of drivers. Only the first call takes effect.
  void setNumDrivers(uint32_t numDrivers);

  /// Called by each driver when it finishes. Returns true if this was the
  /// last driver (triggered finishAndClose). The caller can use this to
  /// attach writer stats to its operator.
  bool noMoreDrivers();

  /// Returns combined writer + buffer stats. Only meaningful after close.
  folly::F14FastMap<std::string, int64_t> stats() const;

  /// Allocate an IOBuf tracked through bufferPool_. Used by ExchangeWrite
  /// to create RowGroup IOBufs that are visible for memory accounting.
  std::unique_ptr<folly::IOBuf> allocateTrackedIOBuf(size_t size);

  int32_t numPartitions() const {
    return numPartitions_;
  }

  velox::memory::MemoryPool* pool() const {
    return bufferPool_.get();
  }

 private:
  struct PartitionQueue {
    std::mutex mutex;
    std::deque<std::unique_ptr<folly::IOBuf>> rowGroups;
    std::atomic<int64_t> bufferedBytes{0};
  };

  void maybeUnblockProducers(std::vector<velox::ContinuePromise>& promises);
  void finishAndClose();

  void flushToWriter(int32_t partition, std::unique_ptr<folly::IOBuf> data);

  std::unique_ptr<folly::IOBuf> coalesceRowGroups(
      std::deque<std::unique_ptr<folly::IOBuf>>& rowGroups);

  bool maybeApplyBackpressure(velox::ContinueFuture* future);

  // Free callback for pool-tracked IOBufs.
  static void freeTrackedIOBuf(void* buf, void* userData);

  // Immutable config — declaration order must match constructor init order.
  const int32_t numPartitions_;
  const int64_t maxBufferedBytes_;
  const int64_t continueBufferedBytes_;
  const int64_t partitionDrainThreshold_;

  // Writer and buffer pool.
  std::shared_ptr<ShuffleWriter> writer_;
  std::shared_ptr<velox::memory::MemoryPool> bufferPool_;

  // Lifecycle flag.
  std::atomic<bool> finished_{false};

  // Per-partition state. Each PartitionQueue has its own mutex that
  // serializes enqueue + drain for that partition.
  std::atomic<int64_t> bufferedBytes_{0};
  std::vector<PartitionQueue> partitionQueues_;

  // Backpressure state — stateMutex_ guards promises_ and driver counts.
  std::mutex stateMutex_;
  uint32_t numDrivers_{0};
  uint32_t numFinishedDrivers_{0};
  std::vector<velox::ContinuePromise> promises_;

  // Stats counters.
  std::atomic<int64_t> totalDrainedBytes_{0};
  std::atomic<int64_t> drainCount_{0};
  std::atomic<int64_t> backpressureCount_{0};
  std::vector<std::atomic<int64_t>> collectCountPerPartition_;
};

} // namespace facebook::presto::operators
