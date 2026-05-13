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

#include <folly/Synchronized.h>
#include <folly/io/IOBuf.h>
#include "presto_cpp/main/operators/ShuffleInterface.h"
#include "velox/common/future/VeloxPromise.h"
#include "velox/common/memory/MemoryPool.h"

namespace facebook::presto::operators {

/// Shared buffer between MaterializedOutput operators and a ShuffleWriter.
///
/// Accepts serialized RowGroups per partition from multiple drivers, buffers
/// them, and drains to the ShuffleWriter when the per-partition threshold is
/// hit. Supports ContinueFuture-based cooperative backpressure.
///
/// Memory for buffered RowGroups is tracked through bufferPool_ (a system
/// pool visible for accounting). The writer uses a separate system pool.
/// Thread-safe shared buffer between MaterializedOutput operators and a
/// ShuffleWriter. Multiple MaterializedOutput drivers enqueue concurrently;
/// the buffer drains to the writer when per-partition thresholds are hit.
class MaterializedOutputBuffer {
 public:
  static constexpr int64_t kDefaultDrainThreshold = 130L * 1024;

  // Stat name constants.
  static constexpr std::string_view kTotalDrainedBytes =
      "materializedOutputBuffer.totalDrainedBytes";
  static constexpr std::string_view kDrainCount =
      "materializedOutputBuffer.drainCount";
  static constexpr std::string_view kBackpressureCount =
      "materializedOutputBuffer.backpressureCount";
  static constexpr std::string_view kCurrentDrainThreshold =
      "materializedOutputBuffer.currentDrainThreshold";
  static constexpr std::string_view kBufferPoolUsedBytes =
      "materializedOutputBuffer.bufferPoolUsedBytes";
  static constexpr std::string_view kBufferPoolPeakBytes =
      "materializedOutputBuffer.bufferPoolPeakBytes";
  static constexpr std::string_view kTotalCollectCalls =
      "materializedOutputBuffer.totalCollectCalls";
  static constexpr std::string_view kPeakBufferedBytes =
      "materializedOutputBuffer.peakBufferedBytes";

  MaterializedOutputBuffer(
      int32_t numPartitions,
      std::shared_ptr<ShuffleWriter> writer,
      std::shared_ptr<velox::memory::MemoryPool> pool,
      int64_t maxBufferedBytes,
      int64_t partitionDrainThreshold = 0);

  ~MaterializedOutputBuffer();

  /// Enqueue a serialized RowGroup for a partition. If total buffered
  /// bytes exceeds maxBufferedBytes, populates *future and returns true.
  bool enqueue(
      int32_t partition,
      std::unique_ptr<folly::IOBuf> rowGroup,
      velox::ContinueFuture* future);

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

  /// For testing: returns the current per-partition drain threshold.
  int64_t testingCurrentDrainThreshold() const {
    return partitionDrainThreshold_;
  }

  /// Record the number of drivers. Called by MaterializedOutput before
  /// task start.
  void setNumDrivers(uint32_t numDrivers);

  /// Called by each driver when it finishes. Returns true if this was the
  /// last driver (triggered finishAndClose). The caller can use this to
  /// attach writer stats to its operator.
  bool noMoreDrivers();

  /// Returns combined writer + buffer stats. Only meaningful after close.
  folly::F14FastMap<std::string, int64_t> stats() const;

  /// Allocate an IOBuf tracked through pool_. Used by MaterializedOutput
  /// to create RowGroup IOBufs that are visible for memory accounting.
  std::unique_ptr<folly::IOBuf> allocateTrackedIOBuf(size_t size);

  int32_t numPartitions() const {
    return numPartitions_;
  }

  velox::memory::MemoryPool* pool() const {
    return pool_.get();
  }

 private:
  // Per-partition buffer. Single mutex serializes enqueue and drain.
  // When buffered bytes exceed the drain threshold, enqueue coalesces
  // and flushes to the writer under the same lock — prevents concurrent
  // collect() calls on the same partition.
  class PartitionBuffer {
   public:
    PartitionBuffer() = default;

    PartitionBuffer(
        int64_t drainThreshold,
        ShuffleWriter* writer,
        MaterializedOutputBuffer* buffer)
        : drainThreshold_(drainThreshold), writer_(writer), buffer_(buffer) {}

    // Append a RowGroup under the partition lock. If threshold is reached,
    // drains under the same lock — coalesces + calls writer->collect().
    // Returns bytes drained (0 if no drain occurred).
    int64_t enqueue(int32_t partition, std::unique_ptr<folly::IOBuf> rowGroup);

   private:
    friend class MaterializedOutputBuffer;

    mutable std::mutex mutex_;
    std::deque<std::unique_ptr<folly::IOBuf>> rowGroups_;
    int64_t bufferedBytes_{0};
    int64_t drainThreshold_{0};
    ShuffleWriter* writer_{nullptr};
    MaterializedOutputBuffer* buffer_{nullptr};
  };

  // Drain a specific partition — called from drainAll() during finishAndClose.
  int64_t drainPartition(int32_t partition);

  // Fulfill promises to unblock producers waiting on backpressure.
  void maybeUnblockProducers(std::vector<velox::ContinuePromise>& promises);

  // Drain all remaining data and close the writer. Called exactly once.
  void finishAndClose();

  // Coalesce data into a contiguous buffer and send to the ShuffleWriter.
  void flushToWriter(int32_t partition, std::unique_ptr<folly::IOBuf> data);

  // Merge a deque of RowGroup IOBufs into a single contiguous IOBuf.
  std::unique_ptr<folly::IOBuf> coalesceRowGroups(
      std::deque<std::unique_ptr<folly::IOBuf>>& rowGroups);

  // Check if total buffered bytes exceeds the threshold and set up a
  // ContinueFuture for the caller to block on.
  bool maybeApplyBackpressure(velox::ContinueFuture* future);

  // Free callback for pool-tracked IOBufs.
  static void freeTrackedIOBuf(void* buf, void* userData);

  // Immutable config.
  const int32_t numPartitions_;
  const int64_t maxBufferedBytes_;
  const int64_t continueBufferedBytes_;
  const int64_t partitionDrainThreshold_;

  // Writer and memory pool.
  const std::shared_ptr<ShuffleWriter> writer_;
  const std::shared_ptr<velox::memory::MemoryPool> pool_;

  // Lifecycle flags.
  std::atomic<bool> finished_{false};
  std::atomic<bool> aborted_{false};

  // Per-partition buffers. Each PartitionBuffer has its own mutex that
  // serializes enqueue + drain for that partition.
  std::atomic<int64_t> bufferedBytes_{0};
  std::vector<std::unique_ptr<PartitionBuffer>> partitionBuffers_;

  // Backpressure state — stateMutex_ guards promises_ and driver counts.
  std::mutex stateMutex_;
  uint32_t numDrivers_{0};
  uint32_t numFinishedDrivers_{0};
  std::vector<velox::ContinuePromise> promises_;

  // Stats counters.
  std::atomic<int64_t> totalDrainedBytes_{0};
  std::atomic<int64_t> drainCount_{0};
  std::atomic<int64_t> backpressureCount_{0};
  std::atomic<int64_t> peakBufferedBytes_{0};
  std::atomic<int64_t> lastLoggedDrainedGB_{0};
  std::vector<std::atomic<int64_t>> collectCountPerPartition_;
};

} // namespace facebook::presto::operators
