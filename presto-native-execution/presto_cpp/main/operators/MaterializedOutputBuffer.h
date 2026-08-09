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
#include <memory>
#include <mutex>
#include <vector>

#include <fmt/format.h>
#include <folly/Synchronized.h>
#include <folly/container/F14Map.h>
#include <folly/io/IOBuf.h>
#include "presto_cpp/main/operators/ShuffleInterface.h"
#include "velox/common/base/RuntimeMetrics.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/exec/MemoryReclaimer.h"

namespace facebook::presto::operators {

/// Thread-safe shared buffer between MaterializedOutput operators and a
/// ShuffleWriter. Multiple MaterializedOutput drivers enqueue concurrently;
/// the buffer drains to the writer when per-partition thresholds are hit.
///
/// Memory pressure is handled by the Velox memory arbitrator via the
/// nested Reclaimer class, not cooperative backpressure.
///
/// Lifecycle state machine:
///   kActive -> kDraining -> kClosed  (noMoreData: success)
///   kActive -> kDraining -> kAborted (noMoreData: writer failure)
///   kActive -> kAborted              (abort: error teardown)
/// Reclaim Phase 1 (flush partitions) runs only in kActive. Phase 2
/// (wait for writer network drain) also runs in kClosed.
class MaterializedOutputBuffer {
 public:
  enum class State : uint8_t {
    kActive,
    kDraining,
    kClosed,
    kAborted,
  };

  static std::string stateName(State state);

  static constexpr int64_t kDefaultDrainThreshold = 130L * 1024;

  /// Default fraction of the per-partition drain threshold used during reclaim.
  static constexpr double kDefaultReclaimDrainThresholdRatio = 0.67;

  // Stat name constants.
  static constexpr std::string_view kDrainedBytes =
      "materializedOutputBuffer.drainedBytes";
  static constexpr std::string_view kDrainCount =
      "materializedOutputBuffer.drainCount";
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
  static constexpr std::string_view kReclaimCount =
      "materializedOutputBuffer.reclaimCount";
  static constexpr std::string_view kReclaimedBytes =
      "materializedOutputBuffer.reclaimedBytes";

  /// Memory reclaimer for the exchange writer pool. Nested inside
  /// MaterializedOutputBuffer so the raw back-pointer is always valid — the
  /// pool owns the reclaimer, and MaterializedOutputBuffer owns the pool.
  ///
  /// Two-phase reclaim:
  /// (1) flush MaterializedOutputBuffer partition buffers to the writer;
  /// (2) wait for writer background threads to drain packages to network.
  ///
  /// Priority kHighReclaimPriority ensures this pool is reclaimed before
  /// operator pools (priority 0+).
  class Reclaimer : public velox::exec::MemoryReclaimer {
   public:
    static constexpr int32_t kHighReclaimPriority = -1;

    explicit Reclaimer(MaterializedOutputBuffer* partitionBuffer);

    bool reclaimableBytes(
        const velox::memory::MemoryPool& pool,
        uint64_t& reclaimableBytes) const override;

    uint64_t reclaim(
        velox::memory::MemoryPool* pool,
        uint64_t targetBytes,
        uint64_t maxWaitMs,
        Stats& stats) override;

   private:
    /// Returns true if the pool has reclaimable bytes and the buffer
    /// is in a state that supports reclaim (kActive or kDraining).
    bool canReclaim(const velox::memory::MemoryPool& pool, uint64_t targetBytes)
        const;

    /// Returns true if partition flush should run: kActive state and
    /// bufferedBytes > 0. During kDraining, noMoreData() is already
    /// draining partitions so we skip to waiting for writer drain.
    bool canReclaimFromPartitionBuffers() const;

    /// Flush partition buffers to the writer. Calls tryDrainPartitions()
    /// which sorts partitions largest-first and flushes each above
    /// reclaimDrainThresholdBytes via try_lock.
    void tryReclaimPartitionBuffers(velox::memory::MemoryPool* pool);

    /// Wait for writer background threads to drain packages to network.
    /// Polls pool->usedBytes() every 10ms until the pool reaches
    /// targetUsedBytes or the deadline expires.
    void waitForWriterDrain(
        velox::memory::MemoryPool* pool,
        uint64_t targetUsedBytes,
        std::chrono::steady_clock::time_point deadline);

    /// Update arbitrator stats and buffer reclaim counters.
    void recordStats(uint64_t freedBytes, Stats& stats);

    MaterializedOutputBuffer* const partitionBuffer_;
  };

  /// Register a buffer in the process-wide registry. Called under the
  /// prestoTask->mutex in createOrUpdateTaskImpl() to prevent duplicate
  /// creation from concurrent createTask HTTP requests.
  static void registerBuffer(
      const std::string& taskId,
      std::shared_ptr<MaterializedOutputBuffer> buffer);

  /// Look up an existing buffer by taskId. Returns nullptr if not found.
  static std::shared_ptr<MaterializedOutputBuffer> getBuffer(
      const std::string& taskId);

  /// Remove a buffer from the registry. Called during task cleanup.
  static void removeBuffer(const std::string& taskId);

  /// Creates its own leaf pool under 'parentPool' and the writer from
  /// the factory.
  MaterializedOutputBuffer(
      int32_t numPartitions,
      const std::string& shuffleWriterInfo,
      ShuffleInterfaceFactory* shuffleWriterFactory,
      const std::string& taskId,
      velox::memory::MemoryPool* pool);

  ~MaterializedOutputBuffer();

  /// Enqueue a serialized RowGroup for a partition.
  void enqueue(int32_t partition, std::unique_ptr<folly::IOBuf> rowGroup);

  /// Best-effort drain for reclaim. Uses try_lock — skips contested
  /// partitions to avoid deadlock with enqueue() -> collect() ->
  /// arbitration. Partitions below reclaimDrainThresholdBytes() are skipped.
  uint64_t tryDrainPartitions();

  /// Minimum partition bytes worth flushing during reclaim.
  int64_t reclaimDrainThresholdBytes() const {
    return reclaimDrainThresholdBytes_;
  }

  /// Returns the bytes that tryDrainPartitions() will actually flush.
  uint64_t reclaimableBufferedBytes() const;

  /// Signal that no more data will be enqueued. Drains remaining data
  /// and calls writer->noMoreData(true).
  void noMoreData();

  /// Abort — clears buffers and calls writer->noMoreData(false).
  void abort();

  State state() const {
    return state_;
  }

  int64_t bufferedBytes() const {
    return bufferedBytes_;
  }

  /// Maximum bytes buffered per partition before draining to the writer.
  int64_t partitionDrainThreshold() const {
    return partitionDrainThreshold_;
  }

  /// For testing: returns the current per-partition drain threshold.
  int64_t testingCurrentDrainThreshold() const {
    return partitionDrainThreshold();
  }

  /// Returns combined writer + buffer stats with typed units
  /// (kBytes, kNone). Only meaningful after close.
  folly::F14FastMap<std::string, velox::RuntimeMetric> stats() const;

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
    std::atomic_int64_t bufferedBytes_{0};
    int64_t drainThreshold_{0};
    // Per-partition safety net: set under the partition lock by
    // drainPartition(close=true). Guards against data loss if enqueue
    // races past the global state_ check.
    bool closed_{false};
    ShuffleWriter* writer_{nullptr};
    MaterializedOutputBuffer* buffer_{nullptr};
  };

  /// Initialize partition buffers and validate invariants.
  void initPartitionBuffers(int32_t numPartitions);

  /// Drain buffered data for a single partition. When force=false (default),
  /// uses try_lock and returns 0 if the partition mutex is contested. When
  /// force=true, uses blocking lock and marks the partition closed.
  int64_t drainPartition(int32_t partition, bool force = false);

  /// Drains all partitions and marks them closed.
  uint64_t close();

  /// Update drain stats and subtract from buffered bytes counter.
  void updateDrainStats(int64_t drainedBytes);

  // Coalesce data into a contiguous buffer and send to the ShuffleWriter.
  void flushToWriter(int32_t partition, std::unique_ptr<folly::IOBuf> data);

  // Merge a deque of RowGroup IOBufs into a single contiguous IOBuf.
  std::unique_ptr<folly::IOBuf> coalesceRowGroups(
      std::deque<std::unique_ptr<folly::IOBuf>>& rowGroups);

  // Free callback for pool-tracked IOBufs.
  static void freeTrackedIOBuf(void* buf, void* userData);

  // Immutable config.
  const std::string taskId_;
  const int32_t numPartitions_;
  const int64_t maxBufferedBytes_;
  const int64_t partitionDrainThreshold_;
  const int64_t reclaimDrainThresholdBytes_;

  // Pool created first so the writer can allocate from it.
  const std::shared_ptr<velox::memory::MemoryPool> pool_;
  const std::shared_ptr<ShuffleWriter> writer_;

  std::atomic<State> state_{State::kActive};

  // Per-partition buffers. Each PartitionBuffer has its own mutex that
  // serializes enqueue + drain for that partition.
  std::atomic_int64_t bufferedBytes_{0};
  std::vector<std::unique_ptr<PartitionBuffer>> partitionBuffers_;

  // Stats counters.
  std::atomic_int64_t drainedBytes_{0};
  std::atomic_int64_t drainCount_{0};
  std::atomic_int64_t peakBufferedBytes_{0};
  std::atomic_int64_t reclaimCount_{0};
  std::atomic_int64_t reclaimedBytes_{0};
  std::atomic_int64_t lastLoggedDrainedGB_{0};
  std::vector<std::atomic<int64_t>> collectCountPerPartition_;

  // Process-wide registry of buffers keyed by taskId, following the same
  // pattern as Velox OutputBufferManager. Buffer creation is done under
  // prestoTask->mutex in createOrUpdateTaskImpl() and registered here;
  // operators look up buffers by taskId at construction time.
  static folly::Synchronized<
      folly::F14FastMap<std::string, std::shared_ptr<MaterializedOutputBuffer>>>
      buffers_;
};

} // namespace facebook::presto::operators

template <>
struct fmt::formatter<
    facebook::presto::operators::MaterializedOutputBuffer::State>
    : formatter<std::string> {
  auto format(
      facebook::presto::operators::MaterializedOutputBuffer::State state,
      format_context& ctx) const {
    return formatter<std::string>::format(
        facebook::presto::operators::MaterializedOutputBuffer::stateName(state),
        ctx);
  }
};
