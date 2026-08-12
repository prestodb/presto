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
#include <folly/concurrency/UnboundedQueue.h>
#include <folly/container/F14Map.h>
#include <folly/io/IOBuf.h>
#include "presto_cpp/main/operators/ShuffleInterface.h"
#include "velox/common/base/RuntimeMetrics.h"
#include "velox/common/future/VeloxPromise.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/exec/Driver.h"
#include "velox/exec/MemoryReclaimer.h"

namespace facebook::presto::operators {

/// Shared MaterializedOutput buffer with lock-free per-partition appends.
/// One CAS-elected flusher preserves writer order per partition. Producers
/// park near 90% capacity and wake below 70%; arbitration is the backstop.
/// Concurrent appends may interleave, so this supports non-sort shuffle only.
///
/// Lifecycle state machine:
///   kActive -> kDraining -> kClosed  (noMoreData: success)
///   kActive -> kDraining -> kAborted (noMoreData: writer failure)
///   kActive -> kAborted              (abort: error teardown)
/// Partition reclaim runs only in kActive. Writer-drain waiting runs in
/// kActive or kDraining; kClosed and kAborted are not reclaimable.
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
  // Lock-free append / backpressure effectiveness counters.
  static constexpr std::string_view kConcurrentAppendCount =
      "materializedOutputBuffer.concurrentAppendCount";
  static constexpr std::string_view kFlushAcquireCount =
      "materializedOutputBuffer.flushAcquireCount";
  static constexpr std::string_view kBackpressureBlockCount =
      "materializedOutputBuffer.backpressureBlockCount";
  static constexpr std::string_view kBackpressureWakeCount =
      "materializedOutputBuffer.backpressureWakeCount";
  static constexpr std::string_view kBackpressureDrainedBytes =
      "materializedOutputBuffer.backpressureDrainedBytes";

  /// Reclaims partition buffers, then waits for writer network drain. The
  /// nested lifetime keeps the raw back-pointer valid. Priority -1 runs before
  /// ordinary operator reclaimers.
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

    /// Partition flush runs only while active with buffered data.
    bool canReclaimFromPartitionBuffers() const;

    /// Flushes eligible partitions largest-first via the CAS flusher gate.
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

  /// Drains at the high watermark, then parks until below the low watermark.
  velox::exec::BlockingReason isBlocked(velox::ContinueFuture* future);

  /// Best-effort reservation for coalesce and compression during one drain.
  void ensureDrainMemoryHeadroom();

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
  // Appenders enqueue without locking. One appender, reclaimer, or closer
  // CAS-acquires 'flushing_' and calls collect(); losers keep appending. This
  // single-flusher gate preserves per-partition writer order and checksums.
  class PartitionBuffer {
   public:
    PartitionBuffer() = default;

    PartitionBuffer(
        int32_t partition,
        int64_t drainThreshold,
        ShuffleWriter* writer,
        MaterializedOutputBuffer* buffer)
        : partition_(partition),
          drainThreshold_(drainThreshold),
          writer_(writer),
          buffer_(buffer) {}

    /// Lock-free append; if over 'drainThreshold_', tries to become flusher and
    /// drain. Returns bytes drained (0 if not the flusher).
    int64_t enqueue(int32_t partition, std::unique_ptr<folly::IOBuf> rowGroup);

   private:
    friend class MaterializedOutputBuffer;

    /// A successful sole-flusher CAS must be paired with releaseFlushing().
    bool tryAcquireFlushing() {
      bool expected = false;
      return flushing_.compare_exchange_strong(expected, true);
    }

    /// Release the flusher flag (seq_cst store). Only the current flusher calls
    /// this.
    void releaseFlushing() {
      flushing_ = false;
    }

    /// Pop all currently-available RowGroups into 'out' (caller must hold
    /// 'flushing_'). Returns total bytes popped.
    int64_t drainAvailable(std::deque<std::unique_ptr<folly::IOBuf>>& out);

    /// Flushes bounded chunks; an oversized RowGroup is sent alone. Caller
    /// holds 'flushing_'. Returns total bytes flushed.
    int64_t drainAndFlush();

    /// Release, recheck, and reacquire until below target so a concurrent
    /// append cannot leave an over-threshold partition without a flusher.
    int64_t tryDrainPartition(int64_t targetBytes);

    /// Closes once and flushes all remaining data. Teardown is quiescent, but
    /// still uses the flusher gate to preserve writer ordering.
    int64_t noMoreData();

    folly::UMPMCQueue<std::unique_ptr<folly::IOBuf>, /*MayBlock=*/false>
        rowGroupsQueue_;
    std::atomic_int64_t bufferedBytes_{0};
    std::atomic<bool> flushing_{false};
    // Set true by close()/abort(); rejects further appends.
    std::atomic<bool> closed_{false};
    // This partition's index in the parent's partitionBuffers_; passed to the
    // writer on flush.
    const int32_t partition_{-1};
    int64_t drainThreshold_{0};
    ShuffleWriter* writer_{nullptr};
    MaterializedOutputBuffer* buffer_{nullptr};
  };

  /// Initialize partition buffers and validate invariants.
  void initPartitionBuffers(int32_t numPartitions);

  /// Drains buffered data for one partition. When force is false, tries the
  /// single-flusher CAS gate and returns 0 if another flusher owns it. When
  /// force is true, closes the partition and fully drains it; teardown must be
  /// quiescent, so the gate is expected to be uncontended.
  int64_t drainPartition(int32_t partition, bool force = false);

  /// Best-effort largest-first pass over reclaimable, uncontended partitions.
  uint64_t tryDrainPartitionsInternal();

  /// True when total buffered bytes are at/above the configured high watermark.
  bool isBufferFull() const {
    return bufferedBytes_ >= highWatermarkBytes_;
  }

  /// Loop tryDrainPartitionsInternal() until buffered drops below the low
  /// watermark (or this thread can drain no more).
  void tryDrainPartitions();

  /// Bytes above the per-partition reclaim thresholds — what a drain can free.
  uint64_t reclaimableBufferedBytes() const;

  /// Register a producer's ContinueFuture, woken once buffered falls below the
  /// low watermark. Moves a fresh future into '*future'.
  void addBlockedPromise(velox::ContinueFuture* future);

  /// Drains all partitions and marks them closed.
  uint64_t close();

  /// Update drain stats and subtract from buffered bytes counter.
  void updateDrainStats(int64_t drainedBytes);

  /// Coalesce data into a contiguous buffer and send to the ShuffleWriter.
  void flushToWriter(int32_t partition, std::unique_ptr<folly::IOBuf> data);

  /// Wake parked producers after crossing the low watermark.
  void maybeWakeBlockedDrivers();

  /// Merge a deque of RowGroup IOBufs into a single contiguous IOBuf.
  std::unique_ptr<folly::IOBuf> coalesceRowGroups(
      std::deque<std::unique_ptr<folly::IOBuf>>& rowGroups);

  /// Free callback for pool-tracked IOBufs.
  static void freeTrackedIOBuf(void* buf, void* userData);

  // Immutable config.
  const std::string taskId_;
  const int32_t numPartitions_;
  const int64_t maxBufferedBytes_;
  const int64_t partitionDrainThreshold_;
  const int64_t reclaimDrainThresholdBytes_;
  // Per-collect cap for lock-free drain overshoot.
  const int64_t drainChunkThresholdBytes_;
  // Global backpressure high/low watermarks: block producers at ~90% of the
  // cap, wake them once total buffered drops below ~70% (hysteresis).
  const int64_t highWatermarkBytes_;
  const int64_t lowWatermarkBytes_;

  // Pool created first so the writer can allocate from it.
  const std::shared_ptr<velox::memory::MemoryPool> pool_;
  const std::shared_ptr<ShuffleWriter> writer_;

  std::atomic<State> state_{State::kActive};

  // Buffer-wide total across partitions; drives the backpressure watermarks.
  std::atomic_int64_t bufferedBytes_{0};
  // Per-partition buffers. Each PartitionBuffer has a lock-free queue and a
  // single-flusher CAS gate that serializes drains for that partition.
  std::vector<std::unique_ptr<PartitionBuffer>> partitionBuffers_;

  // Stats counters.
  std::atomic_int64_t drainedBytes_{0};
  std::atomic_int64_t drainCount_{0};
  std::atomic_int64_t peakBufferedBytes_{0};
  std::atomic_int64_t reclaimCount_{0};
  std::atomic_int64_t reclaimedBytes_{0};

  // Effectiveness counters.
  std::atomic_int64_t concurrentAppendCount_{0};
  std::atomic_int64_t flushAcquireCount_{0};
  std::atomic_int64_t backpressureBlockCount_{0};
  std::atomic_int64_t backpressureWakeCount_{0};
  std::atomic_int64_t backpressureDrainedBytes_{0};

  std::atomic_int64_t lastLoggedDrainedGB_{0};
  std::vector<std::atomic<int64_t>> collectCountPerPartition_;

  // Producer promises parked by backpressure, fulfilled by
  // maybeWakeBlockedDrivers() once buffered drops below the low watermark.
  folly::Synchronized<std::vector<velox::ContinuePromise>> blockedPromises_;

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
