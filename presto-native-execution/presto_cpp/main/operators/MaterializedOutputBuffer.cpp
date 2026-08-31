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

#include "presto_cpp/main/operators/MaterializedOutputBuffer.h"

#include <fmt/format.h>
#include <folly/ExceptionString.h>
#include <folly/ScopeGuard.h>
#include <glog/logging.h>
#include <algorithm>
#include <chrono>
#include <cstring>
#include <memory>
#include <numeric>
#include <thread>

#include "presto_cpp/main/common/Configs.h"
#include "velox/common/Casts.h"
#include "velox/common/base/Exceptions.h"

#define MATERIALIZED_BUFFER_LOG(logEvent, pool, fmt_str, ...)           \
  LOG(INFO) << fmt::format(                                             \
      "MaterializedOutputBuffer::{} pool={} poolUsedBytes={} " fmt_str, \
      logEvent,                                                         \
      (pool)->name(),                                                   \
      velox::succinctBytes((pool)->usedBytes()),                        \
      ##__VA_ARGS__)

namespace facebook::presto::operators {

folly::Synchronized<
    folly::F14FastMap<std::string, std::shared_ptr<MaterializedOutputBuffer>>>
    MaterializedOutputBuffer::buffers_;

void MaterializedOutputBuffer::registerBuffer(
    const std::string& taskId,
    std::shared_ptr<MaterializedOutputBuffer> buffer) {
  buffers_.withWLock(
      [&](auto& buffers) { buffers.emplace(taskId, std::move(buffer)); });
}

std::shared_ptr<MaterializedOutputBuffer> MaterializedOutputBuffer::getBuffer(
    const std::string& taskId) {
  return buffers_.withRLock(
      [&](const auto& buffers) -> std::shared_ptr<MaterializedOutputBuffer> {
        auto it = buffers.find(taskId);
        return it != buffers.end() ? it->second : nullptr;
      });
}

void MaterializedOutputBuffer::removeBuffer(const std::string& taskId) {
  buffers_.withWLock([&](auto& buffers) { buffers.erase(taskId); });
}

std::string MaterializedOutputBuffer::stateName(State state) {
  switch (state) {
    case State::kActive:
      return "kActive";
    case State::kDraining:
      return "kDraining";
    case State::kClosed:
      return "kClosed";
    case State::kAborted:
      return "kAborted";
  }
  return fmt::format("Unknown({})", static_cast<int>(state));
}

// Stored alongside pool-allocated IOBufs so the free callback can return
// memory to the correct pool with the correct size.
struct TrackedBufInfo {
  velox::memory::MemoryPool* pool;
  size_t size;
};

namespace {
bool exchangeMaterializationUseZeroCopyCollect() {
  SystemConfig* config = SystemConfig::instance();
  VELOX_CHECK_NOT_NULL(config);
  return config->exchangeMaterializationUseZeroCopyCollect();
}
} // namespace

int64_t MaterializedOutputBuffer::PartitionBuffer::drainAvailable(
    std::deque<std::unique_ptr<folly::IOBuf>>& out) {
  int64_t drainedBytes = 0;
  std::unique_ptr<folly::IOBuf> item;
  while (rowGroupsQueue_.try_dequeue(item)) {
    drainedBytes += static_cast<int64_t>(item->computeChainDataLength());
    out.push_back(std::move(item));
  }
  return drainedBytes;
}

int64_t MaterializedOutputBuffer::PartitionBuffer::drainAndFlush() {
  // Drain all available RowGroups in bounded chunks to the writer.
  int64_t totalDrainedBytes = 0;
  std::deque<std::unique_ptr<folly::IOBuf>> chunks;
  int64_t chunkBytes = 0;
  const int64_t chunkThresholdBytes = buffer_->drainChunkThresholdBytes_;

  // Flush the current chunk and update its buffered-byte accounting.
  auto flushChunk = [&]() {
    buffer_->flushToWriter(partition_, chunks);
    bufferedBytes_ -= chunkBytes;
    totalDrainedBytes += chunkBytes;
    chunks.clear();
    chunkBytes = 0;
  };

  std::unique_ptr<folly::IOBuf> item;
  // Consume the queue without exceeding the per-collect size target.
  while (rowGroupsQueue_.try_dequeue(item)) {
    const int64_t itemBytes =
        static_cast<int64_t>(item->computeChainDataLength());
    // Roll over before exceeding the cap; send an oversized RowGroup alone.
    if (chunkBytes > 0 && chunkBytes + itemBytes > chunkThresholdBytes) {
      flushChunk();
    }
    chunks.push_back(std::move(item));
    chunkBytes += itemBytes;
  }
  if (!chunks.empty()) {
    flushChunk();
  }
  return totalDrainedBytes;
}

int64_t MaterializedOutputBuffer::PartitionBuffer::tryDrainPartition(
    int64_t targetBytes) {
  if (!tryAcquireFlushing()) {
    // Another driver is already draining and it will pick up our data.
    buffer_->concurrentAppendCount_ += 1;
    return 0;
  }
  buffer_->flushAcquireCount_ += 1;

  int64_t totalDrainedBytes = 0;
  do {
    {
      // Never leave the partition wedged if flushing throws.
      SCOPE_EXIT {
        releaseFlushing();
      };
      totalDrainedBytes += drainAndFlush();
    }
    // Release before rechecking so a racing appender can become the flusher.
  } while (bufferedBytes_ >= targetBytes && tryAcquireFlushing());
  return totalDrainedBytes;
}

int64_t MaterializedOutputBuffer::PartitionBuffer::noMoreData() {
  // Teardown is quiescent, so this acquire must be uncontended.
  VELOX_CHECK(tryAcquireFlushing(), "unexpected concurrent flush at teardown");
  SCOPE_EXIT {
    releaseFlushing();
  };
  if (closed_.exchange(true)) {
    return 0;
  }
  return drainAndFlush();
}

int64_t MaterializedOutputBuffer::PartitionBuffer::enqueue(
    int32_t partition,
    std::unique_ptr<folly::IOBuf> rowGroup) {
  VELOX_DCHECK_EQ(partition, partition_);
  VELOX_CHECK(!closed_, "enqueue called on closed partition");
  const int64_t rowGroupBytes =
      static_cast<int64_t>(rowGroup->computeChainDataLength());
  rowGroupsQueue_.enqueue(std::move(rowGroup));
  const int64_t newBytes = (bufferedBytes_ += rowGroupBytes);
  if (newBytes >= drainThreshold_) {
    return tryDrainPartition(drainThreshold_);
  }
  return 0;
}

void MaterializedOutputBuffer::initPartitionBuffers(int32_t numPartitions) {
  VELOX_CHECK_NOT_NULL(pool_, "MemoryPool must be non-null");
  VELOX_CHECK_NOT_NULL(writer_, "ShuffleWriter must be non-null");
  VELOX_CHECK_GT(numPartitions, 0, "Must have at least one partition");
  // Watermark ordering (guards against a bad ratio config):
  // 0 < low < high <= maxBufferedBytes.
  VELOX_CHECK_GT(
      lowWatermarkBytes_, 0, "backpressure low watermark must be positive");
  VELOX_CHECK_LT(
      lowWatermarkBytes_,
      highWatermarkBytes_,
      "backpressure low watermark must be below the high watermark");
  VELOX_CHECK_LE(
      highWatermarkBytes_,
      maxBufferedBytes_,
      "backpressure high watermark must not exceed the max buffer size");
  // One producer must be able to drain below the wake watermark; otherwise all
  // producers could park with no remaining waker.
  VELOX_CHECK_LT(
      numPartitions_ * reclaimDrainThresholdBytes_,
      lowWatermarkBytes_,
      "reclaim drain threshold too high vs the backpressure low watermark");
  partitionBuffers_.reserve(numPartitions);
  for (int32_t i = 0; i < numPartitions; ++i) {
    partitionBuffers_.push_back(
        std::make_unique<PartitionBuffer>(
            i, partitionDrainThreshold_, writer_.get(), this));
  }
  LOG(INFO) << fmt::format(
      "MaterializedOutputBuffer: partitions={}, maxBufferedBytes={}, "
      "drainThreshold={}, reclaimDrainThreshold={}, blockThreshold={}, "
      "unblockThreshold={}, pool={}",
      numPartitions_,
      velox::succinctBytes(maxBufferedBytes_),
      velox::succinctBytes(partitionDrainThreshold_),
      velox::succinctBytes(reclaimDrainThresholdBytes_),
      velox::succinctBytes(highWatermarkBytes_),
      velox::succinctBytes(lowWatermarkBytes_),
      pool_->name());
}

MaterializedOutputBuffer::MaterializedOutputBuffer(
    int32_t numPartitions,
    const std::string& shuffleWriterInfo,
    ShuffleInterfaceFactory* shuffleWriterFactory,
    const std::string& taskId,
    velox::memory::MemoryPool* pool)
    : taskId_(taskId),
      numPartitions_(numPartitions),
      maxBufferedBytes_(
          SystemConfig::instance()
              ->exchangeMaterializationOutputBufferMaxBytes()),
      partitionDrainThreshold_(
          std::min(
              SystemConfig::instance()
                  ->exchangeMaterializationOutputBufferPerPartitionMaxBytes(),
              maxBufferedBytes_ / numPartitions)),
      reclaimDrainThresholdBytes_(
          static_cast<int64_t>(
              partitionDrainThreshold_ *
              SystemConfig::instance()
                  ->exchangeMaterializationReclaimDrainThresholdRatio())),
      useZeroCopyCollect_(exchangeMaterializationUseZeroCopyCollect()),
      drainChunkThresholdBytes_(
          static_cast<int64_t>(
              partitionDrainThreshold_ *
              SystemConfig::instance()
                  ->exchangeMaterializationOutputBufferDrainChunkMultiplier())),
      highWatermarkBytes_(
          static_cast<int64_t>(
              maxBufferedBytes_ *
              SystemConfig::instance()
                  ->exchangeMaterializationOutputBufferHighWatermarkRatio())),
      lowWatermarkBytes_(
          static_cast<int64_t>(
              maxBufferedBytes_ *
              SystemConfig::instance()
                  ->exchangeMaterializationOutputBufferLowWatermarkRatio())),
      pool_(pool->addLeafChild(
          fmt::format("materialized_output_buffer.{}", taskId),
          true,
          std::make_unique<Reclaimer>(this))),
      writer_(
          shuffleWriterFactory->createWriter(shuffleWriterInfo, pool_.get())),
      collectCountPerPartition_(numPartitions) {
  initPartitionBuffers(numPartitions);
}

MaterializedOutputBuffer::~MaterializedOutputBuffer() {
  removeBuffer(taskId_);
  if (state_ != State::kClosed && state_ != State::kAborted) {
    LOG(WARNING) << "MaterializedOutputBuffer destroyed without calling "
                 << "noMoreData() or abort(). Aborting writer.";
    try {
      abort();
    } catch (...) {
      LOG(ERROR) << "MaterializedOutputBuffer abort failed in destructor";
    }
  }
  // MemoryPool teardown requires the drain-headroom reservation to be released.
  pool_->release();
}

std::unique_ptr<folly::IOBuf> MaterializedOutputBuffer::allocateTrackedIOBuf(
    size_t size) {
  void* buf = pool_->allocate(size);
  auto* info = new TrackedBufInfo{pool_.get(), size};
  auto iobuf = folly::IOBuf::takeOwnership(buf, size, freeTrackedIOBuf, info);
  // takeOwnership sets length=size. Reset to 0 so callers can use
  // writableData()/append() like IOBuf::create().
  iobuf->trimEnd(size);
  return iobuf;
}

// Free callback for pool-tracked IOBufs.
void MaterializedOutputBuffer::freeTrackedIOBuf(void* buf, void* userData) {
  auto* info = static_cast<TrackedBufInfo*>(userData);
  info->pool->free(buf, info->size);
  delete info;
}

void MaterializedOutputBuffer::enqueue(
    int32_t partition,
    std::unique_ptr<folly::IOBuf> rowGroup) {
  VELOX_CHECK_GE(partition, 0);
  VELOX_CHECK_LT(partition, numPartitions_);
  if (state_ == State::kAborted) {
    return;
  }
  VELOX_CHECK_EQ(state_, State::kActive, "enqueue called after noMoreData()");

  auto rowGroupBytes = static_cast<int64_t>(rowGroup->computeChainDataLength());
  auto currentBytes = (bufferedBytes_ += rowGroupBytes);
  int64_t peak = peakBufferedBytes_;
  while (currentBytes > peak &&
         !peakBufferedBytes_.compare_exchange_weak(peak, currentBytes)) {
  }

  auto drainedBytes =
      partitionBuffers_[partition]->enqueue(partition, std::move(rowGroup));

  if (drainedBytes > 0) {
    updateDrainStats(drainedBytes);
    auto currentGB = static_cast<int64_t>(drainedBytes_) >> 30;
    int64_t lastGB = lastLoggedDrainedGB_;
    if (currentGB > lastGB &&
        lastLoggedDrainedGB_.compare_exchange_strong(lastGB, currentGB)) {
      LOG(INFO) << fmt::format(
          "MaterializedOutputBuffer progress: drainedBytes={}, "
          "drainCount={}, bufferedBytes={}",
          velox::succinctBytes(drainedBytes_),
          static_cast<int64_t>(drainCount_),
          velox::succinctBytes(bufferedBytes_));
    }
    maybeWakeBlockedDrivers();
  }
}

void MaterializedOutputBuffer::addBlockedPromise(
    velox::ContinueFuture* future) {
  velox::ContinuePromise promise{"MaterializedOutputBuffer::addBlockedPromise"};
  *future = promise.getSemiFuture();
  backpressureBlockCount_ += 1;
  blockedPromises_.withWLock(
      [&](auto& promises) { promises.push_back(std::move(promise)); });
  // Avoid missing a concurrent low-watermark crossing during registration.
  maybeWakeBlockedDrivers();
}

void MaterializedOutputBuffer::tryDrainPartitions() {
  while (bufferedBytes_ >= lowWatermarkBytes_) {
    // Flush the fullest partitions this thread can acquire.
    const uint64_t drainedBytes = tryDrainPartitionsInternal();
    backpressureDrainedBytes_ += static_cast<int64_t>(drainedBytes);
    if (drainedBytes == 0) {
      // Nothing left this thread can drain (others are flushing the rest).
      break;
    }
  }
}

velox::exec::BlockingReason MaterializedOutputBuffer::isBlocked(
    velox::ContinueFuture* future) {
  if (!isBufferFull()) {
    return velox::exec::BlockingReason::kNotBlocked;
  }

  // Drain on the producer before parking.
  tryDrainPartitions();

  // Park until a drain crosses the low watermark.
  if (isBufferFull() && reclaimableBufferedBytes() > 0) {
    addBlockedPromise(future);
    return velox::exec::BlockingReason::kWaitForConsumer;
  }

  // With nothing reclaimable, parking would have no waker.
  if (isBufferFull()) {
    LOG_EVERY_N(WARNING, 256)
        << "MaterializedOutputBuffer: buffer full but nothing reclaimable; not "
           "parking (bufferedBytes="
        << bufferedBytes() << ")";
  }
  return velox::exec::BlockingReason::kNotBlocked;
}

void MaterializedOutputBuffer::ensureDrainMemoryHeadroom() {
  // Reserve one drain's coalesce and compression allocations before entering
  // the non-reclaimable flush window. Failure falls back to on-demand growth.
  const int64_t headroom = 2 * partitionDrainThreshold_;
  if (pool_->availableReservation() >= headroom) {
    return;
  }
  pool_->maybeReserve(headroom);
}

void MaterializedOutputBuffer::maybeWakeBlockedDrivers() {
  if (bufferedBytes_ >= lowWatermarkBytes_) {
    return;
  }
  std::vector<velox::ContinuePromise> toFulfill;
  blockedPromises_.withWLock([&](auto& promises) { toFulfill.swap(promises); });
  if (!toFulfill.empty()) {
    backpressureWakeCount_ += static_cast<int64_t>(toFulfill.size());
  }
  for (auto& promise : toFulfill) {
    promise.setValue();
  }
}

namespace {
// Link a deque of RowGroup IOBufs into a single chain without copying.
std::unique_ptr<folly::IOBuf> chainRowGroups(
    std::deque<std::unique_ptr<folly::IOBuf>>& rowGroups) noexcept {
  std::unique_ptr<folly::IOBuf> head;
  for (auto& rowGroup : rowGroups) {
    if (head == nullptr) {
      head = std::move(rowGroup);
    } else {
      head->appendToChain(std::move(rowGroup));
    }
  }
  return head;
}
} // namespace

void MaterializedOutputBuffer::flushToWriter(
    int32_t partition,
    std::deque<std::unique_ptr<folly::IOBuf>>& rowGroups) {
  auto* writer = velox::checkedNotNull(writer_.get());
  if (useZeroCopyCollect_) {
    std::unique_ptr<folly::IOBuf> chain = chainRowGroups(rowGroups);
    VELOX_CHECK_NOT_NULL(chain);
    writer->collect(partition, /*key=*/"", std::move(chain));
    ++zeroCopyCollectCount_;
  } else {
    std::unique_ptr<folly::IOBuf> coalesced = coalesceRowGroups(rowGroups);
    auto* coalescedBuffer = velox::checkedNotNull(coalesced.get());
    const size_t dataSize = coalescedBuffer->computeChainDataLength();
    if (dataSize == 0) {
      return;
    }
    writer->collect(
        partition,
        /*key=*/"",
        std::string_view(
            reinterpret_cast<const char*>(coalescedBuffer->data()), dataSize));
  }
  ++collectCountPerPartition_[partition];
}

void MaterializedOutputBuffer::updateDrainStats(int64_t drainedBytes) {
  ++drainCount_;
  drainedBytes_ += drainedBytes;
  bufferedBytes_.fetch_sub(drainedBytes);
}

std::unique_ptr<folly::IOBuf> MaterializedOutputBuffer::coalesceRowGroups(
    std::deque<std::unique_ptr<folly::IOBuf>>& rowGroups) {
  size_t totalBytes = 0;
  for (const auto& rowGroup : rowGroups) {
    totalBytes += rowGroup->computeChainDataLength();
  }
  auto coalesced = allocateTrackedIOBuf(totalBytes);
  for (auto& rowGroup : rowGroups) {
    for (const auto& range : *rowGroup) {
      std::memcpy(coalesced->writableTail(), range.data(), range.size());
      coalesced->append(range.size());
    }
  }
  return coalesced;
}

int64_t MaterializedOutputBuffer::drainPartition(
    int32_t partition,
    bool force) {
  VELOX_CHECK_GE(partition, 0);
  VELOX_CHECK_LT(partition, numPartitions_);

  auto& partitionBuffer = *partitionBuffers_[partition];
  int64_t drainedBytes;
  if (force) {
    drainedBytes = partitionBuffer.noMoreData();
  } else {
    drainedBytes =
        partitionBuffer.tryDrainPartition(reclaimDrainThresholdBytes_);
  }

  if (drainedBytes > 0) {
    updateDrainStats(drainedBytes);
    maybeWakeBlockedDrivers();
  }

  return drainedBytes;
}

uint64_t MaterializedOutputBuffer::reclaimableBufferedBytes() const {
  uint64_t reclaimableBytes = 0;
  for (int32_t i = 0; i < numPartitions_; ++i) {
    auto partBytes = partitionBuffers_[i]->bufferedBytes_.load();
    if (partBytes > reclaimDrainThresholdBytes_) {
      reclaimableBytes += partBytes - reclaimDrainThresholdBytes_;
    }
  }
  return reclaimableBytes;
}

uint64_t MaterializedOutputBuffer::tryDrainPartitionsInternal() {
  std::vector<int32_t> orderedPartitions(numPartitions_);
  std::iota(orderedPartitions.begin(), orderedPartitions.end(), 0);

  std::vector<int64_t> sizes(numPartitions_);
  for (int32_t i = 0; i < numPartitions_; ++i) {
    sizes[i] = partitionBuffers_[i]->bufferedBytes_;
  }
  std::sort(
      orderedPartitions.begin(),
      orderedPartitions.end(),
      [&](int32_t lhs, int32_t rhs) { return sizes[lhs] > sizes[rhs]; });

  uint64_t drainedBytes = 0;
  for (auto partition : orderedPartitions) {
    if (sizes[partition] < reclaimDrainThresholdBytes_) {
      break;
    }
    drainedBytes += drainPartition(partition, /*force=*/false);
  }
  return drainedBytes;
}

uint64_t MaterializedOutputBuffer::close() {
  uint64_t drainedBytes = 0;
  for (int32_t i = 0; i < numPartitions_; ++i) {
    drainedBytes += drainPartition(i, /*force=*/true);
  }
  return drainedBytes;
}

void MaterializedOutputBuffer::noMoreData() {
  auto expected = State::kActive;
  if (!state_.compare_exchange_strong(expected, State::kDraining)) {
    return;
  }
  auto releaseGuard = folly::makeGuard([this]() { pool_->release(); });
  LOG(INFO) << fmt::format(
      "MaterializedOutputBuffer noMoreData: draining, bufferedBytes={}",
      velox::succinctBytes(bufferedBytes_));
  close();
  LOG(INFO) << "MaterializedOutputBuffer: calling writer noMoreData(true)";
  try {
    writer_->noMoreData(/*success=*/true);
  } catch (const std::exception& e) {
    LOG(ERROR) << "MaterializedOutputBuffer: writer noMoreData(true) failed: "
               << folly::exceptionStr(e);
    state_ = State::kAborted;
    throw;
  }
  int64_t totalCollects = 0;
  for (int32_t i = 0; i < numPartitions_; ++i) {
    totalCollects += collectCountPerPartition_[i];
  }
  LOG(INFO) << fmt::format(
      "MaterializedOutputBuffer progress: drainedBytes={}, "
      "drainCount={}, bufferedBytes={}",
      velox::succinctBytes(drainedBytes_),
      static_cast<int64_t>(drainCount_),
      velox::succinctBytes(bufferedBytes_));
  LOG(INFO) << fmt::format(
      "MaterializedOutputBuffer closed: "
      "collectCalls={}, peakBufferedBytes={}",
      totalCollects,
      velox::succinctBytes(peakBufferedBytes_));
  state_ = State::kClosed;
}

void MaterializedOutputBuffer::abort() {
  auto expected = State::kActive;
  if (!state_.compare_exchange_strong(expected, State::kAborted)) {
    return;
  }
  auto releaseGuard = folly::makeGuard([this]() { pool_->release(); });

  // Unpark any producers blocked on backpressure so they observe kAborted.
  maybeWakeBlockedDrivers();

  LOG(INFO)
      << "MaterializedOutputBuffer: calling writer noMoreData(false) [abort]";
  writer_->noMoreData(/*success=*/false);
}

folly::F14FastMap<std::string, velox::RuntimeMetric>
MaterializedOutputBuffer::stats() const {
  using Unit = velox::RuntimeCounter::Unit;
  folly::F14FastMap<std::string, velox::RuntimeMetric> result;

  // Writer stats (unknown units from the ShuffleWriter interface).
  for (const auto& [key, value] : writer_->stats()) {
    result[key] = velox::RuntimeMetric(value);
  }

  // Buffer stats with typed units.
  int64_t totalCollects = 0;
  for (int32_t i = 0; i < numPartitions_; ++i) {
    totalCollects += collectCountPerPartition_[i];
  }
  result[std::string(kDrainedBytes)] =
      velox::RuntimeMetric(drainedBytes_, Unit::kBytes);
  result[std::string(kDrainCount)] = velox::RuntimeMetric(drainCount_);
  result[std::string(kCurrentDrainThreshold)] =
      velox::RuntimeMetric(partitionDrainThreshold_, Unit::kBytes);
  result[std::string(kBufferPoolUsedBytes)] =
      velox::RuntimeMetric(pool_ ? pool_->usedBytes() : 0, Unit::kBytes);
  result[std::string(kBufferPoolPeakBytes)] =
      velox::RuntimeMetric(pool_ ? pool_->peakBytes() : 0, Unit::kBytes);
  result[std::string(kTotalCollectCalls)] = velox::RuntimeMetric(totalCollects);
  result[std::string(kPeakBufferedBytes)] =
      velox::RuntimeMetric(peakBufferedBytes_, Unit::kBytes);
  result[std::string(kReclaimCount)] = velox::RuntimeMetric(reclaimCount_);
  result[std::string(kReclaimedBytes)] =
      velox::RuntimeMetric(reclaimedBytes_, Unit::kBytes);
  result[std::string(kZeroCopyCollectCount)] =
      velox::RuntimeMetric(zeroCopyCollectCount_);
  result[std::string(kConcurrentAppendCount)] =
      velox::RuntimeMetric(concurrentAppendCount_);
  result[std::string(kFlushAcquireCount)] =
      velox::RuntimeMetric(flushAcquireCount_);
  result[std::string(kBackpressureBlockCount)] =
      velox::RuntimeMetric(backpressureBlockCount_);
  result[std::string(kBackpressureWakeCount)] =
      velox::RuntimeMetric(backpressureWakeCount_);
  result[std::string(kBackpressureDrainedBytes)] =
      velox::RuntimeMetric(backpressureDrainedBytes_, Unit::kBytes);
  return result;
}

MaterializedOutputBuffer::Reclaimer::Reclaimer(
    MaterializedOutputBuffer* partitionBuffer)
    : MemoryReclaimer(
          SystemConfig::instance()->exchangeMaterializationReclaimHighPriority()
              ? kHighReclaimPriority
              : 0),
      partitionBuffer_(partitionBuffer) {
  VELOX_CHECK_NOT_NULL(partitionBuffer_, "Reclaimer requires a buffer");
}

bool MaterializedOutputBuffer::Reclaimer::reclaimableBytes(
    const velox::memory::MemoryPool& /*pool*/,
    uint64_t& reclaimableBytes) const {
  reclaimableBytes = partitionBuffer_->reclaimableBufferedBytes();
  return reclaimableBytes > 0;
}

void MaterializedOutputBuffer::Reclaimer::tryReclaimPartitionBuffers(
    velox::memory::MemoryPool* pool) {
  auto flushedBytes = partitionBuffer_->tryDrainPartitionsInternal();
  MATERIALIZED_BUFFER_LOG(
      "FLUSH", pool, "flushedBytes={}", velox::succinctBytes(flushedBytes));
}

void MaterializedOutputBuffer::Reclaimer::waitForWriterDrain(
    velox::memory::MemoryPool* pool,
    uint64_t targetUsedBytes,
    std::chrono::steady_clock::time_point deadline) {
  while (pool->usedBytes() > targetUsedBytes) {
    if (std::chrono::steady_clock::now() >= deadline) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  MATERIALIZED_BUFFER_LOG(
      "WAIT",
      pool,
      "targetUsedBytes={} timedOut={}",
      velox::succinctBytes(targetUsedBytes),
      pool->usedBytes() > targetUsedBytes);
}

void MaterializedOutputBuffer::Reclaimer::recordStats(
    uint64_t freedBytes,
    Stats& stats) {
  stats.reclaimedBytes += freedBytes;
  ++partitionBuffer_->reclaimCount_;
  partitionBuffer_->reclaimedBytes_ += freedBytes;
}

bool MaterializedOutputBuffer::Reclaimer::canReclaim(
    const velox::memory::MemoryPool& pool,
    uint64_t targetBytes) const {
  if (targetBytes == 0 || pool.usedBytes() == 0) {
    return false;
  }
  const auto state = partitionBuffer_->state();
  return state == State::kActive || state == State::kDraining;
}

bool MaterializedOutputBuffer::Reclaimer::canReclaimFromPartitionBuffers()
    const {
  // Only flush partition buffers in kActive state. During kDraining,
  // noMoreData() is already draining them — we skip to waiting for
  // writer network drain. The bufferedBytes check avoids a no-op flush
  // when all data has already been drained by enqueue threshold logic.
  return partitionBuffer_->state() == State::kActive &&
      partitionBuffer_->bufferedBytes() > 0;
}

uint64_t MaterializedOutputBuffer::Reclaimer::reclaim(
    velox::memory::MemoryPool* pool,
    uint64_t targetBytes,
    uint64_t maxWaitMs,
    Stats& stats) {
  if (!canReclaim(*pool, targetBytes)) {
    return 0;
  }

  auto prevUsedBytes = pool->usedBytes();
  auto targetUsedBytes =
      prevUsedBytes > targetBytes ? prevUsedBytes - targetBytes : 0;
  auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(maxWaitMs);

  MATERIALIZED_BUFFER_LOG(
      "START",
      pool,
      "targetBytes={} maxWaitMs={}",
      velox::succinctBytes(targetBytes),
      maxWaitMs);

  // Flush partition buffers to the writer.
  if (canReclaimFromPartitionBuffers()) {
    tryReclaimPartitionBuffers(pool);
    if (pool->usedBytes() <= targetUsedBytes) {
      auto totalFreedBytes = prevUsedBytes - pool->usedBytes();
      recordStats(totalFreedBytes, stats);
      return totalFreedBytes;
    }
  }

  // Optionally wait for the writer to drain packages to the network.
  if (SystemConfig::instance()
          ->exchangeMaterializationReclaimWaitForWriterDrainEnabled()) {
    waitForWriterDrain(pool, targetUsedBytes, deadline);
  }

  auto totalFreedBytes =
      prevUsedBytes > pool->usedBytes() ? prevUsedBytes - pool->usedBytes() : 0;
  recordStats(totalFreedBytes, stats);
  return totalFreedBytes;
}

} // namespace facebook::presto::operators
