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

#include "presto_cpp/main/operators/ExchangeOutputBuffer.h"

#include <glog/logging.h>
#include <algorithm>
#include <cstring>

namespace facebook::presto::operators {

// Stored alongside pool-allocated IOBufs so the free callback can return
// memory to the correct pool with the correct size.
struct TrackedBufInfo {
  velox::memory::MemoryPool* pool;
  size_t size;
};

ExchangeOutputBuffer::ExchangeOutputBuffer(
    int32_t numPartitions,
    std::shared_ptr<ShuffleWriter> writer,
    std::shared_ptr<velox::memory::MemoryPool> bufferPool,
    int64_t maxBufferedBytes,
    int64_t partitionDrainThreshold)
    : numPartitions_(numPartitions),
      maxBufferedBytes_(maxBufferedBytes),
      continueBufferedBytes_(maxBufferedBytes * 9 / 10),
      partitionDrainThreshold_(
          partitionDrainThreshold > 0 ? partitionDrainThreshold
                                      : kDefaultDrainThreshold),
      writer_(std::move(writer)),
      bufferPool_(std::move(bufferPool)),
      partitionQueues_(numPartitions),
      collectCountPerPartition_(numPartitions) {
  VELOX_CHECK_GT(numPartitions, 0, "Must have at least one partition");
  VELOX_CHECK_NOT_NULL(writer_, "ShuffleWriter must be non-null");
}

ExchangeOutputBuffer::~ExchangeOutputBuffer() {
  if (!finished_.load()) {
    LOG(WARNING) << "ExchangeOutputBuffer destroyed without calling "
                 << "noMoreData() or abort(). Aborting writer.";
    try {
      abort();
    } catch (...) {
      LOG(ERROR) << "ExchangeOutputBuffer abort failed in destructor";
    }
  }
}

std::unique_ptr<folly::IOBuf> ExchangeOutputBuffer::allocateTrackedIOBuf(
    size_t size) {
  void* buf = bufferPool_->allocate(size);
  auto* info = new TrackedBufInfo{bufferPool_.get(), size};
  auto iobuf = folly::IOBuf::takeOwnership(buf, size, freeTrackedIOBuf, info);
  // takeOwnership sets length=size. Reset to 0 so callers can use
  // writableData()/append() like IOBuf::create().
  iobuf->trimEnd(size);
  return iobuf;
}

// Free callback for pool-tracked IOBufs.
void ExchangeOutputBuffer::freeTrackedIOBuf(void* buf, void* userData) {
  auto* info = static_cast<TrackedBufInfo*>(userData);
  info->pool->free(buf, info->size);
  delete info;
}

// Check if total buffered bytes exceeds the threshold and set up a
// ContinueFuture for the caller to block on.
bool ExchangeOutputBuffer::maybeApplyBackpressure(
    velox::ContinueFuture* future) {
  if (bufferedBytes_ >= maxBufferedBytes_) {
    std::lock_guard<std::mutex> lock(stateMutex_);
    if (bufferedBytes_ >= maxBufferedBytes_) {
      auto [promise, semiFuture] =
          velox::makeVeloxContinuePromiseContract("ExchangeOutputBuffer");
      promises_.push_back(std::move(promise));
      *future = std::move(semiFuture);
      ++backpressureCount_;
      return true;
    }
  }
  return false;
}

bool ExchangeOutputBuffer::enqueue(
    int32_t partition,
    std::unique_ptr<folly::IOBuf> rowGroup,
    velox::ContinueFuture* future) {
  VELOX_CHECK_GE(partition, 0);
  VELOX_CHECK_LT(partition, numPartitions_);
  VELOX_CHECK(!finished_.load());

  auto rowGroupBytes = static_cast<int64_t>(rowGroup->computeChainDataLength());

  {
    auto& pq = partitionQueues_[partition];
    std::lock_guard<std::mutex> lock(pq.mutex);
    pq.bufferedBytes += rowGroupBytes;
    pq.rowGroups.push_back(std::move(rowGroup));
  }

  bufferedBytes_ += rowGroupBytes;

  if (partitionQueues_[partition].bufferedBytes >= partitionDrainThreshold_) {
    drainPartition(partition);
  }

  return maybeApplyBackpressure(future);
}

// Coalesce data into a contiguous buffer and send to the ShuffleWriter.
void ExchangeOutputBuffer::flushToWriter(
    int32_t partition,
    std::unique_ptr<folly::IOBuf> data) {
  auto dataSize = data->computeChainDataLength();
  if (dataSize == 0) {
    return;
  }
  std::string_view view(reinterpret_cast<const char*>(data->data()), dataSize);
  writer_->collect(partition, /*key=*/"", view);
  ++collectCountPerPartition_[partition];
}

// Merge a deque of RowGroup IOBufs into a single contiguous IOBuf.
std::unique_ptr<folly::IOBuf> ExchangeOutputBuffer::coalesceRowGroups(
    std::deque<std::unique_ptr<folly::IOBuf>>& rowGroups) {
  size_t totalBytes = 0;
  for (const auto& rg : rowGroups) {
    totalBytes += rg->computeChainDataLength();
  }
  auto coalesced = allocateTrackedIOBuf(totalBytes);
  for (auto& rg : rowGroups) {
    for (const auto& range : *rg) {
      std::memcpy(coalesced->writableTail(), range.data(), range.size());
      coalesced->append(range.size());
    }
  }
  return coalesced;
}

// Drain all buffered pages for a partition: coalesce into one IOBuf and
// flush to the writer. Holds the partition lock through the entire sequence.
int64_t ExchangeOutputBuffer::drainPartition(int32_t partition) {
  VELOX_CHECK_GE(partition, 0);
  VELOX_CHECK_LT(partition, numPartitions_);

  auto& pq = partitionQueues_[partition];

  // Hold the partition lock through the entire drain + writeToShuffle
  // sequence. Without this, concurrent enqueue-triggered drains call
  // writeToShuffle -> CoscoWriter::collect() concurrently for the same
  // partition, corrupting per-partition state (records_, collector buffer).
  std::lock_guard<std::mutex> lock(pq.mutex);

  std::deque<std::unique_ptr<folly::IOBuf>> rowGroupsToDrain;
  rowGroupsToDrain.swap(pq.rowGroups);
  int64_t drainedBytes = pq.bufferedBytes.exchange(0);

  if (!rowGroupsToDrain.empty()) {
    auto coalesced = coalesceRowGroups(rowGroupsToDrain);
    flushToWriter(partition, std::move(coalesced));
    ++drainCount_;
    totalDrainedBytes_ += drainedBytes;
  }

  auto prevTotal = bufferedBytes_.fetch_sub(drainedBytes);

  if (prevTotal >= continueBufferedBytes_ &&
      (prevTotal - drainedBytes) < continueBufferedBytes_) {
    std::vector<velox::ContinuePromise> promises;
    {
      std::lock_guard<std::mutex> stateLock(stateMutex_);
      promises.swap(promises_);
    }
    maybeUnblockProducers(promises);
  }

  return drainedBytes;
}

uint64_t ExchangeOutputBuffer::drainAll() {
  uint64_t totalDrained = 0;
  for (int32_t i = 0; i < numPartitions_; ++i) {
    totalDrained += drainPartition(i);
  }
  return totalDrained;
}

void ExchangeOutputBuffer::noMoreData() {
  finishAndClose();
}

void ExchangeOutputBuffer::abort() {
  if (finished_.exchange(true)) {
    return;
  }

  for (int32_t i = 0; i < numPartitions_; ++i) {
    auto& pq = partitionQueues_[i];
    std::lock_guard<std::mutex> lock(pq.mutex);
    auto freedBytes = pq.bufferedBytes.exchange(0);
    pq.rowGroups.clear();
    bufferedBytes_ -= freedBytes;
  }

  // Unblock any waiting producers.
  std::vector<velox::ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> lock(stateMutex_);
    promises.swap(promises_);
  }
  maybeUnblockProducers(promises);

  // Tell the writer to abort.
  writer_->noMoreData(/*success=*/false);
}

void ExchangeOutputBuffer::setNumDrivers(uint32_t numDrivers) {
  std::lock_guard<std::mutex> lock(stateMutex_);
  // Only the first call takes effect — multiple drivers may call this.
  if (numDrivers_ == 0) {
    numDrivers_ = numDrivers;
  }
}

bool ExchangeOutputBuffer::noMoreDrivers() {
  bool isLast = false;
  {
    std::lock_guard<std::mutex> lock(stateMutex_);
    ++numFinishedDrivers_;
    isLast = numDrivers_ > 0 && numFinishedDrivers_ >= numDrivers_;
  }
  if (isLast) {
    finishAndClose();
  }
  return isLast;
}

// Fulfill promises to unblock producers waiting on backpressure.
void ExchangeOutputBuffer::maybeUnblockProducers(
    std::vector<velox::ContinuePromise>& promises) {
  for (auto& promise : promises) {
    promise.setValue();
  }
}

// Drain all remaining data and close the writer. Called exactly once.
void ExchangeOutputBuffer::finishAndClose() {
  if (finished_.exchange(true)) {
    return;
  }
  drainAll();
  writer_->noMoreData(/*success=*/true);
}

folly::F14FastMap<std::string, int64_t> ExchangeOutputBuffer::stats() const {
  auto writerStats = writer_->stats();
  writerStats["exchangeOutputBuffer.totalDrainedBytes"] = totalDrainedBytes_;
  writerStats["exchangeOutputBuffer.drainCount"] = drainCount_;
  writerStats["exchangeOutputBuffer.backpressureCount"] = backpressureCount_;
  writerStats["exchangeOutputBuffer.currentDrainThreshold"] =
      partitionDrainThreshold_;
  writerStats["exchangeOutputBuffer.bufferPoolUsedBytes"] =
      bufferPool_ ? bufferPool_->usedBytes() : 0;
  int64_t totalCollects = 0;
  for (int32_t i = 0; i < numPartitions_; ++i) {
    totalCollects += collectCountPerPartition_[i];
  }
  writerStats["exchangeOutputBuffer.totalCollectCalls"] = totalCollects;
  return writerStats;
}

} // namespace facebook::presto::operators
