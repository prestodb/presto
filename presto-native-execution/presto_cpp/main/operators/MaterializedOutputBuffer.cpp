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

int64_t MaterializedOutputBuffer::PartitionBuffer::enqueue(
    int32_t partition,
    std::unique_ptr<folly::IOBuf> rowGroup) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto dataSize = static_cast<int64_t>(rowGroup->computeChainDataLength());
  rowGroups_.push_back(std::move(rowGroup));
  bufferedBytes_ += dataSize;

  if (bufferedBytes_ < drainThreshold_) {
    return 0;
  }

  // Drain: coalesce + flush under the same lock.
  std::deque<std::unique_ptr<folly::IOBuf>> toDrain;
  toDrain.swap(rowGroups_);
  auto drainedBytes = bufferedBytes_;
  bufferedBytes_ = 0;

  auto coalesced = buffer_->coalesceRowGroups(toDrain);
  buffer_->flushToWriter(partition, std::move(coalesced));
  return drainedBytes;
}

MaterializedOutputBuffer::MaterializedOutputBuffer(
    int32_t numPartitions,
    std::shared_ptr<ShuffleWriter> writer,
    std::shared_ptr<velox::memory::MemoryPool> pool,
    int64_t maxBufferedBytes,
    int64_t partitionDrainThreshold)
    : numPartitions_(numPartitions),
      maxBufferedBytes_(maxBufferedBytes),
      continueBufferedBytes_(maxBufferedBytes * 9 / 10),
      partitionDrainThreshold_(
          std::min(
              partitionDrainThreshold > 0 ? partitionDrainThreshold
                                          : kDefaultDrainThreshold,
              maxBufferedBytes / numPartitions)),
      writer_(std::move(writer)),
      pool_(std::move(pool)),
      collectCountPerPartition_(numPartitions) {
  partitionBuffers_.reserve(numPartitions);
  for (int32_t i = 0; i < numPartitions; ++i) {
    partitionBuffers_.push_back(
        std::make_unique<PartitionBuffer>(
            partitionDrainThreshold_, writer_.get(), this));
  }
  VELOX_CHECK_GT(numPartitions, 0, "Must have at least one partition");
  VELOX_CHECK_NOT_NULL(writer_, "ShuffleWriter must be non-null");
  LOG(INFO) << fmt::format(
      "MaterializedOutputBuffer: partitions={}, maxBufferedBytes={}MB, "
      "effectiveDrainThreshold={}KB (configured={}KB), pool={}",
      numPartitions_,
      maxBufferedBytes_ >> 20,
      partitionDrainThreshold_ >> 10,
      (partitionDrainThreshold > 0 ? partitionDrainThreshold
                                   : kDefaultDrainThreshold) >>
          10,
      pool_->name());
}

MaterializedOutputBuffer::~MaterializedOutputBuffer() {
  if (!finished_.load()) {
    LOG(WARNING) << "MaterializedOutputBuffer destroyed without calling "
                 << "noMoreData() or abort(). Aborting writer.";
    try {
      abort();
    } catch (...) {
      LOG(ERROR) << "MaterializedOutputBuffer abort failed in destructor";
    }
  }
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

bool MaterializedOutputBuffer::maybeApplyBackpressure(
    velox::ContinueFuture* future) {
  if (bufferedBytes_ >= maxBufferedBytes_) {
    std::lock_guard<std::mutex> lock(stateMutex_);
    if (bufferedBytes_ >= maxBufferedBytes_) {
      auto [promise, semiFuture] =
          velox::makeVeloxContinuePromiseContract("MaterializedOutputBuffer");
      promises_.push_back(std::move(promise));
      *future = std::move(semiFuture);
      ++backpressureCount_;
      LOG(INFO) << fmt::format(
          "MaterializedOutputBuffer backpressure: bufferedBytes={}MB >= "
          "max={}MB, waitingDrivers={}",
          bufferedBytes_ >> 20,
          maxBufferedBytes_ >> 20,
          promises_.size());
      return true;
    }
  }
  return false;
}

bool MaterializedOutputBuffer::enqueue(
    int32_t partition,
    std::unique_ptr<folly::IOBuf> rowGroup,
    velox::ContinueFuture* future) {
  VELOX_CHECK_GE(partition, 0);
  VELOX_CHECK_LT(partition, numPartitions_);
  // During error teardown, abort() may have been called while other
  // drivers still flush remaining data. Silently drop.
  if (aborted_.load()) {
    return false;
  }
  VELOX_CHECK(
      !finished_.load(),
      "enqueue called after finishAndClose when pool is not aborted");

  auto rowGroupBytes = static_cast<int64_t>(rowGroup->computeChainDataLength());
  auto currentBytes = (bufferedBytes_ += rowGroupBytes);
  int64_t peak = peakBufferedBytes_;
  while (currentBytes > peak &&
         !peakBufferedBytes_.compare_exchange_weak(peak, currentBytes)) {
  }

  auto drainedBytes =
      partitionBuffers_[partition]->enqueue(partition, std::move(rowGroup));

  if (drainedBytes > 0) {
    ++drainCount_;
    auto totalDrained = (totalDrainedBytes_ += drainedBytes);
    auto currentGB = totalDrained >> 30;
    int64_t lastGB = lastLoggedDrainedGB_;
    if (currentGB > lastGB &&
        lastLoggedDrainedGB_.compare_exchange_strong(lastGB, currentGB)) {
      LOG(INFO) << fmt::format(
          "MaterializedOutputBuffer progress: totalDrained={}GB, "
          "drainCount={}, bufferedBytes={}MB",
          currentGB,
          static_cast<int64_t>(drainCount_),
          bufferedBytes_ >> 20);
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
  }

  return maybeApplyBackpressure(future);
}

void MaterializedOutputBuffer::flushToWriter(
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

std::unique_ptr<folly::IOBuf> MaterializedOutputBuffer::coalesceRowGroups(
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

int64_t MaterializedOutputBuffer::drainPartition(int32_t partition) {
  VELOX_CHECK_GE(partition, 0);
  VELOX_CHECK_LT(partition, numPartitions_);

  std::deque<std::unique_ptr<folly::IOBuf>> toDrain;
  int64_t drainedBytes = 0;
  {
    std::lock_guard<std::mutex> lock(partitionBuffers_[partition]->mutex_);
    toDrain.swap(partitionBuffers_[partition]->rowGroups_);
    drainedBytes = partitionBuffers_[partition]->bufferedBytes_;
    partitionBuffers_[partition]->bufferedBytes_ = 0;
  }

  if (!toDrain.empty()) {
    auto coalesced = coalesceRowGroups(toDrain);
    flushToWriter(partition, std::move(coalesced));
    ++drainCount_;
    totalDrainedBytes_ += drainedBytes;
    bufferedBytes_.fetch_sub(drainedBytes);
  }

  return drainedBytes;
}

uint64_t MaterializedOutputBuffer::drainAll() {
  uint64_t totalDrained = 0;
  for (int32_t i = 0; i < numPartitions_; ++i) {
    totalDrained += drainPartition(i);
  }
  return totalDrained;
}

void MaterializedOutputBuffer::noMoreData() {
  finishAndClose();
}

void MaterializedOutputBuffer::abort() {
  aborted_.store(true);
  if (finished_.exchange(true)) {
    return;
  }

  for (int32_t i = 0; i < numPartitions_; ++i) {
    std::lock_guard<std::mutex> lock(partitionBuffers_[i]->mutex_);
    partitionBuffers_[i]->rowGroups_.clear();
    bufferedBytes_ -= partitionBuffers_[i]->bufferedBytes_;
    partitionBuffers_[i]->bufferedBytes_ = 0;
  }

  // Unblock any waiting producers.
  std::vector<velox::ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> lock(stateMutex_);
    promises.swap(promises_);
  }
  maybeUnblockProducers(promises);

  LOG(INFO)
      << "MaterializedOutputBuffer: calling writer noMoreData(false) [abort]";
  writer_->noMoreData(/*success=*/false);
}

void MaterializedOutputBuffer::setNumDrivers(uint32_t numDrivers) {
  std::lock_guard<std::mutex> lock(stateMutex_);
  // Only the first call takes effect — multiple drivers may call this.
  if (numDrivers_ == 0) {
    numDrivers_ = numDrivers;
  }
}

bool MaterializedOutputBuffer::noMoreDrivers() {
  bool isLast = false;
  uint32_t finished = 0;
  uint32_t total = 0;
  {
    std::lock_guard<std::mutex> lock(stateMutex_);
    ++numFinishedDrivers_;
    finished = numFinishedDrivers_;
    total = numDrivers_;
    isLast = total > 0 && finished >= total;
  }
  LOG(INFO) << fmt::format(
      "MaterializedOutputBuffer noMoreDrivers: {}/{} finished, isLast={}",
      finished,
      total,
      isLast);
  if (isLast) {
    finishAndClose();
  }
  return isLast;
}

void MaterializedOutputBuffer::maybeUnblockProducers(
    std::vector<velox::ContinuePromise>& promises) {
  if (!promises.empty()) {
    LOG(INFO) << fmt::format(
        "MaterializedOutputBuffer resumed: unblocked {} drivers, "
        "bufferedBytes={}MB",
        promises.size(),
        bufferedBytes_ >> 20);
  }
  for (auto& promise : promises) {
    promise.setValue();
  }
}

void MaterializedOutputBuffer::finishAndClose() {
  if (finished_.exchange(true)) {
    return;
  }
  LOG(INFO) << fmt::format(
      "MaterializedOutputBuffer finishAndClose: draining, bufferedBytes={}MB",
      bufferedBytes_ >> 20);
  drainAll();
  LOG(INFO) << "MaterializedOutputBuffer: calling writer noMoreData(true)";
  writer_->noMoreData(/*success=*/true);
  int64_t totalCollects = 0;
  for (int32_t i = 0; i < numPartitions_; ++i) {
    totalCollects += collectCountPerPartition_[i];
  }
  LOG(INFO) << fmt::format(
      "MaterializedOutputBuffer progress: totalDrained={}GB, "
      "drainCount={}, bufferedBytes={}MB",
      totalDrainedBytes_ >> 30,
      static_cast<int64_t>(drainCount_),
      bufferedBytes_ >> 20);
  LOG(INFO) << fmt::format(
      "MaterializedOutputBuffer closed: "
      "backpressureCount={}, collectCalls={}, peakBufferedBytes={}MB",
      static_cast<int64_t>(backpressureCount_),
      totalCollects,
      peakBufferedBytes_ >> 20);
}

folly::F14FastMap<std::string, int64_t> MaterializedOutputBuffer::stats()
    const {
  auto writerStats = writer_->stats();
  writerStats[std::string(kTotalDrainedBytes)] = totalDrainedBytes_;
  writerStats[std::string(kDrainCount)] = drainCount_;
  writerStats[std::string(kBackpressureCount)] = backpressureCount_;
  writerStats[std::string(kCurrentDrainThreshold)] = partitionDrainThreshold_;
  writerStats[std::string(kBufferPoolUsedBytes)] =
      pool_ ? pool_->usedBytes() : 0;
  writerStats[std::string(kBufferPoolPeakBytes)] =
      pool_ ? pool_->peakBytes() : 0;
  int64_t totalCollects = 0;
  for (int32_t i = 0; i < numPartitions_; ++i) {
    totalCollects += collectCountPerPartition_[i];
  }
  writerStats[std::string(kTotalCollectCalls)] = totalCollects;
  writerStats[std::string(kPeakBufferedBytes)] = peakBufferedBytes_;
  return writerStats;
}

} // namespace facebook::presto::operators
