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

#include "velox/dwio/dwrf/common/CacheInputStream.h"
#include <folly/executors/QueuedImmediateExecutor.h>

namespace facebook::velox::dwrf {

using velox::cache::ScanTracker;
using velox::cache::TrackingId;
using velox::memory::MappedMemory;

CacheInputStream::CacheInputStream(
    cache::AsyncDataCache* cache,
    dwio::common::IoStatistics* ioStats,
    const dwio::common::Region& region,
    dwio::common::InputStream& input,
    uint64_t fileNum,
    std::shared_ptr<ScanTracker> tracker,
    TrackingId trackingId,
    uint64_t groupId)
    : cache_(cache),
      ioStats_(ioStats),
      input_(input),
      region_(region),
      fileNum_(fileNum),
      tracker_(std::move(tracker)),
      trackingId_(trackingId),
      groupId_(groupId) {}

bool CacheInputStream::Next(const void** buffer, int32_t* size) {
  if (position_ >= region_.length) {
    *size = 0;
    return false;
  }
  loadPosition();

  *buffer = reinterpret_cast<const void**>(run_ + offsetInRun_);
  *size = runSize_ - offsetInRun_;
  if (position_ + *size > region_.length) {
    *size = region_.length - position_;
  }
  offsetInRun_ += *size;
  position_ += *size;
  if (tracker_) {
    tracker_->recordRead(trackingId_, *size, groupId_);
  }
  return true;
}

void CacheInputStream::BackUp(int32_t count) {
  DWIO_ENSURE_GE(count, 0, "can't backup negative distances");

  uint64_t unsignedCount = static_cast<uint64_t>(count);
  DWIO_ENSURE(unsignedCount <= offsetInRun_, "Can't backup that much!");
  position_ -= unsignedCount;
}

bool CacheInputStream::Skip(int32_t count) {
  if (count < 0) {
    return false;
  }
  uint64_t unsignedCount = static_cast<uint64_t>(count);
  if (unsignedCount + position_ <= region_.length) {
    position_ += unsignedCount;
    return true;
  }
  position_ = region_.length;
  return false;
}

google::protobuf::int64 CacheInputStream::ByteCount() const {
  return static_cast<google::protobuf::int64>(position_);
}

void CacheInputStream::seekToRowGroup(PositionProvider& seekPosition) {
  position_ = seekPosition.next();
}

std::string CacheInputStream::getName() const {
  return fmt::format("CacheInputStream {} of {}", position_, region_.length);
}

size_t CacheInputStream::loadIndices(
    const proto::RowIndex& /*rowIndex*/,
    size_t startIndex) {
  // not compressed, so only need to skip 1 value (uncompressed position)
  return startIndex + 1;
}

namespace {
std::vector<folly::Range<char*>> makeRanges(
    cache::AsyncDataCacheEntry* entry,
    size_t length) {
  std::vector<folly::Range<char*>> buffers;
  if (entry->tinyData() == nullptr) {
    auto& allocation = entry->data();
    buffers.reserve(allocation.numRuns());
    uint64_t offsetInRuns = 0;
    for (int i = 0; i < allocation.numRuns(); ++i) {
      auto run = allocation.runAt(i);
      uint64_t bytes = run.numPages() * MappedMemory::kPageSize;
      uint64_t readSize = std::min(bytes, length - offsetInRuns);
      buffers.push_back(folly::Range<char*>(run.data<char>(), readSize));
      offsetInRuns += readSize;
    }
  } else {
    buffers.push_back(folly::Range<char*>(entry->tinyData(), entry->size()));
  }
  return buffers;
}
} // namespace

void CacheInputStream::loadSync(dwio::common::Region region) {
  do {
    folly::SemiFuture<bool> wait(false);
    cache::RawFileCacheKey key{fileNum_, region.offset};
    pin_.clear();
    pin_ = cache_->findOrCreate(key, region.length, &wait);
    if (pin_.empty()) {
      VELOX_CHECK(wait.valid());
      auto& exec = folly::QueuedImmediateExecutor::instance();
      std::move(wait).via(&exec).wait();
      continue;
    }
    if (pin_.entry()->isExclusive()) {
      auto ranges = makeRanges(pin_.entry(), region.length);
      input_.read(ranges, region.offset, dwio::common::LogType::FILE);
      ioStats_->read().increment(region.length);
      pin_.entry()->setValid(true);
      pin_.entry()->setExclusiveToShared();
    } else {
      if (pin_.entry()->dataValid()) {
        if (!pin_.entry()->getAndClearFirstUseFlag()) {
          ioStats_->ramHit().increment(pin_.entry()->size());
        }
      } else {
        pin_.entry()->ensureLoaded(true);
      }
    }
  } while (pin_.empty());
}

void CacheInputStream::loadPosition() {
  auto offset = region_.offset;
  if (pin_.empty()) {
    auto loadRegion = region_;
    // Quantize position to previous multiple of 'loadQuantum_'.
    loadRegion.offset += (position_ / loadQuantum_) * loadQuantum_;
    // Set length to be the lesser of 'loadQuantum_' and distance to end of
    // 'region_'
    loadRegion.length = std::min<int32_t>(
        loadQuantum_, region_.length - (loadRegion.offset - region_.offset));
    loadSync(loadRegion);
  }
  auto* entry = pin_.entry();
  uint64_t positionInFile = offset + position_;
  if (entry->offset() <= positionInFile &&
      entry->offset() + entry->size() > positionInFile) {
    // The position is inside the range of 'entry'.
    auto offsetInEntry = positionInFile - entry->offset();
    if (entry->data().numPages() == 0) {
      run_ = reinterpret_cast<uint8_t*>(entry->tinyData());
      runSize_ = entry->size();
      offsetInRun_ = offsetInEntry;
      offsetOfRun_ = 0;
    } else {
      entry->data().findRun(offsetInEntry, &runIndex_, &offsetInRun_);
      offsetOfRun_ = offsetInEntry - offsetInRun_;
      auto run = entry->data().runAt(runIndex_);
      run_ = run.data();
      runSize_ = run.numPages() * MappedMemory::kPageSize;
      if (offsetOfRun_ + runSize_ > entry->size()) {
        runSize_ = entry->size() - offsetOfRun_;
      }
    }
  } else {
    pin_.clear();
    loadPosition();
  }
}
} // namespace facebook::velox::dwrf
