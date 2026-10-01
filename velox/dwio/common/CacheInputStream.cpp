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

#include "velox/dwio/common/CacheInputStream.h"
#include "velox/common/process/TraceContext.h"

using facebook::velox::cache::ScanTracker;
using facebook::velox::cache::TrackingId;
using facebook::velox::memory::MemoryAllocator;

namespace facebook::velox::dwio::common {

using cache::AsyncDataCacheEntry;
using cache::CachePin;
using cache::RawFileCacheKey;
using cache::SsdFile;
using cache::SsdPin;
using cache::TrackingId;

CacheInputStream::CacheInputStream(
    cache::AsyncDataCache* cache,
    IoStatistics* ioStats,
    const Region& region,
    std::shared_ptr<ReadFileInputStream> input,
    uint64_t fileNum,
    bool noCacheRetention,
    std::shared_ptr<cache::ScanTracker> tracker,
    cache::TrackingId trackingId,
    uint64_t groupId,
    int32_t loadQuantum)
    : cache_(cache),
      ioStats_(ioStats),
      region_(region),
      input_(std::move(input)),
      fileNum_(fileNum),
      noCacheRetention_(noCacheRetention),
      tracker_(std::move(tracker)),
      trackingId_(trackingId),
      groupId_(groupId),
      loadQuantum_(loadQuantum) {
  nextEndpoint_ = region_.offset;
}

void CacheInputStream::setPercentiles(std::vector<int32_t>* percentiles) {
  percentiles_ = percentiles;
}

bool CacheInputStream::Next(const void** buffer, int32_t* size) {
  if (position_ >= region_.length) {
    *size = 0;
    return false;
  }
  loadPosition();

  auto* entry = pin_.entry();
  VELOX_CHECK_NOT_NULL(entry, "CacheInputStream: no cache entry after load");
  const auto absolutePosition = region_.offset + position_;
  VELOX_CHECK_LE(
      entry->offset(),
      absolutePosition,
      "CacheInputStream: entry offset {} past position {}",
      entry->offset(),
      absolutePosition);
  const auto offsetInEntry = absolutePosition - entry->offset();
  VELOX_CHECK_LT(
      offsetInEntry,
      entry->size(),
      "CacheInputStream: position {} outside entry range [{}, {})",
      absolutePosition,
      entry->offset(),
      entry->offset() + entry->size());

  // Full-read contract: entry must cover requested range with real data.
  // Partial cache/file fills (truncated HDFS, wrong offset/length) must error.
  const auto remainingInEntry = entry->size() - offsetInEntry;
  const auto remainingInRegion = region_.length - position_;
  VELOX_CHECK_GT(
      remainingInEntry,
      0,
      "CacheInputStream: empty read at offset {} (region {}+{})",
      absolutePosition,
      region_.offset,
      region_.length);

  *buffer = entry->data() + offsetInEntry;
  *size = std::min<
      int64_t>(static_cast<int64_t>(remainingInEntry), remainingInRegion);
  VELOX_CHECK_GT(*size, 0, "CacheInputStream: zero-size Next after load");

  if (tracker_) {
    tracker_->recordRead(trackingId_, *size, fileNum_, groupId_);
  }
  position_ += *size;
  return true;
}

void CacheInputStream::BackUp(int32_t count) {
  VELOX_CHECK_GE(count, 0);
  VELOX_CHECK_GE(position_, count);
  position_ -= count;
}

bool CacheInputStream::Skip(int32_t count) {
  if (count < 0) {
    return false;
  }
  position_ = std::min(position_ + static_cast<uint64_t>(count), region_.length);
  return position_ < region_.length;
}

google::protobuf::int64 CacheInputStream::ByteCount() const {
  return position_;
}

void CacheInputStream::seekToPosition(PositionProvider& seek) {
  position_ = seek.next();
  VELOX_CHECK_LE(position_, region_.length);
}

std::string CacheInputStream::getName() const {
  return fmt::format("CacheInputStream {} of {}", position_, region_.length);
}

size_t CacheInputStream::positionSize() {
  return 1;
}

void CacheInputStream::loadPosition() {
  const auto absolutePosition = region_.offset + position_;
  if (pin_.entry() && pin_.entry()->offset() <= absolutePosition &&
      pin_.entry()->offset() + pin_.entry()->size() > absolutePosition) {
    return;
  }

  process::TraceContext trace("CacheInputStream::loadPosition");
  pin_.clear();

  const auto maxEnd = region_.offset + region_.length;
  auto loadOffset = absolutePosition;
  if (loadQuantum_ > 0) {
    loadOffset = absolutePosition - (absolutePosition - region_.offset) %
            static_cast<uint64_t>(loadQuantum_);
  }
  auto loadSize = static_cast<int32_t>(
      std::min(
          static_cast<uint64_t>(loadQuantum_ > 0 ? loadQuantum_ : maxEnd - loadOffset),
          maxEnd - loadOffset));
  VELOX_CHECK_GT(
      loadSize,
      0,
      "CacheInputStream: zero load size at offset {} region {}+{}",
      absolutePosition,
      region_.offset,
      region_.length);

  RawFileCacheKey key{fileNum_, loadOffset};
  pin_ = cache_->findOrCreate(
      key,
      loadSize,
      [&](MemoryAllocator::Allocation& allocation, bool& hasError) {
        hasError = false;
        try {
          auto buf = allocation.data<char>();
          const auto requested = static_cast<uint64_t>(loadSize);
          input_->read(
              buf,
              requested,
              loadOffset,
              thrift::MetricsLog::Category::FILE);
          // Full-read contract: storage read must return exactly requested
          // bytes or throw. Guard against silent partial fills.
          VELOX_CHECK_EQ(
              allocation.byteSize() >= requested,
              true,
              "CacheInputStream: allocation {} < requested {} at offset {}",
              allocation.byteSize(),
              requested,
              loadOffset);
        } catch (const std::exception&) {
          hasError = true;
          throw;
        }
      });

  VELOX_CHECK(
      !pin_.empty() && pin_.entry(),
      "CacheInputStream: failed to load offset {} size {}",
      loadOffset,
      loadSize);
  VELOX_CHECK_EQ(
      pin_.entry()->size(),
      loadSize,
      "CacheInputStream: partial cache entry at offset {}: requested {} got {}",
      loadOffset,
      loadSize,
      pin_.entry()->size());
  VELOX_CHECK_EQ(
      pin_.entry()->offset(),
      loadOffset,
      "CacheInputStream: entry offset mismatch: expected {} got {}",
      loadOffset,
      pin_.entry()->offset());

  if (noCacheRetention_) {
    pin_.entry()->setExclusiveToShared();
  }
  if (ioStats_) {
    ioStats_->incRawBytesRead(loadSize);
  }
}

} // namespace facebook::velox::dwio::common
