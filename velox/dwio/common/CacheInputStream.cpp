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

#include <folly/executors/QueuedImmediateExecutor.h>

#include "velox/common/process/TraceContext.h"
#include "velox/common/time/Timer.h"
#include "velox/dwio/common/CacheInputStream.h"
#include "velox/dwio/common/CachedBufferedInput.h"

using ::facebook::velox::common::Region;

namespace facebook::velox::dwio::common {

using velox::cache::ScanTracker;
using velox::cache::TrackingId;
using velox::memory::MemoryAllocator;

CacheInputStream::CacheInputStream(
    CachedBufferedInput* bufferedInput,
    IoStatistics* ioStats,
    const Region& region,
    std::shared_ptr<ReadFileInputStream> input,
    uint64_t fileNum,
    bool noCacheRetention,
    std::shared_ptr<ScanTracker> tracker,
    TrackingId trackingId,
    uint64_t groupId,
    int32_t loadQuantum)
    : bufferedInput_(bufferedInput),
      cache_(bufferedInput_->cache()),
      noCacheRetention_(noCacheRetention),
      region_(region),
      fileNum_(fileNum),
      tracker_(std::move(tracker)),
      trackingId_(trackingId),
      groupId_(groupId),
      loadQuantum_(loadQuantum),
      ioStats_(ioStats),
      input_(std::move(input)) {}

CacheInputStream::~CacheInputStream() {
  clearCachePin();
  makeCacheEvictable();
}

void CacheInputStream::makeCacheEvictable() {
  if (!noCacheRetention_) {
    return;
  }
  // Walks through the potential prefetch or access cache space of this cache
  // input stream, and marks those exist cache entries as immediate evictable.
  uint64_t position = 0;
  while (position < region_.length) {
    const auto nextRegion = nextQuantizedLoadRegion(position);
    const cache::RawFileCacheKey key{fileNum_, nextRegion.offset};
    cache_->makeEvictable(key);
    position = nextRegion.offset + nextRegion.length;
  }
}

bool CacheInputStream::Next(const void** buffer, int32_t* size) {
  if (position_ >= region_.length) {
    *size = 0;
    return false;
  }
  if (window_.has_value() &&
      position_ >= window_.value().offset + window_.value().length) {
    *size = 0;
    return false;
  }

  loadPosition();

  *buffer = reinterpret_cast<const void**>(run_ + offsetInRun_);
  *size = runSize_ - offsetInRun_;
  if (window_.has_value()) {
    if (position_ + *size > window_->offset + window_->length) {
      *size = window_->offset + window_->length - position_;
    }
  }
  if (position_ + *size > region_.length) {
    *size = region_.length - position_;
  }
  offsetInRun_ += *size;

  if (prefetchPct_ < 100) {
    const auto offsetInQuantum = position_ % loadQuantum_;
    const auto nextQuantumOffset = position_ - offsetInQuantum + loadQuantum_;
    const auto prefetchThreshold = loadQuantum_ * prefetchPct_ / 100;
    if (!prefetchStarted_ && (offsetInQuantum + *size > prefetchThreshold) &&
        (position_ - offsetInQuantum + loadQuantum_ < region_.length)) {
      // We read past 'prefetchPct_' % of the current load quantum and the
      // current load quantum is not the last in the region. Prefetch the next
      // load quantum.
      const auto prefetchSize =
          std::min(region_.length, nextQuantumOffset + loadQuantum_) -
          nextQuantumOffset;
      prefetchStarted_ = bufferedInput_->prefetch(
          Region{region_.offset + nextQuantumOffset, prefetchSize});
    }
  }
  position_ += *size;

  if (tracker_ != nullptr) {
    tracker_->recordRead(trackingId_, *size, fileNum_, groupId_);
  }
  return true;
}

void CacheInputStream::BackUp(int32_t count) {
  VELOX_CHECK_GE(count, 0, "can't backup negative distances");

  const uint64_t unsignedCount = static_cast<uint64_t>(count);
  VELOX_CHECK_LE(unsignedCount, offsetInRun_, "Can't backup that much!");
  position_ -= unsignedCount;
}

bool CacheInputStream::SkipInt64(int64_t count) {
  if (count < 0) {
    return false;
  }
  const uint64_t unsignedCount = static_cast<uint64_t>(count);
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

void CacheInputStream::seekToPosition(PositionProvider& seekPosition) {
  position_ = seekPosition.next();
}

std::string CacheInputStream::getName() const {
  std::string result =
      fmt::format("CacheInputStream {} of {}", position_, region_.length);
  const auto ssdFile = ssdFileName();
  if (!ssdFile.empty()) {
    result += fmt::format(" ssdFile={}", ssdFile);
  }
  return result;
}

size_t CacheInputStream::positionSize() {
  // not compressed, so only need 1 position (uncompressed position)
  return 1;
}

void CacheInputStream::setRemainingBytes(uint64_t remainingBytes) {
  VELOX_CHECK_GE(region_.length, position_ + remainingBytes);
  window_ = Region{static_cast<uint64_t>(position_), remainingBytes};
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
      uint64_t bytes = run.numPages() * memory::AllocationTraits::kPageSize;
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

void CacheInputStream::loadSync(const Region& region) {
  process::TraceContext trace("loadSync");
  int64_t hitSize = region.length;
  if (window_.has_value()) {
    const int64_t regionEnd = region.offset + region.length;
    const int64_t windowStart = region_.offset + window_.value().offset;
    const int64_t windowEnd =
        region_.offset + window_.value().offset + window_.value().length;
    hitSize = std::min(windowEnd, regionEnd) -
        std::max<int64_t>(windowStart, region.offset);
  }

  // rawBytesRead is the number of bytes touched. Whether they come from disk,
  // ssd or memory is itemized in different counters. A coalesced read from
  // InputStream removes itself from this count so as not to double count when
  // the individual parts are hit.
  ioStats_->incRawBytesRead(hitSize);
  prefetchStarted_ = false;
  do {
    folly::SemiFuture<bool> cacheLoadWait(false);
    cache::RawFileCacheKey key{fileNum_, region.offset};
    clearCachePin();
    pin_ = cache_->findOrCreate(key, region.length, &cacheLoadWait);
    if (pin_.empty()) {
      VELOX_CHECK(cacheLoadWait.valid());
      uint64_t waitUs{0};
      {
        MicrosecondTimer timer(&waitUs);
        std::move(cacheLoadWait)
            .via(&folly::QueuedImmediateExecutor::instance())
            .wait();
      }
      ioStats_->queryThreadIoLatency().increment(waitUs);
      continue;
    }

    auto* entry = pin_.checkedEntry();
    if (!entry->getAndClearFirstUseFlag()) {
      // Hit memory cache.
      ioStats_->ramHit().increment(hitSize);
    }
    if (!entry->isExclusive()) {
      return;
    }

    // Missed memory cache. Trying to load from ssd cache, and if again
    // missed, fall back to remote fetching.
    entry->setGroupId(groupId_);
    entry->setTrackingId(trackingId_);
    if (loadFromSsd(region, *entry)) {
      return;
    }
    const auto ranges = makeRanges(entry, region.length);
    uint64_t storageReadUs{0};
    {
      MicrosecondTimer timer(&storageReadUs);
      input_->read(ranges, region.offset, LogType::FILE);
    }
    ioStats_->read().increment(region.length);
    ioStats_->queryThreadIoLatency().increment(storageReadUs);
    ioStats_->incTotalScanTime(storageReadUs * 1'000);
    entry->setExclusiveToShared(!noCacheRetention_);
  } while (pin_.empty());
}

void CacheInputStream::clearCachePin() {
  if (pin_.empty()) {
    return;
  }
  if (noCacheRetention_) {
    pin_.checkedEntry()->makeEvictable();
  }
  pin_.clear();
}

bool CacheInputStream::loadFromSsd(
    const Region& region,
    cache::AsyncDataCacheEntry& entry) {
  auto* ssdCache = cache_->ssdCache();
  if (ssdCache == nullptr) {
    return false;
  }

  auto& file = ssdCache->file(fileNum_);
  auto ssdPin = file.find(cache::RawFileCacheKey{fileNum_, region.offset});
  if (ssdPin.empty()) {
    return false;
  }

  if (ssdPin.run().size() < entry.size()) {
    LOG(INFO) << fmt::format(
        "IOERR: Ssd entry for {} shorter than requested {}",
        entry.toString(),
        ssdPin.run().size());
    return false;
  }

  uint64_t ssdLoadUs{0};
  // SsdFile::load wants vectors of pins. Put the pins in a temp vector and then
  // put 'pin_' back in 'this'. 'pin_' is exclusive and not movable.
  std::vector<cache::SsdPin> ssdPins;
  ssdPins.push_back(std::move(ssdPin));
  std::vector<cache::CachePin> pins;
  pins.push_back(std::move(pin_));
  try {
    MicrosecondTimer timer(&ssdLoadUs);
    file.load(ssdPins, pins);
  } catch (const std::exception& e) {
    LOG(ERROR) << "IOERR: Failed SSD loadSync " << entry.toString() << ' '
               << e.what() << process::TraceContext::statusLine()
               << fmt::format(
                      "stream region {} {}b, start of load {} file {}",
                      region_.offset,
                      region_.length,
                      region.offset - region_.offset,
                      fileIds().string(fileNum_));
    // Remove the non-loadable entry so that next access goes to storage.
    file.erase(cache::RawFileCacheKey{fileNum_, region.offset});
    pin_ = std::move(pins[0]);
    return false;
  }

  VELOX_CHECK(pin_.empty());
  pin_ = std::move(pins[0]);
  ioStats_->ssdRead().increment(region.length);
  ioStats_->queryThreadIoLatency().increment(ssdLoadUs);
  // Skip no-cache retention setting as data is loaded from ssd.
  entry.setExclusiveToShared();
  return true;
}

std::string CacheInputStream::ssdFileName() const {
  auto ssdCache = cache_->ssdCache();
  if (!ssdCache) {
    return "";
  }
  return ssdCache->file(fileNum_).fileName();
}

void CacheInputStream::loadPosition() {
  const auto offset = region_.offset;
  if (pin_.empty()) {
    auto load = bufferedInput_->coalescedLoad(this);
    if (load != nullptr) {
      folly::SemiFuture<bool> waitFuture(false);
      uint64_t loadUs{0};
      {
        MicrosecondTimer timer(&loadUs);
        try {
          if (!load->loadOrFuture(&waitFuture, !noCacheRetention_)) {
            waitFuture.wait();
          }
        } catch (const std::exception& e) {
          // Log the error and continue. The error, if it persists, will be hit
          // again in looking up the specific entry and thrown from there.
          LOG(ERROR) << "IOERR: error in coalesced load " << e.what();
        }
      }
      ioStats_->queryThreadIoLatency().increment(loadUs);
    }

    const auto nextLoadRegion = nextQuantizedLoadRegion(position_);
    // There is no need to update the metric in the loadData method because
    // loadSync is always executed regardless and updates the metric.
    loadSync(nextLoadRegion);
  }

  auto* entry = pin_.checkedEntry();
  const uint64_t positionInFile = offset + position_;
  if (entry->offset() <= positionInFile &&
      entry->offset() + entry->size() > positionInFile) {
    // The position is inside the range of 'entry'.
    const auto offsetInEntry = positionInFile - entry->offset();
    if (entry->data().numPages() == 0) {
      run_ = reinterpret_cast<uint8_t*>(entry->tinyData());
      runSize_ = entry->size();
      offsetInRun_ = offsetInEntry;
      offsetOfRun_ = 0;
    } else {
      entry->data().findRun(offsetInEntry, &runIndex_, &offsetInRun_);
      offsetOfRun_ = offsetInEntry - offsetInRun_;
      const auto run = entry->data().runAt(runIndex_);
      run_ = run.data();
      runSize_ = memory::AllocationTraits::pageBytes(run.numPages());
      if (offsetOfRun_ + runSize_ > entry->size()) {
        runSize_ = entry->size() - offsetOfRun_;
      }
    }
  } else {
    clearCachePin();
    loadPosition();
  }
}

velox::common::Region CacheInputStream::nextQuantizedLoadRegion(
    uint64_t prevLoadedPosition) const {
  auto nextRegion = region_;
  // Quantize position to previous multiple of 'loadQuantum_'.
  nextRegion.offset += (prevLoadedPosition / loadQuantum_) * loadQuantum_;
  // Set length to be the lesser of 'loadQuantum_' and distance to end of
  // 'region_'
  nextRegion.length = std::min<int32_t>(
      loadQuantum_, region_.length - (nextRegion.offset - region_.offset));
  return nextRegion;
}
} // namespace facebook::velox::dwio::common
