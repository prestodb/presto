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

namespace facebook::velox::dwio::common {

using velox::cache::ScanTracker;
using velox::cache::TrackingId;
using velox::memory::MappedMemory;

CacheInputStream::CacheInputStream(
    CachedBufferedInput* bufferedInput,
    IoStatistics* ioStats,
    const Region& region,
    InputStream& input,
    uint64_t fileNum,
    std::shared_ptr<ScanTracker> tracker,
    TrackingId trackingId,
    uint64_t groupId,
    int32_t loadQuantum)
    : bufferedInput_(bufferedInput),
      cache_(bufferedInput_->cache()),
      ioStats_(ioStats),
      input_(input),
      region_(region),
      fileNum_(fileNum),
      tracker_(std::move(tracker)),
      trackingId_(trackingId),
      groupId_(groupId),
      loadQuantum_(loadQuantum) {}

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
    auto window = window_.value();
    if (position_ + *size > window.offset + window.length) {
      *size = window.offset + window.length - position_;
    }
  }
  if (position_ + *size > region_.length) {
    *size = region_.length - position_;
  }
  offsetInRun_ += *size;
  position_ += *size;
  if (tracker_) {
    tracker_->recordRead(trackingId_, *size, fileNum_, groupId_);
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

void CacheInputStream::seekToPosition(PositionProvider& seekPosition) {
  position_ = seekPosition.next();
}

std::string CacheInputStream::getName() const {
  return fmt::format("CacheInputStream {} of {}", position_, region_.length);
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

void CacheInputStream::loadSync(Region region) {
  // rawBytesRead is the number of bytes touched. Whether they come
  // from disk, ssd or memory is itemized in different counters. A
  process::TraceContext trace("loadSync");
  // coalesced read ofrom InputStream removes itself from this count
  // so as not to double count when the individual parts are
  // hit.
  ioStats_->incRawBytesRead(region.length);
  do {
    folly::SemiFuture<bool> wait(false);
    cache::RawFileCacheKey key{fileNum_, region.offset};
    pin_.clear();
    pin_ = cache_->findOrCreate(key, region.length, &wait);
    if (pin_.empty()) {
      VELOX_CHECK(wait.valid());
      auto& exec = folly::QueuedImmediateExecutor::instance();
      uint64_t usec = 0;
      {
        MicrosecondTimer timer(&usec);
        std::move(wait).via(&exec).wait();
      }
      ioStats_->queryThreadIoLatency().increment(usec);
      continue;
    }
    auto entry = pin_.checkedEntry();
    if (entry->isExclusive()) {
      entry->setGroupId(groupId_);
      entry->setTrackingId(trackingId_);
      if (loadFromSsd(region, *entry)) {
        return;
      }
      auto ranges = makeRanges(entry, region.length);
      uint64_t usec = 0;
      {
        MicrosecondTimer timer(&usec);
        input_.read(ranges, region.offset, LogType::FILE);
      }
      ioStats_->read().increment(region.length);
      ioStats_->queryThreadIoLatency().increment(usec);
      entry->setExclusiveToShared();
    } else {
      if (!entry->getAndClearFirstUseFlag()) {
        ioStats_->ramHit().increment(entry->size());
      }
      return;
    }
  } while (pin_.empty());
}

bool CacheInputStream::loadFromSsd(
    Region region,
    cache::AsyncDataCacheEntry& entry) {
  auto ssdCache = cache_->ssdCache();
  if (!ssdCache) {
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
  uint64_t usec = 0;
  // SsdFile::load wants vectors of pins. Put the pins in a
  // temp vector and then put 'pin_' back in 'this'. 'pin_'
  // is exclusive and not movable.
  std::vector<cache::SsdPin> ssdPins;
  ssdPins.push_back(std::move(ssdPin));
  std::vector<cache::CachePin> pins;
  pins.push_back(std::move(pin_));
  try {
    MicrosecondTimer timer(&usec);
    file.load(ssdPins, pins);
  } catch (const std::exception& e) {
    try {
      LOG(ERROR) << "IOERR: Failed SSD loadSync " << entry.toString()
                 << e.what() << process::TraceContext::statusLine()
                 << fmt::format(
                        "stream region {} {}b, start of load {} file {}",
                        region_.offset,
                        region_.length,
                        region.offset - region_.offset,
                        fileIds().string(fileNum_));
      // Remove the non-loadable entry so that next access goes to
      // storage.
      file.erase(cache::RawFileCacheKey{fileNum_, region.offset});
    } catch (const std::exception&) {
      // Ignore error inside logging the error.
    }
    throw;
  }
  pin_ = std::move(pins[0]);
  ioStats_->ssdRead().increment(entry.size());
  ioStats_->queryThreadIoLatency().increment(usec);
  entry.setExclusiveToShared();
  return true;
}

void CacheInputStream::loadPosition() {
  auto offset = region_.offset;
  if (pin_.empty()) {
    auto load = bufferedInput_->coalescedLoad(this);
    if (load) {
      folly::SemiFuture<bool> waitFuture(false);
      uint64_t usec = 0;
      {
        MicrosecondTimer timer(&usec);
        try {
          if (!load->loadOrFuture(&waitFuture)) {
            auto& exec = folly::QueuedImmediateExecutor::instance();
            std::move(waitFuture).via(&exec).wait();
          }
        } catch (const std::exception& e) {
          // Log the error and continue. The error, if it persists,  will be hit
          // again in looking up the specific entry and thrown from there.
          LOG(ERROR) << "IOERR: error in coalesced load " << e.what();
        }
      }
      ioStats_->queryThreadIoLatency().increment(usec);
    }
    auto loadRegion = region_;
    // Quantize position to previous multiple of 'loadQuantum_'.
    loadRegion.offset += (position_ / loadQuantum_) * loadQuantum_;
    // Set length to be the lesser of 'loadQuantum_' and distance to end of
    // 'region_'
    loadRegion.length = std::min<int32_t>(
        loadQuantum_, region_.length - (loadRegion.offset - region_.offset));
    loadSync(loadRegion);
  }
  auto* entry = pin_.checkedEntry();
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
} // namespace facebook::velox::dwio::common
