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

#include "velox/dwio/common/CachedBufferedInput.h"
#include "velox/common/process/TraceContext.h"
#include "velox/dwio/common/CacheInputStream.h"

DEFINE_int32(
    cache_prefetch_min_pct,
    80,
    "Minimum percentage of actual uses over references to a column for prefetching. No prefetch if > 100");

namespace facebook::velox::dwio::common {

using cache::CachePin;
using cache::LoadState;
using cache::RawFileCacheKey;
using cache::ScanTracker;
using cache::SsdFile;
using cache::SsdPin;
using cache::TrackingId;
using memory::MappedMemory;

std::unique_ptr<SeekableInputStream> CachedBufferedInput::enqueue(
    Region region,
    const StreamIdentifier* si = nullptr) {
  if (region.length == 0) {
    return std::make_unique<SeekableArrayInputStream>(
        static_cast<const char*>(nullptr), 0);
  }

  TrackingId id;
  if (si) {
    id = TrackingId(si->getId());
  }
  VELOX_CHECK_LE(region.offset + region.length, fileSize_);
  requests_.emplace_back(
      RawFileCacheKey{fileNum_, region.offset}, region.length, id);
  tracker_->recordReference(id, region.length, fileNum_, groupId_);
  auto stream = std::make_unique<CacheInputStream>(
      this,
      ioStats_.get(),
      region,
      input_,
      fileNum_,
      tracker_,
      id,
      groupId_,
      loadQuantum_);
  requests_.back().stream = stream.get();
  return stream;
}

bool CachedBufferedInput::isBuffered(uint64_t /*offset*/, uint64_t /*length*/)
    const {
  return false;
}

bool CachedBufferedInput::shouldPreload() {
  // True if after scheduling this for preload, half the capacity
  // would be in a loading but not yet accessed state.
  if (requests_.empty()) {
    return false;
  }
  int32_t numPages = 0;
  for (auto& request : requests_) {
    numPages += bits::roundUp(
                    std::min<int32_t>(request.size, loadQuantum_),
                    MappedMemory::kPageSize) /
        MappedMemory::kPageSize;
  }
  auto cachePages = cache_->incrementCachedPages(0);
  auto maxPages = cache_->maxBytes() / MappedMemory::kPageSize;
  auto allocatedPages = cache_->numAllocated();
  if (numPages < maxPages - allocatedPages) {
    // There is free space for the read-ahead.
    return true;
  }
  auto prefetchPages = cache_->incrementPrefetchPages(0);
  if (numPages + prefetchPages < cachePages / 2) {
    // The planned prefetch plus other prefetches are under half the cache.
    return true;
  }
  return false;
}

namespace {

bool isPrefetchPct(int32_t pct) {
  return pct >= FLAGS_cache_prefetch_min_pct;
}

std::vector<CacheRequest*> makeRequestParts(
    CacheRequest& request,
    const cache::TrackingData& trackingData,
    int32_t loadQuantum,
    std::vector<std::unique_ptr<CacheRequest>>& extraRequests) {
  if (request.size <= loadQuantum) {
    return {&request};
  }

  // Large columns will be part of coalesced reads if the access frequency
  // qualifies for read ahead and if over 80% of the column gets accessed. Large
  // metadata columns (empty no trackingData) always coalesce.
  auto readPct =
      (100 * trackingData.numReads) / (1 + trackingData.numReferences);
  auto readDensity =
      (100 * trackingData.readBytes) / (1 + trackingData.referencedBytes);
  bool prefetch = trackingData.referencedBytes > 0 &&
      (isPrefetchPct(readPct) && readDensity >= 80);
  std::vector<CacheRequest*> parts;
  for (uint64_t offset = 0; offset < request.size; offset += loadQuantum) {
    int32_t size = std::min<int32_t>(loadQuantum, request.size - offset);
    extraRequests.push_back(std::make_unique<CacheRequest>(
        RawFileCacheKey{request.key.fileNum, request.key.offset + offset},
        size,
        request.trackingId));
    parts.push_back(extraRequests.back().get());
    parts.back()->coalesces = prefetch;
  }
  return parts;
}

int32_t adjustedReadPct(const cache::TrackingData& trackingData) {
  // When called, there will be one more reference that read, since references
  // are counted before readig.
  if (trackingData.numReferences < 2) {
    return 0;
  }
  return (100 * trackingData.numReads) / (trackingData.numReferences - 1);
}
} // namespace

void CachedBufferedInput::load(const LogType) {
  // 'requests_ is cleared on exit.
  auto requests = std::move(requests_);
  cache::SsdFile* FOLLY_NULLABLE ssdFile = nullptr;
  auto ssdCache = cache_->ssdCache();
  if (ssdCache) {
    ssdFile = &ssdCache->file(fileNum_);
  }
  // Extra requests made for preloadable regions that are larger then
  // 'loadQuantum'.
  std::vector<std::unique_ptr<CacheRequest>> extraRequests;
  // We loop over access frequency buckets. For example readPct 80
  // will get all streams where 80% or more of the referenced data is
  // actually loaded.
  for (auto readPct : std::vector<int32_t>{80, 50, 20, 0}) {
    std::vector<CacheRequest*> storageLoad;
    std::vector<CacheRequest*> ssdLoad;
    for (auto& request : requests) {
      if (request.processed) {
        continue;
      }
      cache::TrackingData trackingData;
      if (!request.trackingId.empty()) {
        trackingData = tracker_->trackingData(request.trackingId);
      }
      if (request.trackingId.empty() ||
          adjustedReadPct(trackingData) >= readPct) {
        request.processed = true;
        auto parts = makeRequestParts(
            request, trackingData, loadQuantum_, extraRequests);
        for (auto part : parts) {
          if (cache_->exists(part->key)) {
            continue;
          }
          if (ssdFile) {
            part->ssdPin = ssdFile->find(part->key);
            if (!part->ssdPin.empty() &&
                part->ssdPin.run().size() < part->size) {
              LOG(INFO) << "IOERR: Ignorin SSD  shorter than requested: "
                        << part->ssdPin.run().size() << " vs " << part->size;
              part->ssdPin.clear();
            }
            if (!part->ssdPin.empty()) {
              ssdLoad.push_back(part);
              continue;
            }
          }
          storageLoad.push_back(part);
        }
      }
    }
    makeLoads(std::move(storageLoad), isPrefetchPct(readPct));
    makeLoads(std::move(ssdLoad), isPrefetchPct(readPct));
  }
}

void CachedBufferedInput::makeLoads(
    std::vector<CacheRequest*> requests,
    bool prefetch) {
  if (requests.empty() || (requests.size() < 2 && !prefetch)) {
    return;
  }
  bool isSsd = !requests[0]->ssdPin.empty();
  int32_t maxDistance = isSsd ? 20000 : maxCoalesceDistance_;
  std::sort(
      requests.begin(),
      requests.end(),
      [&](const CacheRequest* left, const CacheRequest* right) {
        if (isSsd) {
          return left->ssdPin.run().offset() < right->ssdPin.run().offset();
        } else {
          return left->key.offset < right->key.offset;
        }
      });
  // Combine adjacent short reads.

  int32_t numNewLoads = 0;

  coalesceIo<CacheRequest*, CacheRequest*>(
      requests,
      maxDistance,
      // Break batches up. Better load more short ones i parallel.
      40,
      [&](int32_t index) {
        return isSsd ? requests[index]->ssdPin.run().offset()
                     : requests[index]->key.offset;
      },
      [&](int32_t index) { return requests[index]->size; },
      [&](int32_t index) {
        return requests[index]->coalesces ? 1 : kNoCoalesce;
      },
      [&](CacheRequest* request, std::vector<CacheRequest*>& ranges) {
        ranges.push_back(request);
      },
      [&](int32_t /*gap*/, std::vector<CacheRequest*> /*ranges*/) { /*no op*/ },
      [&](const std::vector<CacheRequest*>& /*requests*/,
          int32_t /*begin*/,
          int32_t /*end*/,
          uint64_t /*offset*/,
          const std::vector<CacheRequest*>& ranges) {
        ++numNewLoads;
        readRegion(ranges, prefetch);
      });
  if (prefetch && executor_) {
    for (auto& load : allCoalescedLoads_) {
      if (load->state() == LoadState::kPlanned) {
        executor_->add([pendingLoad = load]() {
          process::TraceContext trace("Read Ahead");
          pendingLoad->loadOrFuture(nullptr);
        });
      }
    }
  }
}

namespace {
// Base class for CoalescedLoads for different storage types.
class DwioCoalescedLoadBase : public cache::CoalescedLoad {
 public:
  DwioCoalescedLoadBase(
      cache::AsyncDataCache& cache,
      std::shared_ptr<IoStatistics> ioStats,
      uint64_t groupId,
      std::vector<CacheRequest*> requests)
      : CoalescedLoad(makeKeys(requests), makeSizes(requests)),
        cache_(cache),
        ioStats_(std::move(ioStats)),
        groupId_(groupId) {
    for (auto& request : requests) {
      requests_.push_back(std::move(*request));
    }
  }

  const std::vector<CacheRequest>& requests() {
    return requests_;
  }

  std::string toString() const override {
    int32_t payload = 0;
    assert(!requests_.empty());
    int32_t total = requests_.back().key.offset + requests_.back().size -
        requests_[0].key.offset;
    for (auto& request : requests_) {
      payload += request.size;
    }
    return fmt::format(
        "<CoalescedLoad: {} entries, {} total {} extra>",
        requests_.size(),
        total,
        total - payload);
  }

 protected:
  void updateStats(const CoalesceIoStats& stats, bool isPrefetch, bool isSsd) {
    if (ioStats_) {
      ioStats_->incRawOverreadBytes(stats.extraBytes);
      if (isSsd) {
        ioStats_->ssdRead().increment(stats.payloadBytes);
      } else {
        ioStats_->read().increment(stats.payloadBytes);
      }
      if (isPrefetch) {
        ioStats_->prefetch().increment(stats.payloadBytes);
      }
    }
  }

  static std::vector<RawFileCacheKey> makeKeys(
      std::vector<CacheRequest*>& requests) {
    std::vector<RawFileCacheKey> keys;
    keys.reserve(requests.size());
    for (auto& request : requests) {
      keys.push_back(request->key);
    }
    return keys;
  }

  std::vector<int32_t> makeSizes(std::vector<CacheRequest*> requests) {
    std::vector<int32_t> sizes;
    sizes.reserve(requests.size());
    for (auto& request : requests) {
      sizes.push_back(request->size);
    }
    return sizes;
  }

  cache::AsyncDataCache& cache_;
  std::vector<CacheRequest> requests_;
  std::shared_ptr<IoStatistics> ioStats_;
  const uint64_t groupId_;
};

// Represents a CoalescedLoad from ReadFile, e.g. disagg disk.
class DwioCoalescedLoad : public DwioCoalescedLoadBase {
 public:
  DwioCoalescedLoad(
      cache::AsyncDataCache& cache,
      std::unique_ptr<AbstractInputStreamHolder> input,
      std::shared_ptr<IoStatistics> ioStats,
      uint64_t groupId,
      std::vector<CacheRequest*> requests,
      int32_t maxCoalesceDistance)
      : DwioCoalescedLoadBase(cache, ioStats, groupId, std::move(requests)),
        input_(std::move(input)),
        maxCoalesceDistance_(maxCoalesceDistance) {}

  std::vector<CachePin> loadData(bool isPrefetch) override {
    auto& stream = input_->get();
    std::vector<CachePin> pins;
    pins.reserve(keys_.size());
    cache_.makePins(
        keys_,
        [&](int32_t index) { return sizes_[index]; },
        [&](int32_t /*index*/, CachePin pin) {
          pins.push_back(std::move(pin));
        });
    if (pins.empty()) {
      return pins;
    }
    auto stats = cache::readPins(
        pins,
        maxCoalesceDistance_,
        1000,
        [&](int32_t i) { return pins[i].entry()->offset(); },
        [&](const std::vector<CachePin>& /*pins*/,
            int32_t /*begin*/,
            int32_t /*end*/,
            uint64_t offset,
            const std::vector<folly::Range<char*>>& buffers) {
          stream.read(buffers, offset, LogType::FILE);
        });
    updateStats(stats, isPrefetch, false);
    return pins;
  }

  std::unique_ptr<AbstractInputStreamHolder> input_;
  const int32_t maxCoalesceDistance_;
};

// Represents a CoalescedLoad from local SSD cache.
class SsdLoad : public DwioCoalescedLoadBase {
 public:
  SsdLoad(
      cache::AsyncDataCache& cache,
      std::shared_ptr<IoStatistics> ioStats,
      uint64_t groupId,
      std::vector<CacheRequest*> requests)
      : DwioCoalescedLoadBase(cache, ioStats, groupId, std::move(requests)) {}

  std::vector<CachePin> loadData(bool isPrefetch) override {
    std::vector<SsdPin> ssdPins;
    std::vector<CachePin> pins;
    cache_.makePins(
        keys_,
        [&](int32_t index) { return sizes_[index]; },
        [&](int32_t index, CachePin pin) {
          pins.push_back(std::move(pin));
          ssdPins.push_back(std::move(requests_[index].ssdPin));
        });
    if (pins.empty()) {
      return pins;
    }
    assert(!ssdPins.empty()); // for lint.
    auto stats = ssdPins[0].file()->load(ssdPins, pins);
    updateStats(stats, isPrefetch, true);
    return pins;
  }
};

} // namespace

void CachedBufferedInput::readRegion(
    std::vector<CacheRequest*> requests,
    bool prefetch) {
  if (requests.empty() || (requests.size() == 1 && !prefetch)) {
    return;
  }
  std::shared_ptr<cache::CoalescedLoad> load;
  if (!requests[0]->ssdPin.empty()) {
    load = std::make_shared<SsdLoad>(*cache_, ioStats_, groupId_, requests);
  } else {
    load = std::make_shared<DwioCoalescedLoad>(
        *cache_,
        streamSource_(),
        ioStats_,
        groupId_,
        requests,
        maxCoalesceDistance_);
  }
  allCoalescedLoads_.push_back(load);
  coalescedLoads_.withWLock([&](auto& loads) {
    for (auto& request : requests) {
      loads[request->stream] = load;
    }
  });
}

std::shared_ptr<cache::CoalescedLoad> CachedBufferedInput::coalescedLoad(
    const SeekableInputStream* stream) {
  return coalescedLoads_.withWLock(
      [&](auto& loads) -> std::shared_ptr<cache::CoalescedLoad> {
        auto it = loads.find(stream);
        if (it == loads.end()) {
          return nullptr;
        }
        auto load = std::move(it->second);
        auto dwioLoad = dynamic_cast<DwioCoalescedLoadBase*>(load.get());
        for (auto& request : dwioLoad->requests()) {
          loads.erase(request.stream);
        }
        return load;
      });
}

std::unique_ptr<SeekableInputStream> CachedBufferedInput::read(
    uint64_t offset,
    uint64_t length,
    LogType /*logType*/) const {
  VELOX_CHECK_LE(offset + length, fileSize_);
  return std::make_unique<CacheInputStream>(
      const_cast<CachedBufferedInput*>(this),
      ioStats_.get(),
      Region{offset, length},
      input_,
      fileNum_,
      nullptr,
      TrackingId(),
      0,
      loadQuantum_);
}

} // namespace facebook::velox::dwio::common
