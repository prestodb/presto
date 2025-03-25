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
#include "velox/common/memory/Allocation.h"
#include "velox/common/process/TraceContext.h"
#include "velox/dwio/common/CacheInputStream.h"

DECLARE_int32(cache_prefetch_min_pct);

using ::facebook::velox::common::Region;

namespace facebook::velox::dwio::common {

using cache::CachePin;
using cache::CoalescedLoad;
using cache::RawFileCacheKey;
using cache::ScanTracker;
using cache::SsdFile;
using cache::SsdPin;
using cache::TrackingId;
using memory::MemoryAllocator;

std::unique_ptr<SeekableInputStream> CachedBufferedInput::enqueue(
    Region region,
    const StreamIdentifier* sid = nullptr) {
  if (region.length == 0) {
    return std::make_unique<SeekableArrayInputStream>(
        static_cast<const char*>(nullptr), 0);
  }

  TrackingId id;
  if (sid != nullptr) {
    id = TrackingId(sid->getId());
  }
  VELOX_CHECK_LE(region.offset + region.length, fileSize_);
  requests_.emplace_back(
      RawFileCacheKey{fileNum_, region.offset}, region.length, id);
  if (tracker_ != nullptr) {
    tracker_->recordReference(id, region.length, fileNum_, groupId_);
  }
  auto stream = std::make_unique<CacheInputStream>(
      this,
      ioStats_.get(),
      region,
      input_,
      fileNum_,
      options_.noCacheRetention(),
      tracker_,
      id,
      groupId_,
      options_.loadQuantum());
  requests_.back().stream = stream.get();
  return stream;
}

bool CachedBufferedInput::isBuffered(uint64_t /*offset*/, uint64_t /*length*/)
    const {
  return false;
}

bool CachedBufferedInput::shouldPreload(int32_t numPages) {
  // True if after scheduling this for preload, half the capacity would be in a
  // loading but not yet accessed state.
  if (requests_.empty() && (numPages == 0)) {
    return false;
  }
  for (const auto& request : requests_) {
    numPages += memory::AllocationTraits::numPages(
        std::min<int32_t>(request.size, options_.loadQuantum()));
  }
  const auto cachePages = cache_->incrementCachedPages(0);
  auto* allocator = cache_->allocator();
  const auto maxPages =
      memory::AllocationTraits::numPages(allocator->capacity());
  const auto allocatedPages = allocator->numAllocated();
  if (numPages < maxPages - allocatedPages) {
    // There is free space for the read-ahead.
    return true;
  }
  const auto prefetchPages = cache_->incrementPrefetchPages(0);
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
  const bool prefetchOne =
      request.trackingId.id() == StreamIdentifier::sequentialFile().id_;
  const auto readDensity =
      trackingData.readBytes / (1 + trackingData.referencedBytes);
  const auto readPct = 100 * readDensity;
  const bool prefetch = trackingData.referencedBytes > 0 &&
      isPrefetchPct(readPct) && readDensity >= 0.8;
  std::vector<CacheRequest*> parts;
  for (uint64_t offset = 0; offset < request.size; offset += loadQuantum) {
    const int32_t size = std::min<int32_t>(loadQuantum, request.size - offset);
    extraRequests.push_back(std::make_unique<CacheRequest>(
        RawFileCacheKey{request.key.fileNum, request.key.offset + offset},
        size,
        request.trackingId));
    parts.push_back(extraRequests.back().get());
    parts.back()->coalesces = prefetch;
    if (prefetchOne) {
      break;
    }
  }
  return parts;
}

template <bool kSsd>
uint64_t getOffset(const CacheRequest& request) {
  if constexpr (kSsd) {
    VELOX_DCHECK(!request.ssdPin.empty());
    return request.ssdPin.run().offset();
  } else {
    return request.key.offset;
  }
}

template <bool kSsd>
bool lessThan(const CacheRequest* left, const CacheRequest* right) {
  auto leftOffset = getOffset<kSsd>(*left);
  auto rightOffset = getOffset<kSsd>(*right);
  return leftOffset < rightOffset ||
      (leftOffset == rightOffset && left->size > right->size);
}

} // namespace

void CachedBufferedInput::load(const LogType /*unused*/) {
  // 'requests_ is cleared on exit.
  auto requests = std::move(requests_);
  cache::SsdFile* ssdFile{nullptr};
  auto* ssdCache = cache_->ssdCache();
  if (ssdCache != nullptr) {
    ssdFile = &ssdCache->file(fileNum_);
  }

  // Extra requests made for pre-loadable regions that are larger than
  // 'loadQuantum'.
  std::vector<std::unique_ptr<CacheRequest>> extraRequests;
  std::vector<CacheRequest*> storageLoad[2];
  std::vector<CacheRequest*> ssdLoad[2];
  for (auto& request : requests) {
    cache::TrackingData trackingData;
    const bool prefetchAnyway = request.trackingId.empty() ||
        request.trackingId.id() == StreamIdentifier::sequentialFile().id_;
    if (!prefetchAnyway && (tracker_ != nullptr)) {
      trackingData = tracker_->trackingData(request.trackingId);
    }
    const int loadIndex =
        (prefetchAnyway || isPrefetchPct(adjustedReadPct(trackingData))) ? 1
                                                                         : 0;
    auto parts = makeRequestParts(
        request, trackingData, options_.loadQuantum(), extraRequests);
    for (auto part : parts) {
      if (cache_->exists(part->key)) {
        continue;
      }
      if (ssdFile != nullptr) {
        part->ssdPin = ssdFile->find(part->key);
        if (!part->ssdPin.empty() && part->ssdPin.run().size() < part->size) {
          LOG(INFO) << "IOERR: Ignoring SSD shorter than requested: "
                    << part->ssdPin.run().size() << " vs " << part->size;
          part->ssdPin.clear();
        }
        if (!part->ssdPin.empty()) {
          ssdLoad[loadIndex].push_back(part);
          continue;
        }
      }
      storageLoad[loadIndex].push_back(part);
    }
  }

  std::sort(storageLoad[0].begin(), storageLoad[0].end(), lessThan<false>);
  std::sort(storageLoad[1].begin(), storageLoad[1].end(), lessThan<false>);
  std::sort(ssdLoad[0].begin(), ssdLoad[0].end(), lessThan<true>);
  std::sort(ssdLoad[1].begin(), ssdLoad[1].end(), lessThan<true>);
  makeLoads<false>(storageLoad);
  makeLoads<true>(ssdLoad);
}

template <bool kSsd>
void CachedBufferedInput::makeLoads(std::vector<CacheRequest*> requests[2]) {
  std::vector<int32_t> groupEnds[2];
  groupEnds[1] = groupRequests<kSsd>(requests[1], true);
  moveCoalesced(
      requests[1],
      groupEnds[1],
      requests[0],
      [](auto* request) { return getOffset<kSsd>(*request); },
      [](auto* request) { return getOffset<kSsd>(*request) + request->size; });
  groupEnds[0] = groupRequests<kSsd>(requests[0], false);
  readRegions(requests[1], true, groupEnds[1]);
  readRegions(requests[0], false, groupEnds[0]);
}

template <bool kSsd>
std::vector<int32_t> CachedBufferedInput::groupRequests(
    const std::vector<CacheRequest*>& requests,
    bool prefetch) const {
  if (requests.empty() || (requests.size() < 2 && !prefetch)) {
    return {};
  }
  const int32_t maxDistance = kSsd ? 20000 : options_.maxCoalesceDistance();

  // Combine adjacent short reads.
  int64_t coalescedBytes = 0;
  std::vector<int32_t> ends;
  ends.reserve(requests.size());
  std::vector<char> ranges;
  coalesceIo<CacheRequest*, char>(
      requests,
      maxDistance,
      std::numeric_limits<int32_t>::max(),
      [&](int32_t index) { return getOffset<kSsd>(*requests[index]); },
      [&](int32_t index) {
        const auto size = requests[index]->size;
        coalescedBytes += size;
        return size;
      },
      [&](int32_t index) {
        if (coalescedBytes > options_.maxCoalesceBytes()) {
          coalescedBytes = 0;
          return kNoCoalesce;
        }
        return requests[index]->coalesces ? 1 : kNoCoalesce;
      },
      [&](CacheRequest* /*request*/, std::vector<char>& ranges) {
        ranges.push_back(0);
      },
      [&](int32_t /*gap*/, std::vector<char> /*ranges*/) { /*no op*/ },
      [&](const std::vector<CacheRequest*>& /*requests*/,
          int32_t /*begin*/,
          int32_t end,
          uint64_t /*offset*/,
          const std::vector<char>& /*ranges*/) { ends.push_back(end); });
  return ends;
}

namespace {
// Base class for CoalescedLoads for different storage types.
class DwioCoalescedLoadBase : public cache::CoalescedLoad {
 public:
  DwioCoalescedLoadBase(
      cache::AsyncDataCache& cache,
      std::shared_ptr<IoStatistics> ioStats,
      std::shared_ptr<filesystems::File::IoStats> fsStats,
      uint64_t groupId,
      std::vector<CacheRequest*> requests)
      : CoalescedLoad(makeKeys(requests), makeSizes(requests)),
        cache_(cache),
        ioStats_(std::move(ioStats)),
        fsStats_(std::move(fsStats)),
        groupId_(groupId) {
    requests_.reserve(requests.size());
    for (const auto& request : requests) {
      size_ += request->size;
      requests_.push_back(std::move(*request));
    }
  }

  const std::vector<CacheRequest>& requests() {
    return requests_;
  }

  int64_t size() const override {
    return size_;
  }

  std::string toString() const override {
    int32_t payload = 0;
    VELOX_CHECK(!requests_.empty());

    int32_t total = requests_.back().key.offset + requests_.back().size -
        requests_[0].key.offset;
    for (const auto& request : requests_) {
      payload += request.size;
    }
    return fmt::format(
        "<CoalescedLoad: {} entries, {} total {} extra>",
        requests_.size(),
        succinctBytes(total),
        succinctBytes(total - payload));
  }

 protected:
  void updateStats(const CoalesceIoStats& stats, bool prefetch, bool ssd) {
    if (ioStats_ == nullptr) {
      return;
    }
    ioStats_->incRawOverreadBytes(stats.extraBytes);
    if (ssd) {
      ioStats_->ssdRead().increment(stats.payloadBytes);
    } else {
      ioStats_->read().increment(stats.payloadBytes);
    }
    if (prefetch) {
      ioStats_->prefetch().increment(stats.payloadBytes);
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
  std::shared_ptr<filesystems::File::IoStats> fsStats_;
  const uint64_t groupId_;
  int64_t size_{0};
};

// Represents a CoalescedLoad from ReadFile, e.g. disagg disk.
class DwioCoalescedLoad : public DwioCoalescedLoadBase {
 public:
  DwioCoalescedLoad(
      cache::AsyncDataCache& cache,
      std::shared_ptr<ReadFileInputStream> input,
      std::shared_ptr<IoStatistics> ioStats,
      std::shared_ptr<filesystems::File::IoStats> fsStats,
      uint64_t groupId,
      std::vector<CacheRequest*> requests,
      int32_t maxCoalesceDistance)
      : DwioCoalescedLoadBase(
            cache,
            std::move(ioStats),
            std::move(fsStats),
            groupId,
            std::move(requests)),
        input_(std::move(input)),
        maxCoalesceDistance_(maxCoalesceDistance) {}

  std::vector<CachePin> loadData(bool prefetch) override {
    std::vector<CachePin> pins;
    pins.reserve(keys_.size());
    cache_.makePins(
        keys_,
        [&](int32_t index) { return sizes_[index]; },
        [&](int32_t /*index*/, CachePin pin) {
          if (prefetch) {
            pin.checkedEntry()->setPrefetch(true);
          }
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
          input_->read(buffers, offset, LogType::FILE);
        });
    updateStats(stats, prefetch, false);
    return pins;
  }

  std::shared_ptr<ReadFileInputStream> input_;
  const int32_t maxCoalesceDistance_;
};

// Represents a CoalescedLoad from local SSD cache.
class SsdLoad : public DwioCoalescedLoadBase {
 public:
  SsdLoad(
      cache::AsyncDataCache& cache,
      std::shared_ptr<IoStatistics> ioStats,
      std::shared_ptr<filesystems::File::IoStats> fsStats,
      uint64_t groupId,
      std::vector<CacheRequest*> requests)
      : DwioCoalescedLoadBase(
            cache,
            std::move(ioStats),
            std::move(fsStats),
            groupId,
            std::move(requests)) {}

  std::vector<CachePin> loadData(bool prefetch) override {
    std::vector<SsdPin> ssdPins;
    std::vector<CachePin> pins;
    cache_.makePins(
        keys_,
        [&](int32_t index) { return sizes_[index]; },
        [&](int32_t index, CachePin pin) {
          if (prefetch) {
            pin.checkedEntry()->setPrefetch(true);
          }
          pins.push_back(std::move(pin));
          ssdPins.push_back(std::move(requests_[index].ssdPin));
        });
    if (pins.empty()) {
      return pins;
    }
    assert(!ssdPins.empty()); // for lint.
    const auto stats = ssdPins[0].file()->load(ssdPins, pins);
    updateStats(stats, prefetch, true);
    return pins;
  }
};

} // namespace

void CachedBufferedInput::readRegion(
    const std::vector<CacheRequest*>& requests,
    bool prefetch) {
  if (requests.empty() || (requests.size() == 1 && !prefetch)) {
    return;
  }

  std::shared_ptr<cache::CoalescedLoad> load;
  if (!requests[0]->ssdPin.empty()) {
    load = std::make_shared<SsdLoad>(
        *cache_, ioStats_, fsStats_, groupId_, requests);
  } else {
    load = std::make_shared<DwioCoalescedLoad>(
        *cache_,
        input_,
        ioStats_,
        fsStats_,
        groupId_,
        requests,
        options_.maxCoalesceDistance());
  }
  allCoalescedLoads_.push_back(load);
  coalescedLoads_.withWLock([&](auto& loads) {
    for (auto& request : requests) {
      loads[request->stream] = load;
    }
  });
}

void CachedBufferedInput::readRegions(
    const std::vector<CacheRequest*>& requests,
    bool prefetch,
    const std::vector<int32_t>& groupEnds) {
  int i = 0;
  std::vector<CacheRequest*> group;
  for (auto end : groupEnds) {
    while (i < end) {
      group.push_back(requests[i++]);
    }
    readRegion(group, prefetch);
    group.clear();
  }
  if (prefetch && executor_) {
    std::vector<int32_t> doneIndices;
    for (auto i = 0; i < allCoalescedLoads_.size(); ++i) {
      auto& load = allCoalescedLoads_[i];
      if (load->state() == CoalescedLoad::State::kPlanned) {
        executor_->add(
            [pendingLoad = load, ssdSavable = !options_.noCacheRetention()]() {
              process::TraceContext trace("Read Ahead");
              pendingLoad->loadOrFuture(nullptr, ssdSavable);
            });
      } else {
        doneIndices.push_back(i);
      }
    }
    // Remove the loads that were complete. There can be done loads if the same
    // CachedBufferedInput has multiple cycles of enqueues and loads.
    for (int i = 0, j = 0, k = 0; i < allCoalescedLoads_.size(); ++i) {
      if (j < doneIndices.size() && doneIndices[j] == i) {
        ++j;
      } else {
        allCoalescedLoads_[k++] = std::move(allCoalescedLoads_[i]);
      }
    }
    allCoalescedLoads_.resize(allCoalescedLoads_.size() - doneIndices.size());
  }
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
        auto* dwioLoad = static_cast<DwioCoalescedLoadBase*>(load.get());
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
      options_.noCacheRetention(),
      nullptr,
      TrackingId(),
      0,
      options_.loadQuantum());
}

bool CachedBufferedInput::prefetch(Region region) {
  const int32_t numPages = memory::AllocationTraits::numPages(region.length);
  if (!shouldPreload(numPages)) {
    return false;
  }
  auto stream = enqueue(region, nullptr);
  load(LogType::FILE);
  // Remove the coalesced load made for the stream. It will not be accessed. The
  // cache entry will be accessed.
  coalescedLoad(stream.get());
  return true;
}

} // namespace facebook::velox::dwio::common
