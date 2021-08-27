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

#include "velox/dwio/dwrf/common/CachedBufferedInput.h"
#include "velox/dwio/dwrf/common/CacheInputStream.h"

namespace facebook::velox::dwrf {

using cache::CachePin;
using cache::LoadState;
using cache::RawFileCacheKey;
using cache::ScanTracker;
using cache::TrackingId;
using memory::MappedMemory;

std::unique_ptr<SeekableInputStream> CachedBufferedInput::enqueue(
    dwio::common::Region region,
    const StreamIdentifier* si = nullptr) {
  TrackingId id;
  if (si) {
    id = TrackingId(si->node, si->kind);
  }
  requests_.emplace_back(CacheRequest{
      RawFileCacheKey{fileNum_, region.offset}, region.length, id, CachePin()});
  tracker_->recordReference(id, region.length, groupId_);
  return std::make_unique<CacheInputStream>(
      cache_, region, input_, fileNum_, tracker_, id, groupId_);
}

  bool CachedBufferedInput::isBuffered(uint64_t /*offset*/, uint64_t /*length*/) const {
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
                    std::min<int32_t>(
                        request.size, CacheInputStream::kDefaultLoadQuantum),
                    MappedMemory::kPageSize) /
        MappedMemory::kPageSize;
  }
  auto cachePages = cache_->incrementCachedPages(0);
  auto maxPages = cache_->maxBytes() / MappedMemory::kPageSize;
  auto allocatedPages = cache_->numAllocated();
  if (numPages < maxPages - allocatedPages) {
    return true;
  }
  auto prefetchPages = cache_->incrementPrefetchPages(0);
  if (numPages + prefetchPages < cachePages / 2) {
    return true;
  }
  return false;
}

void CachedBufferedInput::load(const dwio::common::LogType) {
  std::vector<CacheRequest*> toLoad;
  // 'requests_ is cleared on exit.
  int32_t numNewLoads = 0;
  auto requests = std::move(requests_);
  for (auto& request : requests) {
    if (tracker_->shouldPrefetch(request.trackingId, 3)) {
      request.pin = cache_->findOrCreate(request.key, request.size, nullptr);
      if (request.pin.empty()) {
        // Already loading for another thread.
        continue;
      }
      if (request.pin.entry()->isExclusive()) {
        // A new entry to be filled.
        request.pin.entry()->setPrefetch();
        toLoad.push_back(&request);
      } else {
        // Already in cache, access time is refreshed.
        request.pin.clear();
      }
    }
  }
  if (toLoad.empty()) {
    return;
  }
  loadFromSsd(toLoad);
  if (toLoad.empty()) {
    return;
  }
  std::sort(
      toLoad.begin(),
      toLoad.end(),
      [&](const CacheRequest* left, const CacheRequest* right) {
        return left->key.offset < right->key.offset;
      });
  // Combine adjacent short reads.
  dwio::common::Region last = {0, 0};
  std::vector<CachePin> readBatch;

  for (const auto& request : toLoad) {
    auto* entry = request->pin.entry();
    auto entryRegion = dwio::common::Region{
        static_cast<uint64_t>(entry->offset()),
        static_cast<uint64_t>(entry->size())};
    VELOX_CHECK_LT(0, entryRegion.length);
    if (last.length == 0) {
      // first region
      last = entryRegion;
    } else if (!tryMerge(last, entryRegion)) {
      ++numNewLoads;
      readRegion(std::move(readBatch));
      last = entryRegion;
    }
    readBatch.push_back(std::move(request->pin));
  }
  ++numNewLoads;
  readRegion(std::move(readBatch));
  if (executor_ && numNewLoads > 1) {
    for (auto& load : fusedLoads_) {
      if (load->state() == LoadState::kPlanned) {
        executor_->add(
            [pendingLoad = load]() { pendingLoad->loadOrFuture(nullptr); });
      }
    }
  }
}

bool CachedBufferedInput::tryMerge(
    dwio::common::Region& first,
    const dwio::common::Region& second) {
  DWIO_ENSURE_GE(second.offset, first.offset, "regions should be sorted.");
  int64_t gap = second.offset - first.offset - first.length;
  if (gap < 0) {
    // We do not support one region going to two target buffers.
    return false;
  }
  // compare with 0 since it's comparison in different types
  if (gap <= kMaxMergeDistance) {
    int64_t extension = gap + second.length;

    if (extension > 0) {
      first.length += extension;
      if ((input_.getStats() != nullptr) && gap > 0) {
        input_.getStats()->incRawOverreadBytes(gap);
      }
    }

    return true;
  }

  return false;
}

namespace {
class DwrfFusedLoad : public cache::FusedLoad {
 public:
  void initialize(
      std::vector<CachePin>&& pins,
      std::unique_ptr<AbstractInputStreamHolder> input) {
    input_ = std::move(input);
    cache::FusedLoad::initialize(std::move(pins));
  }

  void loadData() override {
    auto& stream = input_->get();
    std::vector<folly::Range<char*>> buffers;
    uint64_t start = pins_[0].entry()->offset();
    uint64_t lastOffset = start;
    for (auto& pin : pins_) {
      auto& buffer = pin.entry()->data();
      uint64_t startOffset = pin.entry()->offset();
      if (lastOffset < startOffset) {
        buffers.push_back(
            folly::Range<char*>(nullptr, startOffset - lastOffset));
      }

      auto size = pin.entry()->size();
      uint64_t offsetInRuns = 0;
      if (buffer.numPages() == 0) {
        buffers.push_back(folly::Range<char*>(pin.entry()->tinyData(), size));
        offsetInRuns = size;
      } else {
        for (int i = 0; i < buffer.numRuns(); ++i) {
          velox::memory::MappedMemory::PageRun run = buffer.runAt(i);
          uint64_t bytes = run.numBytes();
          uint64_t readSize = std::min(bytes, size - offsetInRuns);
          buffers.push_back(folly::Range<char*>(run.data<char>(), readSize));
          offsetInRuns += readSize;
        }
      }
      DWIO_ENSURE(offsetInRuns == size);
      lastOffset = startOffset + size;
    }

    stream.read(buffers, start, dwio::common::LogType::FILE);
  }

 private:
  std::unique_ptr<AbstractInputStreamHolder> input_;
};
} // namespace

void CachedBufferedInput::readRegion(std::vector<CachePin> pins) {
  auto load = std::make_shared<DwrfFusedLoad>();
  load->initialize(std::move(pins), streamSource_());
  fusedLoads_.push_back(load);
}

std::unique_ptr<SeekableInputStream> CachedBufferedInput::read(
    uint64_t offset,
    uint64_t length,
    dwio::common::LogType /*logType*/) const {
  return std::make_unique<CacheInputStream>(
      cache_,
      dwio::common::Region{offset, length},
      input_,
      fileNum_,
      nullptr,
      TrackingId(),
      0);
}

  void CachedBufferedInput::loadFromSsd(std::vector<CacheRequest*> /*requests*/) {
  // No op placeholder.
}
} // namespace facebook::velox::dwrf
