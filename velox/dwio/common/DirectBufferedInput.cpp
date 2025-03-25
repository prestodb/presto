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

#include "velox/dwio/common/DirectBufferedInput.h"
#include "velox/common/memory/Allocation.h"
#include "velox/common/process/TraceContext.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/dwio/common/DirectInputStream.h"

DECLARE_int32(cache_prefetch_min_pct);

using ::facebook::velox::common::Region;
using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::dwio::common {

using cache::CoalescedLoad;
using cache::ScanTracker;
using cache::TrackingId;

std::unique_ptr<SeekableInputStream> DirectBufferedInput::enqueue(
    Region region,
    const StreamIdentifier* sid = nullptr) {
  if (!coalescedLoads_.empty()) {
    // Results of previous load are no more available here.
    coalescedLoads_.clear();
    streamToCoalescedLoad_.wlock()->clear();
  }
  if (region.length == 0) {
    return std::make_unique<SeekableArrayInputStream>(
        static_cast<const char*>(nullptr), 0);
  }

  TrackingId id;
  if (sid != nullptr) {
    id = TrackingId(sid->getId());
  }
  VELOX_CHECK_LE(region.offset + region.length, fileSize_);
  requests_.emplace_back(region, id);
  if (tracker_) {
    tracker_->recordReference(id, region.length, fileNum_, groupId_);
  }
  auto stream = std::make_unique<DirectInputStream>(
      this,
      ioStats_.get(),
      region,
      input_,
      fileNum_,
      tracker_,
      id,
      groupId_,
      options_.loadQuantum());
  requests_.back().stream = stream.get();
  return stream;
}

bool DirectBufferedInput::isBuffered(uint64_t /*offset*/, uint64_t /*length*/)
    const {
  return false;
}

bool DirectBufferedInput::shouldPreload(int32_t numPages) {
  return false;
}

namespace {

// True if the percentage is high enough to warrant prefetch.
bool isPrefetchablePct(int32_t pct) {
  return pct >= FLAGS_cache_prefetch_min_pct;
}

bool lessThan(const LoadRequest* left, const LoadRequest* right) {
  return *left < *right;
}

} // namespace

void DirectBufferedInput::load(const LogType /*unused*/) {
  // After load, new requests cannot be merged into pre-load ones.
  auto requests = std::move(requests_);
  std::vector<LoadRequest*> storageLoad[2];
  for (auto& request : requests) {
    cache::TrackingData trackingData;
    const bool prefetchAnyway = request.trackingId.empty() ||
        request.trackingId.id() == StreamIdentifier::sequentialFile().id_;
    if (!prefetchAnyway && tracker_) {
      trackingData = tracker_->trackingData(request.trackingId);
    }
    const int loadIndex =
        (prefetchAnyway || isPrefetchablePct(adjustedReadPct(trackingData)))
        ? 1
        : 0;
    storageLoad[loadIndex].push_back(&request);
  }
  std::sort(storageLoad[1].begin(), storageLoad[1].end(), lessThan);
  std::sort(storageLoad[0].begin(), storageLoad[0].end(), lessThan);
  std::vector<int32_t> groupEnds[2];
  groupEnds[1] = groupRequests(storageLoad[1], true);
  moveCoalesced(
      storageLoad[1],
      groupEnds[1],
      storageLoad[0],
      [](auto* request) { return request->region.offset; },
      [](auto* request) {
        return request->region.offset + request->region.length;
      });
  groupEnds[0] = groupRequests(storageLoad[0], false);
  readRegions(storageLoad[1], true, groupEnds[1]);
  readRegions(storageLoad[0], false, groupEnds[0]);
}

std::vector<int32_t> DirectBufferedInput::groupRequests(
    const std::vector<LoadRequest*>& requests,
    bool prefetch) const {
  if (requests.empty() || (requests.size() < 2 && !prefetch)) {
    // A single request has no other requests to coalesce with and is not
    // eligible to prefetch. This will be loaded by itself on first use.
    return {};
  }
  const int32_t maxDistance = options_.maxCoalesceDistance();
  const auto loadQuantum = options_.loadQuantum();
  // If reading densely accessed, coalesce into large for best throughput, if
  // for sparse, coalesce to quantum to reduce overread. Not all sparse access
  // is correlated.
  const auto maxCoalesceBytes =
      prefetch ? options_.maxCoalesceBytes() : loadQuantum;

  // Combine adjacent short reads.
  int64_t coalescedBytes = 0;
  std::vector<int32_t> ends;
  ends.reserve(requests.size());
  std::vector<char> ranges;
  coalesceIo<LoadRequest*, char>(
      requests,
      maxDistance,
      // Break batches up. Better load more short ones i parallel.
      std::numeric_limits<int32_t>::max(), // limit coalesce by size, not count.
      [&](int32_t index) { return requests[index]->region.offset; },
      [&](int32_t index) -> int32_t {
        auto size = requests[index]->region.length;
        if (size > loadQuantum) {
          coalescedBytes += loadQuantum;
          return loadQuantum;
        }
        coalescedBytes += size;
        return size;
      },
      [&](int32_t index) {
        if (coalescedBytes > maxCoalesceBytes) {
          coalescedBytes = 0;
          return kNoCoalesce;
        }
        return 1;
      },
      [&](LoadRequest* /*request*/, std::vector<char>& ranges) {
        // ranges.size() is used in coalesceIo so we cannot leave it empty.
        ranges.push_back(0);
      },
      [&](int32_t /*gap*/, std::vector<char> /*ranges*/) { /*no op*/ },
      [&](const std::vector<LoadRequest*>& /*requests*/,
          int32_t /*begin*/,
          int32_t end,
          uint64_t /*offset*/,
          const std::vector<char>& /*ranges*/) { ends.push_back(end); });
  return ends;
}

void DirectBufferedInput::readRegion(
    const std::vector<LoadRequest*>& requests,
    bool prefetch) {
  if (requests.empty() || (requests.size() == 1 && !prefetch)) {
    return;
  }
  auto load = std::make_shared<DirectCoalescedLoad>(
      input_,
      ioStats_,
      fsStats_,
      groupId_,
      requests,
      pool_,
      options_.loadQuantum());
  coalescedLoads_.push_back(load);
  streamToCoalescedLoad_.withWLock([&](auto& loads) {
    for (auto& request : requests) {
      loads[request->stream] = load;
    }
  });
}

void DirectBufferedInput::readRegions(
    const std::vector<LoadRequest*>& requests,
    bool prefetch,
    const std::vector<int32_t>& groupEnds) {
  int i = 0;
  std::vector<LoadRequest*> group;
  for (auto end : groupEnds) {
    while (i < end) {
      group.push_back(requests[i++]);
    }
    readRegion(group, prefetch);
    group.clear();
  }
  if (prefetch && executor_) {
    for (auto i = 0; i < coalescedLoads_.size(); ++i) {
      auto& load = coalescedLoads_[i];
      if (load->state() == CoalescedLoad::State::kPlanned) {
        AsyncLoadHolder loadHolder{
            .load = load, .pool = pool_->shared_from_this()};
        executor_->add([asyncLoad = std::move(loadHolder)]() {
          process::TraceContext trace("Read Ahead");
          VELOX_CHECK_NOT_NULL(asyncLoad.load);
          asyncLoad.load->loadOrFuture(nullptr);
        });
      }
    }
  }
}

std::shared_ptr<DirectCoalescedLoad> DirectBufferedInput::coalescedLoad(
    const SeekableInputStream* stream) {
  return streamToCoalescedLoad_.withWLock(
      [&](auto& loads) -> std::shared_ptr<DirectCoalescedLoad> {
        auto it = loads.find(stream);
        if (it == loads.end()) {
          return nullptr;
        }
        auto load = std::move(it->second);
        loads.erase(it);
        return load;
      });
}

std::unique_ptr<SeekableInputStream> DirectBufferedInput::read(
    uint64_t offset,
    uint64_t length,
    LogType /*logType*/) const {
  VELOX_CHECK_LE(offset + length, fileSize_);
  return std::make_unique<DirectInputStream>(
      const_cast<DirectBufferedInput*>(this),
      ioStats_.get(),
      Region{offset, length},
      input_,
      fileNum_,
      nullptr,
      TrackingId(),
      0,
      options_.loadQuantum());
}

namespace {
void appendRanges(
    memory::Allocation& allocation,
    size_t length,
    std::vector<folly::Range<char*>>& buffers) {
  uint64_t offsetInRuns = 0;
  for (int i = 0; i < allocation.numRuns(); ++i) {
    auto run = allocation.runAt(i);
    const uint64_t bytes = memory::AllocationTraits::pageBytes(run.numPages());
    const uint64_t readSize = std::min(bytes, length - offsetInRuns);
    buffers.push_back(folly::Range<char*>(run.data<char>(), readSize));
    offsetInRuns += readSize;
  }
}
} // namespace

std::vector<cache::CachePin> DirectCoalescedLoad::loadData(bool prefetch) {
  std::vector<folly::Range<char*>> buffers;
  int64_t lastEnd = requests_[0].region.offset;
  int64_t size = 0;
  int64_t overread = 0;

  for (auto& request : requests_) {
    const auto& region = request.region;
    if (region.offset > lastEnd) {
      buffers.push_back(folly::Range<char*>(
          nullptr,
          reinterpret_cast<char*>(
              static_cast<uint64_t>(region.offset - lastEnd))));
      overread += buffers.back().size();
    }

    if (region.length > DirectBufferedInput::kTinySize) {
      if (&request != &requests_.back()) {
        // Case where request is a little over quantum but is followed by
        // another within the max distance. Coalesces and allows reading the
        // region of max quantum + max distance in one piece.
        request.loadSize = region.length;
      } else {
        request.loadSize = std::min<int32_t>(region.length, loadQuantum_);
      }
      const auto numPages =
          memory::AllocationTraits::numPages(request.loadSize);
      pool_->allocateNonContiguous(numPages, request.data);
      appendRanges(request.data, request.loadSize, buffers);
    } else {
      request.loadSize = region.length;
      request.tinyData.resize(region.length);
      buffers.push_back(folly::Range(request.tinyData.data(), region.length));
    }
    lastEnd = region.offset + request.loadSize;
    size += request.loadSize;
  }

  uint64_t usecs = 0;
  {
    MicrosecondTimer timer(&usecs);
    input_->read(buffers, requests_[0].region.offset, LogType::FILE);
  }

  ioStats_->read().increment(size + overread);
  ioStats_->incRawBytesRead(size);
  ioStats_->incTotalScanTime(usecs * 1'000);
  ioStats_->queryThreadIoLatency().increment(usecs);
  ioStats_->incRawOverreadBytes(overread);
  if (prefetch) {
    ioStats_->prefetch().increment(size + overread);
  }
  TestValue::adjust(
      "facebook::velox::cache::DirectCoalescedLoad::loadData", this);
  return {};
}

int32_t DirectCoalescedLoad::getData(
    int64_t offset,
    memory::Allocation& data,
    std::string& tinyData) {
  auto it = std::lower_bound(
      requests_.begin(), requests_.end(), offset, [](auto& x, auto offset) {
        return x.region.offset < offset;
      });
  if (it == requests_.end() || it->region.offset != offset) {
    return 0;
  }
  data = std::move(it->data);
  tinyData = std::move(it->tinyData);
  return it->loadSize;
}

} // namespace facebook::velox::dwio::common
