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

#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/FileIds.h"
#include "velox/common/caching/SsdFile.h"

#include <folly/executors/QueuedImmediateExecutor.h>
#include "velox/common/caching/FileIds.h"

namespace facebook::velox::cache {

using memory::MachinePageCount;
using memory::MappedMemory;

AsyncDataCacheEntry::AsyncDataCacheEntry(CacheShard* shard)
    : shard_(shard), data_(shard->cache()) {}

void AsyncDataCacheEntry::setExclusiveToShared() {
  VELOX_CHECK(isExclusive());
  numPins_ = 1;
  std::unique_ptr<folly::SharedPromise<bool>> promise;
  {
    std::lock_guard<std::mutex> l(shard_->mutex());
    // Enter the shard's mutex to make sure a promise is not being added during
    // the move.
    promise = std::move(promise_);
  }
  if (promise) {
    promise->setValue(true);
  }

  // The entry may now have other readers, It is safe to do read-only
  // ops like integrity and notifying SSD cache of another candidate.
  auto hook = shard_->cache()->verifyHook();
  if (hook) {
    hook(*this);
  }
}

void AsyncDataCacheEntry::release() {
  VELOX_CHECK_NE(0, numPins_);
  if (numPins_ == kExclusive) {
    // Dereferencing an exclusive entry without converting to shared
    // means that the content could not be shared, e.g. error in
    // loading.
    shard_->removeEntry(this);
    // After the entry is removed from the hash table, a promise can no longer
    // be made. It is safe to move the promise and realize it.
    auto promise = std::move(promise_);
    numPins_ = 0;
    if (promise) {
      promise->setValue(true);
    }
  } else {
    auto oldPins = numPins_.fetch_add(-1);
    VELOX_CHECK_LE(1, oldPins, "pin count goes negative");
  }
}

void AsyncDataCacheEntry::addReference() {
  VELOX_CHECK(!isExclusive());
  ++numPins_;
}

memory::MachinePageCount AsyncDataCacheEntry::setPrefetch(bool flag) {
  isPrefetch_ = flag;
  auto numPages = bits::roundUp(size_, memory::MappedMemory::kPageSize) /
      memory::MappedMemory::kPageSize;
  return shard_->cache()->incrementPrefetchPages(flag ? numPages : -numPages);
}

void AsyncDataCacheEntry::initialize(FileCacheKey key, int32_t size) {
  VELOX_CHECK(isExclusive());
  VELOX_CHECK_EQ(0, size_);
  setSsdFile(nullptr, 0);
  accessStats_.reset();
  key_ = std::move(key);
  auto cache = shard_->cache();
  ClockTimer t(shard_->allocClocks());
  if (size < AsyncDataCacheEntry::kTinyDataSize) {
    tinyData_.resize(size);
  } else {
    tinyData_.clear();
    auto sizePages =
        bits::roundUp(size, MappedMemory::kPageSize) / MappedMemory::kPageSize;
    if (cache->allocate(sizePages, CacheShard::kCacheOwner, data_)) {
      cache->incrementCachedPages(data().numPages());
    } else {
      // No memory to cover 'this'.
      // Remove from 'shard_'s map and unpin.
      shard_->removeEntry(this);
      release();
      _VELOX_THROW(
          VeloxRuntimeError,
          error_source::kErrorSourceRuntime.c_str(),
          error_code::kNoCacheSpace.c_str(),
          /* isRetriable */ true);
    }
  }
  size_ = size;
}

std::unique_ptr<AsyncDataCacheEntry> CacheShard::getFreeEntryWithSize(
    uint64_t /*sizeHint*/) {
  std::unique_ptr<AsyncDataCacheEntry> newEntry;
  if (freeEntries_.empty()) {
    newEntry = std::make_unique<AsyncDataCacheEntry>(this);
  } else {
    newEntry = std::move(freeEntries_.back());
    freeEntries_.pop_back();
  }
  return newEntry;
}

CachePin CacheShard::findOrCreate(
    RawFileCacheKey key,
    uint64_t size,
    folly::SemiFuture<bool>* wait) {
  AsyncDataCacheEntry* entryToInit = nullptr;
  {
    std::lock_guard<std::mutex> l(mutex_);
    ++eventCounter_;
    auto it = entryMap_.find(key);
    if (it != entryMap_.end()) {
      auto found = it->second;
      if (found->isExclusive()) {
        ++numWaitExclusive_;
        if (!wait) {
          return CachePin();
        }
        *wait = found->getFuture();
        return CachePin();
      }
      found->touch();
      // The entry is in a readable state. Add a pin.
      if (found->isPrefetch_) {
        found->isFirstUse_ = true;
        found->setPrefetch(false);
      } else {
        ++numHit_;
      }
      ++found->numPins_;
      CachePin pin;
      pin.setEntry(found);
      return pin;
    } else {
      auto newEntry = getFreeEntryWithSize(size);
      // Initialize the members that must be set inside 'mutex_'.
      newEntry->numPins_ = AsyncDataCacheEntry::kExclusive;
      newEntry->promise_ = nullptr;
      entryToInit = newEntry.get();
      entryMap_[key] = newEntry.get();
      if (emptySlots_.empty()) {
        entries_.push_back(std::move(newEntry));
      } else {
        auto index = emptySlots_.back();
        emptySlots_.pop_back();
        entries_[index] = std::move(newEntry);
      }
    }
    ++numNew_;
  }
  return initEntry(key, entryToInit, size);
}

bool CacheShard::exists(RawFileCacheKey key) const {
  std::lock_guard<std::mutex> l(mutex_);
  auto it = entryMap_.find(key);
  if (it != entryMap_.end()) {
    it->second->touch();
    return true;
  }
  return false;
}

CachePin CacheShard::initEntry(
    RawFileCacheKey key,
    AsyncDataCacheEntry* entry,
    int64_t size) {
  //   The new entry is in the map and is in
  // exclusive mode and is otherwise uninitialized. Other threads may
  // find it and may add a Promis or wait for a promise that another
  // one has added. The new entry is otherwise volatile and
  // uninterpretable except for this thread. Non access serializing
  // members can be set outside of 'mutex_'.
  entry->initialize(
      FileCacheKey{StringIdLease(fileIds(), key.fileNum), key.offset}, size);
  cache_->incrementNew(size);
  CachePin pin;
  pin.setEntry(entry);
  return pin;
}

CoalescedLoad::~CoalescedLoad() {
  // Continue possibly waiting threads.
  setEndState(LoadState::kCancelled);
}

bool CoalescedLoad::loadOrFuture(folly::SemiFuture<bool>* wait) {
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (state_ == LoadState::kCancelled || state_ == LoadState::kLoaded) {
      return true;
    }
    if (state_ == LoadState::kLoading) {
      if (!wait) {
        return false;
      }
      if (!promise_) {
        promise_ = std::make_unique<folly::SharedPromise<bool>>();
      }
      *wait = promise_->getSemiFuture();
      return false;
    }
    VELOX_CHECK_EQ(LoadState::kPlanned, state_);
    state_ = LoadState::kLoading;
  }
  // Outside of 'mutex_'.
  try {
    auto pins = loadData(!wait);
    for (auto& pin : pins) {
      auto entry = pin.checkedEntry();
      VELOX_CHECK(entry->key().fileNum.hasValue());
      VELOX_CHECK(entry->isExclusive());
      entry->setExclusiveToShared();
    }
    setEndState(LoadState::kLoaded);
  } catch (std::exception& e) {
    try {
      setEndState(LoadState::kCancelled);
    } catch (std::exception& inner) {
      // May not throw from inside catch.
    }
    throw;
  }
  return true;
}

void CoalescedLoad::setEndState(LoadState endState) {
  std::lock_guard<std::mutex> l(mutex_);
  state_ = endState;
  if (promise_) {
    promise_->setValue(true);
    promise_.reset();
  }
}

void CacheShard::removeEntry(AsyncDataCacheEntry* entry) {
  std::lock_guard<std::mutex> l(mutex_);
  removeEntryLocked(entry);
}

void CacheShard::removeEntryLocked(AsyncDataCacheEntry* entry) {
  if (entry->key_.fileNum.hasValue()) {
    auto removeIter = entryMap_.find(
        RawFileCacheKey{entry->key_.fileNum.id(), entry->key_.offset});
    VELOX_CHECK(removeIter != entryMap_.end());
    entryMap_.erase(removeIter);
    entry->key_.fileNum.clear();
    entry->setSsdFile(nullptr, 0);
    if (entry->isPrefetch()) {
      entry->setPrefetch(false);
    }
    // An entry can have data allocated if we remove it after failing
    // to fill it. Free the data and account for the difference. In
    // eviction, the data of the evicted entries is moved away, so
    // that freeing while holding the shard mutex is exceptional.
    auto numPages = entry->data().numPages();
    if (numPages) {
      cache_->incrementCachedPages(-numPages);
      cache_->free(entry->data());
    }
  }
}

void CacheShard::evict(uint64_t bytesToFree, bool evictAllUnpinned) {
  int64_t tinyFreed = 0;
  int64_t largeFreed = 0;
  auto now = accessTime();
  std::vector<MappedMemory::Allocation> toFree;
  {
    std::lock_guard<std::mutex> l(mutex_);
    int size = entries_.size();
    if (!size) {
      return;
    }
    int32_t counter = 0;
    int32_t numChecked = 0;
    auto entryIndex = (clockHand_ % size);
    auto iter = entries_.begin() + entryIndex;
    while (++counter <= size) {
      if (++iter == entries_.end()) {
        iter = entries_.begin();
        entryIndex = 0;
      } else {
        ++entryIndex;
      }
      ++numEvictChecks_;
      auto candidate = iter->get();
      if (!candidate) {
        continue;
      }
      ++numChecked;
      ++clockHand_;
      if (evictionThreshold_ == kNoThreshold ||
          eventCounter_ > entries_.size() / 4 ||
          numChecked > entries_.size() / 8) {
        now = accessTime();
        calibrateThreshold();
        numChecked = 0;
        eventCounter_ = 0;
      }
      int32_t score = 0;
      if (candidate->numPins_ == 0 &&
          (!candidate->key_.fileNum.hasValue() || evictAllUnpinned ||
           (score = candidate->score(now)) >= evictionThreshold_)) {
        largeFreed += candidate->data_.byteSize();
        toFree.push_back(std::move(candidate->data()));
        removeEntryLocked(candidate);
        freeEntries_.push_back(std::move(*iter));
        emptySlots_.push_back(entryIndex);
        tinyFreed += candidate->tinyData_.size();
        candidate->tinyData_.clear();
        candidate->size_ = 0;
        ++numEvict_;
        if (score) {
          sumEvictScore_ += score;
        }
        if (largeFreed + tinyFreed > bytesToFree) {
          break;
        }
      }
    }
  }
  ClockTimer t(allocClocks_);
  toFree.clear();
  cache_->incrementCachedPages(
      -largeFreed / static_cast<int32_t>(MappedMemory::kPageSize));
}

void CacheShard::calibrateThreshold() {
  auto numSamples = std::min<int32_t>(10, entries_.size());
  auto now = accessTime();
  auto entryIndex = (clockHand_ % entries_.size());
  auto step = entries_.size() / numSamples;
  auto iter = entries_.begin() + entryIndex;
  evictionThreshold_ = percentile<int32_t>(
      [&]() -> int32_t {
        AsyncDataCacheEntry* element = iter->get();
        int32_t score = element ? element->score(now) : 0;
        if (entryIndex + step >= entries_.size()) {
          entryIndex = (entryIndex + step) % entries_.size();
          iter = entries_.begin() + entryIndex;
        } else {
          entryIndex += step;
          iter += step;
        }
        return score;
      },
      numSamples,
      80);
}

void CacheShard::updateStats(CacheStats& stats) {
  std::lock_guard<std::mutex> l(mutex_);
  for (auto& entry : entries_) {
    if (!entry || !entry->key_.fileNum.hasValue()) {
      ++stats.numEmptyEntries;
      continue;
    } else if (entry->isExclusive()) {
      ++stats.numExclusive;
    } else if (entry->isShared()) {
      ++stats.numShared;
    }
    if (entry->isPrefetch_) {
      ++stats.numPrefetch;
      stats.prefetchBytes += entry->size();
    }
    ++stats.numEntries;
    stats.tinySize += entry->tinyData_.size();
    stats.tinyPadding += entry->tinyData_.capacity() - entry->tinyData_.size();
    stats.largeSize += entry->size_;
    stats.largePadding += entry->data_.byteSize() - entry->size_;
  }
  stats.numHit += numHit_;
  stats.numNew += numNew_;
  stats.numEvict += numEvict_;
  stats.numEvictChecks += numEvictChecks_;
  stats.numWaitExclusive += numWaitExclusive_;
  stats.sumEvictScore += sumEvictScore_;
  stats.allocClocks += allocClocks_;
}

AsyncDataCache::AsyncDataCache(
    std::unique_ptr<MappedMemory> mappedMemory,
    uint64_t maxBytes)
    : mappedMemory_(std::move(mappedMemory)),
      cachedPages_(0),
      maxBytes_(maxBytes) {
  for (auto i = 0; i < kNumShards; ++i) {
    shards_.push_back(std::make_unique<CacheShard>(this));
  }
}

CachePin AsyncDataCache::findOrCreate(
    RawFileCacheKey key,
    uint64_t size,
    folly::SemiFuture<bool>* wait) {
  int shard = std::hash<RawFileCacheKey>()(key) & (kShardMask);
  return shards_[shard]->findOrCreate(key, size, wait);
}

bool AsyncDataCache::exists(RawFileCacheKey key) const {
  int shard = std::hash<RawFileCacheKey>()(key) & (kShardMask);
  return shards_[shard]->exists(key);
}

bool AsyncDataCache::makeSpace(
    MachinePageCount numPages,
    std::function<bool()> allocate) {
  constexpr int32_t kMaxAttempts = kNumShards * 4;

  for (auto nthAttempt = 0; nthAttempt < kMaxAttempts; ++nthAttempt) {
    if (mappedMemory_->numAllocated() + numPages <
        maxBytes_ / MappedMemory::kPageSize) {
      if (allocate()) {
        return true;
      }
    }
    ++shardCounter_;
    // Evict from next shard. If we have gone through all shards once
    // and still have not made the allocation, we go to desperate mode
    // with 'evictAllUnpinned' set to true.
    shards_[shardCounter_ & (kShardMask)]->evict(
        numPages * MappedMemory::kPageSize, nthAttempt >= kNumShards);
  }
  return false;
}

bool AsyncDataCache::allocate(
    MachinePageCount numPages,
    int32_t owner,
    Allocation& out,
    std::function<void(int64_t)> beforeAllocCB,
    MachinePageCount minSizeClass) {
  free(out);
  return makeSpace(numPages, [&]() {
    return mappedMemory_->allocate(
        numPages, owner, out, beforeAllocCB, minSizeClass);
  });
}

bool AsyncDataCache::allocateContiguous(
    MachinePageCount numPages,
    Allocation* collateral,
    ContiguousAllocation& allocation,
    std::function<void(int64_t)> beforeAllocCB) {
  return makeSpace(numPages, [&]() {
    return mappedMemory_->allocateContiguous(
        numPages, collateral, allocation, beforeAllocCB);
  });
}

void AsyncDataCache::incrementNew(uint64_t size) {
  newBytes_ += size;
}

CacheStats AsyncDataCache::refreshStats() const {
  CacheStats stats;
  for (auto& shard : shards_) {
    shard->updateStats(stats);
  }
  return stats;
}

void AsyncDataCache::clear() {
  for (auto& shard : shards_) {
    shard->evict(std::numeric_limits<int32_t>::max(), true);
  }
}

std::string AsyncDataCache::toString() const {
  auto stats = refreshStats();
  std::stringstream out;
  out << "AsyncDataCache: "
      << stats.tinySize + stats.largeSize + stats.tinyPadding +
          stats.largePadding
      << " / " << maxBytes_ << " bytes\n"
      << "Miss: " << stats.numNew << " Hit " << stats.numHit << " evict "
      << stats.numEvict << "\n"
      << " read pins " << stats.numShared << " write pins "
      << stats.numExclusive << " unused prefetch " << stats.numPrefetch
      << " Alloc Megaclocks " << (stats.allocClocks >> 20)
      << " allocated pages " << numAllocated() << " cached pages "
      << cachedPages_;
  out << "\nBacking: " << mappedMemory_->toString();
  return out.str();
}

CoalesceIoStats readPins(
    const std::vector<CachePin>& pins,
    int32_t maxGap,
    int32_t rangesPerIo,
    std::function<uint64_t(int32_t index)> offsetFunc,
    std::function<void(
        const std::vector<CachePin>& pins,
        int32_t begin,
        int32_t end,
        uint64_t offset,
        const std::vector<folly::Range<char*>>& buffers)> readFunc) {
  return coalesceIo<CachePin, folly::Range<char*>>(
      pins,
      maxGap,
      rangesPerIo,
      offsetFunc,
      [&](int32_t index) { return pins[index].checkedEntry()->size(); },
      [&](int32_t index) {
        return std::max<int32_t>(
            1, pins[index].checkedEntry()->data().numRuns());
      },
      [&](const CachePin& pin, std::vector<folly::Range<char*>>& ranges) {
        auto entry = pin.checkedEntry();
        auto& data = entry->data();
        uint64_t offsetInRuns = 0;
        auto size = entry->size();
        if (data.numPages() == 0) {
          ranges.push_back(
              folly::Range<char*>(pin.checkedEntry()->tinyData(), size));
          offsetInRuns = size;
        } else {
          for (int i = 0; i < data.numRuns(); ++i) {
            auto run = data.runAt(i);
            uint64_t bytes = run.numBytes();
            uint64_t readSize = std::min(bytes, size - offsetInRuns);
            ranges.push_back(folly::Range<char*>(run.data<char>(), readSize));
            offsetInRuns += readSize;
          }
        }
        VELOX_CHECK_EQ(offsetInRuns, size);
      },
      [&](int32_t size, std::vector<folly::Range<char*>>& ranges) {
        ranges.push_back(folly::Range<char*>(nullptr, size));
      },
      readFunc);
}

} // namespace facebook::velox::cache
