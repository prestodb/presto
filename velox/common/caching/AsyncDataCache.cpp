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

#include <folly/executors/QueuedImmediateExecutor.h>

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
    promise = std::move(promise_);
  }
  if (promise) {
    promise->setValue(true);
  }
}

void AsyncDataCacheEntry::release() {
  VELOX_CHECK_NE(0, numPins_);
  if (!dataValid_) {
    shard_->removeEntry(this);
  }
  if (numPins_ == kExclusive) {
    if (promise_) {
      promise_->setValue(true);
      promise_.reset();
    }
    load_.reset();
    numPins_ = 0;
  } else {
    auto oldPins = numPins_.fetch_add(-1);
    VELOX_CHECK(oldPins >= 1, "Pin count goes negative");
    if (oldPins == 1) {
      load_.reset();
    }
  }
}

void AsyncDataCacheEntry::addReference() {
  VELOX_CHECK(!isExclusive());
  ++numPins_;
}

void AsyncDataCacheEntry::ensureLoaded(bool wait) {
  VELOX_CHECK(isShared());
  if (dataValid_) {
    return;
  }
  // Copy the shared_ptr to 'load_' so that another loader will not clear
  // it if it gets in first. Competing loaders get serialized on the
  // mutex of 'load'.
  auto load = load_;
  if (load) {
    if (wait) {
      folly::SemiFuture<bool> waitFuture(false);
      if (!load->loadOrFuture(&waitFuture)) {
        auto& exec = folly::QueuedImmediateExecutor::instance();
        std::move(waitFuture).via(&exec).wait();
        VELOX_CHECK(isShared());
      }
      if (!dataValid_) {
        VELOX_FAIL("Waiting for failed load.");
      }
    } else {
      load->loadOrFuture(nullptr);
    }
  }
}

memory::MachinePageCount AsyncDataCacheEntry::setPrefetch(bool flag) {
  isPrefetch_ = flag;
  auto numPages = bits::roundUp(size_, memory::MappedMemory::kPageSize) /
      memory::MappedMemory::kPageSize;
  return shard_->cache()->incrementPrefetchPages(flag ? numPages : -numPages);
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
      newEntry->dataValid_ = false;
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
  }
  return initEntry(key, entryToInit, size);
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
  ++numNew_;
  entry->key_ = FileCacheKey{StringIdLease(fileIds(), key.fileNum), key.offset};
  if (entry->size() < size) {
    ClockTimer t(allocClocks_);
    if (size < AsyncDataCacheEntry::kTinyDataSize) {
      entry->tinyData_.resize(size);
    } else {
      auto sizePages = bits::roundUp(size, MappedMemory::kPageSize) /
          MappedMemory::kPageSize;
      if (cache_->allocate(sizePages, kCacheOwner, entry->data_)) {
        cache_->incrementCachedPages(entry->data().numPages());
      } else {
        // No memory to cover the new entry. The entry is in exclusive
        // mode. We remove it from the map and unpin.
        removeEntry(entry);
        entry->release();
        _VELOX_THROW(
            VeloxRuntimeError,
            error_source::kErrorSourceRuntime.c_str(),
            error_code::kNoCacheSpace.c_str(),
            /* isRetriable */ true);
      }
    }
  }
  entry->touch();
  entry->size_ = size;
  CachePin pin;
  pin.setEntry(entry);
  return pin;
}

//  static
std::atomic<int32_t> FusedLoad::numFusedLoads_{0};

FusedLoad::~FusedLoad() {
  cancel();
  --numFusedLoads_;
}

bool FusedLoad::loadOrFuture(folly::SemiFuture<bool>* wait) {
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
    // If wait is not set this counts as prefetch.
    loadData(!wait);
    for (auto& pin : pins_) {
      pin.entry()->setValid();
    }
    {
      std::lock_guard<std::mutex> l(mutex_);
      pins_.clear();
    }
    setEndState(LoadState::kLoaded);
    return true;
  } catch (const std::exception& e) {
    cancel();
    std::rethrow_exception(std::current_exception());
  }
}

void FusedLoad::cancel() {
  std::unique_ptr<folly::SharedPromise<bool>> promise;
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (state_ == LoadState::kLoading || state_ == LoadState::kCancelled) {
      // Already cancelled or another thread is loading. If loading, we must let
      // the load finish.
      return;
    }
    state_ = LoadState::kCancelled;
    promise = std::move(promise_);
    for (auto& pin : pins_) {
      if (!pin.empty()) {
        pin.entry()->setValid(false);
      }
    }
    pins_.clear();
  }
  if (promise) {
    promise->setValue(true);
  }
}

void FusedLoad::setEndState(LoadState endState) {
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
    if (entry->isPrefetch()) {
      entry->setPrefetch(false);
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
        removeEntryLocked(candidate);
        freeEntries_.push_back(std::move(*iter));
        emptySlots_.push_back(entryIndex);
        tinyFreed += candidate->tinyData_.size();
        candidate->tinyData_.clear();
        largeFreed += candidate->data_.byteSize();
        toFree.push_back(std::move(candidate->data()));
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
    : fileIds_(fileIdsShared()),
      mappedMemory_(std::move(mappedMemory)),
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

bool AsyncDataCache::allocate(
    MachinePageCount numPages,
    int32_t owner,
    Allocation& out,
    std::function<void(int64_t)> beforeAllocCB,
    MachinePageCount minSizeClass) {
  constexpr int32_t kMaxAttempts = kNumShards * 4;
  free(out);
  for (auto nthAttempt = 0; nthAttempt < kMaxAttempts; ++nthAttempt) {
    if (mappedMemory_->numAllocated() + numPages <
        maxBytes_ / MappedMemory::kPageSize) {
      if (mappedMemory_->allocate(
              numPages, owner, out, beforeAllocCB, minSizeClass)) {
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

CacheStats AsyncDataCache::refreshStats() const {
  CacheStats stats;
  for (auto& shard : shards_) {
    shard->updateStats(stats);
  }
  return stats;
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
      << " read pins " << stats.numShared << " unused prefetch "
      << stats.numPrefetch << " Alloc Mclks " << (stats.allocClocks >> 20);
  return out.str();
}

} // namespace facebook::velox::cache
