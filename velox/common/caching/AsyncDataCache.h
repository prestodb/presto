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

#pragma once

#include <deque>

#include <folly/chrono/Hardware.h>
#include <folly/futures/SharedPromise.h>
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/CoalesceIo.h"
#include "velox/common/base/SelectivityInfo.h"
#include "velox/common/caching/StringIdMap.h"
#include "velox/common/memory/MappedMemory.h"

namespace facebook::velox::cache {

class AsyncDataCache;
class CacheShard;
class SsdFile;

// Type for tracking last access. This is based on CPU clock and
// scaled to be around 1ms resolution. This can wrap around and is
// only comparable to other values of the same type. This is a
// ballpark figure and factors like variability of clock speed do not
// count.
using AccessTime = int32_t;

inline AccessTime accessTime() {
  // Divide by 2M. hardware_timestamp is either clocks or
  // nanoseconds. This division brings the resolution to between 0.5
  // and 2 ms.
  return folly::hardware_timestamp() >> 21;
}

struct AccessStats {
  AccessTime lastUse{0};
  int32_t numUses{0};

  // Retention score. A higher number means less worth retaining. This
  // works well with a typical formula of time over use count going to
  // zero as uses go up and time goes down. 'now' is the current
  // accessTime(), passed from the caller since getting the time is
  // expensive and many entries are checked one after the other.
  int32_t score(AccessTime now, uint64_t /*size*/) const {
    return (now - lastUse) / (1 + numUses);
  }

  // Updates the last access.
  void touch() {
    lastUse = accessTime();
    ++numUses;
  }
};

// Owning reference to a file number and an offset.
struct FileCacheKey {
  StringIdLease fileNum;
  uint64_t offset;

  bool operator==(const FileCacheKey& other) const {
    return offset == other.offset && fileNum.id() == other.fileNum.id();
  }
};

// Non-owning reference to a file number and offset.
struct RawFileCacheKey {
  uint64_t fileNum;
  uint64_t offset;

  bool operator==(const RawFileCacheKey& other) const {
    return offset == other.offset && fileNum == other.fileNum;
  }
};
} // namespace facebook::velox::cache
namespace std {
template <>
struct hash<::facebook::velox::cache::FileCacheKey> {
  size_t operator()(const ::facebook::velox::cache::FileCacheKey& key) const {
    return facebook::velox::bits::hashMix(key.fileNum.id(), key.offset);
  }
};

template <>
struct hash<::facebook::velox::cache::RawFileCacheKey> {
  size_t operator()(
      const ::facebook::velox::cache::RawFileCacheKey& key) const {
    return facebook::velox::bits::hashMix(key.fileNum, key.offset);
  }
};

} // namespace std

namespace facebook::velox::cache {

class FusedLoad;

// Represents a contiguous range of bytes cached from a file. This
// is the primary unit of access. These are typically owned via
// CachePin and can be in shared or exclusive mode. 'numPins_'
// counts the shared leases, the special value kExclusive means that
// this is being written to by another thread. It is possible to
// wait for the exclusive mode to finish, at which time one can
// retry getting access. Entries belong to one CacheShard at a
// time. The CacheShard serializes the mapping from a key to the
// entry and the setting entries to exclusive mode. An unpinned
// entry is evictable. CacheShard decides the eviction policy and
// serializes eviction with other access.
class AsyncDataCacheEntry {
 public:
  static constexpr int32_t kExclusive = -10000;
  static constexpr int32_t kTinyDataSize = 2048;

  explicit AsyncDataCacheEntry(CacheShard* FOLLY_NONNULL shard);

  memory::MappedMemory::Allocation& data() {
    return data_;
  }

  const memory::MappedMemory::Allocation& data() const {
    return data_;
  }

  const char* FOLLY_NULLABLE tinyData() const {
    return tinyData_.empty() ? nullptr : tinyData_.data();
  }

  char* FOLLY_NULLABLE tinyData() {
    return tinyData_.empty() ? nullptr : tinyData_.data();
  }

  const FileCacheKey& key() const {
    return key_;
  }

  int64_t offset() const {
    return key_.offset;
  }

  int32_t size() const {
    return size_;
  }

  // Sets 'this' to loading state. Requires exclusive access on
  // entry. Sets the access mode to shared after installing the load.
  void setLoading(std::shared_ptr<FusedLoad> load) {
    VELOX_CHECK(isExclusive());
    load_ = std::move(load);
  }

  // Call this after loading the data and before releasing the pin.
  void setValid(bool success = true) {
    VELOX_CHECK_NE(0, numPins_);
    dataValid_ = success;
    load_.reset();
  }

  void touch() {
    accessStats_.touch();
  }

  int32_t score(AccessTime now) const {
    return accessStats_.score(now, size_);
  }

  bool isShared() const {
    return numPins_ > 0;
  }

  bool isExclusive() const {
    return numPins_ == kExclusive;
  }

  int32_t numPins() const {
    return numPins_;
  }

  // Checks that 'this' has valid data. If a load is scheduled,
  // starts the load on the calling thread if the load is not yet
  // started. If the load is in progress, waits for the load to
  // complete if 'wait' is true. If wait is false and the load is in
  // progress, returns immediately. If there is no scheduled IO and
  // wait is true, checks that the data is valid and throws if it is
  // not. Data might not be valid for example if we stopped to wait
  // for a read and the read errored out.
  void ensureLoaded(bool wait);

  // Sets the 'isPrefetch_' and updates the cache's total prefetch count.
  // Returns the new prefetch pages count.
  memory::MachinePageCount setPrefetch(bool flag = true);

  bool isPrefetch() const {
    return isPrefetch_;
  }

  // Distinguishes between a reuse of a cached entry from first
  // retrieval of a prefetched entry. If this is false, we have an
  // actual reuse of cached data.
  bool getAndClearFirstUseFlag() {
    bool value = isFirstUse_;
    isFirstUse_ = false;
    return value;
  }

  // True if 'data_' is fully loaded from the backing storage.
  bool dataValid() const {
    return dataValid_;
  }

  void setExclusiveToShared();

  void setSsdFile(SsdFile* FOLLY_NULLABLE file, uint64_t offset) {
    ssdFile_ = file;
    ssdOffset_ = offset;
  }

  SsdFile* FOLLY_NULLABLE ssdFile() const {
    return ssdFile_;
  }

  uint64_t ssdOffset() const {
    return ssdOffset_;
  }

 private:
  void release();
  void addReference();

  // Returns a future that will be realized when a caller can retry
  // getting 'this'. Must be called inside the mutex of 'shard_'.
  folly::SemiFuture<bool> getFuture() {
    if (!promise_) {
      promise_ = std::make_unique<folly::SharedPromise<bool>>();
    }
    return promise_->getSemiFuture();
  }

  // Holds an owning reference to the file number.
  FileCacheKey key_;

  CacheShard* const FOLLY_NONNULL shard_;

  // The data being cached.
  memory::MappedMemory::Allocation data_;

  // Contains the cached data if this is much smaller than a MappedMemory page
  // (kTinyDataSize).
  std::string tinyData_;

  // true if 'data_' or 'tinyData_' correspond to the data in the
  // file of 'key', starting at offset of 'key_', for 'size_'
  // bytes. Releasing a pinned entry where 'dataValid_' is false
  // removes the entry from 'shard_'.
  bool dataValid_{false};

  std::unique_ptr<folly::SharedPromise<bool>> promise_;
  int32_t size_{0};

  // Setting this from 0 to 1 or to kExclusive requires owning shard_->mutex_.
  std::atomic<int32_t> numPins_{0};
  AccessStats accessStats_;
  // True if 'this' is speculatively loaded. This is reset on first
  // hit. Allows catching a situation where prefetched entries get
  // evicted before they are hit.
  bool isPrefetch_{false};

  // Set after first use of a prefetched entry. Cleared by
  // getAndClearFirstUseFlag(). Does not require synchronization since used for
  // statistics only.
  bool isFirstUse_{false};

  // Represents a pending coalesced IO that is either loading this or
  // scheduled to load this. Setting/clearing requires the shard
  // mutex. If set, 'this' is pinned for either exclusive or shared.
  std::shared_ptr<FusedLoad> load_;

  // SSD file from which this was loaded or nullptr if not backed by
  // SsdFile. Used to avoid re-adding items that already come from
  // SSD. The exact file and offset are needed to include uses in RAM
  // to uses on SSD. Failing this, we could have the hottest data first in
  // line for eviction from SSD.
  SsdFile* FOLLY_NULLABLE ssdFile_{nullptr};

  // Offset in 'ssdFile_'.
  uint64_t ssdOffset_{0};

  friend class CacheShard;
  friend class CachePin;
};

class CachePin {
 public:
  CachePin() : entry_(nullptr) {}

  CachePin(const CachePin& other) {
    *this = other;
  }

  CachePin(CachePin&& other) noexcept {
    *this = std::move(other);
  }

  ~CachePin() {
    release();
  }

  void operator=(const CachePin& other) {
    other.addReference();
    release();
    entry_ = other.entry_;
  }

  void operator=(CachePin&& other) noexcept {
    release();
    entry_ = other.entry_;
    other.entry_ = nullptr;
  }

  bool empty() const {
    return !entry_;
  }

  void clear() {
    release();
    entry_ = nullptr;
  }
  AsyncDataCacheEntry* FOLLY_NULLABLE entry() const {
    return entry_;
  }

  AsyncDataCacheEntry* FOLLY_NONNULL checkedEntry() const {
    assert(entry_);
    return entry_;
  }

  bool operator<(const CachePin& other) const {
    auto id1 = entry_->key_.fileNum.id();
    auto id2 = other.entry_->key_.fileNum.id();
    if (id1 == id2) {
      return entry_->offset() < other.entry_->offset();
    }
    return id1 < id2;
  }

 private:
  void addReference() const {
    VELOX_CHECK(entry_);
    entry_->addReference();
  }

  void release() {
    if (entry_) {
      entry_->release();
    }
    entry_ = nullptr;
  }

  void setEntry(AsyncDataCacheEntry* FOLLY_NONNULL entry) {
    release();
    VELOX_CHECK(entry->isExclusive() || entry->isShared());
    entry_ = entry;
  }

  AsyncDataCacheEntry* FOLLY_NULLABLE entry_{nullptr};

  friend class CacheShard;
};

// State of a FusedLoad
enum class LoadState { kPlanned, kLoading, kCancelled, kLoaded };

// Represents a possibly multi-entry load from a file system. The
// cache expects to load multiple entries in most IOs. The IO is
// either done by a background prefetch thread or if the query
// thread gets there first, then the query thread will do the
// IO. The IO is also cancelled as a unit. FusedLoad holds a pin on
// all the entries it concerns. The pins are released after the IO
// completes or is cancelled. An entry that references a FusedLoad
// is not readable until the FusedLoad is complete.
class FusedLoad : public std::enable_shared_from_this<FusedLoad> {
 public:
  FusedLoad() : state_(LoadState::kPlanned) {}

  // Registers 'pins' to be all loaded together by 'this' on first use
  // of any. Ownership transfer of pins is required. This is not done
  // in constructor due to shared_from_this not being defined during
  // construction.
  void initialize(std::vector<CachePin>&& pins) {
    pins_ = std::move(pins);
    for (auto& pin : pins_) {
      pin.entry()->setLoading(shared_from_this());
    }
    // All pins to be loaded together must be set in place before we
    // make any available for other threads to load. Else a partial
    // load could result. Do this inside the mutex so that when a load
    // begins we do not have a mix of shared and exclusive pins. Note
    // that a second reader will be continued right after the pin goes
    // shared. The just continued thread will still have to get
    // 'mutex_' before it can start a load.
    std::lock_guard<std::mutex> l(mutex_);
    for (auto& pin : pins_) {
      pin.entry()->setExclusiveToShared();
    }
  }

  virtual ~FusedLoad();

  // Loads the pinned entries on first call. Returns true if the
  // pins are loaded. If returns false, 'wait' is set to a future
  // that is realized when the load is ready. The caller must
  // further check that the pin it holds is in a valid state before
  // using the data since the load may have failed.
  bool loadOrFuture(folly::SemiFuture<bool>* FOLLY_NULLABLE wait);

  // Removes 'this' from the affected entries. If 'this' is already
  // loading, takes no action since this indicates that another thread
  // has control of 'this'.
  void cancel();

  LoadState state() const {
    return state_;
  }
  // Used for testing and server status.
  static int32_t numFusedLoads() {
    return numFusedLoads_;
  }

 protected:
  // Performs the data transfer part of the load. Subclasses will
  // specialize this. All pins will be referring to existing entries
  // in shared state on entry and exit of this function. The contract
  // is that a normal return will have filled all the pins with valid
  // data. A throw means that the data in the pins is undefined.  The
  // caller will release the pins in due time.
  virtual void loadData(bool isPrefetch) = 0;

  // Sets a final state and resumes waiting threads.
  void setEndState(LoadState endState);

  // Serializes access to all members. Note that the cache pins will be in
  // different shards and each shard has its own mutex.
  std::mutex mutex_;

  LoadState state_;
  // Allows waiting for load or cancellation.
  std::unique_ptr<folly::SharedPromise<bool>> promise_;

  // Non-overlapping entries in order of start offset, all in the same
  // file. There may be gaps between the entries but the idea is that
  // the gaps are small enough to make it worthwhile loading all the
  // entries in a single IO. 'this' is installed on the entries when the
  // entries are in exclusive mode. After setup, this continues to hold the
  // entries in shared mode. The entries will block other readers until 'this'
  // lets go of the entries because 'load_' of the entry is set.
  std::vector<CachePin> pins_;
  static std::atomic<int32_t> numFusedLoads_;
};

// Struct for CacheShard stats. Stats from all shards are added into
// this struct to provide a snapshot of state.
struct CacheStats {
  // Total size in 'tynyData_'
  int64_t tinySize{};
  // Total size in 'data_'
  int64_t largeSize{};
  // Unused capacity in 'tinyData_'.
  int64_t tinyPadding{};
  // Unused capacity in 'data_'.
  int64_t largePadding{};
  // Total number of entries.
  int32_t numEntries{};
  // Number of entries that do not cache anything.
  int32_t numEmptyEntries{};
  // Number of entries pinned for shared access.
  int32_t numShared{};
  // Number of entries pinned for exclusive access.
  int32_t numExclusive{};
  // Number of entries that are being or have been prefetched but have not been
  // hit.
  int32_t numPrefetch{};
  // Total size of entries in prefetch state.
  int64_t prefetchBytes{};
  // Number of hits (saved IO). The first hit to a prefetched entry does not
  // count.
  int64_t numHit{};
  // Number of new entries created.
  int64_t numNew{};
  // Number of times a valid entry was removed in order to make space.
  int64_t numEvict{};
  // Number of entries considered for evicting.
  int64_t numEvictChecks{};
  // Number of times a user waited for an entry to transit from exclusive to
  // shared mode.
  int64_t numWaitExclusive{};
  // Cumulative clocks spent in allocating or freeing memory  for backing cache
  // entries.
  uint64_t allocClocks{};
  // Sum of scores of evicted entries. This serves to infer an average
  // lifetime for entries in cache.
  int64_t sumEvictScore{};
};

class ClockTimer {
 public:
  explicit ClockTimer(std::atomic<uint64_t>& total)
      : total_(&total), start_(folly::hardware_timestamp()) {}
  ~ClockTimer() {
    *total_ += folly::hardware_timestamp() - start_;
  }

 private:
  std::atomic<uint64_t>* FOLLY_NONNULL total_;
  uint64_t start_;
};

// Collection of cache entries whose key hashes to the same shard of
// the hash number space.  The cache population is divided into shards
// to decrease contention on the mutex for the key to entry mapping
// and other housekeeping.
class CacheShard {
 public:
  static constexpr int32_t kCacheOwner = -4;

  explicit CacheShard(AsyncDataCache* FOLLY_NONNULL cache) : cache_(cache) {}

  // See AsyncDataCache::findOrCreate.
  CachePin findOrCreate(
      RawFileCacheKey key,
      uint64_t size,
      folly::SemiFuture<bool>* FOLLY_NULLABLE readyFuture);

  AsyncDataCache* FOLLY_NONNULL cache() {
    return cache_;
  }
  std::mutex& mutex() {
    return mutex_;
  }

  // removes 'bytesToFree' worth of entries or as many entries as are
  // not pinned. This favors first removing older and less frequently
  // used entries. If 'evictAllUnpinned' is true, anything that is
  // not pinned is evicted at first sight. This is for out of memory
  // emergencies.
  void evict(uint64_t bytesToFree, bool evictAllUnpinned);

  // Removes 'entry' from 'this'.
  void removeEntry(AsyncDataCacheEntry* FOLLY_NONNULL entry);

  // Adds the stats of 'this' to 'stats'.
  void updateStats(CacheStats& stats);

 private:
  static constexpr int32_t kNoThreshold = std::numeric_limits<int32_t>::max();
  void calibrateThreshold();
  void removeEntryLocked(AsyncDataCacheEntry* FOLLY_NONNULL entry);
  // Returns an unused entry if found. 'size' is a hint for selecting an entry
  // that already has the right amount of memory associated with it.
  std::unique_ptr<AsyncDataCacheEntry> getFreeEntryWithSize(uint64_t sizeHint);
  CachePin initEntry(
      RawFileCacheKey key,
      AsyncDataCacheEntry* FOLLY_NONNULL entry,
      int64_t size);

  std::mutex mutex_;
  folly::F14FastMap<RawFileCacheKey, AsyncDataCacheEntry * FOLLY_NONNULL>
      entryMap_;
  // Entries associated to a key.
  std::deque<std::unique_ptr<AsyncDataCacheEntry>> entries_;
  // Unused indices in 'entries_'.
  std::vector<int32_t> emptySlots_;
  // A reserve of entries that are not associated to a key. Keeps a
  // few around to avoid allocating one inside 'mutex_'.
  std::vector<std::unique_ptr<AsyncDataCacheEntry>> freeEntries_;
  AsyncDataCache* const FOLLY_NONNULL cache_;
  // Index in 'entries_' for the next eviction candidate.
  uint32_t clockHand_{};
  // Number of gets  since last stats sampling.
  uint32_t eventCounter_{};
  // Maximum retainable entry score(). Anything above this is evictable.
  int32_t evictionThreshold_{kNoThreshold};
  // Cumulative count of cache hits.
  uint64_t numHit_{};
  // Cumulative count of hits on entries held in exclusive mode.
  uint64_t numWaitExclusive_{};
  // Cumulative count of new entry creation.
  uint64_t numNew_{};
  // Count of entries evicted.
  uint64_t numEvict_{};
  // Count of entries considered for eviction. This divided by
  // 'numEvict_' measured efficiency of eviction.
  uint64_t numEvictChecks_{};
  // Sum of evict scores. This divided by 'numEvict_' correlates to
  // time data stays in cache.
  uint64_t sumEvictScore_{};
  // Tracker of time spent in allocating/freeing MappedMemory space
  // for backing cached data.
  std::atomic<uint64_t> allocClocks_;
};

class AsyncDataCache : public memory::MappedMemory,
                       public std::enable_shared_from_this<AsyncDataCache> {
 public:
  AsyncDataCache(
      std::unique_ptr<memory::MappedMemory> mappedMemory,
      uint64_t maxBytes);

  // Finds or creates a cache entry corresponding to 'key'. The entry
  // is returned in 'pin'. If the entry is new, it is pinned in
  // exclusive mode and its 'data_' has uninitialized space for at
  // least 'size' bytes. If the entry is in cache and already filled,
  // the pin is in shared mode.  If the entry is in exclusive mode for
  // some other pin, the pin is empty. If 'waitFuture' is not nullptr
  // and the pin is exclusive on some other pin, this is set to a
  // future that is realized when the pin is no longer exclusive. When
  // the future is realized, the caller may retry findOrCreate().
  // runtime error with code kNoCacheSpace if there is no space to create the
  // new entry after evicting any unpinned content.
  CachePin findOrCreate(
      RawFileCacheKey key,
      uint64_t size,
      folly::SemiFuture<bool>* FOLLY_NULLABLE waitFuture = nullptr);

  bool allocate(
      memory::MachinePageCount numPages,
      int32_t owner,
      Allocation& out,
      std::function<void(int64_t)> beforeAllocCB = nullptr,
      memory::MachinePageCount minSizeClass = 0) override;

  int64_t free(Allocation& allocation) override {
    return mappedMemory_->free(allocation);
  }

  bool allocateContiguous(
      memory::MachinePageCount numPages,
      Allocation* FOLLY_NULLABLE collateral,
      ContiguousAllocation& allocation,
      std::function<void(int64_t)> beforeAllocCB = nullptr) override;

  void freeContiguous(ContiguousAllocation& allocation) override {
    mappedMemory_->freeContiguous(allocation);
  }

  bool checkConsistency() const override {
    return mappedMemory_->checkConsistency();
  }

  const std::vector<memory::MachinePageCount>& sizeClasses() const override {
    return mappedMemory_->sizeClasses();
  }

  memory::MachinePageCount numAllocated() const override {
    return mappedMemory_->numAllocated();
  }

  memory::MachinePageCount numMapped() const override {
    return mappedMemory_->numMapped();
  }

  CacheStats refreshStats() const;

  std::string toString() const override;

  memory::MachinePageCount incrementCachedPages(int64_t pages) {
    // The counter is unsigned and the increment is signed.
    return cachedPages_.fetch_add(pages) + pages;
  }

  memory::MachinePageCount incrementPrefetchPages(int64_t pages) {
    // The counter is unsigned and the increment is signed.
    return prefetchPages_.fetch_add(pages) + pages;
  }

  uint64_t maxBytes() const {
    return maxBytes_;
  }

 private:
  static constexpr int32_t kNumShards = 4; // Must be power of 2.
  static constexpr int32_t kShardMask = kNumShards - 1;

  bool makeSpace(
      memory::MachinePageCount numPages,
      std::function<bool()> allocate);

  std::unique_ptr<memory::MappedMemory> mappedMemory_;
  std::vector<std::unique_ptr<CacheShard>> shards_;
  int32_t shardCounter_{};
  std::atomic<memory::MachinePageCount> cachedPages_{0};
  // Number of pages that are allocated and not yet loaded or loaded
  // but not yet hit for the first time.
  std::atomic<memory::MachinePageCount> prefetchPages_{0};
  uint64_t maxBytes_;
  CacheStats stats_;
};

// Samples a set of values T from 'numSamples' calls of
// 'iter'. Returns the value where 'percent' of the samples are less than the
// returned value.
template <typename T, typename Next>
T percentile(Next next, int32_t numSamples, int percent) {
  std::vector<T> values;
  values.reserve(numSamples);
  for (auto i = 0; i < numSamples; ++i) {
    values.push_back(next());
  }
  std::sort(values.begin(), values.end());
  return values.empty() ? 0 : values[(values.size() * percent) / 100];
}

// Utility function for loading multiple pins with coalesced
// IO. 'pins' is a vector of CachePins to fill. 'maxGap' is the
// largest allowed distance in bytes between the end of one entry and
// the start of the next. If the gap is larger or the next is before
// the end of the previous, the entries will be fetched separately.
//
//'offsetFunc' returns the starting offset of the data in the
// file given a pin and the pin's index in 'pins'. The pins are expected to be
// sorted by this offset. 'readFunc' reads from the appropriate media. It gets
// the 'pins' and the index of the first pin included in the read and the index
// of the first pin not included. It gets the starting offset of the read and a
// vector of memory ranges to fill by ReadFile::preadv or a similar
// function.
// The caller is responsible for calling setValid on the pins after a successful
// read.
//
// Returns the number of distinct IOs, the number of bytes loaded into pins and
// the number of extr bytes read.
CoalesceIoStats readPins(
    const std::vector<CachePin>& pins,
    int32_t maxGap,
    int32_t maxBatch,
    std::function<uint64_t(int32_t index)> offsetFunc,
    std::function<void(
        const std::vector<CachePin>& pins,
        int32_t begin,
        int32_t end,
        uint64_t offset,
        const std::vector<folly::Range<char*>>& buffers)> readFunc);

// Generic template for grouping IOs into batches of <
// rangesPerIo ranges separated by gaps of size >= maxGap. Element
// represents the object of the IO, Range is the type representing
// the IO, e.g. pointer + size, offsetFunc and SizeFunc return the
// offset and size of an Element, AddRange adds the ranges that
// correspond to an Element, skipRange adds a gap between
// neighboring items, ioFunc takes the items, the first item to
// process, the first item not to process, the offset of the first
// item and a vector of Ranges.
template <
    typename Item,
    typename Range,
    typename ItemOffset,
    typename ItemSize,
    typename ItemNumRanges,
    typename AddRanges,
    typename SkipRange,
    typename IoFunc>
CoalesceIoStats coalescedIo(
    const std::vector<Item>& items,
    int32_t maxGap,
    int32_t rangesPerIo,
    ItemOffset offsetFunc,
    ItemSize sizeFunc,
    ItemNumRanges numRanges,
    AddRanges addRanges,
    SkipRange skipRange,
    IoFunc ioFunc) {
  std::vector<Range> buffers;
  auto start = offsetFunc(0);
  auto lastOffset = start;
  std::vector<Range> ranges;
  CoalesceIoStats result;
  int32_t firstItem = 0;
  for (int32_t i = 0; i < items.size(); ++i) {
    auto& item = items[i];
    auto startOffset = offsetFunc(i);
    auto size = sizeFunc(i);
    result.payloadBytes += size;
    bool enoughRanges =
        ranges.size() + numRanges(item) >= rangesPerIo && !ranges.empty();
    if (lastOffset != startOffset || enoughRanges) {
      int64_t gap = startOffset - lastOffset;
      if (gap > 0 && gap < maxGap && !enoughRanges) {
        // The next one is after the previous and no farther than maxGap bytes,
        // we read the gap but drop the bytes.
        result.extraBytes += gap;
        skipRange(gap, ranges);
      } else {
        ioFunc(items, firstItem, i, start, ranges);
        ranges.clear();
        firstItem = i;
        ++result.numIos;
        start = startOffset;
      }
    }
    addRanges(item, ranges);
    lastOffset = startOffset + size;
  }
  ioFunc(items, firstItem, items.size(), start, ranges);
  ++result.numIos;
  return result;
}

} // namespace facebook::velox::cache
