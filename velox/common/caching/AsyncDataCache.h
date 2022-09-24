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

#include <fmt/format.h>
#include <folly/chrono/Hardware.h>
#include <folly/futures/SharedPromise.h>
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/CoalesceIo.h"
#include "velox/common/base/SelectivityInfo.h"
#include "velox/common/caching/FileGroupStats.h"
#include "velox/common/caching/ScanTracker.h"
#include "velox/common/caching/StringIdMap.h"
#include "velox/common/file/File.h"
#include "velox/common/memory/MappedMemory.h"

namespace facebook::velox::cache {

class AsyncDataCache;
class CacheShard;
class SsdCache;
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

  // Resets the access tracking to not accessed. This is used after
  // evicting the previous contents of the entry, so that the new data
  // does not inherit the history of the previous.
  void reset() {
    lastUse = accessTime();
    numUses = 0;
  }

  // Updates the last access.
  void touch() {
    lastUse = accessTime();
    ++numUses;
  }
};

// Owning reference to a file id and an offset.
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

  // Sets the key and allocates the entry's memory.  Resets
  //  all other state. The entry must be held exclusively and must
  //  hold no memory when calling this.
  void initialize(FileCacheKey key);

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

  void setExclusiveToShared();

  void setSsdFile(SsdFile* FOLLY_NULLABLE file, uint64_t offset) {
    ssdFile_ = file;
    ssdOffset_ = offset;
    ssdSaveable_ = false;
  }

  SsdFile* FOLLY_NULLABLE ssdFile() const {
    return ssdFile_;
  }

  uint64_t ssdOffset() const {
    return ssdOffset_;
  }

  void setTrackingId(TrackingId id) {
    trackingId_ = id;
  }

  void setGroupId(uint64_t groupId) {
    groupId_ = groupId;
  }

  std::string toString() const;

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
  std::atomic<bool> isFirstUse_{false};

  // Group id. Used for deciding if 'this' should be written to SSD.
  uint64_t groupId_{0};

  // Tracking id. Used for deciding if this should be written to SSD.
  TrackingId trackingId_;

  // SSD file from which this was loaded or nullptr if not backed by
  // SsdFile. Used to avoid re-adding items that already come from
  // SSD. The exact file and offset are needed to include uses in RAM
  // to uses on SSD. Failing this, we could have the hottest data first in
  // line for eviction from SSD.
  SsdFile* FOLLY_NULLABLE ssdFile_{nullptr};

  // Offset in 'ssdFile_'.
  uint64_t ssdOffset_{0};

  // True if this should be saved to SSD.
  bool ssdSaveable_{false};

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

// State of a CoalescedLoad
enum class LoadState { kPlanned, kLoading, kCancelled, kLoaded };

// Represents a possibly multi-entry load from a file system. The
// cache expects to load multiple entries in most IOs. The IO is
// either done by a background prefetch thread or if the query
// thread gets there first, then the query thread will do the
// IO. The IO is also cancelled as a unit.
class CoalescedLoad {
 public:
  CoalescedLoad(std::vector<RawFileCacheKey> keys, std::vector<int32_t> sizes)
      : state_(LoadState::kPlanned),
        keys_(std::move(keys)),
        sizes_(std::move(sizes)) {}

  virtual ~CoalescedLoad();

  // Makes entries for the keys that are not yet loaded and does a coalesced
  // load of the entries that are not yet present. If another thread is in the
  // process of doing this, returns immediately if 'wait' is false and waits for
  // the other thread to be done if 'wait' is true.
  bool loadOrFuture(folly::SemiFuture<bool>* FOLLY_NULLABLE wait);

  LoadState state() const {
    return state_;
  }

  void cancel() {
    setEndState(LoadState::kCancelled);
  }

  virtual std::string toString() const {
    return "<CoalescedLoad>";
  }

 protected:
  // Makes entries for 'keys_' and loads their content. Elements of
  // 'keys' that are already loaded or loading are expected to be left
  // out. The returned pins are expected to be exclusive with data
  // loaded. The caller will set them to shared state on success. If
  // loadData() throws, the pins it may have made will be destructed in
  // their exclusive state so that they do not become visible to other
  // users of the cache.
  virtual std::vector<CachePin> loadData(bool isPrefetch) = 0;

  // Sets a final state and resumes waiting threads.
  void setEndState(LoadState endState);

  // Serializes access to all members.
  std::mutex mutex_;

  LoadState state_;

  // Allows waiting for load or cancellation.
  std::unique_ptr<folly::SharedPromise<bool>> promise_;

  std::vector<RawFileCacheKey> keys_;
  std::vector<int32_t> sizes_;
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

  // Returns true if there is an entry for 'key'. Updates access time.
  bool exists(RawFileCacheKey key) const;

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

  // Appends a batch of non-saved SSD saveable entries in 'this' to
  // 'pins'. This may have to be called several times since this keeps
  // limits on the batch to write at one time. The saveable entries
  // are pinned for read. 'pins' should be written or dropped before
  // calling this a second time.
  void appendSsdSaveable(std::vector<CachePin>& pins);

  auto& allocClocks() {
    return allocClocks_;
  }

 private:
  static constexpr int32_t kNoThreshold = std::numeric_limits<int32_t>::max();

  void calibrateThreshold();

  void removeEntryLocked(AsyncDataCacheEntry* FOLLY_NONNULL entry);

  // Returns an unused entry if found. 'size' is a hint for selecting an entry
  // that already has the right amount of memory associated with it.
  std::unique_ptr<AsyncDataCacheEntry> getFreeEntryWithSize(uint64_t sizeHint);

  CachePin initEntry(
      RawFileCacheKey key,
      AsyncDataCacheEntry* FOLLY_NONNULL entry);

  mutable std::mutex mutex_;
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

class AsyncDataCache : public memory::MappedMemory {
 public:
  AsyncDataCache(
      const std::shared_ptr<memory::MappedMemory>& mappedMemory,
      uint64_t maxBytes,
      std::unique_ptr<SsdCache> ssdCache = nullptr);

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

  // Returns true if there is an entry for 'key'. Updates access time.
  bool exists(RawFileCacheKey key) const;

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

  void* FOLLY_NULLABLE allocateBytes(
      uint64_t bytes,
      uint64_t maxMallocSize = kMaxMallocBytes) override;

  void freeBytes(
      void* FOLLY_NONNULL p,
      uint64_t size,
      uint64_t maxMallocSize = kMaxMallocBytes) noexcept override {
    mappedMemory_->freeBytes(p, size, maxMallocSize);
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

  SsdCache* FOLLY_NULLABLE ssdCache() const {
    return ssdCache_.get();
  }

  // Updates stats for creation of a new cache entry of 'size' bytes,
  // i.e. a cache miss. Periodically updates SSD admission criteria,
  // i.e. reconsider criteria every half cache capacity worth of misses.
  void incrementNew(uint64_t size);

  // Updates statistics after bringing in 'bytes' worth of data that
  // qualifies for SSD save and is not backed by SSD. Periodically
  // triggers a background write of eligible entries to SSD.
  void possibleSsdSave(uint64_t bytes);

  // Sets a callback applied to new entries at the point where
  //  they are set to shared mode. Used for testing and can be used for
  // e.g. checking checksums.
  void setVerifyHook(std::function<void(const AsyncDataCacheEntry&)> hook) {
    verifyHook_ = hook;
  }

  const auto& verifyHook() const {
    return verifyHook_;
  }

  // Looks up a pin for each in 'keys' and skips all loading or
  // loaded pins. Calls processPin for each exclusive
  // pin. processPin must move its argument if it wants to use it
  // afterwards. sizeFunc(i) returns the size of the ith item in
  // 'keys'.
  template <typename SizeFunc, typename ProcessPin>
  void makePins(
      const std::vector<RawFileCacheKey>& keys,
      SizeFunc sizeFunc,
      ProcessPin processPin) {
    for (auto i = 0; i < keys.size(); ++i) {
      auto pin = findOrCreate(keys[i], sizeFunc(i), nullptr);
      if (pin.empty() || pin.checkedEntry()->isShared()) {
        continue;
      }
      processPin(i, std::move(pin));
    }
  }

  // Drops all unpinned entries. Pins stay valid.
  void clear();

  // Saves all entries with 'ssdSaveable_' to 'ssdCache_'.
  void saveToSsd();

  int32_t& numSkippedSaves() {
    return numSkippedSaves_;
  }

  memory::Stats stats() const override {
    return mappedMemory_->stats();
  }

 private:
  static constexpr int32_t kNumShards = 4; // Must be power of 2.
  static constexpr int32_t kShardMask = kNumShards - 1;

  // Waits a pseudorandom delay times 'counter'.
  void backoff(int32_t counter);

  // Calls 'allocate' until this returns true. Returns true if
  // allocate returns true. and Tries to evict at least 'numPages' of
  // cache after each failed call to 'allocate'.  May pause to wait
  // for SSD cache flush if ''ssdCache_' is set and is busy
  // writing. Does random back-off after several failures and
  // eventually gives up. Allocation must not be serialized by a mutex
  // for memory arbitration to work.
  bool makeSpace(
      memory::MachinePageCount numPages,
      std::function<bool()> allocate);

  std::shared_ptr<memory::MappedMemory> mappedMemory_;
  std::unique_ptr<SsdCache> ssdCache_;
  std::vector<std::unique_ptr<CacheShard>> shards_;
  int32_t shardCounter_{};
  std::atomic<memory::MachinePageCount> cachedPages_{0};
  // Number of pages that are allocated and not yet loaded or loaded
  // but not yet hit for the first time.
  std::atomic<memory::MachinePageCount> prefetchPages_{0};
  uint64_t maxBytes_;

  // Approximate counter of bytes allocated to cover misses. When this
  // exceeds 'nextSsdScoreSize_' we update the SSD admission criteria.
  std::atomic<uint64_t> newBytes_{0};

  // 'newBytes_' value after which SSD admission should be reconsidered.
  std::atomic<uint64_t> nextSsdScoreSize_{0};

  // Approximate counter tracking new entries that could be saved to SSD.
  uint64_t ssdSaveable_{0};

  CacheStats stats_;

  std::function<void(const AsyncDataCacheEntry&)> verifyHook_;
  // Count of skipped saves to 'ssdCache_' due to 'ssdCache_' being
  // busy with write.
  int32_t numSkippedSaves_{0};

  // Used for pseudorandom backoff after failed allocation
  // attempts. Serialization with a mutex is not allowed for
  // allocations, so use backoff.
  std::atomic<uint16_t> backoffCounter_{0};

  // Counter of threads competing for allocation in makeSpace(). Used
  // for setting staggered backoff. Mutexes are not allowed for this.
  std::atomic<int32_t> numThreadsInAllocate_{0};
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

} // namespace facebook::velox::cache

template <>
struct fmt::formatter<facebook::velox::cache::LoadState> : formatter<int> {
  auto format(facebook::velox::cache::LoadState s, format_context& ctx) {
    return formatter<int>::format(static_cast<int>(s), ctx);
  }
};
