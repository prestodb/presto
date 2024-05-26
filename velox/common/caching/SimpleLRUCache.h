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

#include <cstdint>
#include <functional>
#include <list>

#include <glog/logging.h>
#include "folly/IntrusiveList.h"
#include "folly/container/F14Map.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/time/Timer.h"

namespace facebook::velox {

struct SimpleLRUCacheStats {
  SimpleLRUCacheStats(
      size_t _maxSize,
      size_t _expireDurationMs,
      size_t _curSize,
      size_t _pinnedSize,
      size_t _numElements,
      size_t _numHits,
      size_t _numLookups)
      : maxSize{_maxSize},
        expireDurationMs(_expireDurationMs),
        curSize{_curSize},
        pinnedSize{_pinnedSize},
        numElements{_numElements},
        numHits{_numHits},
        numLookups{_numLookups} {}

  SimpleLRUCacheStats() = default;

  /// Capacity of the cache.
  size_t maxSize{0};

  size_t expireDurationMs{0};

  /// Current cache size used.
  size_t curSize{0};

  /// Current cache size used by pinned entries.
  size_t pinnedSize{0};

  /// Total number of elements in the cache.
  size_t numElements{0};

  /// Total number of cache hits since server start.
  size_t numHits{0};

  /// Total number of cache lookups since server start.
  size_t numLookups{0};

  std::string toString() const {
    return fmt::format(
        "{{\n"
        "  maxSize: {}\n"
        "  expireDurationMs: {}\n"
        "  curSize: {}\n"
        "  pinnedSize: {}\n"
        "  numElements: {}\n"
        "  numHits: {}\n"
        "  numLookups: {}\n"
        "}}\n",
        maxSize,
        expireDurationMs,
        curSize,
        pinnedSize,
        numElements,
        numHits,
        numLookups);
  }

  bool operator==(const SimpleLRUCacheStats& other) const {
    return std::tie(
               curSize,
               expireDurationMs,
               maxSize,
               pinnedSize,
               numElements,
               numHits,
               numLookups) ==
        std::tie(
               other.curSize,
               other.expireDurationMs,
               other.maxSize,
               other.pinnedSize,
               other.numElements,
               other.numHits,
               other.numLookups);
  }
};

/// A simple LRU cache that allows each element to occupy an arbitrary amount of
/// space in the cache. Useful when the size of the cached elements can vary a
/// lot; if they are all roughly the same size something that only tracks the
/// number of elements in the cache like common/datastruct/LRUCacheMap.h may be
/// better.
///
/// NOTE:
/// 1. NOT Thread-Safe: All the public calls modify internal structures and
/// hence require external write locks if used from multiple threads.
/// 2. 'Key' is required to be copyable and movable.
template <
    typename Key,
    typename Value,
    typename Comparator = std::equal_to<Key>,
    typename Hash = std::hash<Key>>
class SimpleLRUCache {
 public:
  /// Constructs a cache of the specified size. This size can represent whatever
  /// you want -- slots, or bytes, or etc; you provide the size of each element
  /// whenever you add a new value to the cache. If 'expireDurationMs' is not
  /// zero, then a cache value will be evicted out of cache after
  /// 'expireDurationMs' time passed since its insertion into the cache no
  /// matter if it been accessed or not.
  explicit SimpleLRUCache(size_t maxSize, size_t expireDurationMs = 0);

  /// Frees all owned data. Check-fails if any element remains pinned.
  ~SimpleLRUCache();

  /// Adds a key-value pair that will occupy the provided size, evicting
  /// older elements repeatedly until enough room is avialable in the cache.
  /// Returns whether insertion succeeded. If it did, the cache takes
  /// ownership of |value|. Insertion will fail in two cases:
  ///   1) There isn't enough room in the cache even after all unpinned
  ///      elements are freed.
  ///   2) The key you are adding is already present in the cache. In
  ///      this case the element currently existing in the cache remains
  ///      totally unchanged.
  ///
  /// If you use size to represent in-memory size, keep in mind that the
  /// total space used per entry is roughly 2 * key_size + value_size + 30 bytes
  /// (nonexact because we use a hash map internally, so the ratio of reserved
  /// slot to used slots will vary).
  bool add(Key key, Value* value, size_t size);

  /// Same as add(), but the value starts pinned. Saves a map lookup if you
  /// would otherwise do add() then get(). Keep in mind that if insertion
  /// fails the key's pin count has NOT been incremented.
  bool addPinned(Key key, Value* value, size_t size);

  /// Gets an unowned pointer to the value associated with key.
  /// Returns nullptr if the key is not present in the cache.
  /// Once you are done using the returned non-null *value, you must call
  /// release with the same key you passed to get.
  ///
  /// The returned pointer is guaranteed to remain valid until release
  /// is called.
  ///
  /// Note that we return a non-const pointer, and multiple callers
  /// can lease the same object, so if you're mutating it you need
  /// to manage your own locking.
  Value* get(const Key& key);

  /// Unpins a key. You MUST call release on every key you have
  /// get'd once are you done using the value or bad things will
  /// happen (namely, memory leaks).
  void release(const Key& key);

  /// Total size of elements in the cache (NOT the maximum size/limit).
  size_t currentSize() const {
    return curSize_;
  }

  /// The maximum size of the cache.
  size_t maxSize() const {
    return maxSize_;
  }

  SimpleLRUCacheStats stats() const {
    return {
        maxSize_,
        expireDurationMs_,
        curSize_,
        pinnedSize_,
        lruList_.size(),
        numHits_,
        numLookups_,
    };
  }

  /// Removes unpinned elements until at least size space is freed. Returns
  /// the size actually freed, which may be less than requested if the
  /// remaining are all pinned.
  size_t free(size_t size);

 private:
  struct Element {
    Key key;
    Value* value;
    size_t size;
    uint32_t numPins;
    size_t expireTimeMs;
    folly::IntrusiveListHook lruEntry;
    folly::IntrusiveListHook expireEntry;
  };
  using LruList = folly::IntrusiveList<Element, &Element::lruEntry>;
  using ExpireList = folly::IntrusiveList<Element, &Element::expireEntry>;

  bool addInternal(Key key, Value* value, size_t size, bool pinned);

  // Removes the expired and unpinned cache entries from the cache. The function
  // is invoked upon cache lookup, cache insertion and cache entry release.
  void removeExpiredEntries();

  // Removes entry 'e' from cache by unlinking it from 'lruList_' and
  // 'expireList_', and destroy the object at the end.
  size_t freeEntry(Element* e);

  const size_t maxSize_;
  const size_t expireDurationMs_;
  size_t curSize_{0};
  size_t pinnedSize_{0};
  size_t numHits_{0};
  size_t numLookups_{0};

  // Elements get newer as we evict from lruList_.begin() to lruList_.end().
  LruList lruList_;
  ExpireList expireList_;
  folly::F14FastMap<Key, Element*, Hash, Comparator> keys_;
};

///
/// End of public API. Implementation follows.
///
template <typename Key, typename Value, typename Comparator, typename Hash>
inline SimpleLRUCache<Key, Value, Comparator, Hash>::SimpleLRUCache(
    size_t maxSize,
    size_t expireDurationMs)
    : maxSize_(maxSize), expireDurationMs_(expireDurationMs) {}

template <typename Key, typename Value, typename Comparator, typename Hash>
inline SimpleLRUCache<Key, Value, Comparator, Hash>::~SimpleLRUCache() {
  VELOX_CHECK_EQ(pinnedSize_, 0);
  // We could be more optimal than calling free here, but in
  // general this destructor will never get called during normal
  // usage so we don't bother.
  free(maxSize_);
  VELOX_CHECK(lruList_.empty());
  VELOX_CHECK(expireList_.empty());
  VELOX_CHECK(keys_.empty());
  VELOX_CHECK_EQ(curSize_, 0);
}

template <typename Key, typename Value, typename Comparator, typename Hash>
inline bool SimpleLRUCache<Key, Value, Comparator, Hash>::add(
    Key key,
    Value* value,
    size_t size) {
  return addInternal(key, value, size, /*pinned=*/false);
}

template <typename Key, typename Value, typename Comparator, typename Hash>
inline bool SimpleLRUCache<Key, Value, Comparator, Hash>::addPinned(
    Key key,
    Value* value,
    size_t size) {
  return addInternal(key, value, size, /*pinned=*/true);
}

template <typename Key, typename Value, typename Comparator, typename Hash>
inline void
SimpleLRUCache<Key, Value, Comparator, Hash>::removeExpiredEntries() {
  if (expireDurationMs_ == 0) {
    return;
  }
  const auto currentTimeMs = getCurrentTimeMs();
  auto it = expireList_.begin();
  while (it != expireList_.end()) {
    if (it->expireTimeMs > currentTimeMs) {
      return;
    }
    if (it->numPins > 0) {
      ++it;
      continue;
    }
    Element* expiredEntry = &*it;
    it = expireList_.erase(it);
    freeEntry(expiredEntry);
  }
}

template <typename Key, typename Value, typename Comparator, typename Hash>
inline bool SimpleLRUCache<Key, Value, Comparator, Hash>::addInternal(
    Key key,
    Value* value,
    size_t size,
    bool pinned) {
  removeExpiredEntries();

  if (keys_.find(key) != keys_.end()) {
    return false;
  }
  if (pinnedSize_ + size > maxSize_) {
    return false;
  }
  const int64_t spaceNeeded = curSize_ + size - maxSize_;
  if (spaceNeeded > 0) {
    free(spaceNeeded);
  }

  Element* e = new Element;
  e->key = std::move(key);
  e->value = value;
  e->size = size;
  e->numPins = !!pinned;
  if (pinned) {
    pinnedSize_ += size;
  }
  keys_.emplace(e->key, e);
  lruList_.push_back(*e);
  if (expireDurationMs_ != 0) {
    e->expireTimeMs = getCurrentTimeMs() + expireDurationMs_;
    expireList_.push_back(*e);
  }
  curSize_ += size;
  return true;
}

template <typename Key, typename Value, typename Comparator, typename Hash>
inline Value* SimpleLRUCache<Key, Value, Comparator, Hash>::get(
    const Key& key) {
  removeExpiredEntries();

  ++numLookups_;
  auto it = keys_.find(key);
  if (it == keys_.end()) {
    return nullptr;
  }
  Element* entry = it->second;
  if (entry->numPins++ == 0) {
    pinnedSize_ += entry->size;
  }
  VELOX_DCHECK(entry->lruEntry.is_linked());
  entry->lruEntry.unlink();
  lruList_.push_back(*entry);
  ++numHits_;
  return it->second->value;
}

template <typename Key, typename Value, typename Comparator, typename Hash>
inline void SimpleLRUCache<Key, Value, Comparator, Hash>::release(
    const Key& key) {
  Element* e = keys_[key];
  if (--e->numPins == 0) {
    pinnedSize_ -= e->size;
  }
  removeExpiredEntries();
}

template <typename Key, typename Value, typename Comparator, typename Hash>
inline size_t SimpleLRUCache<Key, Value, Comparator, Hash>::free(size_t size) {
  auto it = lruList_.begin();
  size_t freed = 0;
  while (it != lruList_.end() && freed < size) {
    if (it->numPins == 0) {
      Element* evictedEntry = &*it;
      it = lruList_.erase(it);
      freed += freeEntry(evictedEntry);
    } else {
      ++it;
    }
  }
  return freed;
}

template <typename Key, typename Value, typename Comparator, typename Hash>
inline size_t SimpleLRUCache<Key, Value, Comparator, Hash>::freeEntry(
    Element* e) {
  VELOX_CHECK_EQ(e->numPins, 0);
  // NOTE: the list hook dtor will unlink the entry from list so we don't need
  // to explicitly unlink here.
  const auto freedSize = e->size;
  curSize_ -= freedSize;
  keys_.erase(e->key);
  delete e->value;
  delete e;
  return freedSize;
}
} // namespace facebook::velox
