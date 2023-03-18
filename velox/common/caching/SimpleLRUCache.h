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

// A simple LRU cache that allows each element to occupy an arbitrary
// amount of space in the cache. Useful when your the size of the
// cached elements can vary a lot; if they are all roughly the same
// size something that only tracks the number of elements in the
// cache like common/datastruct/LRUCacheMap.h may be better.
//
// Keep in mind that all the public calls modify internal structures
// and hence require external write locks if used from multiple threads.

#pragma once

#include <cstdint>
#include <functional>
#include <list>

#include "folly/container/F14Map.h"
#include "glog/logging.h"

namespace facebook::velox {

struct SimpleLRUCacheStats {
  // Capacity of the cache.
  int64_t maxSize{0};

  // Current cache size used.
  int64_t curSize{0};

  // Current cache size used by pinned entries.
  int64_t pinnedSize{0};

  // Total number of elements in the cache.
  size_t numElements{0};

  std::string toString() const {
    return fmt::format(
        "{{\n"
        "  maxSize: {}\n"
        "  curSize: {}\n"
        "  pinnedSize: {}\n"
        "  numElements: {}\n"
        "}}",
        maxSize,
        curSize,
        pinnedSize,
        numElements);
  }
};

// Requires that key be copyable and movable.
template <
    typename Key,
    typename Value,
    typename Comparator = std::equal_to<Key>,
    typename Hash = std::hash<Key>>
class SimpleLRUCache {
 public:
  // Constructs a cache of the specified size. This size can represent whatever
  // you want -- slots, or bytes, or etc; you provide the size of each element
  // whenever you add a new value to the cache. Note that in certain
  // circumstances this max_size may be exceeded -- see add().
  SimpleLRUCache(int64_t maxSize);

  // Frees all owned data. Check-fails if any element remains pinned.
  ~SimpleLRUCache();

  // Add a key-value pair that will occupy the provided size, evicting
  // older elements repeatedly until enough room is avialable in the cache.
  // Returns whether insertion succeeded. If it did, the cache takes
  // ownership of |value|. Insertion will fail in two cases:
  //   1) There isn't enough room in the cache even after all unpinned
  //      elements are freed.
  //   2) The key you are adding is already present in the cache. In
  //      this case the element currently existing in the cache remains
  //      totally unchanged.
  //
  // If you use size to represent in-memory size, keep in mind that the
  // total space used per entry is roughly 2 * key_size + value_size + 30 bytes
  // (nonexact because we use a hash map internally, so the ratio of reserved
  // slot to used slots will vary).
  bool add(Key key, Value* value, int64_t size);

  // Same as add(), but the value starts pinned. Saves a map lookup if you
  // would otherwise do add() then get(). Keep in mind that if insertion
  // fails the key's pin count has NOT been incremented.
  bool addPinned(Key key, Value* value, int64_t size);

  // Gets an unowned pointer to the value associated with key.
  // Returns nullptr if the key is not present in the cache.
  // Once you are done using the returned non-null *value, you must call
  // release with the same key you passed to get.
  //
  // The returned pointer is guaranteed to remain valid until release
  // is called.
  //
  // Note that we return a non-const pointer, and multiple callers
  // can lease the same object, so if you're mutating it you need
  // to manage your own locking.
  Value* get(const Key& key);

  // Unpins a key. You MUST call release on every key you have
  // get'd once are you done using the value or bad things will
  // happen (namely, memory leaks).
  void release(const Key& key);

  // Total size of elements in the cache (NOT the maximum size/limit).
  int64_t currentSize() const {
    return curSize_;
  }

  // The maximum size of the cache.
  int64_t maxSize() const {
    return maxSize_;
  }

  SimpleLRUCacheStats getStats() const {
    SimpleLRUCacheStats stats;
    stats.numElements = elements_.size();
    CHECK_EQ(stats.numElements, keys_.size());
    stats.maxSize = maxSize_;
    stats.curSize = curSize_;
    stats.pinnedSize = pinnedSize_;
    return stats;
  }

  // Remove unpinned elements until at least size space is freed. Returns
  // the size actually freed, which may be less than requested if the
  // remaining are all pinned.
  int64_t free(int64_t size);

 private:
  bool addInternal(Key key, Value* value, int64_t size, bool pinned);

  const int64_t maxSize_;
  int64_t curSize_ = 0;
  int64_t pinnedSize_ = 0;

  struct Element {
    Key key;
    Value* value;
    int size;
    int pinCount;
  };
  // Elements get newer as we move from elements_.begin() to elements_.end().
  std::list<Element*> elements_;
  folly::F14FastMap<Key, Element*, Hash, Comparator> keys_;
};

//
//  End of public API. Imlementation follows.
//

template <typename Key, typename Value, typename Comparator, typename Hash>
inline SimpleLRUCache<Key, Value, Comparator, Hash>::SimpleLRUCache(
    int64_t maxSize)
    : maxSize_(maxSize) {}

template <typename Key, typename Value, typename Comparator, typename Hash>
inline SimpleLRUCache<Key, Value, Comparator, Hash>::~SimpleLRUCache() {
  // We could be more optimal than calling free here, but in
  // general this destructor will never get called during normal
  // usage so we don't bother.
  free(maxSize_);
  CHECK(elements_.empty());
  CHECK(keys_.empty());
  CHECK_EQ(curSize_, 0);
}

template <typename Key, typename Value, typename Comparator, typename Hash>
inline bool SimpleLRUCache<Key, Value, Comparator, Hash>::add(
    Key key,
    Value* value,
    int64_t size) {
  return addInternal(key, value, size, /*pinned=*/false);
}

template <typename Key, typename Value, typename Comparator, typename Hash>
inline bool SimpleLRUCache<Key, Value, Comparator, Hash>::addPinned(
    Key key,
    Value* value,
    int64_t size) {
  return addInternal(key, value, size, /*pinned=*/true);
}

template <typename Key, typename Value, typename Comparator, typename Hash>
inline bool SimpleLRUCache<Key, Value, Comparator, Hash>::addInternal(
    Key key,
    Value* value,
    int64_t size,
    bool pinned) {
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
  e->pinCount = pinned;
  if (pinned)
    pinnedSize_ += size;
  keys_.emplace(e->key, e);
  elements_.push_back(e);
  curSize_ += size;
  return true;
}

template <typename Key, typename Value, typename Comparator, typename Hash>
inline Value* SimpleLRUCache<Key, Value, Comparator, Hash>::get(
    const Key& key) {
  auto it = keys_.find(key);
  if (it == keys_.end()) {
    return nullptr;
  }
  if (it->second->pinCount == 0) {
    pinnedSize_ += it->second->size;
  }
  it->second->pinCount++;
  return it->second->value;
}

template <typename Key, typename Value, typename Comparator, typename Hash>
inline void SimpleLRUCache<Key, Value, Comparator, Hash>::release(
    const Key& key) {
  Element* e = keys_[key];
  --e->pinCount;
  if (e->pinCount == 0) {
    pinnedSize_ -= e->size;
  }
}

template <typename Key, typename Value, typename Comparator, typename Hash>
inline int64_t SimpleLRUCache<Key, Value, Comparator, Hash>::free(
    int64_t size) {
  auto it = elements_.begin();
  auto end = elements_.end();
  int64_t freed = 0;
  while (it != end && freed < size) {
    if ((*it)->pinCount == 0) {
      freed += (*it)->size;
      curSize_ -= (*it)->size;
      keys_.erase((*it)->key);
      delete (*it)->value;
      delete *it;
      auto to_be_erased = it;
      ++it;
      elements_.erase(to_be_erased);
    } else {
      ++it;
    }
  }
  return freed;
}
} // namespace facebook::velox
