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
#include <optional>

#include "folly/container/EvictingCacheMap.h"
#include "glog/logging.h"

namespace facebook::velox {

struct SimpleLRUCacheStats {
  SimpleLRUCacheStats(
      size_t _maxSize,
      size_t _curSize,
      size_t _numHits,
      size_t _numLookups)
      : maxSize{_maxSize},
        curSize{_curSize},
        numHits{_numHits},
        numLookups{_numLookups},
        numElements{curSize},
        pinnedSize{curSize} {}

  // Capacity of the cache.
  const size_t maxSize;

  // Current cache size used.
  const size_t curSize;

  // Total number of cache hits since server start.
  const size_t numHits;

  // Total number of cache lookups since server start.
  const size_t numLookups;

  // TODO: These 2 are unused, but open source Presto depends on them
  // Remove the usage in open source presto and get rid of them.
  const size_t numElements;
  const size_t pinnedSize;

  std::string toString() const {
    return fmt::format(
        "{{\n"
        "  maxSize: {}\n"
        "  curSize: {}\n"
        "  numHits: {}\n"
        "  numLookups: {}\n"
        "}}\n",
        maxSize,
        curSize,
        numHits,
        numLookups);
  }

  bool operator==(const SimpleLRUCacheStats& rhs) const {
    return std::tie(curSize, maxSize, numHits, numLookups) ==
        std::tie(rhs.curSize, rhs.maxSize, rhs.numHits, rhs.numLookups);
  }
};

/// A simple wrapper on top of the folly::EvictingCacheMap that tracks
/// hit/miss counters. Key/Value evicted are immediately destructed.
/// So the Key/Value should be a value type or self managing lifecycle
/// shared_ptr.
///
/// NOTE:
/// 1. NOT Thread-Safe: All the public calls modify internal structures
/// and hence require external write locks if used from multiple threads.
template <typename Key, typename Value>
class SimpleLRUCache {
 public:
  /// Constructs a cache of the specified size. The maxSize represents the
  /// number of entries in the cache. clearSize represents the number of entries
  /// to evict in a given time, when the cache is full.
  explicit SimpleLRUCache(size_t maxSize, size_t clearSize = 1);

  /// Add an item to the cache. Returns true if the item is successfully
  /// added, false otherwise.
  bool add(const Key& key, const Value& value);

  /// Gets value associated with key.
  /// returns std::nullopt when the key is missing
  /// returns the cached value, when the key is present.
  std::optional<Value> get(const Key& key);

  void clear();

  /// Total size of elements in the cache (NOT the maximum size/limit).
  size_t currentSize() const {
    return lru_.size();
  }

  /// The maximum size of the cache.
  size_t maxSize() const {
    return lru_.getMaxSize();
  }

  SimpleLRUCacheStats getStats() const {
    return {
        lru_.getMaxSize(),
        lru_.size(),
        numHits_,
        numLookups_,
    };
  }

 private:
  size_t numHits_{0};
  size_t numLookups_{0};

  folly::EvictingCacheMap<Key, Value> lru_;
};

//
//  End of public API. Imlementation follows.
//

template <typename Key, typename Value>
inline SimpleLRUCache<Key, Value>::SimpleLRUCache(
    size_t maxSize,
    size_t clearSize)
    : lru_(maxSize, clearSize) {}

template <typename Key, typename Value>
inline bool SimpleLRUCache<Key, Value>::add(
    const Key& key,
    const Value& value) {
  return lru_.insert(key, value).second;
}

template <typename Key, typename Value>
inline std::optional<Value> SimpleLRUCache<Key, Value>::get(const Key& key) {
  ++numLookups_;
  auto it = lru_.find(key);
  if (it == lru_.end()) {
    return std::nullopt;
  }

  ++numHits_;
  return it->second;
}

template <typename Key, typename Value>
inline void SimpleLRUCache<Key, Value>::clear() {
  lru_.clear();
}
} // namespace facebook::velox
