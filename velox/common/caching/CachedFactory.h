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

// A cached-backed 'factory'. Basic premise is that a user asks for a key, and
// is either returned a pointer to an existing object in cache, or we execute
// the 'Generator' for that object, place the value the generator created into
// the cache, and then return a pointer to it. From the user's perspective
// whether the Generator actually ran is irrelevant. We handle all the
// lifetime issues/cache pinning/etcetera automatically.
//
// See the test file for some basic examples.

#pragma once

#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <utility>

#include <glog/logging.h>
#include "folly/container/F14Set.h"

#include "velox/common/caching/SimpleLRUCache.h"
#include "velox/common/process/TraceContext.h"

namespace facebook::velox {

// CachedFactory provides a thread-safe way of backing a keyed generator
// (e.g. the key is filename, and the value is the file data) by a cache.
//
// Generator should take a single Key argument and return a Value;
// The Value should be either a value type or should manage its own lifecycle
// (shared_ptr). If it is not thread-safe it must do its own internal locking.
template <typename Key, typename Value, typename Generator>
class CachedFactory {
 public:
  // It is generally expected that most inserts into the cache will succeed,
  // i.e. the cache is large compared to the size of the elements and the number
  // of elements that are pinned. Everything should still work if this is not
  // true, but performance will suffer.
  // If 'cache' is nullptr, this means the cache is disabled. 'generator' is
  // invoked directly in 'generate' function.
  CachedFactory(
      std::unique_ptr<SimpleLRUCache<Key, Value>> cache,
      std::unique_ptr<Generator> generator)
      : cache_(std::move(cache)), generator_(std::move(generator)) {}

  // Returns the generator's output on the given key. If the output is
  // in the cache, returns immediately. Otherwise, blocks until the output
  // is ready.
  // The function returns a pair. The boolean in the pair indicates whether a
  // cache hit or miss. The Value is the generator output for the key if cache
  // miss, or Value in the cache if cache hit.
  std::pair<bool, Value> generate(const Key& key);

  // Advanced function taking in a group of keys. Separates those keys into
  // one's present in the cache (returning CachedPtrs for them) and those not
  // in the cache. Does NOT call the Generator for any key.
  void retrieveCached(
      const std::vector<Key>& keys,
      std::vector<std::pair<Key, Value>>* cached,
      std::vector<Key>* missing);

  // Total size of elements cached (NOT the maximum size/limit).
  int64_t currentSize() const {
    if (cache_) {
      return cache_->currentSize();
    } else {
      return 0;
    }
  }

  // The maximum size of the underlying cache.
  int64_t maxSize() const {
    if (cache_) {
      return cache_->maxSize();
    } else {
      return 0;
    }
  }

  SimpleLRUCacheStats cacheStats() {
    if (cache_) {
      std::lock_guard l(cacheMu_);
      return cache_->getStats();
    } else {
      return {0, 0, 0, 0};
    }
  }

  // Clear the cache and return the current cache status
  SimpleLRUCacheStats clearCache() {
    if (cache_) {
      std::lock_guard l(cacheMu_);
      cache_->clear();
      return cache_->getStats();
    } else {
      return {0, 0, 0, 0};
    }
  }

  // Move allowed, copy disallowed.
  CachedFactory(CachedFactory&&) = default;
  CachedFactory& operator=(CachedFactory&&) = default;
  CachedFactory(const CachedFactory&) = delete;
  CachedFactory& operator=(const CachedFactory&) = delete;

 private:
  std::unique_ptr<SimpleLRUCache<Key, Value>> cache_;
  std::unique_ptr<Generator> generator_;
  folly::F14FastSet<Key> pending_;

  std::mutex cacheMu_;
  std::mutex pendingMu_;
  std::condition_variable pendingCv_;
};

//
// End of public API. Implementation follows.
//

template <typename Key, typename Value, typename Generator>
std::pair<bool, Value> CachedFactory<Key, Value, Generator>::generate(
    const Key& key) {
  process::TraceContext trace("CachedFactory::generate");
  if (!cache_) {
    return std::make_pair(false, (*generator_)(key));
  }

  std::unique_lock<std::mutex> pending_lock(pendingMu_);
  {
    std::lock_guard<std::mutex> cache_lock(cacheMu_);
    auto value = cache_->get(key);
    if (value) {
      return std::make_pair(true, value.value());
    }
  }

  if (pending_.contains(key)) {
    pendingCv_.wait(pending_lock, [&]() { return !pending_.contains(key); });
    // Will normally hit the cache now.
    {
      std::lock_guard<std::mutex> cache_lock(cacheMu_);
      auto value = cache_->get(key);
      if (value) {
        return std::make_pair(true, value.value());
      }
    }
    pending_lock.unlock();
    return generate(key); // Regenerate in the edge case.
  } else {
    pending_.insert(key);
    pending_lock.unlock();
    Value generatedValue;
    // TODO: consider using folly/ScopeGuard here.
    try {
      generatedValue = (*generator_)(key);
    } catch (const std::exception&) {
      {
        std::lock_guard<std::mutex> pending_lock_2(pendingMu_);
        pending_.erase(key);
      }
      pendingCv_.notify_all();
      throw;
    }
    cacheMu_.lock();
    cache_->add(key, generatedValue);
    cacheMu_.unlock();

    // TODO: this code is exception unsafe and can leave pending_ in an
    // inconsistent state. Eventually this code should move to
    // folly:synchronized and rewritten with better primitives.
    {
      std::lock_guard<std::mutex> pending_lock_2(pendingMu_);
      pending_.erase(key);
    }
    pendingCv_.notify_all();
    return std::make_pair(false, generatedValue);
  }
}

template <typename Key, typename Value, typename Generator>
void CachedFactory<Key, Value, Generator>::retrieveCached(
    const std::vector<Key>& keys,
    std::vector<std::pair<Key, Value>>* cached,
    std::vector<Key>* missing) {
  if (cache_) {
    std::lock_guard<std::mutex> cache_lock(cacheMu_);
    for (const Key& key : keys) {
      auto value = cache_->get(key);
      if (value) {
        cached->emplace_back(key, value.value());
      } else {
        missing->push_back(key);
      }
    }
  } else {
    for (const Key& key : keys) {
      missing->push_back(key);
    }
  }
}

} // namespace facebook::velox
