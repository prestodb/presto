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

#include "folly/container/F14Set.h"
#include "glog/logging.h"

#include "velox/common/caching/SimpleLRUCache.h"

namespace facebook::velox {

// A smart pointer that represents data that may be in a cache and is thus
// not owned, or is owned like a unique_ptr. We could also implement this
// by a unique_ptr with a custom deleter, but I find this representation nicer.
template <
    typename Key,
    typename Value,
    typename Comparator = std::equal_to<Key>,
    typename Hash = std::hash<Key>>
class CachedPtr {
 public:
  // Nullptr case.
  CachedPtr();

  // Data is not in cache, ownership taken by *this.
  explicit CachedPtr(Value* value);

  // Data is in the provided cache referenced by the given key. The cache is
  // not guarded by a mutex.
  CachedPtr(
      bool wasCached,
      Value* value,
      SimpleLRUCache<Key, Value, Comparator, Hash>* cache,
      std::unique_ptr<Key> key);
  // Same as above, but the cache IS guarded by a mutex.
  CachedPtr(
      bool wasCached,
      Value* value,
      SimpleLRUCache<Key, Value, Comparator, Hash>* cache,
      std::unique_ptr<Key> key,
      std::mutex* mu);

  // The destructor handles the in-cache and non-in-cache cases appropriately.
  ~CachedPtr();

  // Move allowed, copy disallowed. Moving a new value into a non-null CachedPtr
  // will clear the previous value.
  CachedPtr(CachedPtr&&);
  CachedPtr& operator=(CachedPtr&&);
  CachedPtr(const CachedPtr&) = delete;
  CachedPtr& operator=(const CachedPtr&) = delete;

  // Whether this value was cached. If we had to wait for a generation
  // (whether the actual generation was done in this thread or another) then
  // this is false. Has no effect on this's behavior, but may be useful for
  // monitoring cache hit rates/etc.
  bool wasCached() {
    return wasCached_;
  }

  Value* operator->() const {
    return value_;
  }
  Value& operator*() const {
    return *value_;
  }
  Value* get() const {
    return value_;
  }

 private:
  // Delete or release owned value.
  void clear();

  bool wasCached_;
  Value* value_;
  // If value_ is in the cache, cache_ and key_ will be non-null,
  // and mu_ MAY be non-null. If mu_ is non-null_, we use it
  // to protect our cache operations.
  SimpleLRUCache<Key, Value, Comparator, Hash>* cache_;
  std::unique_ptr<Key> key_;
  std::mutex* mu_;
};

template <typename Value>
struct DefaultSizer {
  int64_t operator()(const Value& value) const {
    return 1;
  }
};

// CachedFactory provides a thread-safe way of backing a keyed generator
// (e.g. the key is filename, and the value is the file data) by a cache.
//
// Generator should take a single Key argument and return a unique_ptr<Value>;
// If it is not thread-safe it must do its own internal locking.
// Sizer takes a Value and returns how much cache space it will occupy. The
// DefaultSizer says each value occupies 1 space.
template <
    typename Key,
    typename Value,
    typename Generator,
    typename Sizer = DefaultSizer<Value>,
    typename Comparator = std::equal_to<Key>,
    typename Hash = std::hash<Key>>
class CachedFactory {
 public:
  // It is generally expected that most inserts into the cache will succeed,
  // i.e. the cache is large compared to the size of the elements and the number
  // of elements that are pinned. Everything should still work if this is not
  // true, but performance will suffer.
  CachedFactory(
      std::unique_ptr<SimpleLRUCache<Key, Value, Comparator, Hash>> cache,
      std::unique_ptr<Generator> generator)
      : cache_(std::move(cache)), generator_(std::move(generator)) {}

  // Returns the generator's output on the given key. If the output is
  // in the cache, returns immediately. Otherwise, blocks until the output
  // is ready. For a given key we will only ever be running the Generator
  // function once. E.g., if N threads ask for the same key at once, the
  // generator will be fired once and all N will receive a pointer from
  // the cache.
  //
  // Actually the last sentence is not quite true in the edge case where
  // inserts into the cache fail; in that case we will re-run the generator
  // repeatedly, handing off the results to one thread at a time until the
  // all pending requests are satisfied or a cache insert succeeds. This
  // will probably mess with your memory model, so really try to avoid it.
  CachedPtr<Key, Value, Comparator, Hash> generate(const Key& key);

  // Advanced function taking in a group of keys. Separates those keys into
  // one's present in the cache (returning CachedPtrs for them) and those not
  // in the cache. Does NOT call the Generator for any key.
  void retrieveCached(
      const std::vector<Key>& keys,
      std::vector<std::pair<Key, CachedPtr<Key, Value, Comparator, Hash>>>*
          cached,
      std::vector<Key>* missing);

  // Total size of elements cached (NOT the maximum size/limit).
  int64_t currentSize() const {
    return cache_->currentSize();
  }

  // The maximum size of the underlying cache.
  int64_t maxSize() const {
    return cache_->maxSize();
  }

  SimpleLRUCacheStats cacheStats() {
    std::lock_guard l(cacheMu_);
    return cache_->getStats();
  }

  // Clear the cache and return the current cache status
  SimpleLRUCacheStats clearCache() {
    std::lock_guard l(cacheMu_);
    cache_->free(cache_->maxSize());
    return cache_->getStats();
  }

  // Move allowed, copy disallowed.
  CachedFactory(CachedFactory&&) = default;
  CachedFactory& operator=(CachedFactory&&) = default;
  CachedFactory(const CachedFactory&) = delete;
  CachedFactory& operator=(const CachedFactory&) = delete;

 private:
  std::unique_ptr<SimpleLRUCache<Key, Value, Comparator, Hash>> cache_;
  std::unique_ptr<Generator> generator_;
  folly::F14FastSet<Key, Hash, Comparator> pending_;

  std::mutex cacheMu_;
  std::mutex pendingMu_;
  std::condition_variable pendingCv_;
};

//
// End of public API. Implementation follows.
//

template <typename Key, typename Value, typename Comparator, typename Hash>
CachedPtr<Key, Value, Comparator, Hash>::CachedPtr()
    : wasCached_(false),
      value_(nullptr),
      cache_(nullptr),
      key_(nullptr),
      mu_(nullptr) {}

template <typename Key, typename Value, typename Comparator, typename Hash>
CachedPtr<Key, Value, Comparator, Hash>::CachedPtr(Value* value)
    : wasCached_(false),
      value_(value),
      cache_(nullptr),
      key_(nullptr),
      mu_(nullptr) {}

template <typename Key, typename Value, typename Comparator, typename Hash>
CachedPtr<Key, Value, Comparator, Hash>::CachedPtr(
    bool wasCached,
    Value* value,
    SimpleLRUCache<Key, Value, Comparator, Hash>* cache,
    std::unique_ptr<Key> key)
    : wasCached_(wasCached),
      value_(value),
      cache_(cache),
      key_(std::move(key)),
      mu_(nullptr) {}

template <typename Key, typename Value, typename Comparator, typename Hash>
CachedPtr<Key, Value, Comparator, Hash>::CachedPtr(
    bool wasCached,
    Value* value,
    SimpleLRUCache<Key, Value, Comparator, Hash>* cache,
    std::unique_ptr<Key> key,
    std::mutex* mu)
    : wasCached_(wasCached),
      value_(value),
      cache_(cache),
      key_(std::move(key)),
      mu_(mu) {}

template <typename Key, typename Value, typename Comparator, typename Hash>
CachedPtr<Key, Value, Comparator, Hash>::~CachedPtr() {
  clear();
}

template <typename Key, typename Value, typename Comparator, typename Hash>
CachedPtr<Key, Value, Comparator, Hash>::CachedPtr(CachedPtr&& other) {
  wasCached_ = other.wasCached_;
  value_ = other.value_;
  key_ = std::move(other.key_);
  cache_ = other.cache_;
  mu_ = other.mu_;
  other.value_ = nullptr;
}

template <typename Key, typename Value, typename Comparator, typename Hash>
CachedPtr<Key, Value, Comparator, Hash>&
CachedPtr<Key, Value, Comparator, Hash>::operator=(CachedPtr&& other) {
  clear();
  wasCached_ = other.wasCached_;
  value_ = other.value_;
  key_ = std::move(other.key_);
  cache_ = other.cache_;
  mu_ = other.mu_;
  other.value_ = nullptr;
  return *this;
}

template <typename Key, typename Value, typename Comparator, typename Hash>
void CachedPtr<Key, Value, Comparator, Hash>::clear() {
  if (!value_)
    return;
  if (cache_) {
    if (mu_) {
      std::lock_guard<std::mutex> l(*mu_);
      cache_->release(*key_);
    } else {
      cache_->release(*key_);
    }
  } else {
    delete value_;
  }
}

template <
    typename Key,
    typename Value,
    typename Generator,
    typename Sizer,
    typename Comparator,
    typename Hash>
CachedPtr<Key, Value, Comparator, Hash>
CachedFactory<Key, Value, Generator, Sizer, Comparator, Hash>::generate(
    const Key& key) {
  std::unique_lock<std::mutex> pending_lock(pendingMu_);
  {
    std::lock_guard<std::mutex> cache_lock(cacheMu_);
    Value* value = cache_->get(key);
    if (value) {
      return CachedPtr<Key, Value, Comparator, Hash>(
          /*wasCached=*/true,
          value,
          cache_.get(),
          std::make_unique<Key>(key),
          &cacheMu_);
    }
  }
  if (pending_.contains(key)) {
    pendingCv_.wait(pending_lock, [&]() { return !pending_.contains(key); });
    // Will normally hit the cache now.
    {
      std::lock_guard<std::mutex> cache_lock(cacheMu_);
      Value* value = cache_->get(key);
      if (value) {
        return CachedPtr<Key, Value, Comparator, Hash>(
            /*wasCached=*/false,
            value,
            cache_.get(),
            std::make_unique<Key>(key),
            &cacheMu_);
      }
    }
    pending_lock.unlock();
    return generate(key); // Regenerate in the edge case.
  } else {
    pending_.insert(key);
    pending_lock.unlock();
    std::unique_ptr<Value> generatedValue;
    // TODO: consider using folly/ScopeGuard here.
    try {
      generatedValue = (*generator_)(key);
    } catch (const std::exception& e) {
      {
        std::lock_guard<std::mutex> pending_lock(pendingMu_);
        pending_.erase(key);
      }
      pendingCv_.notify_all();
      throw;
    }
    const uint64_t sizeOccupied = Sizer()(*generatedValue);
    Value* rawValue = generatedValue.release();
    cacheMu_.lock();
    const bool inserted = cache_->addPinned(key, rawValue, sizeOccupied);
    cacheMu_.unlock();
    CachedPtr<Key, Value, Comparator, Hash> result;
    if (inserted) {
      result = CachedPtr<Key, Value, Comparator, Hash>(
          /*wasCached=*/false,
          rawValue,
          cache_.get(),
          std::make_unique<Key>(key),
          &cacheMu_);
    } else {
      // We want a LOG_EVERY_N_SEC warning here, but it doesn't seem to
      // be available in glog?
      // LOG_EVERY_N_SEC(WARNING, 60) << "Unable to insert into cache!";
      result = CachedPtr<Key, Value, Comparator, Hash>(rawValue);
    }
    {
      std::lock_guard<std::mutex> pending_lock(pendingMu_);
      pending_.erase(key);
    }
    pendingCv_.notify_all();
    return result;
  }
}

template <
    typename Key,
    typename Value,
    typename Generator,
    typename Sizer,
    typename Comparator,
    typename Hash>
void CachedFactory<Key, Value, Generator, Sizer, Comparator, Hash>::
    retrieveCached(
        const std::vector<Key>& keys,
        std::vector<std::pair<Key, CachedPtr<Key, Value, Comparator, Hash>>>*
            cached,
        std::vector<Key>* missing) {
  std::lock_guard<std::mutex> cache_lock(cacheMu_);
  for (const Key& key : keys) {
    Value* value = cache_->get(key);
    if (value) {
      cached->emplace_back(
          key,
          CachedPtr<Key, Value, Comparator, Hash>(
              /*wasCached=*/true,
              value,
              cache_.get(),
              std::make_unique<Key>(key),
              &cacheMu_));
    } else {
      missing->push_back(key);
    }
  }
}

} // namespace facebook::velox
