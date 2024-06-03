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

#include <folly/GLog.h>
#include <glog/logging.h>
#include "folly/container/F14Set.h"

#include "velox/common/caching/SimpleLRUCache.h"
#include "velox/common/process/TraceContext.h"

namespace facebook::velox {

/// A smart pointer that represents data that may be in a cache and is thus not
/// owned, or is owned like a unique_ptr. We could also implement this by a
/// unique_ptr with a custom deleter.
template <
    typename Key,
    typename Value,
    typename Comparator = std::equal_to<Key>,
    typename Hash = std::hash<Key>>
class CachedPtr {
 public:
  /// Nullptr case.
  CachedPtr();

  /// Data is not in cache, ownership taken by *this.
  explicit CachedPtr(Value* value);

  /// Data is in the provided cache referenced by the given key. The cache is
  /// not guarded by a mutex.
  CachedPtr(
      bool cached,
      Value* value,
      SimpleLRUCache<Key, Value, Comparator, Hash>* cache,
      std::unique_ptr<Key> key);

  /// Same as above, but the cache is guarded by a mutex.
  CachedPtr(
      bool cached,
      Value* value,
      SimpleLRUCache<Key, Value, Comparator, Hash>* cache,
      std::unique_ptr<Key> key,
      std::mutex* cacheMu);

  /// The destructor handles the in-cache and non-in-cache cases appropriately.
  ~CachedPtr();

  /// Move allowed, copy disallowed. Moving a new value into a non-null
  /// CachedPtr will clear the previous value.
  CachedPtr(CachedPtr&&);
  CachedPtr& operator=(CachedPtr&&);
  CachedPtr(const CachedPtr&) = delete;
  CachedPtr& operator=(const CachedPtr&) = delete;

  /// Whether this value is load from cache. If we had to wait for a generation
  /// (whether the actual generation was done in this thread or another) then
  /// this is false. Has no effect on this behavior, but may be useful for
  /// monitoring cache hit rates/etc.
  bool fromCache() const {
    return fromCache_;
  }

  /// Indicates if this value is cached or not.
  bool cached() const {
    return cache_ != nullptr;
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

  void testingClear() {
    clear();
    key_.reset();
    value_ = nullptr;
    cache_ = nullptr;
    cacheMu_ = nullptr;
  }

 private:
  // Delete or release owned value.
  void clear();

  bool fromCache_;
  std::unique_ptr<Key> key_;
  Value* value_;
  std::mutex* cacheMu_;
  // If 'value_' is in cache, 'cache_' and 'key_' will be non-null, and
  // 'cacheMu_' may be non-null. If cacheMu_ is non-null, we use it to protect
  // our operations to 'cache_'.
  SimpleLRUCache<Key, Value, Comparator, Hash>* cache_;
};

template <typename Value>
struct DefaultSizer {
  int64_t operator()(const Value& value) const {
    return 1;
  }
};

/// CachedFactory provides a thread-safe way of backing a keyed generator
/// (e.g. the key is filename, and the value is the file data) by a cache.
///
/// Generator should take a single Key argument and return a unique_ptr<Value>;
/// If it is not thread-safe it must do its own internal locking.
/// Sizer takes a Value and returns how much cache space it will occupy. The
/// DefaultSizer says each value occupies 1 space.
template <
    typename Key,
    typename Value,
    typename Generator,
    typename Properties = void,
    typename Sizer = DefaultSizer<Value>,
    typename Comparator = std::equal_to<Key>,
    typename Hash = std::hash<Key>>
class CachedFactory {
 public:
  /// It is generally expected that most inserts into the cache will succeed,
  /// i.e. the cache is large compared to the size of the elements and the
  /// number of elements that are pinned. Everything should still work if this
  /// is not true, but performance will suffer. If 'cache' is nullptr, this
  /// means the cache is disabled. 'generator' is invoked directly in 'generate'
  /// function.
  CachedFactory(
      std::unique_ptr<SimpleLRUCache<Key, Value, Comparator, Hash>> cache,
      std::unique_ptr<Generator> generator)
      : generator_(std::move(generator)), cache_(std::move(cache)) {}

  CachedFactory(std::unique_ptr<Generator> generator)
      : CachedFactory(nullptr, std::move(generator)) {}

  /// Returns the generator's output on the given key. If the output is in the
  /// cache, returns immediately. Otherwise, blocks until the output is ready.
  /// For a given key we will only ever be running the Generator function once.
  /// E.g., if N threads ask for the same key at once, the generator will be
  /// fired once and all N will receive a pointer from the cache.
  ///
  /// Actually the last sentence is not quite true in the edge case where
  /// inserts into the cache fail; in that case we will re-run the generator
  /// repeatedly, handing off the results to one thread at a time until the
  /// all pending requests are satisfied or a cache insert succeeds. This
  /// will probably mess with your memory model, so really try to avoid it.
  CachedPtr<Key, Value, Comparator, Hash> generate(
      const Key& key,
      const Properties* properties = nullptr);

  /// Advanced function taking in a group of keys. Separates those keys into
  /// one's present in the cache (returning CachedPtrs for them) and those not
  /// in the cache. Does NOT call the Generator for any key.
  void retrieveCached(
      const std::vector<Key>& keys,
      std::vector<std::pair<Key, CachedPtr<Key, Value, Comparator, Hash>>>&
          cached,
      std::vector<Key>& missing);

  /// Total size of elements cached (NOT the maximum size/limit).
  int64_t currentSize() const {
    if (cache_ == nullptr) {
      return 0;
    }
    return cache_->currentSize();
  }

  /// The maximum size of the underlying cache.
  int64_t maxSize() const {
    if (cache_ == nullptr) {
      return 0;
    }
    return cache_->maxSize();
  }

  SimpleLRUCacheStats cacheStats() {
    if (cache_ == nullptr) {
      return {};
    }
    std::lock_guard l(cacheMu_);
    return cache_->stats();
  }

  // Clear the cache and return the current cache status
  SimpleLRUCacheStats clearCache() {
    if (cache_ == nullptr) {
      return {};
    }
    std::lock_guard l(cacheMu_);
    cache_->free(cache_->maxSize());
    return cache_->stats();
  }

  /// Move allowed, copy disallowed.
  CachedFactory(CachedFactory&&) = default;
  CachedFactory& operator=(CachedFactory&&) = default;
  CachedFactory(const CachedFactory&) = delete;
  CachedFactory& operator=(const CachedFactory&) = delete;

 private:
  void removePending(const Key& key) {
    std::lock_guard<std::mutex> pendingLock(pendingMu_);
    pending_.erase(key);
  }

  bool addCache(const Key& key, Value* value, int64_t size) {
    std::lock_guard<std::mutex> cacheLock(cacheMu_);
    return cache_->addPinned(key, value, size);
  }

  Value* getCache(const Key& key) {
    std::lock_guard<std::mutex> cacheLock(cacheMu_);
    return getCacheLocked(key);
  }

  Value* getCacheLocked(const Key& key) {
    return cache_->get(key);
  }

  std::unique_ptr<Generator> generator_;

  std::mutex cacheMu_;
  std::unique_ptr<SimpleLRUCache<Key, Value, Comparator, Hash>> cache_;

  std::mutex pendingMu_;
  folly::F14FastSet<Key, Hash, Comparator> pending_;
  std::condition_variable pendingCv_;
};

//
// End of public API. Implementation follows.
//

template <typename Key, typename Value, typename Comparator, typename Hash>
CachedPtr<Key, Value, Comparator, Hash>::CachedPtr()
    : fromCache_(false),
      key_(nullptr),
      value_(nullptr),
      cacheMu_(nullptr),
      cache_(nullptr) {}

template <typename Key, typename Value, typename Comparator, typename Hash>
CachedPtr<Key, Value, Comparator, Hash>::CachedPtr(Value* value)
    : fromCache_(false),
      key_(nullptr),
      value_(value),
      cacheMu_(nullptr),
      cache_(nullptr) {}

template <typename Key, typename Value, typename Comparator, typename Hash>
CachedPtr<Key, Value, Comparator, Hash>::CachedPtr(
    bool cached,
    Value* value,
    SimpleLRUCache<Key, Value, Comparator, Hash>* cache,
    std::unique_ptr<Key> key)
    : fromCache_(cached),
      key_(std::move(key)),
      value_(value),
      cacheMu_(nullptr),
      cache_(cache) {}

template <typename Key, typename Value, typename Comparator, typename Hash>
CachedPtr<Key, Value, Comparator, Hash>::CachedPtr(
    bool cached,
    Value* value,
    SimpleLRUCache<Key, Value, Comparator, Hash>* cache,
    std::unique_ptr<Key> key,
    std::mutex* cacheMu)
    : fromCache_(cached),
      key_(std::move(key)),
      value_(value),
      cacheMu_(cacheMu),
      cache_(cache) {}

template <typename Key, typename Value, typename Comparator, typename Hash>
CachedPtr<Key, Value, Comparator, Hash>::~CachedPtr() {
  clear();
}

template <typename Key, typename Value, typename Comparator, typename Hash>
CachedPtr<Key, Value, Comparator, Hash>::CachedPtr(CachedPtr&& other) {
  fromCache_ = other.fromCache_;
  value_ = other.value_;
  key_ = std::move(other.key_);
  cache_ = other.cache_;
  cacheMu_ = other.cacheMu_;
  other.value_ = nullptr;
}

template <typename Key, typename Value, typename Comparator, typename Hash>
CachedPtr<Key, Value, Comparator, Hash>&
CachedPtr<Key, Value, Comparator, Hash>::operator=(CachedPtr&& other) {
  clear();
  fromCache_ = other.fromCache_;
  value_ = other.value_;
  key_ = std::move(other.key_);
  cache_ = other.cache_;
  cacheMu_ = other.cacheMu_;
  other.value_ = nullptr;
  return *this;
}

template <typename Key, typename Value, typename Comparator, typename Hash>
void CachedPtr<Key, Value, Comparator, Hash>::clear() {
  if (value_ == nullptr) {
    return;
  }
  if (cache_ == nullptr) {
    delete value_;
    return;
  }
  if (cacheMu_ != nullptr) {
    std::lock_guard<std::mutex> l(*cacheMu_);
    cache_->release(*key_);
  } else {
    cache_->release(*key_);
  }
}

template <
    typename Key,
    typename Value,
    typename Generator,
    typename Properties,
    typename Sizer,
    typename Comparator,
    typename Hash>
CachedPtr<Key, Value, Comparator, Hash>
CachedFactory<Key, Value, Generator, Properties, Sizer, Comparator, Hash>::
    generate(const Key& key, const Properties* properties) {
  process::TraceContext trace("CachedFactory::generate");
  if (cache_ == nullptr) {
    return CachedPtr<Key, Value, Comparator, Hash>{
        /*fromCache=*/false,
        (*generator_)(key, properties).release(),
        nullptr,
        std::make_unique<Key>(key)};
  }

  std::unique_lock<std::mutex> pendingLock(pendingMu_);
  {
    if (Value* value = getCache(key)) {
      return CachedPtr<Key, Value, Comparator, Hash>(
          /*fromCache=*/true,
          value,
          cache_.get(),
          std::make_unique<Key>(key),
          &cacheMu_);
    }
  }
  if (pending_.contains(key)) {
    pendingCv_.wait(pendingLock, [&]() { return !pending_.contains(key); });
    // Will normally hit the cache now.
    if (Value* value = getCache(key)) {
      return CachedPtr<Key, Value, Comparator, Hash>(
          /*fromCache=*/false,
          value,
          cache_.get(),
          std::make_unique<Key>(key),
          &cacheMu_);
    }
    pendingLock.unlock();
    // Regenerates in the edge case.
    return generate(key, properties);
  }

  pending_.insert(key);
  pendingLock.unlock();

  SCOPE_EXIT {
    removePending(key);
    pendingCv_.notify_all();
  };

  std::unique_ptr<Value> generatedValue = (*generator_)(key, properties);
  const uint64_t valueSize = Sizer()(*generatedValue);
  Value* rawValue = generatedValue.release();
  const bool inserted = addCache(key, rawValue, valueSize);

  CachedPtr<Key, Value, Comparator, Hash> result;
  if (inserted) {
    result = CachedPtr<Key, Value, Comparator, Hash>(
        /*fromCache=*/false,
        rawValue,
        cache_.get(),
        std::make_unique<Key>(key),
        &cacheMu_);
  } else {
    FB_LOG_EVERY_MS(WARNING, 60'000) << "Unable to insert into cache!";
    result = CachedPtr<Key, Value, Comparator, Hash>(rawValue);
  }
  return result;
}

template <
    typename Key,
    typename Value,
    typename Generator,
    typename Properties,
    typename Sizer,
    typename Comparator,
    typename Hash>
void CachedFactory<Key, Value, Generator, Properties, Sizer, Comparator, Hash>::
    retrieveCached(
        const std::vector<Key>& keys,
        std::vector<std::pair<Key, CachedPtr<Key, Value, Comparator, Hash>>>&
            cached,
        std::vector<Key>& missing) {
  if (cache_ == nullptr) {
    missing.insert(missing.end(), keys.begin(), keys.end());
    return;
  }

  std::lock_guard<std::mutex> l(cacheMu_);
  for (const Key& key : keys) {
    Value* value = getCacheLocked(key);
    if (value != nullptr) {
      cached.emplace_back(
          key,
          CachedPtr<Key, Value, Comparator, Hash>(
              /*fromCache=*/true,
              value,
              cache_.get(),
              std::make_unique<Key>(key),
              &cacheMu_));
    } else {
      missing.push_back(key);
    }
  }
}

} // namespace facebook::velox
