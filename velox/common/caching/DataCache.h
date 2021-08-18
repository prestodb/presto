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

// Minimal interface for caching data. We provide a simple implementation in
// this header that uses an LRU cache under the hood.
//
// All implementations of DataCache are threadsafe.

#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>

#include "velox/common/caching/SimpleLRUCache.h"

namespace facebook::velox {

class DataCache {
 public:
  // Puts a key/value pair into the cache. Returns whether the value
  // was successfully inserted. If true, its likely (but not guaranteed)
  // that a subsequent Get will retrieve the value. Replaces any previous
  // value for this key.
  virtual bool put(std::string_view key, std::string_view value) = 0;

  // Retrieves a value of known size from the cache, putting the bytes
  // into the (pre-allocated) |buf|. Returns whether the retrieval was
  // successful; if it wasn't, don't rely on |buf|'s contents. If the item
  // retrieves from the cache did not have the provided |size|, the
  // retrieval is considered failed.
  virtual bool get(std::string_view key, uint64_t size, void* buf) = 0;

  // Retrieves a value of unknown size. Returns true if the cache contained
  // the entry, in which case |value| is filled in. Generally prefer the
  // above presized method if you know the size as it may be more efficient.
  virtual bool get(std::string_view key, std::string* value) = 0;

  // Estimated amount of memory used currently by the cache.
  virtual int64_t currentSize() const = 0;

  // The maximum amount of memory the cache can hold.
  virtual int64_t maxSize() const = 0;

  virtual ~DataCache() {}
};

class SimpleLRUDataCache final : public DataCache {
 public:
  // Cache size in bytes.
  explicit SimpleLRUDataCache(int64_t cacheSize) : cache_(cacheSize) {}

  bool put(std::string_view key, std::string_view value) final;
  bool get(std::string_view key, uint64_t size, void* buf) final;
  bool get(std::string_view key, std::string* buf) final;

  int64_t currentSize() const final {
    return cache_.currentSize();
  }

  int64_t maxSize() const final {
    return cache_.maxSize();
  }

 private:
  SimpleLRUCache<std::string, std::string> cache_;
  std::mutex mu_;
};

} // namespace facebook::velox
