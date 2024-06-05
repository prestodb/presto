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

#include "velox/common/memory/Memory.h"

namespace facebook::velox::io {

constexpr uint64_t DEFAULT_AUTO_PRELOAD_SIZE =
    (static_cast<const uint64_t>((1ul << 20) * 72));

/**
 * Mode for prefetching data.
 *
 * This mode may be ignored for a reader, such as DWRF, where it does not
 * make sense.
 *
 * To enable single-buffered reading, using the default autoPreloadLength:
 *         ReaderOptions readerOpts;
 *         readerOpts.setPrefetchMode(PrefetchMode::PRELOAD);
 * To enable double-buffered reading, using the default autoPreloadLength:
 *         ReaderOptions readerOpts;
 *         readerOpts.setPrefetchMode(PrefetchMode::PREFETCH);
 * To select unbuffered reading:
 *         ReaderOptions readerOpts;
 *         readerOpts.setPrefetchMode(PrefetchMode::NOT_SET);
 *
 * Single-buffered reading (as in dwio::PreloadableInputStream)
 * reads ahead into a buffer.   Double-buffered reading additionally reads
 * asynchronously into a second buffer, swaps the buffers when the
 * first is fully consumed and the second has been filled, and then starts
 * a new parallel read.  For clients with a slow network connection to
 * Warm Storage, enabling PREFETCH reduces elapsed time by 10% or more,
 * at the cost of a second buffer.   The relative improvment would be greater
 * for cases where the network throughput is higher.
 */
enum class PrefetchMode {
  NOT_SET = 0,
  PRELOAD = 1, // read a buffer of autoPreloadLength bytes on a read beyond the
               // current buffer, if any.
  PREFETCH = 2, // read a second buffer of autoPreloadLength bytes ahead of
                // actual reads.
};

class ReaderOptions {
 public:
  static constexpr int32_t kDefaultLoadQuantum = 8 << 20; // 8MB
  static constexpr int32_t kDefaultCoalesceDistance = 512 << 10; // 512K
  static constexpr int32_t kDefaultCoalesceBytes = 128 << 20; // 128M
  static constexpr int32_t kDefaultPrefetchRowGroups = 1;

  explicit ReaderOptions(velox::memory::MemoryPool* pool)
      : memoryPool_(pool),
        autoPreloadLength_(DEFAULT_AUTO_PRELOAD_SIZE),
        prefetchMode_(PrefetchMode::PREFETCH) {}

  ReaderOptions& operator=(const ReaderOptions& other) {
    memoryPool_ = other.memoryPool_;
    autoPreloadLength_ = other.autoPreloadLength_;
    prefetchMode_ = other.prefetchMode_;
    maxCoalesceDistance_ = other.maxCoalesceDistance_;
    maxCoalesceBytes_ = other.maxCoalesceBytes_;
    prefetchRowGroups_ = other.prefetchRowGroups_;
    loadQuantum_ = other.loadQuantum_;
    noCacheRetention_ = other.noCacheRetention_;
    return *this;
  }

  ReaderOptions(const ReaderOptions& other) {
    *this = other;
  }

  /// Sets the memory pool for allocation.
  ReaderOptions& setMemoryPool(velox::memory::MemoryPool& pool) {
    memoryPool_ = &pool;
    return *this;
  }

  /// Modifies the autoPreloadLength
  ReaderOptions& setAutoPreloadLength(uint64_t len) {
    autoPreloadLength_ = len;
    return *this;
  }

  /// Modifies the prefetch mode.
  ReaderOptions& setPrefetchMode(PrefetchMode mode) {
    prefetchMode_ = mode;
    return *this;
  }

  /// Modifies the load quantum.
  ReaderOptions& setLoadQuantum(int32_t quantum) {
    loadQuantum_ = quantum;
    return *this;
  }

  /// Modifies the maximum load coalesce distance.
  ReaderOptions& setMaxCoalesceDistance(int32_t distance) {
    maxCoalesceDistance_ = distance;
    return *this;
  }

  /// Modifies the maximum load coalesce bytes.
  ReaderOptions& setMaxCoalesceBytes(int64_t bytes) {
    maxCoalesceBytes_ = bytes;
    return *this;
  }

  /// Modifies the number of row groups to prefetch.
  ReaderOptions& setPrefetchRowGroups(int32_t numPrefetch) {
    prefetchRowGroups_ = numPrefetch;
    return *this;
  }

  /// Gets the memory allocator.
  velox::memory::MemoryPool& memoryPool() const {
    return *memoryPool_;
  }

  uint64_t autoPreloadLength() const {
    return autoPreloadLength_;
  }

  PrefetchMode prefetchMode() const {
    return prefetchMode_;
  }

  int32_t loadQuantum() const {
    return loadQuantum_;
  }

  int32_t maxCoalesceDistance() const {
    return maxCoalesceDistance_;
  }

  int64_t maxCoalesceBytes() const {
    return maxCoalesceBytes_;
  }

  int64_t prefetchRowGroups() const {
    return prefetchRowGroups_;
  }

  bool noCacheRetention() const {
    return noCacheRetention_;
  }

  void setNoCacheRetention(bool noCacheRetention) {
    noCacheRetention_ = noCacheRetention;
  }

 protected:
  velox::memory::MemoryPool* memoryPool_;
  uint64_t autoPreloadLength_;
  PrefetchMode prefetchMode_;
  int32_t loadQuantum_{kDefaultLoadQuantum};
  int32_t maxCoalesceDistance_{kDefaultCoalesceDistance};
  int64_t maxCoalesceBytes_{kDefaultCoalesceBytes};
  int32_t prefetchRowGroups_{kDefaultPrefetchRowGroups};
  bool noCacheRetention_{false};
};
} // namespace facebook::velox::io
