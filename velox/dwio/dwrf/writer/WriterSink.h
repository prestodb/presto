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

#include <folly/container/Array.h>

#include "velox/dwio/dwrf/common/Checksum.h"
#include "velox/dwio/dwrf/common/Config.h"
#include "velox/dwio/dwrf/common/DataBufferHolder.h"

namespace facebook::velox::dwrf {

namespace {

constexpr auto ORC_MAGIC = folly::make_array('O', 'R', 'C');
constexpr auto ORC_MAGIC_LEN = ORC_MAGIC.size();
constexpr uint32_t SLICE_SIZE = 128 * 1024;

} // namespace

class WriterSink {
 public:
  enum Mode : uint8_t { None = 0, Data = 1, Index = 2, Footer = 3 };

  WriterSink(
      dwio::common::DataSink& sink,
      memory::MemoryPool& pool,
      const Config& configs)
      : sink_{sink},
        checksum_{
            ChecksumFactory::create(configs.get(Config::CHECKSUM_ALGORITHM))},
        cacheMode_{configs.get(Config::STRIPE_CACHE_MODE)},
        mode_{Mode::None},
        shouldBuffer_{!sink.isBuffered()},
        size_{0},
        maxCacheSize_{configs.get(Config::STRIPE_CACHE_SIZE)},
        cacheHolder_{pool, SLICE_SIZE, SLICE_SIZE},
        cacheBuffer_{pool},
        exceedsLimit_{false} {
    if (cacheMode_ != StripeCacheMode::NA) {
      offsets_.push_back(0);
      cacheBuffer_.reserve(SLICE_SIZE);
    }

    // initialize the buffer with the orc header
    addBuffer(pool, ORC_MAGIC.data(), ORC_MAGIC_LEN);
  }

  ~WriterSink() {
    if (!buffers_.empty() || size_ != 0) {
      LOG(WARNING) << "Unflushed data in writer sink!";
    }
  }

  uint64_t size() const {
    return sink_.size() + size_;
  }

  void addBuffer(memory::MemoryPool& pool, const char* data, size_t size) {
    dwio::common::DataBuffer<char> buf{pool, size};
    std::memcpy(buf.data(), data, size);
    addBuffer(std::move(buf));
  }

  void addBuffer(dwio::common::DataBuffer<char> buffer);

  void addBuffers(DataBufferHolder& holder) {
    auto& other = holder.getBuffers();
    for (auto& buf : other) {
      addBuffer(std::move(buf));
    }
    other.clear();
  }

  void flush() {
    sink_.write(buffers_);
    buffers_.clear();
    size_ = 0;
  }

  Checksum* getChecksum() {
    return checksum_.get();
  }

  StripeCacheMode getCacheMode() const {
    return cacheMode_;
  }

  const std::vector<uint32_t>& getCacheOffsets() const {
    return offsets_;
  }

  uint32_t getCacheSize() const {
    auto size = offsets_.size();
    return size ? offsets_.at(size - 1) : 0;
  }

  void writeCache() {
    DWIO_ENSURE_EQ(mode_, Mode::None);

    auto size = getCacheSize();
    DWIO_ENSURE_EQ(size, getCurrentCacheSize());
    if (size > 0) {
      addBuffers(cacheHolder_);
      if (cacheBuffer_.size()) {
        addBuffer(std::move(cacheBuffer_));
      }
    }
  }

  void setMode(Mode mode) {
    if (mode != mode_ && shouldCache()) {
      offsets_.push_back(getCurrentCacheSize());
    }
    mode_ = mode;
  }

 private:
  dwio::common::DataSink& sink_;
  std::unique_ptr<Checksum> checksum_;
  StripeCacheMode cacheMode_;
  Mode mode_;
  bool shouldBuffer_;
  uint64_t size_;

  // members used by stripe metadata cache
  uint32_t maxCacheSize_;
  DataBufferHolder cacheHolder_;
  dwio::common::DataBuffer<char> cacheBuffer_;
  std::vector<uint32_t> offsets_;
  bool exceedsLimit_;

  std::vector<dwio::common::DataBuffer<char>> buffers_;

  bool shouldChecksum() {
    // checksum is captured in all modes except None and if checksum algorithm
    // is available
    return mode_ != Mode::None && checksum_;
  }

  bool shouldCache() {
    // metadata cache is captured in Index and Footer mode when corresponsing
    // mode is also enabled thru config, and if cache size is not exceeding
    // configured limit. For example, if current mode is Index and if configured
    // mode is BOTH, then cache is captured. However, if configuered mode is
    // FOOTER, then it's not captured.
    // Below, we do bitwise operator to identify a match in mode
    return (mode_ & 0x02) &&
        (((mode_ - 1) & 0x03) & static_cast<uint8_t>(cacheMode_)) &&
        !exceedsLimit_;
  }

  uint32_t getCurrentCacheSize() const {
    return static_cast<uint32_t>(cacheHolder_.size() + cacheBuffer_.size());
  }

  // Truncate cache data that is beyond the last offset.
  void truncateCache() {
    // Cache can't accommodate all Stripe's index streams and/or Footer and stay
    // within the Cache size limit. Since there are many index streams for a
    // stripe, some of them could be already in cache. This method truncates
    // any such buffer that is appended and is past the Cache size.
    auto sizeRemaining = getCacheSize();
    if (sizeRemaining < getCurrentCacheSize()) {
      if (sizeRemaining < cacheHolder_.size()) {
        cacheHolder_.truncate(sizeRemaining);
        cacheBuffer_.resize(0);
      } else {
        sizeRemaining -= cacheHolder_.size();
        cacheBuffer_.resize(sizeRemaining);
      }
    }
  }
};

} // namespace facebook::velox::dwrf
