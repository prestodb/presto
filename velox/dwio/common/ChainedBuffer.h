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

#include <bitset>
#include "velox/common/base/GTestMacros.h"
#include "velox/dwio/common/DataBuffer.h"

namespace facebook {
namespace velox {
namespace dwio {
namespace common {

namespace {

constexpr uint32_t DEFAULT_MAX_PAGE_BYTES = 256 * 1024;
constexpr uint32_t DEFAULT_INITIAL_CAPACITY = 1024;

} // namespace

template <typename T>
class ChainedBuffer {
 public:
  explicit ChainedBuffer(
      velox::memory::MemoryPool& pool,
      uint32_t initialCapacity = DEFAULT_INITIAL_CAPACITY,
      uint32_t maxPageBytes = DEFAULT_MAX_PAGE_BYTES)
      : pool_{pool},
        size_{0},
        capacity_{0},
        initialCapacity_{initialCapacity},
        maxPageCapacity_{maxPageBytes / static_cast<uint32_t>(sizeof(T))},
        bits_{trailingZeros(maxPageCapacity_)},
        mask_{~(~0ull << bits_)} {
    DWIO_ENSURE_GT(initialCapacity, 0);
    DWIO_ENSURE_EQ(
        bitCount(maxPageBytes), 1, "must be power of 2: ", maxPageBytes);
    DWIO_ENSURE_EQ(bitCount(sizeof(T)), 1, "must be power of 2: ", sizeof(T));

    while (capacity_ < initialCapacity_) {
      addPage(std::min(initialCapacity_, maxPageCapacity_));
    }
  }

  T& operator[](uint64_t index) {
    return getPageUnsafe(index)[getPageOffset(index)];
  }

  const T& operator[](uint64_t index) const {
    return getPageUnsafe(index)[getPageOffset(index)];
  }

  void reserve(uint32_t capacity) {
    if (UNLIKELY(capacity > capacity_)) {
      // capacity grow rules
      // 1. if capacity < pageCapacity (only first page is needed), grow to next
      // power of 2
      // 2. if capacity > pageCapacity, allocate pages until it fits
      auto pageCount = ((capacity - 1) / maxPageCapacity_) + 1;

      if (pages_[0].capacity() < maxPageCapacity_) {
        resizeFirstPage(
            pageCount == 1 ? nextPowOf2(capacity) : maxPageCapacity_);
      }

      while (pages_.size() < pageCount) {
        addPage(maxPageCapacity_);
      }
    }
  }

  void append(T t) {
    reserve(size_ + 1);
    unsafeAppend(t);
  }

  void unsafeAppend(T t) {
    (*this)[size_++] = t;
  }

  uint64_t capacity() const {
    return capacity_;
  }

  uint64_t size() const {
    return size_;
  }

  void clear() {
    while (pages_.size() > 1) {
      pages_.pop_back();
    }
    capacity_ = pages_.back().capacity();
    size_ = 0;
  }

  void applyRange(
      uint64_t begin,
      uint64_t end,
      std::function<void(const T*, uint64_t, uint64_t)> fn) {
    DWIO_ENSURE_LE(begin, end);
    DWIO_ENSURE_LE(end, size_);
    auto pageBegin = getPageIndex(begin);
    auto indexBegin = getPageOffset(begin);
    auto pageEnd = getPageIndex(end);
    auto indexEnd = getPageOffset(end);
    while (pageBegin < pageEnd ||
           (pageBegin == pageEnd && indexBegin < indexEnd)) {
      fn(pages_.at(pageBegin).data(),
         indexBegin,
         pageBegin == pageEnd ? indexEnd : maxPageCapacity_);
      ++pageBegin;
      indexBegin = 0;
    }
  }

 private:
  velox::memory::MemoryPool& pool_;
  std::vector<DataBuffer<T>> pages_;
  uint64_t size_;
  uint64_t capacity_;

  const uint32_t initialCapacity_;
  const uint32_t maxPageCapacity_;
  const uint8_t bits_;
  const uint64_t mask_;

  void addPage(uint32_t capacity) {
    DWIO_ENSURE_LE(capacity, maxPageCapacity_);
    // Either there is no page or the first page is at its max capacity
    DWIO_ENSURE(
        pages_.empty() ||
        (pages_[0].capacity() == maxPageCapacity_ &&
         capacity == maxPageCapacity_));

    pages_.emplace_back(pool_);
    pages_.back().reserve(capacity);
    capacity_ += capacity;
  }

  void resizeFirstPage(uint32_t capacity) {
    DWIO_ENSURE(
        pages_.size() == 1 && capacity <= maxPageCapacity_ &&
        capacity_ < capacity);

    pages_.back().reserve(capacity);
    capacity_ = capacity;
  }

  DataBuffer<T>& getPageUnsafe(uint64_t index) {
    return pages_[getPageIndex(index)];
  }

  const DataBuffer<T>& getPageUnsafe(uint64_t index) const {
    return pages_[getPageIndex(index)];
  }

  uint64_t getPageIndex(uint64_t index) const {
    return index >> bits_;
  }

  uint64_t getPageOffset(uint64_t index) const {
    return index & mask_;
  }

  static size_t bitCount(uint32_t val) {
    return std::bitset<64>(val).count();
  }

  static uint8_t trailingZeros(uint32_t val) {
    DWIO_ENSURE(val);
    uint8_t ret = 0;
    val = (val ^ (val - 1)) >> 1;
    while (val) {
      val >>= 1;
      ++ret;
    }
    return ret;
  }

  static uint32_t nextPowOf2(uint32_t val) {
    val -= 1;
    val |= (val >> 1);
    val |= (val >> 2);
    val |= (val >> 4);
    val |= (val >> 8);
    val |= (val >> 16);
    return val + 1;
  }

  VELOX_FRIEND_TEST(ChainedBufferTests, testCreate);
  VELOX_FRIEND_TEST(ChainedBufferTests, testReserve);
  VELOX_FRIEND_TEST(ChainedBufferTests, testAppend);
  VELOX_FRIEND_TEST(ChainedBufferTests, testClear);
  VELOX_FRIEND_TEST(ChainedBufferTests, testGetPage);
  VELOX_FRIEND_TEST(ChainedBufferTests, testGetPageIndex);
  VELOX_FRIEND_TEST(ChainedBufferTests, testGetPageOffset);
  VELOX_FRIEND_TEST(ChainedBufferTests, testBitCount);
  VELOX_FRIEND_TEST(ChainedBufferTests, testTrailingZeros);
  VELOX_FRIEND_TEST(ChainedBufferTests, testPowerOf2);
};

} // namespace common
} // namespace dwio
} // namespace velox
} // namespace facebook
