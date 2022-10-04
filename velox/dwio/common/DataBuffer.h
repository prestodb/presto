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

#include <cstring>
#include <memory>
#include <type_traits>
#include "velox/buffer/Buffer.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/exception/Exception.h"

namespace facebook {
namespace velox {
namespace dwio {
namespace common {

template <typename T, typename = std::enable_if_t<std::is_trivial_v<T>>>
class DataBuffer final {
 private:
  velox::memory::AbstractMemoryPool& pool_;
  T* buf_;
  // current size
  uint64_t size_;
  // maximal capacity (actual allocated memory)
  uint64_t capacity_;
  // the referenced velox buffer. buf_ owns the memory when this veloxRef_ is
  // nullptr.
  const velox::BufferPtr veloxRef_;

  DataBuffer(
      velox::memory::AbstractMemoryPool& pool,
      const velox::BufferPtr& buffer)
      : pool_{pool}, veloxRef_{buffer} {
    buf_ = const_cast<T*>(veloxRef_->as<T>());
    size_ = veloxRef_->size() / sizeof(T);
    capacity_ = size_;
  }

  uint64_t sizeInBytes(uint64_t items) const {
    return sizeof(T) * items;
  }

 public:
  DataBuffer(velox::memory::AbstractMemoryPool& pool, uint64_t size = 0)
      : pool_(pool),
        // Initial allocation uses calloc, to avoid memset.
        buf_(reinterpret_cast<T*>(
            pool_.allocateZeroFilled(sizeInBytes(size), 1))),
        size_(size),
        capacity_(size) {
    DWIO_ENSURE(buf_ != nullptr || size == 0);
  }

  DataBuffer(DataBuffer&& other) noexcept
      : pool_{other.pool_},
        buf_{other.buf_},
        size_{other.size_},
        capacity_{other.capacity_} {
    other.buf_ = nullptr;
    other.size_ = 0;
    other.capacity_ = 0;
  }

  ~DataBuffer() {
    clear();
  }

  T* data() {
    return buf_;
  }

  const T* data() const {
    return buf_;
  }

  uint64_t size() const {
    return size_;
  }

  uint64_t capacity() const {
    return capacity_;
  }

  uint64_t capacityInBytes() const {
    return sizeInBytes(capacity_);
  }

  T& operator[](uint64_t i) {
    return data()[i];
  }

  // Get with range check introduces significant overhead. Use index operator[]
  // when possible
  const T& at(uint64_t i) const {
    DWIO_ENSURE_LT(
        i, size_, "Accessing index out of range: ", i, " >= ", size_);
    return data()[i];
  }

  const T& operator[](uint64_t i) const {
    return data()[i];
  }

  void reserve(uint64_t capacity) {
    // After resetting the buffer, capacity always resets to zero.
    if (capacity > capacity_ || !buf_) {
      auto newSize = sizeInBytes(capacity);
      if (!buf_) {
        buf_ = reinterpret_cast<T*>(pool_.allocate(newSize));
      } else {
        buf_ = reinterpret_cast<T*>(
            pool_.reallocate(buf_, sizeInBytes(capacity_), newSize));
      }
      DWIO_ENSURE(buf_ != nullptr || newSize == 0);
      capacity_ = capacity;
    }
  }

  void extend(uint64_t size) {
    auto newSize = size_ + size;
    if (newSize > capacity_) {
      reserve(newSize + ((newSize + 1) / 2) + 1);
    }
  }

  void resize(uint64_t size) {
    reserve(size);
    if (size > size_) {
      std::memset(data() + size_, 0, sizeInBytes(size - size_));
    }
    size_ = size;
  }

  void append(
      uint64_t offset,
      const DataBuffer<T>& src,
      uint64_t srcOffset,
      uint64_t items) {
    // Does src have insufficient data
    DWIO_ENSURE_GE(src.size(), srcOffset + items);
    append(offset, src.data() + srcOffset, items);
  }

  void append(uint64_t offset, const T* src, uint64_t items) {
    reserve(offset + items);
    unsafeAppend(offset, src, items);
  }

  void extendAppend(uint64_t offset, const T* src, uint64_t items) {
    auto newSize = offset + items;
    if (UNLIKELY(newSize > capacity_)) {
      reserve(newSize + ((newSize + 1) / 2) + 1);
    }
    unsafeAppend(offset, src, items);
  }

  void unsafeAppend(uint64_t offset, const T* src, uint64_t items) {
    if (LIKELY(items > 0)) {
      std::memcpy(data() + offset, src, sizeInBytes(items));
    }
    size_ = (offset + items);
  }

  void unsafeAppend(T value) {
    buf_[size_++] = value;
  }

  void append(T value) {
    if (size_ >= capacity_) {
      reserve(capacity_ + ((capacity_ + 1) / 2) + 1);
    }
    unsafeAppend(value);
  }

  /**
   * Set a value to the specified offset - if offset overflows current capacity
   * It safely allocate more space to meet the request
   */
  void safeSet(uint64_t offset, T value) {
    if (offset >= capacity_) {
      // 50% increasing capacity or offset;
      auto size = std::max(offset + 1, capacity_ + ((capacity_ + 1) / 2) + 1);
      reserve(size);
      VLOG(1) << "reserve size: " << size << " for offset set: " << offset;
    }

    buf_[offset] = value;
    if (offset >= size_) {
      size_ = offset + 1;
    }
  }

  void clear() {
    if (!veloxRef_ && buf_) {
      pool_.free(buf_, sizeInBytes(capacity_));
    }
    size_ = 0;
    capacity_ = 0;
    buf_ = nullptr;
  }

  DataBuffer(const DataBuffer&) = delete;
  DataBuffer& operator=(DataBuffer&) = delete;
  DataBuffer& operator=(DataBuffer&&) = delete;

  friend class DataBufferFactory;
};

class DataBufferFactory {
 public:
  template <typename T>
  static std::shared_ptr<const DataBuffer<T>> wrap(
      const velox::BufferPtr& buffer) {
    return std::shared_ptr<DataBuffer<T>>(
        new DataBuffer<T>(failAllOperationPool(), buffer));
  }

 private:
  static velox::memory::AbstractMemoryPool& failAllOperationPool();
};

} // namespace common
} // namespace dwio
} // namespace velox
} // namespace facebook
