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

#include <folly/Range.h>
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/SimdUtil.h"
#include "velox/common/memory/MemoryPool.h"

#include <type_traits>

namespace facebook::velox {

/// Class template similar to std::vector with no default construction and a
/// SIMD load worth of padding below and above the data. The idea is that one
/// can access the data at full SIMD width at both ends.
///
/// `T` should name a trivially copyable and trivially destructible type.
template <typename T>
class raw_vector {
 public:
  static_assert(
      std::is_trivially_destructible_v<T> && std::is_trivially_copyable_v<T>);

  explicit raw_vector(memory::MemoryPool* pool = nullptr) : pool_(pool) {}

  explicit raw_vector(int32_t size, memory::MemoryPool* pool = nullptr)
      : pool_(pool) {
    resize(size);
  }

  ~raw_vector() {
    free();
  }

  // Constructs  a copy of 'other'. See operator=. 'data_' must be copied.
  raw_vector(const raw_vector<T>& other) {
    *this = other;
  }

  raw_vector(raw_vector<T>&& other) noexcept {
    *this = std::move(other);
  }

  // Copies 'other' to this, leaves 'other' unchanged.
  void operator=(const raw_vector<T>& other) {
    if (this == &other) {
      return;
    }
    if (pool_ != other.pool_) {
      free();
      pool_ = other.pool_;
    }
    resize(other.size());
    if (other.data_) {
      memcpy(
          data_,
          other.data(),
          bits::roundUp(size_ * sizeof(T), simd::kPadding));
    }
  }

  // Moves 'other' to this, leaves 'other' empty, as after default
  // construction.
  void operator=(raw_vector<T>&& other) noexcept {
    free();
    data_ = other.data_;
    size_ = other.size_;
    capacity_ = other.capacity_;
    pool_ = other.pool_;
    other.data_ = nullptr;
    other.size_ = 0;
    other.capacity_ = 0;
    other.pool_ = nullptr;
  }

  bool empty() const {
    return size_ == 0;
  }

  int32_t size() const {
    return size_;
  }

  int32_t capacity() const {
    return capacity_;
  }

  T* data() const {
    return data_;
  }

  T& operator[](int32_t index) {
    return data_[index];
  }

  const T& operator[](int32_t index) const {
    return data_[index];
  }

  void push_back(T x) {
    if (UNLIKELY(size_ >= capacity_)) {
      grow(size_ + 1);
    }
    data_[size_++] = x;
  }

  operator folly::Range<const T*>() const {
    return folly::Range<const T*>(data(), size());
  }

  void clear() {
    size_ = 0;
  }

  void resize(int32_t size) {
    if (LIKELY(size <= capacity_)) {
      size_ = size;
      return;
    }
    reserve(size);
    size_ = size;
  }

  void reserve(int32_t size) {
    if (capacity_ < size) {
      grow(size);
    }
  }

  auto begin() const {
    return &data_[0];
  }

  auto end() const {
    return &data_[size_];
  }

  T& back() {
    return data_[size_ - 1];
  }
  const T& back() const {
    return data_[size_ - 1];
  }

  operator std::vector<T>() {
    std::vector<T> copy(begin(), end());
    return copy;
  }

 private:
  // Returns the raw pointer that points to the start of the allocated raw
  // buffer that accommodates both paddings and 'data'.
  static inline uint8_t* getBufferFromData(T* data) {
    return reinterpret_cast<uint8_t*>(data) - simd::kPadding;
  }

  // Returns the pointer of type 'T' that points to the data content given the
  // raw 'buffer'.
  static inline T* getDataFromBuffer(uint8_t* buffer) {
    return reinterpret_cast<T*>(buffer + simd::kPadding);
  }

  // Returns the size in bytes of the allocated raw buffer.
  static inline int32_t bufferSize(int32_t capacity) {
    return capacity * sizeof(T) + 2 * simd::kPadding;
  }

  // Returns the corresponding capacity given the number of elements size of the
  // container.
  static inline int32_t calculateCapacity(int32_t size) {
    return (paddedSize(sizeof(T) * size) - 2 * simd::kPadding) / sizeof(T);
  }

  // Size with one full width SIMD load worth data above and below, rounded to
  // power of 2.
  static inline int32_t paddedSize(int32_t size) {
    return bits::nextPowerOfTwo(size + (2 * simd::kPadding));
  }

  T* allocateData(int32_t size) {
    const auto bytes = paddedSize(sizeof(T) * size);
    uint8_t* buffer;
    if (pool_ != nullptr) {
      buffer =
          reinterpret_cast<uint8_t*>(pool_->allocate(bytes, simd::kPadding));
    } else {
      buffer = reinterpret_cast<uint8_t*>(aligned_alloc(simd::kPadding, bytes));
    }
    // Clear the word below the pointer so that we do not get read of
    // uninitialized when reading a partial word that extends below
    // the pointer.
    *reinterpret_cast<int64_t*>(
        reinterpret_cast<uint8_t*>(getDataFromBuffer(buffer)) -
        sizeof(int64_t)) = 0;
    return getDataFromBuffer(buffer);
  }

  void free() {
    if (data_ == nullptr) {
      return;
    }
    auto* buffer = getBufferFromData(data_);
    if (pool_ != nullptr) {
      pool_->free(buffer, bufferSize(capacity_));
    } else {
      ::free(buffer);
    }
    data_ = nullptr;
  }

  FOLLY_NOINLINE void grow(int32_t size) {
    auto* newData = allocateData(size);
    const auto newCapacity = calculateCapacity(size);
    if (data_ != nullptr) {
      memcpy(newData, data_, size_ * sizeof(T));
      try {
        free();
      } catch (...) {
        auto* newBuffer = getBufferFromData(newData);
        if (pool_ != nullptr) {
          pool_->free(newBuffer, bufferSize(newCapacity));
        } else {
          ::free(newBuffer);
        }
        throw;
      }
    }
    data_ = newData;
    capacity_ = newCapacity;
  }

  // Move constructor may change the underlying memory pool.
  memory::MemoryPool* pool_{nullptr};

  // The data_ pointer points to the start of the data. The actual allocated raw
  // buffer is larger than it. The layout is as follows:
  // | padding | data_ | padding |
  T* data_{nullptr};
  int32_t size_{0};
  int32_t capacity_{0};
};

// Returns a pointer to 'size' int32_t's with consecutive values
// starting at 0. There are at least kPadding / sizeof(int32_t) values
// past 'size', so that it is safe to access the returned pointer at maximum
// SIMD width. Typically returns preallocated memory but if this is
// not large enough,resizes and initializes 'storage' to the requested
// size and returns storage.data().
const int32_t*
iota(int32_t size, raw_vector<int32_t>& storage, int32_t offset = 0);

} // namespace facebook::velox
