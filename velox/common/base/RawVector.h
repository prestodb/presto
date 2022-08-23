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

namespace facebook::velox {

/// Class template similar to std::vector with no default construction and a
/// SIMD load worth of padding below and above the data. The idea is that one
/// can access the data at full SIMD width at both ends.
template <typename T>
class raw_vector {
 public:
  raw_vector() {
    static_assert(std::is_trivially_destructible<T>::value);
  }

  explicit raw_vector(int32_t size) {
    resize(size);
  }

  ~raw_vector() {
    if (data_) {
      freeData(data_);
    }
  }

  // Constructs  a copy of 'other'. See operator=. 'data_' must be copied.
  raw_vector(const raw_vector<T>& other) {
    *this = other;
  }

  raw_vector(raw_vector<T>&& other) noexcept {
    *this = std::move(other);
  }

  // Moves 'other' to this, leaves 'other' empty, as after default
  // construction.
  void operator=(const raw_vector<T>& other) {
    resize(other.size());
    if (other.data_) {
      memcpy(
          data_,
          other.data(),
          bits::roundUp(size_ * sizeof(T), simd::kPadding));
    }
  }

  void operator=(raw_vector<T>&& other) noexcept {
    data_ = other.data_;
    size_ = other.size_;
    capacity_ = other.capacity_;
    simd::memset(&other, 0, sizeof(other));
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

  T operator[](int32_t index) const {
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
      T* newData = allocateData(size, capacity_);
      if (data_) {
        memcpy(newData, data_, size_ * sizeof(T));
        freeData(data_);
      }
      data_ = newData;
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
  // Adds 'bytes' to the address 'pointer'.
  inline T* addBytes(T* pointer, int32_t bytes) {
    return reinterpret_cast<T*>(reinterpret_cast<uint64_t>(pointer) + bytes);
  }

  // Size with one full width SIMD load worth data above and below, rounded to
  // power of 2.
  int32_t paddedSize(int32_t size) {
    return bits::nextPowerOfTwo(size + (2 * simd::kPadding));
  }

  T* allocateData(int32_t size, int32_t& capacity) {
    auto bytes = paddedSize(sizeof(T) * size);
    auto ptr = reinterpret_cast<T*>(aligned_alloc(simd::kPadding, bytes));
    // Clear the word below the pointer so that we do not get read of
    // uninitialized when reading a partial word that extends below
    // the pointer.
    *reinterpret_cast<int64_t*>(
        addBytes(ptr, simd::kPadding - sizeof(int64_t))) = 0;
    capacity = (bytes - 2 * simd::kPadding) / sizeof(T);
    return addBytes(ptr, simd::kPadding);
  }

  void freeData(T* data) {
    if (data_) {
      ::free(addBytes(data, -simd::kPadding));
    }
  }

  FOLLY_NOINLINE void grow(int32_t size) {
    T* newData = allocateData(size, capacity_);
    if (data_) {
      memcpy(newData, data_, size_ * sizeof(T));
      freeData(data_);
    }
    data_ = newData;
  }

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
const int32_t* iota(int32_t size, raw_vector<int32_t>& storage);

} // namespace facebook::velox
