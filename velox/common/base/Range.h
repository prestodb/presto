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

#include <cstdint>
#include "velox/common/base/BitUtil.h"

namespace facebook::velox {

template <typename T>
class Range {
 public:
  Range(const void* data, int32_t begin, int32_t end)
      : data_(reinterpret_cast<const T*>(data)), begin_(begin), end_(end) {}

  const T* data() const {
    return data_;
  }

  const uint64_t* bits() const {
    return reinterpret_cast<const uint64_t*>(data_);
  }

  int32_t begin() const {
    return begin_;
  }

  int32_t end() const {
    return end_;
  }

  T operator[](int32_t idx) const {
    return data_[begin_ + idx];
  }

 private:
  const T* data_;
  int32_t begin_;
  int32_t end_;
};

template <>
inline bool Range<bool>::operator[](int32_t idx) const {
  return bits::isBitSet(reinterpret_cast<const uint64_t*>(data_), begin_ + idx);
}

template <typename T>
class WritablePosition {
 public:
  WritablePosition(uint64_t* pointer, int8_t bitIndex)
      : pointer_(
            reinterpret_cast<uint64_t>(pointer) |
            (static_cast<uint64_t>(bitIndex) << 56)) {}

  explicit WritablePosition(T* pointer)
      : pointer_(reinterpret_cast<uint64_t>(pointer)) {}

  operator T() const {
    return *reinterpret_cast<T*>(pointer_);
  }
  T operator=(T value) const {
    return *reinterpret_cast<T*>(pointer_) = value;
  }

 private:
  uint64_t* bitsPointer() const {
    return reinterpret_cast<uint64_t*>(pointer_ & ((1LL << 56) - 1));
  }

  int32_t bitPosition() const {
    return pointer_ >> 56;
  }

  uint64_t pointer_;
};

template <>
inline WritablePosition<bool>::operator bool() const {
  return *bitsPointer() & (1L << bitPosition());
}

template <>
inline bool WritablePosition<bool>::operator=(bool value) const {
  if (value) {
    *bitsPointer() |= 1L << bitPosition();
  } else {
    *bitsPointer() &= ~(1L << bitPosition());
  }
  return value;
}

template <typename T>
class MutableRange {
 public:
  MutableRange(T* data, int32_t begin, int32_t end)
      : data_(data), begin_(begin), end_(end) {}

  MutableRange(void* data, int32_t begin, int32_t end)
      : MutableRange(reinterpret_cast<T*>(data), begin, end) {}

  T* data() const {
    return data_;
  }

  uint64_t* bits() {
    return reinterpret_cast<uint64_t*>(data_);
  }

  int32_t begin() const {
    return begin_;
  }

  int32_t end() const {
    return end_;
  }

  WritablePosition<T> operator[](int32_t idx) {
    return WritablePosition<T>(&data_[begin_ + idx]);
  }

 private:
  T* data_;
  int32_t begin_;
  int32_t end_;
};

template <>
inline WritablePosition<bool> MutableRange<bool>::operator[](int32_t idx) {
  int32_t bit = begin_ + idx;
  return WritablePosition<bool>(
      reinterpret_cast<uint64_t*>(data_) + (bit / 64),
      static_cast<int8_t>(bit & 63));
}

} // namespace facebook::velox
