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
#include <functional>
#include <memory>

#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/SimdUtil.h"

namespace facebook::velox::exec::prefixsort {

/// Provides encode/decode methods for PrefixSort.
class PrefixSortEncoder {
 public:
  /// 1. Only int64_t is supported now.
  /// 2. Encoding is compatible with sorting ascending with no nulls.
  template <typename T>
  static FOLLY_ALWAYS_INLINE void encode(T value, char* row);
  template <typename T>
  static FOLLY_ALWAYS_INLINE void decode(char* row);

 private:
  FOLLY_ALWAYS_INLINE static uint8_t flipSignBit(uint8_t byte) {
    return byte ^ 128;
  }
};

/// Assuming that value is little-endian encoded, we encode it as follows to
/// make sure encoding is compatible with sorting ascending:
/// 1 Reverse each byte, for example, 0xaabbccdd becomes 0xddccbbaa.
/// 2 Flip the sign bit.
/// The decode logic is exactly the opposite of the above approach.
template <>
FOLLY_ALWAYS_INLINE void PrefixSortEncoder::encode(int64_t value, char* row) {
  const auto v = __builtin_bswap64(static_cast<uint64_t>(value));
  simd::memcpy(row, &v, sizeof(int64_t));
  row[0] = flipSignBit(row[0]);
}

template <>
FOLLY_ALWAYS_INLINE void PrefixSortEncoder::decode<int64_t>(char* row) {
  row[0] = flipSignBit(row[0]);
  const auto v = __builtin_bswap64(*reinterpret_cast<uint64_t*>(row));
  simd::memcpy(row, &v, sizeof(int64_t));
}

/// For testing only.
namespace {
template <typename T>
FOLLY_ALWAYS_INLINE void testingEncodeInPlace(const std::vector<T>& data) {
  for (auto i = 0; i < data.size(); i++) {
    PrefixSortEncoder::encode(data[i], (char*)data.data() + i * sizeof(T));
  }
}

template <typename T>
FOLLY_ALWAYS_INLINE void testingDecodeInPlace(const std::vector<T>& data) {
  for (auto i = 0; i < data.size(); i++) {
    PrefixSortEncoder::decode<T>((char*)data.data() + i * sizeof(T));
  }
}
} // namespace
} // namespace facebook::velox::exec::prefixsort
