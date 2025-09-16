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

#include "velox/common/base/SimdUtil.h"
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/type/Timestamp.h"
#include "velox/type/Type.h"

namespace facebook::velox::exec::prefixsort {

/// Provides encode/decode methods for PrefixSort.
class PrefixSortEncoder {
 public:
  PrefixSortEncoder(bool ascending, bool nullsFirst)
      : ascending_(ascending), nullsFirst_(nullsFirst) {}

  /// Encode native primitive types(such as uint64_t, int64_t, uint32_t,
  /// int32_t, uint16_t, int16_t, float, double, Timestamp).
  /// 1. The first byte of the encoded result is null byte. The value is 0 if
  ///    (nulls first and value is null) or (nulls last and value is not null).
  ///    Otherwise, the value is 1. If this column has no null values, we can
  ///    skip the null byte.
  /// 2. The remaining bytes are the encoding result of value:
  ///    -If value is null, we set the remaining sizeof(T) bytes to '0', they
  ///     do not affect the comparison results at all.
  ///    -If value is not null, the result is set by calling encodeNoNulls.
  template <typename T>
  FOLLY_ALWAYS_INLINE void encode(
      std::optional<T> value,
      char* dest,
      uint32_t encodeSize,
      bool includeNullByte) const {
    if (!includeNullByte) {
      encodeNoNulls(value.value(), dest, encodeSize);
    } else if (value.has_value()) {
      dest[0] = nullsFirst_ ? 1 : 0;
      encodeNoNulls(value.value(), dest + 1, encodeSize - 1);
    } else {
      dest[0] = nullsFirst_ ? 0 : 1;
      simd::memset(dest + 1, 0, encodeSize - 1);
    }
  }

  /// @tparam T Type of value. Supported type are: uint64_t, int64_t, uint32_t,
  /// int32_t, int16_t, uint16_t, float, double, Timestamp.
  template <typename T>
  FOLLY_ALWAYS_INLINE void
  encodeNoNulls(T value, char* dest, uint32_t encodeSize) const;

  bool isAscending() const {
    return ascending_;
  }

  bool isNullsFirst() const {
    return nullsFirst_;
  }

  /// @return For supported types, returns the encoded size, assume nullable.
  ///         For not supported types, returns 'std::nullopt'.
  FOLLY_ALWAYS_INLINE static std::optional<uint32_t> encodedSize(
      TypeKind typeKind,
      uint32_t maxStringPrefixLength,
      bool columnHasNulls) {
    // NOTE: if columnHasNulls is true, one byte is reserved for nullable
    // comparison.
    const uint32_t nullByteSize = columnHasNulls ? 1 : 0;
    switch ((typeKind)) {
#define SCALAR_CASE(kind) \
  case TypeKind::kind:    \
    return nullByteSize + sizeof(TypeTraits<TypeKind::kind>::NativeType);
      SCALAR_CASE(SMALLINT)
      SCALAR_CASE(INTEGER)
      SCALAR_CASE(BIGINT)
      SCALAR_CASE(HUGEINT)
      SCALAR_CASE(REAL)
      SCALAR_CASE(DOUBLE)
      SCALAR_CASE(TIMESTAMP)
#undef SCALAR_CASE
      case TypeKind::VARBINARY:
        [[fallthrough]];
      case TypeKind::VARCHAR: {
        return nullByteSize + maxStringPrefixLength;
      }
      default:
        return std::nullopt;
    }
  }

 private:
  const bool ascending_;
  const bool nullsFirst_;
};

/// Assuming that value is little-endian encoded, means:
/// for an unsigned integer '0x aa bb cc dd', The content of bytes,
/// starting at the address of it, would be '0xdd 0xcc 0xbb 0xaa'. If we store
/// them into a buffer, and reverse the bytes of the buffer : [0xaa, 0xbb,
/// 0xcc, 0xdd], and then we can compare two buffers from the first byte to
/// last byte, the compare result is equal to value-compare. For any two
/// unsigned integers, a < b <==> ~a > ~b so we invert bits when descending
/// order.
template <>
FOLLY_ALWAYS_INLINE void PrefixSortEncoder::encodeNoNulls(
    uint32_t value,
    char* dest,
    uint32_t /*encodeSize*/) const {
  auto& v = *reinterpret_cast<uint32_t*>(dest);
  v = __builtin_bswap32(value);
  if (!ascending_) {
    v = ~v;
  }
}

/// Compare two positive signed integers: storage layout is as same as
/// unsigned integer, their sign-bit are same, flip sign-bit do not change
/// result. Compare two negative signed integers: -n = ~n + 1, we can treat ~n
/// + 1 as an unsigned integer, so the logic is as same as unsigned integer,
/// also flip sign-bit do not change result. Compare positive vs negative:
/// flip sign-bit to promise that positive always bigger than negative.
template <>
FOLLY_ALWAYS_INLINE void PrefixSortEncoder::encodeNoNulls(
    int32_t value,
    char* dest,
    uint32_t encodeSize) const {
  encodeNoNulls((uint32_t)(value ^ (1u << 31)), dest, encodeSize);
}

/// Logic is as same as int32_t.
template <>
FOLLY_ALWAYS_INLINE void PrefixSortEncoder::encodeNoNulls(
    uint64_t value,
    char* dest,
    uint32_t /*encodeSize*/) const {
  auto& v = *reinterpret_cast<uint64_t*>(dest);
  v = __builtin_bswap64(value);
  if (!ascending_) {
    v = ~v;
  }
}

template <>
FOLLY_ALWAYS_INLINE void PrefixSortEncoder::encodeNoNulls(
    int64_t value,
    char* dest,
    uint32_t encodeSize) const {
  encodeNoNulls((uint64_t)(value ^ (1ull << 63)), dest, encodeSize);
}

/// Logic is as same as int32_t.
template <>
FOLLY_ALWAYS_INLINE void PrefixSortEncoder::encodeNoNulls(
    uint16_t value,
    char* dest,
    uint32_t /*encodeSize*/) const {
  auto& v = *reinterpret_cast<uint16_t*>(dest);
  v = __builtin_bswap16(value);
  if (!ascending_) {
    v = ~v;
  }
}

template <>
FOLLY_ALWAYS_INLINE void PrefixSortEncoder::encodeNoNulls(
    int16_t value,
    char* dest,
    uint32_t encodeSize) const {
  encodeNoNulls(static_cast<uint16_t>(value ^ (1u << 15)), dest, encodeSize);
}

template <>
FOLLY_ALWAYS_INLINE void PrefixSortEncoder::encodeNoNulls(
    int128_t value,
    char* dest,
    uint32_t encodeSize) const {
  encodeNoNulls<int64_t>(HugeInt::upper(value), dest, encodeSize);
  encodeNoNulls<uint64_t>(
      HugeInt::lower(value), dest + sizeof(int64_t), encodeSize);
}

namespace detail {
/// Convert double to a uint64_t, their value comparison semantics remain
/// consistent.
static FOLLY_ALWAYS_INLINE uint64_t encodeDouble(double value) {
  // Zero is the smallest positive value.
  if (value == 0) {
    return 1ull << 63;
  }
  // Nan is max value.
  if (std::isnan(value)) {
    return std::numeric_limits<uint64_t>::max();
  }
  // Infinity is the second max value.
  if (value > std::numeric_limits<double>::max()) {
    return std::numeric_limits<uint64_t>::max() - 1;
  }
  // -Infinity is the smallest value.
  if (value < -std::numeric_limits<double>::max()) {
    return 0;
  }
  auto encoded = *reinterpret_cast<uint64_t*>(&value);
  if ((encoded & (1ull << 63)) == 0) {
    // For positive numbers, set sign bit to 1.
    encoded |= (1ull << 63);
  } else {
    // For negative numbers, invert bits to get the opposite order.
    encoded = ~encoded;
  }
  return encoded;
}

// Logic is as same as double.
static FOLLY_ALWAYS_INLINE uint32_t encodeFloat(float value) {
  if (value == 0) {
    return 1u << 31;
  }
  if (std::isnan(value)) {
    return std::numeric_limits<uint32_t>::max();
  }
  if (value > std::numeric_limits<float>::max()) {
    return std::numeric_limits<uint32_t>::max() - 1;
  }
  if (value < -std::numeric_limits<float>::max()) {
    return 0;
  }
  auto encoded = *reinterpret_cast<uint32_t*>(&value);
  if ((encoded & (1u << 31)) == 0) {
    encoded |= (1u << 31);
  } else {
    encoded = ~encoded;
  }
  return encoded;
}
} // namespace detail

/// The result of encodeDouble() keeps value comparison semantics, then we
/// can treat it as an unsigned-integer.
template <>
FOLLY_ALWAYS_INLINE void PrefixSortEncoder::encodeNoNulls(
    double value,
    char* dest,
    uint32_t encodeSize) const {
  encodeNoNulls(detail::encodeDouble(value), dest, encodeSize);
}

template <>
FOLLY_ALWAYS_INLINE void PrefixSortEncoder::encodeNoNulls(
    float value,
    char* dest,
    uint32_t encodeSize) const {
  encodeNoNulls(detail::encodeFloat(value), dest, encodeSize);
}

/// When comparing Timestamp, first compare seconds and then compare nanos, so
/// when encoding, just encode seconds and nanos in sequence.
template <>
FOLLY_ALWAYS_INLINE void PrefixSortEncoder::encodeNoNulls(
    Timestamp value,
    char* dest,
    uint32_t encodeSize) const {
  encodeNoNulls(value.getSeconds(), dest, encodeSize);
  encodeNoNulls(value.getNanos(), dest + 8, encodeSize);
}

/// Encode String type.
/// The string prefix is formatted as 'null byte + string content + padding
/// zeros'. If `!ascending_`, the bits for both the content and padding zeros
/// need to be inverted.
template <>
FOLLY_ALWAYS_INLINE void PrefixSortEncoder::encodeNoNulls(
    StringView value,
    char* dest,
    uint32_t encodeSize) const {
  const uint32_t copySize = std::min<uint32_t>(value.size(), encodeSize);
  if (value.isInline() ||
      HashStringAllocator::headerOf(value.data())->size() >= value.size()) {
    // The string is inline or all in one piece out of line.
    std::memcpy(dest, value.data(), copySize);
  } else {
    // 'data' is stored in non-contiguous allocation pieces in the row
    // container, we only read prefix size data out.
    HashStringAllocator::InputStream stream(
        HashStringAllocator::headerOf(value.data()));
    stream.ByteInputStream::readBytes(dest, copySize);
  }

  if (value.size() < encodeSize) {
    std::memset(dest + value.size(), 0, encodeSize - value.size());
  }

  if (!ascending_) {
    for (auto i = 0; i < encodeSize; ++i) {
      dest[i] = ~dest[i];
    }
  }
}

} // namespace facebook::velox::exec::prefixsort
