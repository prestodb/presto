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
#include "velox/common/base/Exceptions.h"

#include <folly/Likely.h>
#include <xsimd/xsimd.hpp>

namespace facebook::velox::simd {

// Return width of the widest store.
template <typename A = xsimd::default_arch>
constexpr int32_t batchByteSize(const A& = {}) {
  return sizeof(xsimd::types::simd_register<int8_t, A>);
}

// The largest batch size we ever support (in case we support multiple
// architectures).  This much space is left at ends of blocks to allow
// full width writes at block ends.  Minimum to 32 to keep backward
// compatibility.
constexpr int32_t kPadding = std::max(32, batchByteSize());

// Initializes constant lookup tables. Call this explicitly if the
// lookup tables are referenced in static initialization. The tables
// are statically initialized, so this is not needed after main() has been
// called.
bool initializeSimdUtil();

// Returns positions of set bits in 'bits' in 'indices'. Bits from
// 'begin' to 'end' are considered and the return value is the number
// of found set bits. For bits 0xff and begin 2 and end 5 we have a return value
// of 3 and indices is set to {2, 3, 4}.
template <typename A = xsimd::default_arch>
int32_t indicesOfSetBits(
    const uint64_t* bits,
    int32_t begin,
    int32_t end,
    int32_t* indices,
    const A& = {});

namespace detail {
extern int32_t byteSetBits[256][8];
}

// Offsets of set bits in a byte. For example, for byte 42 it returns
// {1, 3, 5, 3, 4, 5, 6, 7}, because 42 has bits 1, 3 and 5 set. The
// remaining positions are set to be their own index in the
// array. This can be used as a permute index vector for extracting
// values at positions in a bit mask. Suppose that a comparison
// produced bit mask 42. The vector at 42 would extract the values for
// which the compare was true to the left of the permute result
// vector. Another use of this is for translating bitmaps to vectors
// of positions of set bits.  See indicesOfSetBits for example of
// usage.
inline const int32_t* byteSetBits(uint8_t byte) {
  return detail::byteSetBits[byte];
}

namespace detail {
template <typename T, typename A, typename = void>
struct HalfBatchImpl;
}

// Get a batch with same element type but half the number of lanes.
// For example if the max width of batch is 256 bits, this returns 128
// bits register type.
template <typename T, typename A = xsimd::default_arch>
using HalfBatch = typename detail::HalfBatchImpl<T, A>::Type;

// A type to represent HalfBatch of 128 bits vectors.  The field and
// method names here are to match xsimd::batch, thus not consistent
// with the rest of Velox.
template <typename T>
struct Batch64 {
  static constexpr size_t size = [] {
    static_assert(8 % sizeof(T) == 0);
    return 8 / sizeof(T);
  }();

  T data[size];

  static Batch64 from(std::initializer_list<T> values) {
    VELOX_DCHECK_EQ(values.size(), size);
    Batch64 ans;
    for (int i = 0; i < size; ++i) {
      ans.data[i] = *(values.begin() + i);
    }
    return ans;
  }

  void store_unaligned(T* out) const {
    std::copy(std::begin(data), std::end(data), out);
  }

  static Batch64 load_aligned(const T* mem) {
    return load_unaligned(mem);
  }

  static Batch64 load_unaligned(const T* mem) {
    Batch64 ans;
    std::copy(mem, mem + size, ans.data);
    return ans;
  }

  friend Batch64 operator+(Batch64 x, T y) {
    for (int i = 0; i < size; ++i) {
      x.data[i] += y;
    }
    return x;
  }

  friend Batch64 operator-(Batch64 x, T y) {
    return x + (-y);
  }
};

namespace detail {
template <typename T, typename IndexType, typename A, int kSizeT = sizeof(T)>
struct Gather;
}

// Load certain number of indices from memory into a batch that is
// suitable for calling 'gather' with data type 'T' and index type
// 'IndexType'.
//
// For example, on an architecture with maximum 256-bits vector, when
// T = int64_t and IndexType = int32_t, 'gather' will return a
// batch<int64_t> which has 4 lanes, so this function will load 4
// 32-bits indices.
template <typename T, typename IndexType, typename A = xsimd::default_arch>
auto loadGatherIndices(const IndexType* indices, const A& arch = {}) {
  return detail::Gather<T, IndexType, A>::loadIndices(indices, arch);
}

// Gather data from memory location specified in 'base' and 'vindex'
// into a batch, i.e. returning 'dst' where
//
//   dst[i] = *(base + vindex[i])
template <
    typename T,
    typename IndexType,
    int kScale = sizeof(T),
    typename IndexArch,
    typename A = xsimd::default_arch>
xsimd::batch<T, A> gather(
    const T* base,
    xsimd::batch<IndexType, IndexArch> vindex,
    const A& arch = {}) {
  using Impl = detail::Gather<T, IndexType, A>;
  return Impl::template apply<kScale>(base, vindex, arch);
}

template <
    typename T,
    typename IndexType,
    int kScale = sizeof(T),
    typename A = xsimd::default_arch>
xsimd::batch<T, A>
gather(const T* base, Batch64<IndexType> vindex, const A& arch = {}) {
  using Impl = detail::Gather<T, IndexType, A>;
  return Impl::template apply<kScale>(base, vindex.data, arch);
}

// Same as 'gather' above except the indices are read from memory.
template <
    typename T,
    typename IndexType,
    int kScale = sizeof(T),
    typename A = xsimd::default_arch>
xsimd::batch<T, A>
gather(const T* base, const IndexType* indices, const A& arch = {}) {
  using Impl = detail::Gather<T, IndexType, A>;
  return Impl::template apply<kScale>(base, indices, arch);
}

// Gather only data where mask[i] is set; otherwise keep the data in
// src[i].
template <
    typename T,
    typename IndexType,
    int kScale = sizeof(T),
    typename IndexArch,
    typename A = xsimd::default_arch>
xsimd::batch<T, A> maskGather(
    xsimd::batch<T, A> src,
    xsimd::batch_bool<T, A> mask,
    const T* base,
    xsimd::batch<IndexType, IndexArch> vindex,
    const A& arch = {}) {
  using Impl = detail::Gather<T, IndexType, A>;
  return Impl::template maskApply<kScale>(src, mask, base, vindex, arch);
}

template <
    typename T,
    typename IndexType,
    int kScale = sizeof(T),
    typename A = xsimd::default_arch>
xsimd::batch<T, A> maskGather(
    xsimd::batch<T, A> src,
    xsimd::batch_bool<T, A> mask,
    const T* base,
    Batch64<IndexType> vindex,
    const A& arch = {}) {
  using Impl = detail::Gather<T, IndexType, A>;
  return Impl::template maskApply<kScale>(src, mask, base, vindex.data, arch);
}

// Same as 'maskGather' above but read indices from memory.
template <
    typename T,
    typename IndexType,
    int kScale = sizeof(T),
    typename A = xsimd::default_arch>
xsimd::batch<T, A> maskGather(
    xsimd::batch<T, A> src,
    xsimd::batch_bool<T, A> mask,
    const T* base,
    const IndexType* indices,
    const A& arch = {}) {
  using Impl = detail::Gather<T, IndexType, A>;
  return Impl::template maskApply<kScale>(src, mask, base, indices, arch);
}

// Loads up to 16 non-contiguous 16 bit values from 32-bit indices at 'indices'.
template <int kScale = sizeof(int16_t), typename A = xsimd::default_arch>
xsimd::batch<int16_t, A> gather(
    const int16_t* base,
    const int32_t* indices,
    int numIndices,
    const A& = {});

// Loads up to 8 disjoint bits at bit offsets 'indices' and returns
// these as a bit mask.
template <typename A = xsimd::default_arch>
uint8_t gather8Bits(
    const void* bits,
    xsimd::batch<int32_t, A> vindex,
    int32_t numIndices,
    const A& = {});

// Same as 'gather8Bits' above but read indices from memory.
template <typename A = xsimd::default_arch>
uint8_t gather8Bits(
    const void* bits,
    const int32_t* indices,
    int32_t numIndices,
    const A& arch = {}) {
  return gather8Bits(
      bits, loadGatherIndices<int32_t>(indices, arch), numIndices, arch);
}

namespace detail {
template <typename T, typename A, size_t kSizeT = sizeof(T)>
struct BitMask;
}

// Returns a mask with the left 'n' lanes enabled.
template <typename T, typename A = xsimd::default_arch>
xsimd::batch_bool<T, A> leadingMask(int n, const A& = {});

// Returns a word representing the result of a vector comparison,
// using 1 bit per element.
template <typename T, typename A = xsimd::default_arch>
auto toBitMask(xsimd::batch_bool<T, A> mask, const A& arch = {}) {
  return detail::BitMask<T, A>::toBitMask(mask, arch);
}

// Get a vector mask from bit mask.
template <typename T, typename BitMaskType, typename A = xsimd::default_arch>
xsimd::batch_bool<T, A> fromBitMask(BitMaskType bitMask, const A& arch = {}) {
  return detail::BitMask<T, A>::fromBitMask(bitMask, arch);
}

// Returns a bitmask equivalent to a batch_bool with all lanes set to
// true.
template <typename T, typename A = xsimd::default_arch>
auto allSetBitMask(const A& = {}) {
  return detail::BitMask<T, A>::kAllSet;
}

namespace detail {
template <typename T, typename A, size_t kSizeT = sizeof(T)>
struct Filter;
}

// Rearrange elements in data, move data[i] to front of the vector if
// bitMask[i] is set.
template <typename T, typename BitMaskType, typename A = xsimd::default_arch>
xsimd::batch<T, A>
filter(xsimd::batch<T, A> data, BitMaskType bitMask, const A& arch = {}) {
  return detail::Filter<T, A>::apply(data, bitMask, arch);
}

// Same as 'filter' on full-sized vector, except this one operate on a
// half-sized vector.
template <typename T, typename BitMaskType, typename A = xsimd::default_arch>
HalfBatch<T, A>
filter(HalfBatch<T, A> data, BitMaskType bitMask, const A& arch = {}) {
  return detail::Filter<T, A>::apply(data, bitMask, arch);
}

namespace detail {
template <typename To, typename From, typename A>
struct GetHalf;
}

// Get either first or second half of the vector.  The type 'To' is as
// twice as large as 'From' to keep the vector size same.
template <
    typename To,
    bool kSecond,
    typename From,
    typename A = xsimd::default_arch>
xsimd::batch<To, A> getHalf(xsimd::batch<From, A> data, const A& arch = {}) {
  return detail::GetHalf<To, From, A>::template apply<kSecond>(data, arch);
}

namespace detail {
template <typename T, typename A>
struct Crc32;
}

// Calculate the CRC32 checksum.
template <typename A = xsimd::default_arch>
uint32_t crc32U64(uint32_t checksum, uint64_t value, const A& arch = {}) {
  return detail::Crc32<uint64_t, A>::apply(checksum, value, arch);
}

// Return a vector consisting {0, 1, ..., n} where 'n' is the number
// of lanes.
template <typename T, typename A = xsimd::default_arch>
xsimd::batch<T, A> iota(const A& = {});

// Returns a batch with all elements set to value.  For batch<bool> we
// use one bit to represent one element.
template <typename T, typename A = xsimd::default_arch>
xsimd::batch<T, A> setAll(T value, const A& = {}) {
  if constexpr (std::is_same_v<T, bool>) {
    return xsimd::batch<T, A>(xsimd::broadcast<int64_t, A>(value ? -1 : 0));
  } else {
    return xsimd::broadcast<T, A>(value);
  }
}

// Adds 'bytes' bytes to an address of arbitrary type.
template <typename T>
inline T* addBytes(T* pointer, int32_t bytes) {
  return reinterpret_cast<T*>(reinterpret_cast<uint64_t>(pointer) + bytes);
}

// 'memcpy' implementation that copies at maximum width and unrolls
// when 'bytes' is constant.
template <typename A = xsimd::default_arch>
void memcpy(void* to, const void* from, int32_t bytes, const A& = {});

// memset implementation that writes at maximum width and unrolls for
// constant values of 'bytes'.
template <typename A = xsimd::default_arch>
void memset(void* to, char data, int32_t bytes, const A& = {});

// Calls a different instantiation of a template function according to
// 'numBytes'.
#define VELOX_WIDTH_DISPATCH(numBytes, TEMPLATE_FUNC, ...) \
  [&]() {                                                  \
    switch (numBytes) {                                    \
      case 2: {                                            \
        return TEMPLATE_FUNC<int16_t>(__VA_ARGS__);        \
      }                                                    \
      case 4: {                                            \
        return TEMPLATE_FUNC<int32_t>(__VA_ARGS__);        \
      }                                                    \
      case 8: {                                            \
        return TEMPLATE_FUNC<int64_t>(__VA_ARGS__);        \
      }                                                    \
      case 16: {                                           \
        return TEMPLATE_FUNC<int128_t>(__VA_ARGS__);       \
      }                                                    \
      default:                                             \
        VELOX_FAIL("Bad data size {}", numBytes);          \
    }                                                      \
  }()

// Returns true if 'values[0]' to 'values[size - 1]' are consecutive
// values of T. The values are expected to be sorted.
template <typename T>
inline bool isDense(const T* values, int32_t size) {
  return (values[size - 1] - values[0] == size - 1);
}

} // namespace facebook::velox::simd

#include "velox/common/base/SimdUtil-inl.h"
