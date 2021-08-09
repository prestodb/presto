/*
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

#include <immintrin.h>
#include <cstdint>
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Exceptions.h"

#include <folly/Likely.h>

// Types for 256 bit vectors of short and int, not defined in avx
// headers. Global namespace, like in the avx headers. AVX headers use
// __m256i (4x64 bits) for all integer vectors regardless of lane
// width. Here we declare types that differentiate between different
// lane widths so that we can use gcc/clang vector extensions,
// e.g. write vector + scalar and have the right addition generated
// based on the number of lanes in the type.

typedef long long __m256i
    __attribute__((__vector_size__(32), __may_alias__, __aligned__(32)));

typedef long long __m256i_u
    __attribute__((__vector_size__(32), __may_alias__, __aligned__(1)));

typedef int32_t __m256si __attribute__((__vector_size__(32), __may_alias__));

typedef int32_t __m256si_u
    __attribute__((__vector_size__(32), __may_alias__, __aligned__(1)));

typedef int16_t __m256hi __attribute__((__vector_size__(32), __may_alias__));

typedef int16_t __m256hi_u
    __attribute__((__vector_size__(32), __may_alias__, __aligned__(1)));

typedef int8_t __m256qi_u
    __attribute__((__vector_size__(32), __may_alias__, __aligned__(1)));

typedef int8_t __m256qi __attribute__((__vector_size__(32), __may_alias__));

// 128 bit types missing from avx headers.
typedef int32_t __m128si __attribute__((__vector_size__(16), __may_alias__));

typedef int32_t __m128si_u
    __attribute__((__vector_size__(16), __may_alias__, __aligned__(1)));

typedef int16_t __m128hi __attribute__((__vector_size__(16), __may_alias__));

typedef int16_t __m128hi_u
    __attribute__((__vector_size__(16), __may_alias__, __aligned__(1)));

typedef int8_t __m128qi __attribute__((__vector_size__(16), __may_alias__));

typedef int8_t __m128qi_u
    __attribute__((__vector_size__(16), __may_alias__, __aligned__(1)));

namespace facebook::velox::simd {
// Width of the widest store. This much space is left at ends of
// blocks to allow full width writes at block ends.
constexpr int32_t kPadding = 32;

// Initializes constant lookup tables. Call this explicitly if the
// lookup tables are referenced in static initialization. The tables
// are statically initialized, so this is not needed after main() has been
// called.
bool initializeSimdUtil();

// Returns positions of set bits in 'bits' in 'indices'. Bits from
// 'begin' to 'end' are considered and the return value is the number
// of found set bits. For bits 0xff and begin 2 and end 5 we have a return value
// of 3 and indices is set to {2, 3, 4}.
int32_t indicesOfSetBits(
    const uint64_t* bits,
    int32_t begin,
    int32_t end,
    int32_t* indices);

// Casts an arbitrary 256 bit vector to __m256i, which all integer intrinsics
// expect.
template <typename T>
inline __m256i to256i(T x) {
  return reinterpret_cast<__m256i>(x);
}
using ByteSetBitsType = int32_t[256][8];

const ByteSetBitsType& byteSetBits();

// Struct template defining common SIMD operations for different lane widths.
// See comments in Vectors<int64_t>.
template <typename T>
struct Vectors {
  // Number of elements in the widest supported vector of T.
  static constexpr int32_t VSize =
      std::is_same<T, bool>::value ? 256 : 32 / sizeof(T);
};

template <>
struct Vectors<int64_t> {
  // Type of one lane.
  using T = int64_t;

  // Number of lanes in widest supported vector.
  static constexpr int32_t VSize = 4;

  // Type to use for referenced type of an unaligned pointer. The type
  // accurately represents the number and width of lanes.
  using TV = __m256i_u;
  // Result type of comparison. Integer vector with the same lane
  // width and number of lanes. Differs from TV for floating point
  // vectors.
  using CompareType = __m256i;

  // Value of compareResult(xx) where all lanes are
  // true. compareResult produces a scalar word representing the truth
  // values for a SIMD compare of this type, e.g. compareEq. Note that
  // in AVX the compare result is a vector of the shape of the compare
  // operands and in AVX512 this is a bit mask. So compareResult is a
  // mask extracting load in AVX and no-op in AVX512.
  static constexpr uint32_t kAllTrue = 0xffffffff;

  // Returns a vector with all lanes set to 'value'.
  static auto setAll(int64_t value) {
    return _mm256_set1_epi64x(value);
  }

  // Returns the equality comparison result of 'left' and 'right'. Use
  // compareResult(compareEq(x, y)) to get the result as a word and
  // compareBitMask(compareResult(compareEq(x, y)) to get a bit mask
  // with one bit per lane of x and y. This abstracts away the
  // different return types of compares in AVX and AVX512.
  static auto compareEq(TV left, TV right) {
    return _mm256_cmpeq_epi64(left, right);
  }

  // Returns x > y for each lane. See compareEq for interpretation of return
  // value.
  static auto compareGt(TV left, TV right) {
    return _mm256_cmpgt_epi64(left, right);
  }

  // Returns a word representing the result of a vector comparison.
  static auto compareResult(TV wide) {
    return _mm256_movemask_epi8(wide);
  }

  // Reduces the result of compareResult to one bit per lane.
  static auto compareBitMask(int32_t compareResult) {
    return _pext_u32(compareResult, 0x80808080);
  }

  // Returns the indices of one bits in the result of compareBitMask
  // as a vector of int32 of the ppropriate width. (4x32 for 64 bit
  // lanes and 8x32 for 32 bit lanes).
  static auto compareSetBits(uint8_t bits) {
    return *reinterpret_cast<const __m128si_u*>(byteSetBits()[bits]);
  }

  // Loads the vector at 'values'.
  static TV load(const void* values) {
    return *reinterpret_cast<const TV*>(values);
  }

  // Stores 'data' into '*ptr'.
  static void store(void* ptr, TV data) {
    *reinterpret_cast<TV*>(ptr) = data;
  }

  // Casts 'source' to be a pointer to TV.
  static TV* pointer(void* source) {
    return reinterpret_cast<TV*>(source);
  }

  // Loads a vector of indices forgather32 of of TV. This loads 4x32
  // for 64 bit and 8x32 for 32 bit gather.
  static __m128si loadGather32Indices(const int32_t* indices) {
    return *(__m128si_u*)indices;
  }

  // Returns a mask with the left 'n' lanes enabled. Note that AVX
  // masks have the width of the operands and AVX512 maska have a bit
  // per lane.
  static __m256i leadingMask(int32_t n) {
    return LIKELY(n >= VSize) ? setAll(-1) : load(&int64LeadingMasks_[n]);
  }

  // Returns a mask for a masked operation where a one bit in 'mask'
  // indicates an active lane. This would be identity in AVX 512 and
  // in other systems this is widening each bit to a full lane. A
  // lookup table seems to be the most effective means for this. A
  // pdep from bits to uint64 and then widening with sign extension
  // would be more instructions.
  static __m256i mask(int32_t mask) {
    return UNLIKELY(mask == (1 << VSize) - 1) ? setAll(-1)
                                              : load(&int64Masks_[mask]);
  }

  // Loads 4x64 non-contiguous words with 64 bit indices.
  template <uint8_t scale = 8>
  static auto gather64(const void* base, TV indices) {
    return _mm256_i64gather_epi64(
        reinterpret_cast<const long long int*>(base), indices, scale);
  }

  // Loads  non-contiguous words with 32 bit indices.
  template <uint8_t scale = 8>
  static auto gather32(const void* base, __m128si indices) {
    return _mm256_i32gather_epi64(
        reinterpret_cast<const long long int*>(base), (__m128i)indices, scale);
  }

  // Loads 4x 64 bit words with 32 bit indices for the lanes selected by
  // 'mask'. 'source' is used to fill the lanes not selected by
  // 'mask'.
  template <uint8_t scale = 8>
  static auto
  maskGather32(TV source, TV mask, const void* base, __m128si indices) {
    return _mm256_mask_i32gather_epi64(
        source,
        reinterpret_cast<const long long int*>(base),
        (__m128i)indices,
        mask,
        scale);
  }

  // Widens 4x32 signed values to 4x64.
  static auto from32u(__m128si x) {
    return _mm256_cvtepu32_epi64((__m128i)x);
  }

  // Returns the value of the 'ith' lane.
  template <uint8_t i>
  static int64_t extract(TV v) {
    return _mm256_extract_epi64(v, i);
  }

  static const auto& permuteIndices() {
    return int64PermuteIndices_;
  }

  static void initialize();

 private:
  // Indices to use in 8x32 bit permute for extracting words from 4x64
  // bits.  The entry at 5 (bits 0 and 2 set) is {0, 1, 4, 5, 4, 5, 6,
  // 7}, meaning 64 bit words at 0 and 2 are moved
  // in front (to 0, 1).
  static int32_t int64PermuteIndices_[16][8];

  // Masks for masked instructions on 64 bit operations. Each bit of the
  // byte index is repeated 64 times.
  static int64_t int64Masks_[16][4];

  // Masks for masked 64 bit instructions that select the first n lanes.
  static int64_t int64LeadingMasks_[4][4];
};

// See Vectors<int64_t> for comments.
template <>
struct Vectors<int32_t> {
  using T = int32_t;
  using TV = __m256si_u;
  using CompareType = __m256si;
  static constexpr int32_t VSize = 8;
  static constexpr uint32_t kAllTrue = 0xffffffff;

  // Returns a vector of <0, 1, 2, 3, 4, 5, 6, 7}.
  static TV iota() {
    return load(&iota_);
  }

  static auto setAll(T value) {
    return (__m256si)_mm256_set1_epi32(value);
  }

  static auto compareEq(TV left, TV right) {
    return (TV)_mm256_cmpeq_epi32(to256i(left), to256i(right));
  }

  static auto compareGt(TV left, TV right) {
    return (TV)_mm256_cmpgt_epi32(to256i(left), to256i(right));
  }

  static auto compareResult(TV wide) {
    return _mm256_movemask_epi8(to256i(wide));
  }

  static auto compareResult(__m256i wide) {
    return _mm256_movemask_epi8(wide);
  }

  static auto compareBitMask(int32_t compareResult) {
    return _pext_u32(compareResult, 0x88888888);
  }

  static auto compareSetBits(uint8_t bits) {
    return *reinterpret_cast<const __m256si_u*>(&byteSetBits()[bits]);
  }

  static TV load(const void* values) {
    return *reinterpret_cast<const TV*>(values);
  }

  static void store(void* ptr, TV data) {
    *reinterpret_cast<__m256i_u*>(ptr) = (__m256i)data;
  }

  static void store(void* ptr, __m128si data) {
    *reinterpret_cast<__m128i_u*>(ptr) = (__m128i)data;
  }

  static TV* pointer(void* source) {
    return reinterpret_cast<TV*>(source);
  }

  static auto loadGather32Indices(const int32_t* indices) {
    return load(indices);
  }

  static __m256si leadingMask(int32_t n) {
    return LIKELY(n >= VSize) ? setAll(-1) : load(&int32LeadingMasks_[n]);
  }

  static __m256si mask(int32_t mask) {
    return UNLIKELY(mask == (1 << VSize) - 1) ? setAll(-1)
                                              : load(&int32Masks_[mask]);
  }

  template <uint8_t scale = 4>
  static auto gather32(const void* base, TV indices) {
    return (TV)_mm256_i32gather_epi32(
        reinterpret_cast<const int*>(base), to256i(indices), scale);
  }

  template <uint8_t scale = 4>
  static auto maskGather32(TV source, TV mask, const void* base, TV indices) {
    return (TV)_mm256_mask_i32gather_epi32(
        to256i(source),
        reinterpret_cast<const int*>(base),
        to256i(indices),
        to256i(mask),
        scale);
  }

  template <uint8_t i>
  static int32_t extract(TV v) {
    return _mm256_extract_epi32(to256i(v), i);
  }

  // Widens the lower or upper 4 lanes to unsigned 4x64.
  template <uint8_t i>
  static auto as4x64u(TV x) {
    return _mm256_cvtepu32_epi64(_mm256_extracti128_si256(to256i(x), i));
  }

  static const ByteSetBitsType& byteSetBits() {
    return byteSetBits_;
  }

  static void initialize();

 private:
  static int32_t iota_[8];
  // Offsets of set bits in all distinct bytes. For example, element at
  // index 42 is {1, 3, 5, 3, 4, 5, 6, 7}, because 42 has bits 1, 3 and
  // 5 set. The remaining positions are set to be their own index in the
  // array. This can be used as a permute index vector for extracting
  // values at positions in a bit mask. Suppose that a comparison
  // produced bit mask 42. The vector at 42 would extract the values for
  // which the compare was true to the left of the permute result
  // vector. Another use of this is for translating bitmaps to vectors
  // of positions of set bits.  See indicesOfSetBits for example of
  // usage.
  static int32_t byteSetBits_[256][8];

  // Mask for 32 bit masked instructions that selects n first lanes.
  static int32_t int32LeadingMasks_[8][8];

  // Masks for masked instructions on 32 bit operations. Each bit of the
  // byte index is repeated 32 times.
  static int32_t int32Masks_[256][8];
};

inline const ByteSetBitsType& byteSetBits() {
  return Vectors<int32_t>::byteSetBits();
}

// See Vectors<int64_t> for comments.
template <>
struct Vectors<int16_t> {
  using T = int16_t;
  using TV = __m256hi_u;
  using CompareType = __m256hi;
  static constexpr int32_t VSize = 16;
  static constexpr uint32_t kAllTrue = 0xffffffff;

  static auto setAll(T value) {
    return (TV)_mm256_set1_epi16(value);
  }

  static auto compareEq(TV left, TV right) {
    return (TV)_mm256_cmpeq_epi16(to256i(left), to256i(right));
  }

  static auto compareGt(TV left, TV right) {
    return (TV)_mm256_cmpgt_epi16(to256i(left), to256i(right));
  }

  static auto compareResult(TV wide) {
    return _mm256_movemask_epi8(to256i(wide));
  }

  static auto compareResult(__m256i wide) {
    return _mm256_movemask_epi8(wide);
  }

  static auto compareBitMask(int32_t compareResult) {
    return _pext_u32(compareResult, 0xaaaaaaaa);
  }

  static auto compareSetBits(uint8_t bits) {
    return *(__m256si_u*)&byteSetBits()[bits];
  }

  static TV load(const void* values) {
    return *reinterpret_cast<const TV*>(values);
  }

  static TV* pointer(void* source) {
    return reinterpret_cast<TV*>(source);
  }

  static void store(void* ptr, TV data) {
    *reinterpret_cast<TV*>(ptr) = data;
  }

  // Loads 8x32. There is no gather32 for 16x16. See gather16x32.
  static auto loadGather32Indices(const int32_t* indices) {
    return Vectors<int32_t>::load(indices);
  }

  static __m256si leadingMask(int32_t /*n*/) {
    static_assert("No masks for 16 bit width. Use gather16x32.");
    return __m256si();
  }

  static TV gather32(const void* /*base*/, __m256si /*indices*/) {
    static_assert("Use gather16x32 instead");
    return TV();
  }

  template <uint8_t scale = 4>
  static TV maskGather32(
      TV /*source*/,
      __m256si /*mask*/,
      const void* /*base*/,
      __m256si /*indices*/) {
    static_assert("Use gather16x32 instead");
    return TV();
  }

  // Widens the lower or upper 8x16 to 8x32 unsigned.
  static __m256si as8x32u(__m128hi x) {
    return reinterpret_cast<__m256si>(
        _mm256_cvtepi16_epi32(reinterpret_cast<__m128i>(x)));
  }
};

// See Vectors<int64_t> for comments.
template <>
struct Vectors<int8_t> {
  using T = int8_t;
  using TV = __m256qi_u;
  using CompareType = __m256qi;
  static constexpr int32_t VSize = 32;
  static constexpr uint32_t kAllTrue = 0xffffffff;

  static auto setAll(T value) {
    return (__m256qi)_mm256_set1_epi8(value);
  }

  static auto compareEq(TV left, TV right) {
    return (TV)_mm256_cmpeq_epi8(to256i(left), to256i(right));
  }

  static auto compareGt(TV left, TV right) {
    return (TV)_mm256_cmpgt_epi8(to256i(left), to256i(right));
  }

  static auto compareResult(TV wide) {
    return _mm256_movemask_epi8(to256i(wide));
  }
  static auto compareBitMask(int32_t compareResult) {
    return compareResult;
  }

  static TV load(const void* values) {
    return *reinterpret_cast<const TV*>(values);
  }

  static TV* pointer(void* source) {
    return reinterpret_cast<TV*>(source);
  }

  __m256si as8x32u(__m128hi x) {
    return reinterpret_cast<__m256si>(
        _mm256_cvtepi8_epi32(reinterpret_cast<__m128i>(x)));
  }
};

// See Vectors<int64_t> for comments.
template <>
struct Vectors<double> {
  using T = double;
  static constexpr int32_t VSize = 4;
  using CompareType = __m256i;
  static constexpr uint32_t kAllTrue = 0xffffffff;

  using TV4 = __m256d_u;
  using TV = __m256d_u;

  static auto setAll(T value) {
    return _mm256_set1_pd(value);
  }

  static auto compareEq(TV left, TV right) {
    return reinterpret_cast<__m256i>(_mm256_cmp_pd(left, right, _CMP_EQ_OQ));
  }

  static auto compareGt(TV left, TV right) {
    return reinterpret_cast<__m256i>(_mm256_cmp_pd(left, right, _CMP_GT_OQ));
  }

  static auto compareResult(__m256i wide) {
    return _mm256_movemask_epi8(wide);
  }
  static auto compareBitMask(int32_t compareResult) {
    return _pext_u32(compareResult, 0x80808080);
  }

  static auto compareSetBits(uint8_t bits) {
    return *(__m128si_u*)&byteSetBits()[bits];
  }

  static TV load(const void* values) {
    return *reinterpret_cast<const TV*>(values);
  }

  static void store(void* ptr, TV data) {
    *reinterpret_cast<TV*>(ptr) = data;
  }

  static TV* pointer(void* source) {
    return reinterpret_cast<TV*>(source);
  }

  static __m128si loadGather32Indices(const int32_t* indices) {
    return *(__m128si_u*)indices;
  }

  static __m256i leadingMask(int32_t n) {
    return Vectors<int64_t>::leadingMask(n);
  }

  template <uint8_t scale = 8>
  static auto gather32(const void* base, __m128si indices) {
    return _mm256_i32gather_pd(
        reinterpret_cast<const double*>(base), (__m128i)indices, scale);
  }

  template <uint8_t scale = 8>
  static auto
  maskGather32(TV source, __m256i mask, const void* base, __m128si indices) {
    return _mm256_mask_i32gather_pd(
        source,
        reinterpret_cast<const double*>(base),
        (__m128i)indices,
        reinterpret_cast<__m256d>(mask),
        scale);
  }
};

// See Vectors<int64_t> for comments.
template <>
struct Vectors<float> {
  using T = float;
  using TV = __m256_u;
  using CompareType = __m256si;
  static constexpr int32_t VSize = 8;
  static constexpr uint32_t kAllTrue = 0xffffffff;

  static auto setAll(T value) {
    return _mm256_set1_ps(value);
  }

  static auto compareEq(TV left, TV right) {
    return reinterpret_cast<__m256si>(_mm256_cmp_ps(left, right, _CMP_EQ_OQ));
  }

  static auto compareGt(TV left, TV right) {
    return reinterpret_cast<__m256si>(_mm256_cmp_ps(left, right, _CMP_GT_OQ));
  }

  static auto compareResult(__m256si wide) {
    return _mm256_movemask_epi8(reinterpret_cast<__m256i>(wide));
  }
  static auto compareBitMask(int32_t compareResult) {
    return _pext_u32(compareResult, 0x88888888);
  }

  static auto compareSetBits(uint8_t bits) {
    return *(__m256si_u*)&byteSetBits()[bits];
  }

  static TV load(const void* values) {
    return *reinterpret_cast<const TV*>(values);
  }

  static void store(void* ptr, TV data) {
    *reinterpret_cast<TV*>(ptr) = data;
  }

  static TV* pointer(void* source) {
    return reinterpret_cast<TV*>(source);
  }

  static __m256si loadGather32Indices(const int32_t* indices) {
    return *(__m256si_u*)indices;
  }

  static auto leadingMask(int32_t n) {
    return Vectors<int32_t>::leadingMask(n);
  }

  template <uint8_t scale = 4>
  static auto gather32(const void* base, __m256si indices) {
    return _mm256_i32gather_ps(
        reinterpret_cast<const float*>(base), to256i(indices), scale);
  }

  template <uint8_t scale = 4>
  static auto
  maskGather32(TV source, __m256si mask, const void* base, __m256si indices) {
    return _mm256_mask_i32gather_ps(
        source,
        reinterpret_cast<const float*>(base),
        (__m256i)indices,
        reinterpret_cast<__m256>(mask),
        scale);
  }
};

// Returns a __m256i with all elements set to value. The width of each element
// is the width of T.
template <typename T>
__m256i setAll256i(T value) {
  if constexpr (
      std::is_same<T, int64_t>::value || std::is_same<T, uint64_t>::value) {
    return _mm256_set1_epi64x(value);
  } else if constexpr (
      std::is_same<T, int32_t>::value || std::is_same<T, uint32_t>::value) {
    return _mm256_set1_epi32(value);
  } else if constexpr (
      std::is_same<T, int16_t>::value || std::is_same<T, uint16_t>::value) {
    return _mm256_set1_epi16(value);
  } else if constexpr (
      std::is_same<T, int8_t>::value || std::is_same<T, uint8_t>::value) {
    return _mm256_set1_epi8(value);
  } else if constexpr (std::is_same<T, bool>::value) {
    return value ? _mm256_set1_epi64x(-1) : _mm256_set1_epi64x(0);
  } else if constexpr (std::is_same<T, double>::value) {
    return reinterpret_cast<__m256i>(_mm256_set1_pd(value));
  } else if constexpr (std::is_same<T, float>::value) {
    return reinterpret_cast<__m256i>(_mm256_set1_ps(value));
  } else {
    VELOX_FAIL("Vectors::setAll256i - Invalid simd type");
  }
}

// Adds 'bytes' bytes to an address of arbitrary type.
template <typename T>
inline T* addBytes(T* pointer, int32_t bytes) {
  return reinterpret_cast<T*>(reinterpret_cast<uint64_t>(pointer) + bytes);
}

// 'memcpy' implementation that copies at maximum width and unrolls
// when 'bytes' is constant.
inline void memcpy(void* to, const void* from, int32_t bytes);

//  memset implementation that writes at maximum width and unrolls
//  for constant values of 'bytes'.
inline void memset(void* to, char data, int32_t bytes);

// Concatenates the low 16 bits of each lane in 'first8' and 'last8'
// and returns the result as 16x16 bits.
inline __m256hi concat8x32to16x16u(__m256si first8, __m256si last8) {
  constexpr int64_t k64Low16 = 0x0000ffff0000ffff;
  auto lows = _mm256_inserti128_si256(
      to256i(first8), _mm256_extracti128_si256(to256i(last8), 0), 1);
  auto highs = _mm256_inserti128_si256(
      to256i(last8), _mm256_extracti128_si256(to256i(first8), 1), 0);
  return (__m256hi)_mm256_packus_epi32(lows & k64Low16, highs & k64Low16);
}

// Loads up to 16 non-contiguous 16 bit values from 32-bit indices at 'indices'.
template <uint8_t scale = 2>
inline __m256hi
gather16x32(const void* base, const int32_t* indices, int numIndices) {
  using V32 = Vectors<int32_t>;
  auto first = V32::maskGather32<scale>(
      V32::setAll(0), V32::leadingMask(numIndices), base, V32::load(indices));
  __m256si second;
  if (numIndices > 8) {
    second = V32::maskGather32<scale>(
        V32::setAll(0),
        V32::leadingMask(numIndices - 8),
        base,
        V32::load(indices + 8));
  } else {
    second = V32::setAll(0);
  }
  return concat8x32to16x16u(first, second);
}

// Loads up to 8 disjoint bits at bit offsets [indices' and returns
// these as a bit mask.
inline uint8_t
gather8Bits(const uint64_t* bits, __m256si indices, int32_t numIndices) {
  using V32 = Vectors<int32_t>;
  static const __m256si byteBits = {1, 2, 4, 8, 16, 32, 64, 128};
  auto zero = V32::setAll(0);
  auto maskV = (__m256si)_mm256_permutevar8x32_epi32(
      (__m256i)byteBits, (__m256i)(indices & 7));
  auto data = V32::maskGather32<1>(
      zero, V32::leadingMask(numIndices), bits, indices >> 3);
  return V32::compareBitMask(
      ~V32::compareResult(V32::compareEq(data & maskV, zero)));
}

inline uint8_t
gather8Bits(const uint64_t* bits, const int32_t* indices, int32_t numIndices) {
  return gather8Bits(bits, Vectors<int32_t>::load(indices), numIndices);
}

template <typename TData, typename TIndices>
void storePermute(void* /*ptr*/, TData /*data*/, TIndices /*indices*/) {
  static_assert("storePermute undefined");
}

template <>
inline void storePermute(void* ptr, __m256si data, __m256si indices) {
  *(__m256i_u*)ptr =
      _mm256_permutevar8x32_epi32((__m256i)data, (__m256i)indices);
}

template <>
inline void storePermute(void* ptr, __m128si data, __m128si indices) {
  *(__m128_u*)ptr = _mm_permutevar_ps((__m128)data, (__m128i)indices);
}

// Stores the half of 'data' given by 'lane' into 'ptr' after
// permuting it by 'indices'. 'data' is interpreted as a vector of
// 16x16 bits.
template <uint8_t lane, typename TData, typename TIndices>
void storePermute16(void* /*ptr*/, TData /*data*/, TIndices /*indices*/) {
  static_assert("storePermute16 not defined");
}

template <uint8_t lane>
void storePermute16(void* ptr, __m256i data, __m256si indices) {
  __m256i data32 =
      _mm256_cvtepi16_epi32(_mm256_extracti128_si256(simd::to256i(data), lane));
  auto permuted = _mm256_permutevar8x32_epi32(data32, to256i(indices));
  *reinterpret_cast<__m128i_u*>(ptr) = _mm_packs_epi32(
      _mm256_extractf128_si256(permuted, 0),
      _mm256_extractf128_si256(permuted, 1));
}

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
