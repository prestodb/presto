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

#include <numeric>

namespace facebook::velox::simd {

namespace detail {

template <typename T, typename A>
xsimd::batch_bool<T, A> fromBitMaskImpl(int mask) {
  static const auto kMemo = ({
    constexpr int N = xsimd::batch_bool<T, A>::size;
    static_assert(N <= 8);
    std::array<xsimd::batch_bool<T, A>, (1 << N)> memo;
    for (int i = 0; i < (1 << N); ++i) {
      bool tmp[N];
      for (int bit = 0; bit < N; ++bit) {
        tmp[bit] = (i & (1 << bit)) ? true : false;
      }
      memo[i] = xsimd::batch_bool<T, A>::load_unaligned(tmp);
    }
    memo;
  });
  return kMemo[mask];
}

template <typename T, typename A>
struct BitMask<T, A, 1> {
  static constexpr int kAllSet = bits::lowMask(xsimd::batch_bool<T, A>::size);

#if XSIMD_WITH_AVX2
  static int toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::avx2&) {
    return _mm256_movemask_epi8(mask);
  }
#endif

#if XSIMD_WITH_SSE2
  static int toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::sse2&) {
    return _mm_movemask_epi8(mask);
  }
#endif
};

template <typename T, typename A>
struct BitMask<T, A, 2> {
  static constexpr int kAllSet = bits::lowMask(xsimd::batch_bool<T, A>::size);

#if XSIMD_WITH_AVX2
  static int toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::avx2&) {
    // There is no intrinsic for extracting high bits of a 16x16
    // vector.  Hence take every second bit of the high bits of a 32x1
    // vector.
    //
    // NOTE: TVL might have a more efficient implementation for this.
    return bits::extractBits<uint32_t>(_mm256_movemask_epi8(mask), 0xAAAAAAAA);
  }
#endif

#if XSIMD_WITH_SSE2
  static int toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::sse2&) {
    return bits::extractBits<uint32_t>(_mm_movemask_epi8(mask), 0xAAAA);
  }
#endif
};

template <typename T, typename A>
struct BitMask<T, A, 4> {
  static constexpr int kAllSet = bits::lowMask(xsimd::batch_bool<T, A>::size);

#if XSIMD_WITH_AVX
  static int toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::avx&) {
    return _mm256_movemask_ps(reinterpret_cast<__m256>(mask.data));
  }
#endif

#if XSIMD_WITH_SSE2
  static int toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::sse2&) {
    return _mm_movemask_ps(reinterpret_cast<__m128>(mask.data));
  }
#endif

  static xsimd::batch_bool<T, A> fromBitMask(int mask, const A&) {
    return UNLIKELY(mask == kAllSet) ? xsimd::batch_bool<T, A>(true)
                                     : fromBitMaskImpl<T, A>(mask);
  }
};

template <typename T, typename A>
struct BitMask<T, A, 8> {
  static constexpr int kAllSet = bits::lowMask(xsimd::batch_bool<T, A>::size);

#if XSIMD_WITH_AVX
  static int toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::avx&) {
    return _mm256_movemask_pd(reinterpret_cast<__m256d>(mask.data));
  }
#endif

#if XSIMD_WITH_SSE2
  static int toBitMask(xsimd::batch_bool<T, A> mask, const xsimd::sse2&) {
    return _mm_movemask_pd(reinterpret_cast<__m128d>(mask.data));
  }
#endif

  static xsimd::batch_bool<T, A> fromBitMask(int mask, const A&) {
    return UNLIKELY(mask == kAllSet) ? xsimd::batch_bool<T, A>(true)
                                     : fromBitMaskImpl<T, A>(mask);
  }
};

} // namespace detail

template <typename A>
int32_t indicesOfSetBits(
    const uint64_t* bits,
    int32_t begin,
    int32_t end,
    int32_t* result,
    const A&) {
  if (end <= begin) {
    return 0;
  }
  int32_t row = begin & ~63;
  auto originalResult = result;
  int32_t endWord = bits::roundUp(end, 64) / 64;
  auto firstWord = begin / 64;
  for (auto wordIndex = firstWord; wordIndex < endWord; ++wordIndex) {
    uint64_t word = bits[wordIndex];
    if (!word) {
      row += 64;
      continue;
    }
    if (wordIndex == firstWord && begin) {
      word &= bits::highMask(64 - (begin - firstWord * 64));
      if (!word) {
        row += 64;
        continue;
      }
    }
    if (wordIndex == endWord - 1) {
      int32_t lastBits = end - (endWord - 1) * 64;
      if (lastBits < 64) {
        word &= bits::lowMask(lastBits);
        if (!word) {
          break;
        }
      }
    }
    if (result - originalResult < (row >> 2)) {
      do {
        *result++ = __builtin_ctzll(word) + row;
        word = word & (word - 1);
      } while (word);
      row += 64;
    } else {
      for (auto byteCnt = 0; byteCnt < 8; ++byteCnt) {
        uint8_t byte = word;
        word = word >> 8;
        if (byte) {
          using Batch = xsimd::batch<int32_t, A>;
          auto indices = byteSetBits(byte);
          if constexpr (Batch::size == 8) {
            (Batch::load_aligned(indices) + row).store_unaligned(result);
            result += __builtin_popcount(byte);
          } else {
            static_assert(Batch::size == 4);
            auto lo = byte & ((1 << 4) - 1);
            auto hi = byte >> 4;
            int pop = 0;
            if (lo) {
              (Batch::load_aligned(indices) + row).store_unaligned(result);
              pop = __builtin_popcount(lo);
              result += pop;
            }
            if (hi) {
              (Batch::load_unaligned(indices + pop) + row)
                  .store_unaligned(result);
              result += __builtin_popcount(hi);
            }
          }
        }
        row += 8;
      }
    }
  }
  return result - originalResult;
}

template <typename T, typename A>
xsimd::batch_bool<T, A> leadingMask(int n, const A&) {
  constexpr int N = xsimd::batch_bool<T, A>::size;
  static const auto kMemo = ({
    std::array<xsimd::batch_bool<T, A>, N> memo;
    bool tmp[N]{};
    for (int i = 0; i < N; ++i) {
      memo[i] = xsimd::batch_bool<T, A>::load_unaligned(tmp);
      tmp[i] = true;
    }
    memo;
  });
  return LIKELY(n >= N) ? xsimd::batch_bool<T, A>(true) : kMemo[n];
}

namespace detail {

template <typename T, typename A>
struct CopyWord {
  static void apply(void* to, const void* from) {
    *reinterpret_cast<T*>(to) = *reinterpret_cast<const T*>(from);
  }
};

template <typename A>
struct CopyWord<xsimd::batch<int8_t, A>, A> {
  static void apply(void* to, const void* from) {
    xsimd::batch<int8_t, A>::load_unaligned(
        reinterpret_cast<const int8_t*>(from))
        .store_unaligned(reinterpret_cast<int8_t*>(to));
  }
};

// Copies one element of T and advances 'to', 'from', and 'bytes' by
// sizeof(T). Returns false if 'bytes' went to 0.
template <typename T, typename A>
inline bool copyNextWord(void*& to, const void*& from, int32_t& bytes) {
  if (bytes >= sizeof(T)) {
    CopyWord<T, A>::apply(to, from);
    bytes -= sizeof(T);
    if (!bytes) {
      return false;
    }
    from = addBytes(from, sizeof(T));
    to = addBytes(to, sizeof(T));
    return true;
  }
  return true;
}
} // namespace detail

template <typename A>
void memcpy(void* to, const void* from, int32_t bytes, const A& arch) {
  while (bytes >= batchByteSize(arch)) {
    if (!detail::copyNextWord<xsimd::batch<int8_t, A>, A>(to, from, bytes)) {
      return;
    }
  }
  while (bytes >= sizeof(int64_t)) {
    if (!detail::copyNextWord<int64_t, A>(to, from, bytes)) {
      return;
    }
  }
  if (!detail::copyNextWord<int32_t, A>(to, from, bytes)) {
    return;
  }
  if (!detail::copyNextWord<int16_t, A>(to, from, bytes)) {
    return;
  }
  detail::copyNextWord<int8_t, A>(to, from, bytes);
}

namespace detail {

template <typename T, typename A>
struct SetWord {
  static void apply(void* to, T data) {
    *reinterpret_cast<T*>(to) = data;
  }
};

template <typename A>
struct SetWord<xsimd::batch<int8_t, A>, A> {
  static void apply(void* to, xsimd::batch<int8_t, A> data) {
    data.store_unaligned(reinterpret_cast<int8_t*>(to));
  }
};

template <typename T, typename A>
inline bool setNextWord(void*& to, T data, int32_t& bytes, const A&) {
  if (bytes >= sizeof(T)) {
    SetWord<T, A>::apply(to, data);
    bytes -= sizeof(T);
    if (!bytes) {
      return false;
    }
    to = addBytes(to, sizeof(T));
    return true;
  }
  return true;
}

} // namespace detail

template <typename A>
void memset(void* to, char data, int32_t bytes, const A& arch) {
  auto v = xsimd::batch<int8_t, A>::broadcast(data);
  while (bytes >= batchByteSize(arch)) {
    if (!detail::setNextWord(to, v, bytes, arch)) {
      return;
    }
  }
  int64_t data64 = *reinterpret_cast<int64_t*>(&v);
  while (bytes >= sizeof(int64_t)) {
    if (!detail::setNextWord<int64_t>(to, data64, bytes, arch)) {
      return;
    }
  }
  if (!detail::setNextWord<int32_t>(to, data64, bytes, arch)) {
    return;
  }
  if (!detail::setNextWord<int16_t>(to, data64, bytes, arch)) {
    return;
  }
  detail::setNextWord<int8_t>(to, data64, bytes, arch);
}

namespace detail {

template <typename T, typename A, int kScale, typename IndexType>
xsimd::batch<T, A> genericGather(const T* base, const IndexType* indices) {
  constexpr int N = xsimd::batch<T, A>::size;
  alignas(A::alignment()) T dst[N];
  auto bytes = reinterpret_cast<const char*>(base);
  for (int i = 0; i < N; ++i) {
    dst[i] = *reinterpret_cast<const T*>(bytes + indices[i] * kScale);
  }
  return xsimd::load_aligned(dst);
}

template <typename T, typename A, int kScale, typename IndexType>
xsimd::batch<T, A> genericMaskGather(
    xsimd::batch<T, A> src,
    xsimd::batch_bool<T, A> mask,
    const T* base,
    const IndexType* indices) {
  constexpr int N = xsimd::batch<T, A>::size;
  alignas(A::alignment()) T dst[N];
  alignas(A::alignment()) T sr[N];
  alignas(A::alignment()) bool ma[N];
  src.store_aligned(sr);
  mask.store_aligned(ma);
  auto bytes = reinterpret_cast<const char*>(base);
  for (int i = 0; i < N; ++i) {
    if (ma[i]) {
      dst[i] = *reinterpret_cast<const T*>(bytes + indices[i] * kScale);
    } else {
      dst[i] = sr[i];
    }
  }
  return xsimd::load_aligned(dst);
}

template <typename T, typename A>
struct Gather<T, int32_t, A, 2> {
  using VIndexType = xsimd::batch<int32_t, A>;

  // Load 8 indices only.
  static VIndexType loadIndices(const int32_t* indices, const A& arch) {
    return Gather<int32_t, int32_t, A>::loadIndices(indices, arch);
  }
};

template <typename T, typename A>
struct Gather<T, int32_t, A, 4> {
  using VIndexType = xsimd::batch<int32_t, A>;

#if XSIMD_WITH_AVX
  static VIndexType loadIndices(const int32_t* indices, const xsimd::avx&) {
    return _mm256_loadu_si256(reinterpret_cast<const __m256i*>(indices));
  }
#endif

#if XSIMD_WITH_SSE2
  static VIndexType loadIndices(const int32_t* indices, const xsimd::sse2&) {
    return _mm_loadu_si128(reinterpret_cast<const __m128i*>(indices));
  }
#endif

  template <int kScale>
  static xsimd::batch<T, A>
  apply(const T* base, const int32_t* indices, const xsimd::avx2& arch) {
    return apply<kScale>(base, loadIndices(indices, arch), arch);
  }

  template <int kScale>
  static xsimd::batch<T, A>
  apply(const T* base, const int32_t* indices, const xsimd::generic&) {
    return genericGather<T, A, kScale>(base, indices);
  }

#if XSIMD_WITH_AVX2
  template <int kScale>
  static xsimd::batch<T, A>
  apply(const T* base, VIndexType vindex, const xsimd::avx2&) {
    return reinterpret_cast<typename xsimd::batch<T, A>::register_type>(
        _mm256_i32gather_epi32(
            reinterpret_cast<const int32_t*>(base), vindex, kScale));
  }
#endif

  template <int kScale>
  static xsimd::batch<T, A>
  apply(const T* base, VIndexType vindex, const xsimd::generic&) {
    alignas(A::alignment()) int32_t indices[vindex.size];
    vindex.store_aligned(indices);
    return genericGather<T, A, kScale>(base, indices);
  }

  template <int kScale>
  static xsimd::batch<T, A> maskApply(
      xsimd::batch<T, A> src,
      xsimd::batch_bool<T, A> mask,
      const T* base,
      const int32_t* indices,
      const xsimd::avx2& arch) {
    return maskApply<kScale>(src, mask, base, loadIndices(indices, arch), arch);
  }

  template <int kScale>
  static xsimd::batch<T, A> maskApply(
      xsimd::batch<T, A> src,
      xsimd::batch_bool<T, A> mask,
      const T* base,
      const int32_t* indices,
      const xsimd::generic&) {
    return genericMaskGather<T, A, kScale>(src, mask, base, indices);
  }

#if XSIMD_WITH_AVX2
  template <int kScale>
  static xsimd::batch<T, A> maskApply(
      xsimd::batch<T, A> src,
      xsimd::batch_bool<T, A> mask,
      const T* base,
      VIndexType vindex,
      const xsimd::avx2&) {
    return reinterpret_cast<typename xsimd::batch<T, A>::register_type>(
        _mm256_mask_i32gather_epi32(
            reinterpret_cast<__m256i>(src.data),
            reinterpret_cast<const int32_t*>(base),
            vindex,
            reinterpret_cast<__m256i>(mask.data),
            kScale));
  }
#endif

  template <int kScale>
  static xsimd::batch<T, A> maskApply(
      xsimd::batch<T, A> src,
      xsimd::batch_bool<T, A> mask,
      const T* base,
      VIndexType vindex,
      const xsimd::generic&) {
    alignas(A::alignment()) int32_t indices[vindex.size];
    vindex.store_aligned(indices);
    return genericMaskGather<T, A, kScale>(src, mask, base, indices);
  }
};

template <typename T, typename A>
struct Gather<T, int32_t, A, 8> {
#if XSIMD_WITH_AVX
  static xsimd::batch<int32_t, xsimd::sse2> loadIndices(
      const int32_t* indices,
      const xsimd::avx&) {
    return _mm_lddqu_si128(reinterpret_cast<const __m128i*>(indices));
  }
#endif

  static Batch64<int32_t> loadIndices(
      const int32_t* indices,
      const xsimd::sse2&) {
    return Batch64<int32_t>::load_unaligned(indices);
  }

#if XSIMD_WITH_AVX2
  template <int kScale>
  static xsimd::batch<T, A>
  apply(const T* base, const int32_t* indices, const xsimd::avx2& arch) {
    return apply<kScale>(base, loadIndices(indices, arch), arch);
  }
#endif

  template <int kScale>
  static xsimd::batch<T, A>
  apply(const T* base, const int32_t* indices, const xsimd::generic&) {
    return genericGather<T, A, kScale>(base, indices);
  }

#if XSIMD_WITH_AVX2
  template <int kScale>
  static xsimd::batch<T, A> apply(
      const T* base,
      xsimd::batch<int32_t, xsimd::sse2> vindex,
      const xsimd::avx2&) {
    return reinterpret_cast<typename xsimd::batch<T, A>::register_type>(
        _mm256_i32gather_epi64(
            reinterpret_cast<const long long*>(base), vindex, kScale));
  }
#endif

  template <int kScale>
  static xsimd::batch<T, A> maskApply(
      xsimd::batch<T, A> src,
      xsimd::batch_bool<T, A> mask,
      const T* base,
      const int32_t* indices,
      const xsimd::sse2&) {
    return genericMaskGather<T, A, kScale>(src, mask, base, indices);
  }

#if XSIMD_WITH_AVX2
  template <int kScale>
  static xsimd::batch<T, A> maskApply(
      xsimd::batch<T, A> src,
      xsimd::batch_bool<T, A> mask,
      const T* base,
      const int32_t* indices,
      const xsimd::avx2& arch) {
    return maskApply<kScale>(src, mask, base, loadIndices(indices, arch), arch);
  }
#endif

#if XSIMD_WITH_AVX2
  template <int kScale>
  static xsimd::batch<T, A> maskApply(
      xsimd::batch<T, A> src,
      xsimd::batch_bool<T, A> mask,
      const T* base,
      xsimd::batch<int32_t, xsimd::sse2> vindex,
      const xsimd::avx2&) {
    return reinterpret_cast<typename xsimd::batch<T, A>::register_type>(
        _mm256_mask_i32gather_epi64(
            reinterpret_cast<__m256i>(src.data),
            reinterpret_cast<const long long*>(base),
            vindex,
            reinterpret_cast<__m256i>(mask.data),
            kScale));
  }
#endif
};

template <typename T, typename A>
struct Gather<T, int64_t, A, 8> {
  using VIndexType = xsimd::batch<int64_t, A>;

#if XSIMD_WITH_AVX
  static VIndexType loadIndices(const int64_t* indices, const xsimd::avx&) {
    return _mm256_loadu_si256(reinterpret_cast<const __m256i*>(indices));
  }
#endif

#if XSIMD_WITH_SSE2
  static VIndexType loadIndices(const int64_t* indices, const xsimd::sse2&) {
    return _mm_loadu_si128(reinterpret_cast<const __m128i*>(indices));
  }
#endif

#if XSIMD_WITH_AVX2
  template <int kScale>
  static xsimd::batch<T, A>
  apply(const T* base, const int64_t* indices, const xsimd::avx2& arch) {
    return apply<kScale>(base, loadIndices(indices, arch), arch);
  }
#endif

#if XSIMD_WITH_AVX2
  template <int kScale>
  static xsimd::batch<T, A>
  apply(const T* base, VIndexType vindex, const xsimd::avx2&) {
    return reinterpret_cast<typename xsimd::batch<T, A>::register_type>(
        _mm256_i64gather_epi64(
            reinterpret_cast<const long long*>(base), vindex, kScale));
  }
#endif

  template <int kScale>
  static xsimd::batch<T, A>
  apply(const T* base, const int64_t* indices, const xsimd::generic&) {
    return genericGather<T, A, kScale>(base, indices);
  }

  template <int kScale>
  static xsimd::batch<T, A> maskApply(
      xsimd::batch<T, A> src,
      xsimd::batch_bool<T, A> mask,
      const T* base,
      const int64_t* indices,
      const xsimd::avx2& arch) {
    return maskApply<kScale>(src, mask, base, loadIndices(indices, arch), arch);
  }

#if XSIMD_WITH_AVX2
  template <int kScale>
  static xsimd::batch<T, A> maskApply(
      xsimd::batch<T, A> src,
      xsimd::batch_bool<T, A> mask,
      const T* base,
      VIndexType vindex,
      const xsimd::avx2&) {
    return reinterpret_cast<typename xsimd::batch<T, A>::register_type>(
        _mm256_mask_i64gather_epi64(
            src,
            reinterpret_cast<const long long*>(base),
            vindex,
            mask,
            kScale));
  }
#endif

  template <int kScale>
  static xsimd::batch<T, A> maskApply(
      xsimd::batch<T, A> src,
      xsimd::batch_bool<T, A> mask,
      const T* base,
      VIndexType vindex,
      const xsimd::generic&) {
    alignas(A::alignment()) int64_t indices[vindex.size];
    vindex.store_aligned(indices);
    return genericMaskGather<T, A, kScale>(src, mask, base, indices);
  }
};

// Concatenates the low 16 bits of each lane in 'x' and 'y' and
// returns the result as 16x16 bits.
template <typename A>
xsimd::batch<int16_t, A> pack32(
    xsimd::batch<int32_t, A> x,
    xsimd::batch<int32_t, A> y,
    const xsimd::generic&);

#if XSIMD_WITH_SSE4_1
template <typename A>
xsimd::batch<int16_t, A> pack32(
    xsimd::batch<int32_t, A> x,
    xsimd::batch<int32_t, A> y,
    const xsimd::sse4_1&) {
  return _mm_packus_epi32(x & 0xFFFF, y & 0xFFFF);
}
#endif

#if XSIMD_WITH_AVX2
template <typename A>
xsimd::batch<int16_t, A> pack32(
    xsimd::batch<int32_t, A> x,
    xsimd::batch<int32_t, A> y,
    const xsimd::avx2&) {
  constexpr int64_t k64Low16 = 0x0000ffff0000ffff;
  auto lows = _mm256_inserti128_si256(x, _mm256_extracti128_si256(y, 0), 1);
  auto highs = _mm256_inserti128_si256(y, _mm256_extracti128_si256(x, 1), 0);
  return _mm256_packus_epi32(lows & k64Low16, highs & k64Low16);
}
#endif

template <typename T, typename A>
xsimd::batch<T, A> genericPermute(xsimd::batch<T, A> data, const int32_t* idx) {
  constexpr int N = xsimd::batch<T, A>::size;
  alignas(A::alignment()) T src[N];
  alignas(A::alignment()) T dst[N];
  data.store_aligned(src);
  for (int i = 0; i < N; ++i) {
    dst[i] = src[idx[i]];
  }
  return xsimd::load_aligned<A>(dst);
}

template <typename T, typename A>
xsimd::batch<T, A> genericPermute(
    xsimd::batch<T, A> data,
    xsimd::batch<int32_t, A> idx) {
  static_assert(data.size >= idx.size);
  alignas(A::alignment()) int32_t pos[idx.size];
  idx.store_aligned(pos);
  return genericPermute(data, pos);
}

template <typename T>
Batch64<T> genericPermute(Batch64<T> data, Batch64<int32_t> idx) {
  static_assert(data.size >= idx.size);
  Batch64<T> ans;
  for (int i = 0; i < idx.size; ++i) {
    ans.data[i] = data.data[idx.data[i]];
  }
  return ans;
}

template <typename T, typename A, size_t kSizeT = sizeof(T)>
struct Permute;

template <typename T, typename A>
struct Permute<T, A, 4> {
  static xsimd::batch<T, A> apply(
      xsimd::batch<T, A> data,
      xsimd::batch<int32_t, A> idx,
      const xsimd::generic&) {
    return genericPermute(data, idx);
  }

  static HalfBatch<T, A> apply(
      HalfBatch<T, A> data,
      HalfBatch<int32_t, A> idx,
      const xsimd::generic&) {
    return genericPermute(data, idx);
  }

#if XSIMD_WITH_AVX2
  static xsimd::batch<T, A> apply(
      xsimd::batch<T, A> data,
      xsimd::batch<int32_t, A> idx,
      const xsimd::avx2&) {
    return reinterpret_cast<typename xsimd::batch<T, A>::register_type>(
        _mm256_permutevar8x32_epi32(reinterpret_cast<__m256i>(data.data), idx));
  }
#endif

#if XSIMD_WITH_AVX
  static HalfBatch<T, A>
  apply(HalfBatch<T, A> data, HalfBatch<int32_t, A> idx, const xsimd::avx&) {
    return reinterpret_cast<typename HalfBatch<T, A>::register_type>(
        _mm_permutevar_ps(reinterpret_cast<__m128>(data.data), idx));
  }
#endif
};

} // namespace detail

template <int kScale, typename A>
xsimd::batch<int16_t, A> gather(
    const int16_t* base,
    const int32_t* indices,
    int numIndices,
    const A& arch) {
  auto first = maskGather<int32_t, int32_t, kScale>(
      xsimd::batch<int32_t, A>::broadcast(0),
      leadingMask<int32_t>(numIndices, arch),
      reinterpret_cast<const int32_t*>(base),
      indices,
      arch);
  xsimd::batch<int32_t, A> second;
  constexpr int kIndicesBatchSize = xsimd::batch<int32_t, A>::size;
  if (numIndices > kIndicesBatchSize) {
    second = maskGather<int32_t, int32_t, kScale>(
        xsimd::batch<int32_t, A>::broadcast(0),
        leadingMask<int32_t>(numIndices - kIndicesBatchSize, arch),
        reinterpret_cast<const int32_t*>(base),
        indices + kIndicesBatchSize,
        arch);
  } else {
    second = xsimd::batch<int32_t, A>::broadcast(0);
  }
  return detail::pack32(first, second, arch);
}

namespace detail {

template <typename A>
uint8_t gather8BitsImpl(
    const void* bits,
    xsimd::batch<int32_t, A> vindex,
    int32_t numIndices,
    const xsimd::generic&) {
  alignas(A::alignment()) int32_t indices[vindex.size];
  vindex.store_aligned(indices);
  auto base = reinterpret_cast<const char*>(bits);
  uint8_t ans = 0;
  for (int i = 0, n = std::min<int>(vindex.size, numIndices); i < n; ++i) {
    bits::setBit(&ans, i, bits::isBitSet(base, indices[i]));
  }
  return ans;
}

#if XSIMD_WITH_AVX2
template <typename A>
uint8_t gather8BitsImpl(
    const void* bits,
    xsimd::batch<int32_t, A> vindex,
    int32_t numIndices,
    const xsimd::avx2&) {
  // Computes 8 byte addresses, and 8 bit masks.  The low bits of the
  // row select the bit mask, the rest of the bits are the byte
  // offset.  There is an AND wich will be zero if the bit is not set.
  // This is finally converted to a mask with a negated SIMD
  // comparison with 0.
  static const xsimd::batch<int32_t, A> kByteBits = {
      1, 2, 4, 8, 16, 32, 64, 128};
  auto maskV = detail::Permute<int32_t, A>::apply(kByteBits, vindex & 7, A{});
  auto zero = xsimd::batch<int32_t, A>::broadcast(0);
  auto data = detail::Gather<int32_t, int32_t, A>::template maskApply<1>(
      zero,
      leadingMask<int32_t>(numIndices, A{}),
      reinterpret_cast<const int32_t*>(bits),
      vindex >> 3,
      A{});
  return allSetBitMask<int32_t>(A{}) ^ toBitMask((data & maskV) == zero, A{});
}
#endif

} // namespace detail

template <typename A>
uint8_t gather8Bits(
    const void* bits,
    xsimd::batch<int32_t, A> vindex,
    int32_t numIndices,
    const A& arch) {
  return detail::gather8BitsImpl(bits, vindex, numIndices, arch);
}

namespace detail {

template <typename A>
struct GetHalf<int64_t, int32_t, A> {
#if XSIMD_WITH_AVX2
  template <bool kSecond>
  static xsimd::batch<int64_t, A> apply(
      xsimd::batch<int32_t, A> data,
      const xsimd::avx2&) {
    return _mm256_cvtepi32_epi64(_mm256_extracti128_si256(data, kSecond));
  }
#endif

#if XSIMD_WITH_SSE4_1
  template <bool kSecond>
  static xsimd::batch<int64_t, A> apply(
      xsimd::batch<int32_t, A> data,
      const xsimd::sse4_1&) {
    return _mm_cvtepi32_epi64(
        _mm_set_epi64x(0, _mm_extract_epi64(data, kSecond)));
  }
#endif
};

template <typename A>
struct GetHalf<uint64_t, int32_t, A> {
#if XSIMD_WITH_AVX2
  template <bool kSecond>
  static xsimd::batch<uint64_t, A> apply(
      xsimd::batch<int32_t, A> data,
      const xsimd::avx2&) {
    return _mm256_cvtepu32_epi64(_mm256_extracti128_si256(data, kSecond));
  }
#endif

#if XSIMD_WITH_SSE4_1
  template <bool kSecond>
  static xsimd::batch<uint64_t, A> apply(
      xsimd::batch<int32_t, A> data,
      const xsimd::sse4_1&) {
    return _mm_cvtepu32_epi64(
        _mm_set_epi64x(0, _mm_extract_epi64(data, kSecond)));
  }
#endif
};

} // namespace detail

namespace detail {

// Indices to use in 8x32 bit permute for extracting words from 4x64
// bits.  The entry at 5 (bits 0 and 2 set) is {0, 1, 4, 5, 4, 5, 6,
// 7}, meaning 64 bit words at 0 and 2 are moved in front (to 0, 1).
extern int32_t permute4x64Indices[16][8];

#if XSIMD_WITH_AVX2
template <typename A, int kLane>
__m128i
filterHalf(xsimd::batch<int16_t, A> data, int mask, const xsimd::avx2&) {
  xsimd::batch<int32_t, A> data32 =
      _mm256_cvtepi16_epi32(_mm256_extracti128_si256(data, kLane));
  auto out32 = filter(data32, mask, A{});
  return _mm_packs_epi32(
      _mm256_extractf128_si256(out32, 0), _mm256_extractf128_si256(out32, 1));
}
#endif

template <typename T, typename A>
struct Filter<T, A, 2> {
  static xsimd::batch<T, A>
  apply(xsimd::batch<T, A> data, int mask, const xsimd::generic&) {
    return genericPermute(data, byteSetBits[mask]);
  }

#if XSIMD_WITH_AVX2
  static xsimd::batch<T, A>
  apply(xsimd::batch<T, A> data, int mask, const xsimd::avx2& arch) {
    xsimd::batch<T, A> ans;
    auto mask1 = mask & 0xFF;
    *reinterpret_cast<__m128i_u*>(&ans) =
        detail::filterHalf<A, 0>(data, mask1, arch);
    *reinterpret_cast<__m128i_u*>(
        reinterpret_cast<int16_t*>(&ans) + __builtin_popcount(mask1)) =
        detail::filterHalf<A, 1>(data, mask >> 8, arch);
    return ans;
  }
#endif
};

template <typename T, typename A>
struct Filter<T, A, 4> {
  static xsimd::batch<T, A>
  apply(xsimd::batch<T, A> data, int mask, const A& arch) {
    auto vindex = xsimd::batch<int32_t, A>::load_aligned(byteSetBits[mask]);
    return Permute<T, A>::apply(data, vindex, arch);
  }

  static HalfBatch<T, A> apply(HalfBatch<T, A> data, int mask, const A& arch) {
    auto vindex = HalfBatch<int32_t, A>::load_aligned(byteSetBits[mask]);
    return Permute<T, A>::apply(data, vindex, arch);
  }
};

template <typename T, typename A>
struct Filter<T, A, 8> {
  static xsimd::batch<T, A>
  apply(xsimd::batch<T, A> data, int mask, const xsimd::generic&) {
    return genericPermute(data, byteSetBits[mask]);
  }

#if XSIMD_WITH_AVX2
  static xsimd::batch<T, A>
  apply(xsimd::batch<T, A> data, int mask, const xsimd::avx2&) {
    auto vindex =
        xsimd::batch<int32_t, A>::load_aligned(permute4x64Indices[mask]);
    return reinterpret_cast<typename xsimd::batch<T, A>::register_type>(
        _mm256_permutevar8x32_epi32(
            reinterpret_cast<__m256i>(data.data), vindex));
  }
#endif
};

template <typename A>
struct Crc32<uint64_t, A> {
#if XSIMD_WITH_SSE4_2
  static uint32_t
  apply(uint32_t checksum, uint64_t value, const xsimd::sse4_2&) {
    return _mm_crc32_u64(checksum, value);
  }
#endif

#if XSIMD_WITH_AVX
  static uint32_t apply(uint32_t checksum, uint64_t value, const xsimd::avx&) {
    return apply(checksum, value, xsimd::sse4_2{});
  }
#endif
};

} // namespace detail

template <typename T, typename A>
xsimd::batch<T, A> iota(const A&) {
  static const auto kMemo = ({
    constexpr int N = xsimd::batch<T, A>::size;
    T tmp[N];
    std::iota(tmp, tmp + N, 0);
    xsimd::load_unaligned(tmp);
  });
  return kMemo;
}

namespace detail {

template <typename T, typename A>
struct HalfBatchImpl<
    T,
    A,
    std::enable_if_t<std::is_base_of<xsimd::avx, A>::value>> {
  using Type = xsimd::batch<T, xsimd::sse2>;
};

template <typename T, typename A>
struct HalfBatchImpl<
    T,
    A,
    std::enable_if_t<std::is_base_of<xsimd::sse2, A>::value>> {
  using Type = Batch64<T>;
};

} // namespace detail

} // namespace facebook::velox::simd
