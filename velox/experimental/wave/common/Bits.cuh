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
#include <cub/warp/warp_scan.cuh>
#include "velox/experimental/wave/common/Block.cuh"
#include "velox/experimental/wave/common/CudaUtil.cuh"

namespace facebook::velox::wave {

template <typename T, typename U>
inline void __device__ __host__ setBit(T* bits, U index, bool bit = true) {
  constexpr int32_t kShift = sizeof(T) == 1 ? 3
      : sizeof(T) == 2                      ? 4
      : sizeof(T) == 4                      ? 5
      : sizeof(T) == 8                      ? 6
                                            : 0;
  constexpr U kMask = (static_cast<U>(1) << kShift) - 1;
  if (bit == true) {
    bits[index >> kShift] |= static_cast<T>(1) << (index & kMask);
  } else {
    bits[index >> kShift] &= (static_cast<T>(1) << ~(index & kMask));
  }
}

template <typename T, typename U>
inline bool __device__ isBitSet(T* bits, U index) {
  constexpr int32_t kShift = sizeof(T) == 1 ? 3
      : sizeof(T) == 2                      ? 4
      : sizeof(T) == 4                      ? 5
      : sizeof(T) == 8                      ? 6
                                            : 0;
  constexpr U kMask = (static_cast<U>(1) << kShift) - 1;
  return (bits[index >> kShift] & (static_cast<T>(1) << (index & kMask))) != 0;
}

// From libcudf
inline __device__ uint32_t unalignedLoad32(void const* p) {
  uint32_t ofs = 3 & reinterpret_cast<uintptr_t>(p);
  auto const* p32 =
      reinterpret_cast<uint32_t const*>(reinterpret_cast<uintptr_t>(p) - ofs);
  uint32_t v = p32[0];
  return (ofs) ? __funnelshift_r(v, p32[1], ofs * 8) : v;
}

/// From libcudf
inline __device__ uint64_t unalignedLoad64(void const* p) {
  uint32_t ofs = 3 & reinterpret_cast<uintptr_t>(p);
  auto const* p32 =
      reinterpret_cast<uint32_t const*>(reinterpret_cast<uintptr_t>(p) - ofs);
  uint32_t v0 = p32[0];
  uint32_t v1 = p32[1];
  if (ofs) {
    v0 = __funnelshift_r(v0, v1, ofs * 8);
    v1 = __funnelshift_r(v1, p32[2], ofs * 8);
  }
  return (((uint64_t)v1) << 32) | v0;
}

// Loads uint32 at bit offset 'bits' from 'p' at 'width' bits.
inline __device__ uint32_t
loadBits32(void const* p, uint32_t bitIdx, int32_t width) {
  uint32_t bytes = bitIdx / 8;
  uint32_t bit = bitIdx & 7;
  auto uptr = reinterpret_cast<uintptr_t>(p) + bytes;
  uint32_t ofs = 3 & uptr;
  bit += ofs * 8;
  auto const* p32 = reinterpret_cast<uint32_t const*>(uptr - ofs);
  uint32_t v = p32[0];
  uint32_t mask = lowMask<uint32_t>(width);
  if (bit + width <= 32) {
    return (v >> (bit)) & mask;
  }
  return (__funnelshift_r(v, p32[1], bit)) & mask;
}

inline __device__ uint64_t
loadBits64(void const* p, uint32_t bitIdx, uint32_t width) {
  uint32_t bytes = bitIdx / 8;
  uint32_t bit = bitIdx & 7;
  auto uptr = reinterpret_cast<uintptr_t>(p) + bytes;
  uint32_t ofs = (3 & uptr);
  auto const* p32 = reinterpret_cast<uint32_t const*>(uptr - ofs);
  bit += ofs * 8;
  uint32_t v0 = p32[0];
  uint32_t v1 = p32[1];
  if (bit) {
    v0 = __funnelshift_r(v0, v1, bit);
    v1 = __funnelshift_r(v1, (width + bit > 64 ? p32[2] : 0), bit);
  }
  return (static_cast<uint64_t>(v1 & lowMask<uint32_t>(width - 32)) << 32) | v0;
}

/// Sets 'bits', 'begin' and 'end' so that 'bits' is aligned at 8.
inline __device__ void alignBits(uint64_t* bits, int32_t& begin, int32_t& end) {
  int32_t align = 7 & reinterpret_cast<uintptr_t>(bits);
  if (align) {
    bits = addBytes(bits, -align);
    begin += align * 8;
    end += align * 8;
  }
}

inline __device__ void
alignBits(const uint64_t* bits, int32_t& begin, int32_t& end) {
  int32_t align = 7 & reinterpret_cast<uintptr_t>(bits);
  if (align) {
    bits = addBytes(bits, -align);
    begin += align * 8;
    end += align * 8;
  }
}

/**
 * Invokes a function for each batch of bits (partial or full words)
 * in a given range.
 *
 * @param begin first bit to check (inclusive)
 * @param end last bit to check (exclusive)
 * @param partialWordFunc function to invoke for a partial word;
 *  takes index of the word and mask
 * @param fullWordFunc function to invoke for a full word;
 *  takes index of the word
 */
template <typename PartialWordFunc, typename FullWordFunc>
inline __device__ void forEachWord(
    int32_t begin,
    int32_t end,
    PartialWordFunc partialWordFunc,
    FullWordFunc fullWordFunc) {
  if (begin >= end) {
    return;
  }
  int32_t firstWord = roundUp(begin, 64);
  int32_t lastWord = end & ~63L;
  if (lastWord < firstWord) {
    partialWordFunc(
        lastWord / 64,
        lowMask<uint64_t>(end - lastWord) &
            highMask<uint64_t>(firstWord - begin));
    return;
  }
  if (begin != firstWord) {
    partialWordFunc(begin / 64, highMask<uint64_t>(firstWord - begin));
  }
  for (int32_t i = firstWord; i + 64 <= lastWord; i += 64) {
    fullWordFunc(i / 64);
  }
  if (end != lastWord) {
    partialWordFunc(lastWord / 64, lowMask<uint64_t>(end - lastWord));
  }
}

///
inline int32_t __device__
countBits(const uint64_t* bits, int32_t begin, int32_t end) {
  int32_t numBits = end - begin;
  if (numBits == 0) {
    return 0;
  }
  if (numBits <= 32) {
    return __popc(loadBits32(bits, begin, numBits));
  }
  int32_t count = 0;
  alignBits(bits, begin, end);
  forEachWord(
      begin,
      end,
      [&count, bits](int32_t idx, uint64_t mask) {
        count += __popcll(bits[idx] & mask);
      },
      [&count, bits](int32_t idx) { count += __popcll(bits[idx]); });
  return count;
}

inline int32_t __device__ __host__ scatterBitsDeviceSize(int32_t blockSize) {
  // One int32 per warp + two int32_t.
  return sizeof(int32_t) * (2 + (blockSize / 32));
}

namespace detail {
__device__ inline uint32_t
scatterInWord(uint32_t mask, const uint32_t* source, int32_t& sourceBit) {
  auto result = mask;
  auto sourceWord = source[sourceBit / 32];
  int32_t sourceMask = 1 << (sourceBit & 31);
  auto nextMask = mask;
  while (nextMask) {
    if ((sourceWord & sourceMask) == 0) {
      auto lowBit = __ffs(nextMask) - 1;
      result = result & ~(1 << lowBit);
    }
    ++sourceBit;
    if ((sourceBit & 31) == 0) {
      sourceWord = source[sourceBit / 32];
      sourceMask = 1;
    } else {
      sourceMask = sourceMask << 1;
    }
    nextMask &= nextMask - 1;
  }
  return result;
}

inline __device__ int32_t* warpBase(int32_t* smem) {
  constexpr int32_t kWarpThreads = 32;
  return smem + (threadIdx.x / kWarpThreads);
}

inline __device__ auto* warpScanTemp(void* smem) {
  // The temp storage is empty. Nothing is written. Return base of smem.
  return reinterpret_cast<typename cub::WarpScan<uint32_t>::TempStorage*>(smem);
}
} // namespace detail

/// Sets 'target' so that a 0 bit in 'mask' is 0 and a 1 bit in 'mask' is the
/// nth bit in 'source', where nth is the number of set bits in 'mask' below th
/// target bit. 'mask' and 'target' must be 8 byte aligned. 'source' needs no
/// alignment but the partial int32-s at either end must be addressable.
template <int32_t kWordsPerThread>
__device__ void scatterBitsDevice(
    int32_t numSource,
    int32_t numTarget,
    const char* source,
    const uint64_t* targetMask,
    char* target,
    int32_t* smem) {
  using Scan32 = cub::WarpScan<uint32_t>;
  constexpr int32_t kWarpThreads = 32;
  int32_t align = reinterpret_cast<uintptr_t>(source) & 3;
  source -= align;
  int32_t sourceBitBase = align * 8;
  for (auto targetIdx = 0; targetIdx * 64 < numTarget;
       targetIdx += blockDim.x * kWordsPerThread) {
    int32_t firstTargetIdx = targetIdx + threadIdx.x * kWordsPerThread;
    int32_t bitsForThread =
        min(kWordsPerThread * 64, numTarget - firstTargetIdx * 64);
    uint32_t count = 0;
    for (auto bit = 0; bit < bitsForThread; bit += 64) {
      if (bit + 64 <= bitsForThread) {
        count += __popcll(targetMask[firstTargetIdx + (bit / 64)]);
      } else {
        auto mask = lowMask<uint64_t>(bitsForThread - bit);
        count += __popcll(targetMask[firstTargetIdx + (bit / 64)] & mask);
        break;
      }
    }
    uint32_t threadFirstBit = 0;
    Scan32(*detail::warpScanTemp(smem)).ExclusiveSum(count, threadFirstBit);
    if ((threadIdx.x & (kWarpThreads - 1)) == kWarpThreads - 1) {
      // Last thread in warp sets warpBase to warp bit count.
      *detail::warpBase(smem) = threadFirstBit + count;
    }

    __syncthreads();
    if (threadIdx.x < kWarpThreads) {
      uint32_t start =
          (threadIdx.x < blockDim.x / kWarpThreads) ? smem[threadIdx.x] : 0;
      Scan32(*detail::warpScanTemp(smem)).ExclusiveSum(start, start);
      if (threadIdx.x == (blockDim.x / kWarpThreads) - 1) {
        // The last thread records total sum of bits in smem[blockDim.x / 32].
        smem[blockDim.x / kWarpThreads] =
            start + smem[(blockDim.x / kWarpThreads) - 1];
      }
      if (threadIdx.x < blockDim.x / kWarpThreads) {
        smem[threadIdx.x] = start;
      }
    }
    __syncthreads();
    // Each thread knows its range in source and target.
    int32_t sourceBit =
        sourceBitBase + *detail::warpBase(smem) + threadFirstBit;
    for (auto bit = 0; bit < bitsForThread; bit += 64) {
      uint64_t maskWord;
      if (bit + 64 <= bitsForThread) {
        maskWord = targetMask[firstTargetIdx + (bit / 64)];
      } else {
        auto mask = lowMask<uint64_t>(bitsForThread - bit);
        maskWord = targetMask[firstTargetIdx + (bit / 64)] & mask;
      }
      int2 result;
      result.x = detail::scatterInWord(
          static_cast<uint32_t>(maskWord),
          reinterpret_cast<const uint32_t*>(source),
          sourceBit);
      result.y = detail::scatterInWord(
          maskWord >> 32, reinterpret_cast<const uint32_t*>(source), sourceBit);
      reinterpret_cast<int2*>(target)[firstTargetIdx + (bit / 64)] = result;
    }
    // All threads increment the count of consumed source bits from smem.
    sourceBitBase += smem[blockDim.x / kWarpThreads];
  }
}

/// True if rows[i...last-1] are all consecutive.
inline __device__ bool isDense(const int32_t* rows, int32_t i, int32_t last) {
  return rows[last - 1] - rows[i] == last - i - 1;
}

/// Identifies threads that have a non-null value in a 256 thread
/// block. Consecutive threads process consecutive values. If the thread falls
/// on a null, -1 is returned, else the ordinal of the non-null corresponding to
/// the thread. Must be called on all threads of the TB. 'nonNullOffset' is
/// added to the returned index. 'nonNullOffset' is incremented by the number of
/// non-null rows in this set of 256 rows if numRows == 256. This can be called
/// on a loop where each iteration processes 256 consecutive rows and
/// nonNullOffset c carries the offset between iterations.
inline __device__ int32_t nonNullIndex256(
    char* nulls,
    int32_t bitOffset,
    int32_t numRows,
    int32_t* nonNullOffset,
    int32_t* temp) {
  int32_t group = threadIdx.x / 32;
  uint32_t bits =
      threadIdx.x < numRows ? loadBits32(nulls, bitOffset + group * 32, 32) : 0;
  if (threadIdx.x == kWarpThreads * group) {
    temp[group] = __popc(bits);
  }
  auto previousOffset = *nonNullOffset;
  __syncthreads();
  if (threadIdx.x < kWarpThreads) {
    using Scan = cub::WarpScan<uint32_t, 8>;
    uint32_t count =
        threadIdx.x < (blockDim.x / kWarpThreads) ? temp[threadIdx.x] : 0;
    uint32_t start;
    Scan(*reinterpret_cast<Scan::TempStorage*>(temp))
        .ExclusiveSum(count, start);
    if (threadIdx.x < blockDim.x / kWarpThreads) {
      temp[threadIdx.x] = start;
      if (threadIdx.x == (blockDim.x / kWarpThreads) - 1 && numRows == 256) {
        *nonNullOffset += start + count;
      }
    }
  }
  __syncthreads();
  if (bits & (1 << (threadIdx.x & 31))) {
    return temp[group] + previousOffset +
        __popc(bits & lowMask<uint32_t>(threadIdx.x & 31));
  } else {
    return -1;
  }
}

/// Like nonNullIndex256 but takes an array of non-contiguous but
/// ascending row numbers. 'nulls' must have enough valid bits to
/// cover rows[numRows-1] bits. Each thread returns -1 if
/// rows[threadIdx.x] falls on a null and the corresponding index in
/// non-null rows otherwise. This can be called multiple times on
/// consecutive groups of 256 row numbers. the non-null offset of
/// the last row is carried in 'non-nulloffset'. 'rowOffset' is the
/// offset of the first row of the batch of 256 in 'rows', so it has
/// 0, 256, 512.. in consecutive calls.
inline __device__ int32_t nonNullIndex256Sparse(
    char* nulls,
    int32_t* rows,
    int32_t rowOffset,
    int32_t numRows,
    int32_t* nonNullOffset,
    int32_t* extraNonNulls,
    int32_t* temp) {
  using Scan32 = cub::WarpScan<uint32_t>;
  auto rowIdx = rowOffset + threadIdx.x;
  bool isNull = true;
  uint32_t nonNullsBelow = 0;
  if (rowIdx < numRows) {
    isNull = !isBitSet(nulls, rows[rowIdx]);
    nonNullsBelow = !isNull;
    int32_t previousRow = rowIdx == 0 ? 0 : rows[rowIdx - 1];
    nonNullsBelow += countBits(
        reinterpret_cast<uint64_t*>(nulls), previousRow + 1, rows[rowIdx]);
  }
  Scan32(*detail::warpScanTemp(temp))
      .InclusiveSum(nonNullsBelow, nonNullsBelow);
  if (detail::isLastInWarp()) {
    // The last thread of the warp writes warp total.
    temp[threadIdx.x / kWarpThreads] = nonNullsBelow;
  }
  int32_t previousOffset = *nonNullOffset;
  __syncthreads();
  if (threadIdx.x < kWarpThreads) {
    int32_t start = 0;
    if (threadIdx.x < blockDim.x / kWarpThreads) {
      start = temp[threadIdx.x];
    }
    using Scan8 = cub::WarpScan<int32_t, 8>;
    int32_t sum = 0;
    Scan8(*reinterpret_cast<Scan8::TempStorage*>(temp))
        .ExclusiveSum(start, sum);
    if (threadIdx.x == (blockDim.x / kWarpThreads) - 1) {
      // The last sum thread increments the running count of non-nulls
      // by the block total. It adds the optional extraNonNulls to the
      // total between the barriers. 'extraNonNulls' or the running
      // non-null count do not affect the result of the 256 lanes of
      // this.
      if (extraNonNulls) {
        start += *extraNonNulls;
        *extraNonNulls = 0;
      }
      *nonNullOffset += start + sum;
    }
    if (threadIdx.x < blockDim.x / kWarpThreads) {
      temp[threadIdx.x] = sum;
    }
  }
  __syncthreads();
  if (isNull) {
    return -1;
  }
  return temp[threadIdx.x / kWarpThreads] + previousOffset + nonNullsBelow - 1;
}

} // namespace facebook::velox::wave
