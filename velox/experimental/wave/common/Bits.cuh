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
#include "velox/experimental/wave/common/CudaUtil.cuh"

namespace facebook::velox::wave {

inline int32_t __device__ __host__ scatterBitsDeviceSize(int32_t blockSize) {
  // One int32 per warp + one int32_t.
  return sizeof(int32_t) * (1 + (blockSize / 32));
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

} // namespace facebook::velox::wave
