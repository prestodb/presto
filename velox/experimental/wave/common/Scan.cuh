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

#include "velox/experimental/wave/common/BitUtil.cuh"

namespace facebook::velox::wave {

template <typename T, int32_t kNumLanes = kWarpThreads>
struct WarpScan {
  enum {
    /// Whether the logical warp size and the PTX warp size coincide
    IS_ARCH_WARP = (kNumLanes == kWarpThreads),

    /// The number of warp scan steps
    STEPS = Log2<kNumLanes>::VALUE,
  };

  int laneId;

  static constexpr unsigned int member_mask =
      kNumLanes == 32 ? 0xffffffff : (1 << kNumLanes) - 1;

  __device__ WarpScan() : laneId(LaneId()) {}

  __device__ __forceinline__ void exclusiveSum(
      T input, ///< [in] Calling thread's input item.
      T& exclusive_output) ///< [out] Calling thread's output item.  May be
                           ///< aliased with \p input.
  {
    T initial_value = 0;
    exclusiveSum(input, exclusive_output, initial_value);
  }

  __device__ __forceinline__ void exclusiveSum(
      T input, ///< [in] Calling thread's input item.
      T& exclusive_output, ///< [out] Calling thread's output item.  May be
                           ///< aliased with \p input.
      T initial_value) {
    T inclusive_output;
    inclusiveSum(input, inclusive_output);

    exclusive_output = initial_value + inclusive_output - input;
  }

  __device__ __forceinline__ void exclusiveSum(
      T input,
      T& exclusive_output,
      T initial_value,
      T& warp_aggregate) {
    T inclusive_output;
    inclusivesum(input, inclusive_output);
    warp_aggregate = __shfl_sync(member_mask, inclusive_output, kNumLanes - 1);
    exclusive_output = initial_value + inclusive_output - input;
  }

  __device__ __forceinline__ void inclusiveSum(T input, T& inclusive_output) {
    inclusive_output = input;
    if (IS_ARCH_WARP || (member_mask & (1 << laneId)) != 0) {
#pragma unroll
      for (int STEP = 0; STEP < STEPS; STEP++) {
        int offset = (1 << STEP);
        T other = __shfl_up_sync(member_mask, inclusive_output, offset);
        if (laneId >= offset) {
          inclusive_output += other;
        }
      }
    }
  }
};

template <typename T, int32_t kNumLanes = kWarpThreads>
struct WarpReduce {
  static constexpr int32_t STEPS = Log2<kNumLanes>::VALUE;

  int laneId;

  /// 32-thread physical warp member mask of logical warp

  static constexpr unsigned int member_mask =
      kNumLanes == 32 ? 0xffffffff : (1 << kNumLanes) - 1;

  __device__ WarpReduce() : laneId(LaneId()) {}

  template <typename Func>
  __device__ __forceinline__ T reduce(T val, Func func) {
    for (int32_t offset = kNumLanes / 2; offset > 0; offset = offset >> 1) {
      T other = __shfl_down_sync(0xffffffff, val, offset);
      if (laneId + offset < kNumLanes) {
        val = func(val, other);
      }
    }
    return val;
  }
};

/// Returns the block wide exclusive sum (sum of 'input' for all
/// lanes below threadIdx.x). If 'total' is non-nullptr, the block
/// wide sum is returned in '*total'. 'temp' must have
/// exclusiveSumTempSize() writable bytes aligned for T.
template <typename T, int32_t kBlockSize>
inline __device__ T exclusiveSum(T input, T* total, T* temp) {
  constexpr int32_t kNumWarps = kBlockSize / kWarpThreads;
  using Scan = WarpScan<T>;
  T sum;
  Scan().exclusiveSum(input, sum);
  if (kBlockSize == kWarpThreads) {
    if (total) {
      if (threadIdx.x == kWarpThreads - 1) {
        *total = input + sum;
      }
      __syncthreads();
    }
    return sum;
  }
  if (detail::isLastInWarp()) {
    temp[threadIdx.x / kWarpThreads] = input + sum;
  }
  __syncthreads();
  using InnerScan = WarpScan<T, kNumWarps>;
  T warpSum = threadIdx.x < kNumWarps ? temp[threadIdx.x] : 0;
  T blockSum;
  InnerScan().exclusiveSum(warpSum, blockSum);
  if (threadIdx.x < kNumWarps) {
    temp[threadIdx.x] = blockSum;
    if (total && threadIdx.x == kNumWarps - 1) {
      *total = warpSum + blockSum;
    }
  }
  __syncthreads();
  return sum + temp[threadIdx.x / kWarpThreads];
}

/// Returns the block wide inclusive sum (sum of 'input' for all
/// lanes below threadIdx.x). 'temp' must have
/// exclusiveSumTempSize() writable bytes aligned for T. '*total' is set to the
/// TB-wide total if 'total' is not nullptr.
template <typename T, int32_t kBlockSize>
inline __device__ T inclusiveSum(T input, T* total, T* temp) {
  constexpr int32_t kNumWarps = kBlockSize / kWarpThreads;
  using Scan = WarpScan<T>;
  T sum;
  Scan().inclusiveSum(input, sum);
  if (kBlockSize <= kWarpThreads) {
    if (total != nullptr) {
      if (threadIdx.x == kBlockSize - 1) {
        *total = sum;
      }
      __syncthreads();
    }
    return sum;
  }
  if (detail::isLastInWarp()) {
    temp[threadIdx.x / kWarpThreads] = sum;
  }
  __syncthreads();
  constexpr int32_t kInnerWidth = kNumWarps < 2 ? 2 : kNumWarps;
  using InnerScan = WarpScan<T, kInnerWidth>;
  T warpSum = threadIdx.x < kInnerWidth ? temp[threadIdx.x] : 0;
  T blockSum;
  InnerScan().exclusiveSum(warpSum, blockSum);
  if (threadIdx.x < kInnerWidth) {
    temp[threadIdx.x] = blockSum;
  }
  if (total != nullptr && threadIdx.x == kInnerWidth - 1) {
    *total = blockSum + warpSum;
  }
  __syncthreads();
  return sum + temp[threadIdx.x / kWarpThreads];
}

} // namespace facebook::velox::wave
