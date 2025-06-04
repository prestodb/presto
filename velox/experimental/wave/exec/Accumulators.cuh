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

#include "velox/experimental/wave/common/Scan.cuh"

namespace facebook::velox::wave {

template <typename T>
__device__ inline void atomicInc(T* ptr, T inc) {
  atomicAdd(ptr, inc);
}

template <>
__device__ inline void atomicInc(int64_t* ptr, int64_t inc) {
  atomicAdd((unsigned long long*)ptr, (unsigned long long)inc);
}

template <typename AccType, typename IncType, typename Reduce2>
void __device__ __forceinline__ simpleAccumulate(
    uint32_t peers,
    int32_t leader,
    int32_t lane,
    AccType* acc,
    uint32_t nulls,
    uint32_t* aggNulls,
    uint32_t aggNullMask,
    IncType input,
    bool inputIsNull,
    Reduce2 reduce) {
  IncType agg;
  auto toUpdate = peers;
  bool isAny = false;
  for (;;) {
    auto peer = __ffs(toUpdate) - 1;
    auto inc = __shfl_sync(peers, input, peer);
    auto isNull = __shfl_sync(peers, inputIsNull, peer);
    if (lane == leader) {
      if (!isNull) {
        agg = !isAny ? inc : reduce(agg, inc);
        isAny = true;
      }
      if (peer == leader) {
        if (isAny) {
          atomicInc(acc, agg);
          if ((nulls & aggNullMask) == 0) {
            atomicOr(aggNulls, aggNullMask);
            nulls |= aggNullMask;
          }
        }
        break;
      }
    }
    if ((toUpdate &= toUpdate - 1) == 0) {
      return;
    }
  }
}

template <typename T>
__device__ __forceinline__ void sumReduce(
    T input,
    bool isNull,
    ErrorCode laneStatus,
    uint32_t nulls,
    T* result,
    uint32_t* resultNulls,
    uint32_t nullMask,
    void* smem) {
  using Reduce = WarpReduce<T>;
  using Reduce8 = WarpReduce<T, 8>;
  constexpr int32_t kNumWarps = kBlockSize / kWarpThreads;
  T* warpSum = reinterpret_cast<T*>(smem);
  bool* warpAny = reinterpret_cast<bool*>(warpSum + kNumWarps);
  bool nonNull = laneStatus == ErrorCode::kOk && !isNull;
  bool warpFlag = __ballot_sync(0xffffffff, nonNull) != 0;
  T laneValue = nonNull ? input : 0;
  T warpResult = Reduce().reduce(laneValue, [](T x, T y) { return x + y; });
  if ((threadIdx.x & 31) == 0) {
    warpAny[threadIdx.x / kWarpThreads] = warpFlag;
    warpSum[threadIdx.x / kWarpThreads] = warpResult;
  }
  __syncthreads();
  bool anyAtAll;
  if (threadIdx.x < kNumWarps) {
    anyAtAll =
        __ballot_sync(lowMask<uint32_t>(kNumWarps), warpAny[threadIdx.x]) != 0;
  }
  T finalSum;
  if (threadIdx.x < kWarpThreads) {
    finalSum = Reduce8().reduce(
        threadIdx.x < kWarpThreads ? warpSum[threadIdx.x] : 0,
        [](T x, T y) { return x + y; });
    if (threadIdx.x == 0) {
      if (anyAtAll) {
        atomicInc(result, finalSum);
        if ((nulls & nullMask) == 0) {
          atomicOr(resultNulls, nullMask);
        }
      }
    }
  }
}

} // namespace facebook::velox::wave
