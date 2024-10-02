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

#include "velox/experimental/wave/common/HashTable.cuh"
#include "velox/experimental/wave/common/tests/BlockTest.h"

namespace facebook::velox::wave {

using Mutex = cuda::binary_semaphore<cuda::thread_scope_device>;

inline void __device__ testingLock(int32_t* mtx) {
  reinterpret_cast<Mutex*>(mtx)->acquire();
}

inline void __device__ testingUnlock(int32_t* mtx) {
  reinterpret_cast<Mutex*>(mtx)->release();
}

void __device__ testSumNoSync(TestingRow* rows, HashProbe* probe) {
  auto keys = reinterpret_cast<int64_t**>(probe->keys);
  auto indices = keys[0];
  auto deltas = keys[1];
  int32_t base = probe->numRowsPerThread * blockDim.x * blockIdx.x;
  int32_t end = base + probe->numRows[blockIdx.x];

  for (auto i = base + threadIdx.x; i < end; i += blockDim.x) {
    auto* row = &rows[indices[i]];
    row->count += deltas[i];
  }
}

void __device__ testSumPart(
    TestingRow* rows,
    int32_t numParts,
    HashProbe* probe,
    int32_t* part,
    int32_t* partEnd,
    int32_t numGroups,
    int32_t groupStride) {
  auto keys = reinterpret_cast<int64_t**>(probe->keys);
  auto indices = keys[0];
  auto deltas = keys[1];
  for (auto groupIdx = 0; groupIdx < numGroups; ++groupIdx) {
    auto groupStart = groupIdx * groupStride;
    int32_t linear = threadIdx.x + blockIdx.x * blockDim.x;
    if (linear > numParts) {
      break;
    }
    int32_t begin = linear == 0 ? groupStart
                                : groupStart + partEnd[groupStart + linear - 1];
    int32_t end = groupStart + partEnd[groupStart + linear];

    for (auto i = begin; i < end; ++i) {
      auto index = groupStart + part[i];
      auto* row = &rows[indices[index]];
      row->count += deltas[index];
    }
  }
  __syncthreads();
}

void __device__ testSumMtx(TestingRow* rows, HashProbe* probe) {
  auto keys = reinterpret_cast<int64_t**>(probe->keys);
  auto indices = keys[0];
  auto deltas = keys[1];
  int32_t base = probe->numRowsPerThread * blockDim.x * blockIdx.x;
  int32_t end = base + probe->numRows[blockIdx.x];

  for (auto i = base + threadIdx.x; i < end; i += blockDim.x) {
    auto* row = &rows[indices[i]];
    testingLock(&row->flags);
    row->count += deltas[i];
    testingUnlock(&row->flags);
  }
}

void __device__ testSumAtomic(TestingRow* rows, HashProbe* probe) {
  auto keys = reinterpret_cast<int64_t**>(probe->keys);
  auto indices = keys[0];
  auto deltas = keys[1];
  int32_t base = probe->numRowsPerThread * blockDim.x * blockIdx.x;
  int32_t end = base + probe->numRows[blockIdx.x];

  for (auto i = base + threadIdx.x; i < end; i += blockDim.x) {
    auto* row = &rows[indices[i]];
    atomicAdd((unsigned long long*)&row->count, (unsigned long long)deltas[i]);
  }
}

void __device__ testSumAtomicCoalesceShmem(TestingRow* rows, HashProbe* probe) {
  constexpr int32_t kWarpThreads = 32;
  auto keys = reinterpret_cast<int64_t**>(probe->keys);
  auto indices = keys[0];
  auto deltas = keys[1];
  int32_t base = probe->numRowsPerThread * blockDim.x * blockIdx.x;
  int32_t lane = cub::LaneId();
  int32_t end = base + probe->numRows[blockIdx.x];
  extern __shared__ char smem[];

  int64_t* totals = (int64_t*)smem;

  for (auto count = base; count < end; count += blockDim.x) {
    auto i = threadIdx.x + count;

    if (i < end) {
      totals[threadIdx.x] = 0;
      __syncwarp();
      uint32_t laneMask = count + kWarpThreads <= end
          ? 0xffffffff
          : lowMask<uint32_t>(end - count);
      auto index = indices[i];
      auto delta = deltas[i];
      uint32_t allPeers = __match_any_sync(laneMask, index);
      int32_t leader = __ffs(allPeers) - 1;
      atomicAdd(
          (unsigned long long*)&totals
              [(threadIdx.x & (~(kWarpThreads - 1))) | leader],
          (unsigned long long)delta);
      __syncwarp();
      if (lane == leader) {
        auto* row = &rows[index];
        atomicAdd(
            (unsigned long long*)&row->count,
            (unsigned long long)totals[threadIdx.x]);
      }
    }
  }
}

void __device__ testSumAtomicCoalesceShfl(TestingRow* rows, HashProbe* probe) {
  constexpr int32_t kWarpThreads = 32;
  auto keys = reinterpret_cast<int64_t**>(probe->keys);
  auto indices = keys[0];
  auto deltas = keys[1];
  int32_t base = probe->numRowsPerThread * blockDim.x * blockIdx.x;
  int32_t lane = cub::LaneId();
  int32_t end = base + probe->numRows[blockIdx.x];

  for (auto count = base; count < end; count += blockDim.x) {
    auto i = threadIdx.x + count;

    if (i < end) {
      uint32_t laneMask = count + kWarpThreads <= end
          ? 0xffffffff
          : lowMask<uint32_t>(end - count);
      auto index = indices[i];
      auto delta = deltas[i];
      uint32_t allPeers = __match_any_sync(laneMask, index);
      int32_t leader = __ffs(allPeers) - 1;
      auto peers = allPeers;
      int64_t total = 0;
      auto currentPeer = leader;
      for (;;) {
        total += __shfl_sync(allPeers, delta, currentPeer);
        peers &= peers - 1;
        if (peers == 0) {
          break;
        }
        currentPeer = __ffs(peers) - 1;
      }
      if (lane == leader) {
        auto* row = &rows[index];
        atomicAdd((unsigned long long*)&row->count, (unsigned long long)total);
      }
    }
  }
}

void __device__ testSumMtxCoalesce(TestingRow* rows, HashProbe* probe) {
  constexpr int32_t kWarpThreads = 32;
  auto keys = reinterpret_cast<int64_t**>(probe->keys);
  auto indices = keys[0];
  auto deltas = keys[1];
  int32_t base = probe->numRowsPerThread * blockDim.x * blockIdx.x;
  int32_t lane = cub::LaneId();
  int32_t end = base + probe->numRows[blockIdx.x];

  for (auto count = base; count < end; count += blockDim.x) {
    auto i = threadIdx.x + count;

    if (i < end) {
      uint32_t laneMask = count + kWarpThreads <= end
          ? 0xffffffff
          : lowMask<uint32_t>(end - count);
      auto index = indices[i];
      auto delta = deltas[i];
      uint32_t allPeers = __match_any_sync(laneMask, index);
      int32_t leader = __ffs(allPeers) - 1;
      auto peers = allPeers;
      int64_t total = 0;
      auto currentPeer = leader;
      for (;;) {
        total += __shfl_sync(allPeers, delta, currentPeer);
        peers &= peers - 1;
        if (peers == 0) {
          break;
        }
        currentPeer = __ffs(peers) - 1;
      }
      if (lane == leader) {
        auto* row = &rows[index];
        testingLock(&row->flags);
        row->count += total;
        testingUnlock(&row->flags);
      }
    }
  }
}

void __device__ testSumOrder(TestingRow* rows, HashProbe* probe) {
  auto keys = reinterpret_cast<int64_t**>(probe->keys);
  auto indices = keys[0];
  auto deltas = keys[1];
  int32_t base = probe->numRowsPerThread * blockDim.x * blockIdx.x;
  int32_t end = base + probe->numRows[blockIdx.x];

  for (auto i = base + threadIdx.x; i < end; i += blockDim.x) {
    auto* row = &rows[indices[i]];
    int32_t waitNano = 1;
    auto d = deltas[i];
    for (;;) {
      if (0 ==
          asDeviceAtomic<int32_t>(&row->flags)
              ->exchange(1, cuda::memory_order_consume)) {
        row->count += d;
        asDeviceAtomic<int32_t>(&row->flags)
            ->store(0, cuda::memory_order_release);
        break;
      } else {
        __nanosleep(10 * waitNano);
        waitNano += threadIdx.x & 31;
      }
    }
  }
}

} // namespace facebook::velox::wave
