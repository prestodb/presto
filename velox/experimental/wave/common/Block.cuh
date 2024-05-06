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

#include <cub/block/block_radix_sort.cuh>
#include <cub/block/block_reduce.cuh>
#include <cub/block/block_scan.cuh>
#include <cub/block/block_store.cuh>
#include "velox/experimental/wave/common/CudaUtil.cuh"

/// Utilities for  booleans and indices and thread blocks.

namespace facebook::velox::wave {

/// Converts an array of flags to an array of indices of set flags. The first
/// index is given by 'start'. The number of indices is returned in 'size', i.e.
/// this is 1 + the index of the last set flag.
template <
    typename T,
    int32_t blockSize,
    cub::BlockScanAlgorithm Algorithm = cub::BLOCK_SCAN_RAKING>
inline int32_t __device__ __host__ boolToIndicesSharedSize() {
  typedef cub::BlockScan<T, blockSize, Algorithm> BlockScanT;

  return sizeof(typename BlockScanT::TempStorage);
}

/// Converts an array of flags to an array of indices of set flags. The first
/// index is given by 'start'. The number of indices is returned in 'size', i.e.
/// this is 1 + the index of the last set flag.
template <
    int32_t blockSize,
    typename T,
    cub::BlockScanAlgorithm Algorithm = cub::BLOCK_SCAN_RAKING,
    typename Getter>
__device__ inline void
boolBlockToIndices(Getter getter, T start, T* indices, void* shmem, T& size) {
  typedef cub::BlockScan<T, blockSize, Algorithm> BlockScanT;

  auto* temp = reinterpret_cast<typename BlockScanT::TempStorage*>(shmem);
  T data[1];
  uint8_t flag = getter();
  data[0] = flag;
  __syncthreads();
  T aggregate;
  BlockScanT(*temp).ExclusiveSum(data, data, aggregate);
  if (flag) {
    indices[data[0]] = threadIdx.x + start;
  }
  if (threadIdx.x == 0) {
    size = aggregate;
  }
  __syncthreads();
}

inline int32_t __device__ __host__ bool256ToIndicesSize() {
  return sizeof(typename cub::WarpScan<uint16_t>::TempStorage) +
      33 * sizeof(uint16_t);
}

/// Returns indices of set bits for 256 one byte flags. 'getter8' is
/// invoked for 8 flags at a time, with the ordinal of the 8 byte
/// flags word as argument, so that an index of 1 means flags
/// 8..15. The indices start at 'start' and last index + 1 is
/// returned in 'size'.
template <typename T, typename Getter8>
__device__ inline void
bool256ToIndices(Getter8 getter8, T start, T* indices, T& size, char* smem) {
  using Scan = cub::WarpScan<uint16_t>;
  auto* smem16 = reinterpret_cast<uint16_t*>(smem);
  int32_t group = threadIdx.x / 8;
  uint64_t bits = getter8(group) & 0x0101010101010101;
  if ((threadIdx.x & 7) == 0) {
    smem16[group] = __popcll(bits);
    if (threadIdx.x == blockDim.x - 8) {
      smem16[32] = smem16[31];
    }
  }
  __syncthreads();
  if (threadIdx.x < 32) {
    auto* temp = reinterpret_cast<typename Scan::TempStorage*>((smem + 72));
    uint16_t data = smem16[threadIdx.x];
    Scan(*temp).ExclusiveSum(data, data);
    smem16[threadIdx.x] = data;
  }
  __syncthreads();
  int32_t tidInGroup = threadIdx.x & 7;
  if (bits & (1UL << (tidInGroup * 8))) {
    int32_t base =
        smem16[group] + __popcll(bits & lowMask<uint64_t>(tidInGroup * 8));
    indices[base] = threadIdx.x + start;
  }
  if (threadIdx.x == 0) {
    size = smem16[31] + smem16[32];
  }
  __syncthreads();
}

template <int32_t blockSize, typename T, typename Getter>
__device__ inline void blockSum(Getter getter, void* shmem, T* result) {
  typedef cub::BlockReduce<T, blockSize> BlockReduceT;

  auto* temp = reinterpret_cast<typename BlockReduceT::TempStorage*>(shmem);
  T data[1];
  data[0] = getter();
  T aggregate = BlockReduceT(*temp).Reduce(data, cub::Sum());

  if (threadIdx.x == 0) {
    result[blockIdx.x] = aggregate;
  }
}

template <
    int32_t kBlockSize,
    int32_t kItemsPerThread,
    typename Key,
    typename Value>
using RadixSort =
    typename cub::BlockRadixSort<Key, kBlockSize, kItemsPerThread, Value>;

template <
    int32_t kBlockSize,
    int32_t kItemsPerThread,
    typename Key,
    typename Value>
inline int32_t __host__ __device__ blockSortSharedSize() {
  return sizeof(
      typename RadixSort<kBlockSize, kItemsPerThread, Key, Value>::TempStorage);
}

template <
    int32_t kBlockSize,
    int32_t kItemsPerThread,
    typename Key,
    typename Value,
    typename KeyGetter,
    typename ValueGetter>
void __device__ blockSort(
    KeyGetter keyGetter,
    ValueGetter valueGetter,
    Key* keyOut,
    Value* valueOut,
    char* smem) {
  using Sort = cub::BlockRadixSort<Key, kBlockSize, kItemsPerThread, Value>;

  // Per-thread tile items
  Key keys[kItemsPerThread];
  Value values[kItemsPerThread];

  // Our current block's offset
  int blockOffset = 0;

  // Load items into a blocked arrangement
  for (auto i = 0; i < kItemsPerThread; ++i) {
    int32_t idx = blockOffset + i * kBlockSize + threadIdx.x;
    values[i] = valueGetter(idx);
    keys[i] = keyGetter(idx);
  }

  __syncthreads();
  auto* temp_storage = reinterpret_cast<typename Sort::TempStorage*>(smem);

  Sort(*temp_storage).SortBlockedToStriped(keys, values);

  // Store output in striped fashion
  cub::StoreDirectStriped<kBlockSize>(
      threadIdx.x, valueOut + blockOffset, values);
  cub::StoreDirectStriped<kBlockSize>(threadIdx.x, keyOut + blockOffset, keys);
  __syncthreads();
}

template <int kBlockSize>
int32_t partitionRowsSharedSize(int32_t numPartitions) {
  using Scan = cub::BlockScan<int, kBlockSize>;
  auto scanSize = sizeof(typename Scan::TempStorage) + sizeof(int32_t);
  int32_t counterSize = sizeof(int32_t) * numPartitions;
  if (counterSize <= scanSize) {
    return scanSize;
  }
  static_assert(
      sizeof(typename Scan::TempStorage) >= sizeof(int32_t) * kBlockSize);
  return scanSize + counterSize; // - kBlockSize * sizeof(int32_t);
}

/// Partitions a sequence of indices into runs where the indices
/// belonging to the same partition are contiguous. Indices from 0 to
/// 'numKeys-1' are partitioned into 'partitionedRows', which must
/// have space for 'numKeys' row numbers. The 0-based partition number
/// for row 'i' is given by 'getter(i)'.  The row numbers for
/// partition 0 start at 0. The row numbers for partition i start at
/// 'partitionStarts[i-1]'. There must be at least the amount of
/// shared memory given by partitionSharedSize(numPartitions).
/// 'ranks' is a temporary array of 'numKeys' elements.
template <int32_t kBlockSize, typename RowNumber, typename Getter>
void __device__ partitionRows(
    Getter getter,
    uint32_t numKeys,
    uint32_t numPartitions,
    RowNumber* ranks,
    RowNumber* partitionStarts,
    RowNumber* partitionedRows) {
  using Scan = cub::BlockScan<int32_t, kBlockSize>;
  constexpr int32_t kWarpThreads = 1 << CUB_LOG_WARP_THREADS(0);
  auto warp = threadIdx.x / kWarpThreads;
  auto lane = cub::LaneId();
  extern __shared__ __align__(16) char smem[];
  auto* counters = reinterpret_cast<uint32_t*>(
      numPartitions <= kBlockSize ? smem
                                  : smem +
              sizeof(typename Scan::
                         TempStorage) /*- kBlockSize * sizeof(uint32_t)*/);
  for (auto i = threadIdx.x; i < numPartitions; i += kBlockSize) {
    counters[i] = 0;
  }
  __syncthreads();
  for (auto start = 0; start < numKeys; start += kBlockSize) {
    int32_t warpStart = start + warp * kWarpThreads;
    if (start >= numKeys) {
      break;
    }
    uint32_t laneMask = warpStart + kWarpThreads <= numKeys
        ? 0xffffffff
        : lowMask<uint32_t>(numKeys - warpStart);
    if (warpStart + lane < numKeys) {
      int32_t key = getter(warpStart + lane);
      uint32_t mask = __match_any_sync(laneMask, key);
      int32_t leader = (kWarpThreads - 1) - __clz(mask);
      uint32_t cnt = __popc(mask & lowMask<uint32_t>(lane + 1));
      uint32_t base;
      if (lane == leader) {
        base = atomicAdd(&counters[key], cnt);
      }
      base = __shfl_sync(laneMask, base, leader);
      ranks[warpStart + lane] = base + cnt - 1;
    }
  }
  // Prefix sum the counts. All counters must have their final value.
  __syncthreads();
  auto* temp = reinterpret_cast<typename Scan::TempStorage*>(smem);
  int32_t* aggregate = reinterpret_cast<int32_t*>(smem);
  for (auto start = 0; start < numPartitions; start += kBlockSize) {
    int32_t localCount[1];
    localCount[0] =
        threadIdx.x + start < numPartitions ? counters[start + threadIdx.x] : 0;
    if (threadIdx.x == 0 && start > 0) {
      // The sum of the previous round is carried over as start of this.
      localCount[0] += *aggregate;
    }
    Scan(*temp).InclusiveSum(localCount, localCount);
    if (start + threadIdx.x < numPartitions) {
      partitionStarts[start + threadIdx.x] = localCount[0];
    }
    if (threadIdx.x == kBlockSize - 1 && start + kBlockSize < numPartitions) {
      *aggregate = localCount[0];
    }
    __syncthreads();
  }
  if (threadIdx.x == 0) {
    if (partitionStarts[numPartitions - 1] != numKeys) {
      *(long*)0 = 0;
    }
  }
  // Write the row numbers of the inputs into the rankth position in each
  // partition.
  for (auto i = threadIdx.x; i < numKeys; i += kBlockSize) {
    auto key = getter(i);
    auto keyStart = key == 0 ? 0 : partitionStarts[key - 1];
    partitionedRows[keyStart + ranks[i]] = i;
  }
  __syncthreads();
}

} // namespace facebook::velox::wave
