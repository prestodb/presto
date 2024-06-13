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

#include "velox/experimental/wave/common/Bits.cuh"
#include "velox/experimental/wave/common/Block.cuh"
#include "velox/experimental/wave/common/CudaUtil.cuh"
#include "velox/experimental/wave/common/HashTable.cuh"
#include "velox/experimental/wave/common/tests/BlockTest.h"
#include "velox/experimental/wave/common/tests/HashTestUtil.h"
#include "velox/experimental/wave/common/tests/Updates.cuh"

namespace facebook::velox::wave {

using ScanAlgorithm = cub::BlockScan<int, 256, cub::BLOCK_SCAN_RAKING>;

__global__ void
boolToIndicesKernel(uint8_t** bools, int32_t** indices, int32_t* sizes) {
  extern __shared__ char smem[];
  int32_t idx = blockIdx.x;
  // Start cycle timer
  uint8_t* blockBools = bools[idx];
  boolBlockToIndices<256>(
      [&]() { return blockBools[threadIdx.x]; },
      idx * 256,
      indices[idx],
      smem,
      sizes[idx]);
  __syncthreads();
}

void BlockTestStream::testBoolToIndices(
    int32_t numBlocks,
    uint8_t** flags,
    int32_t** indices,
    int32_t* sizes) {
  CUDA_CHECK(cudaGetLastError());
  auto tempBytes = sizeof(typename ScanAlgorithm::TempStorage);
  boolToIndicesKernel<<<numBlocks, 256, tempBytes, stream_->stream>>>(
      flags, indices, sizes);
  CUDA_CHECK(cudaGetLastError());
}

__global__ void boolToIndicesNoSharedKernel(
    uint8_t** bools,
    int32_t** indices,
    int32_t* sizes,
    void* temp) {
  int32_t idx = blockIdx.x;

  uint8_t* blockBools = bools[idx];
  char* smem = reinterpret_cast<char*>(temp) +
      blockIdx.x * sizeof(typename ScanAlgorithm::TempStorage);
  boolBlockToIndices<256>(
      [&]() { return blockBools[threadIdx.x]; },
      idx * 256,
      indices[idx],
      smem,
      sizes[idx]);
  __syncthreads();
}

void BlockTestStream::testBoolToIndicesNoShared(
    int32_t numBlocks,
    uint8_t** flags,
    int32_t** indices,
    int32_t* sizes,
    void* temp) {
  CUDA_CHECK(cudaGetLastError());
  boolToIndicesNoSharedKernel<<<numBlocks, 256, 0, stream_->stream>>>(
      flags, indices, sizes, temp);
  CUDA_CHECK(cudaGetLastError());
}

int32_t BlockTestStream::boolToIndicesSize() {
  return sizeof(typename ScanAlgorithm::TempStorage);
}

__global__ void
bool256ToIndicesKernel(uint8_t** bools, int32_t** indices, int32_t* sizes) {
  extern __shared__ char smem[];
  int32_t idx = blockIdx.x;
  auto* bool64 = reinterpret_cast<uint64_t*>(bools[idx]);
  bool256ToIndices(
      [&](int32_t index8) { return bool64[index8]; },
      idx * 256,
      indices[idx],
      sizes[idx],
      smem);
  __syncthreads();
}

void BlockTestStream::testBool256ToIndices(
    int32_t numBlocks,
    uint8_t** flags,
    int32_t** indices,
    int32_t* sizes) {
  CUDA_CHECK(cudaGetLastError());
  auto tempBytes = bool256ToIndicesSize();
  bool256ToIndicesKernel<<<numBlocks, 256, tempBytes, stream_->stream>>>(
      flags, indices, sizes);
  CUDA_CHECK(cudaGetLastError());
}

__global__ void bool256ToIndicesNoSharedKernel(
    uint8_t** bools,
    int32_t** indices,
    int32_t* sizes,
    void* temp) {
  int32_t idx = blockIdx.x;
  auto* bool64 = reinterpret_cast<uint64_t*>(bools[idx]);
  char* smem = reinterpret_cast<char*>(temp) + blockIdx.x * 80;
  bool256ToIndices(
      [&](int32_t index8) { return bool64[index8]; },
      idx * 256,
      indices[idx],
      sizes[idx],
      smem);
  __syncthreads();
}

void BlockTestStream::testBool256ToIndicesNoShared(
    int32_t numBlocks,
    uint8_t** flags,
    int32_t** indices,
    int32_t* sizes,
    void* temp) {
  CUDA_CHECK(cudaGetLastError());
  bool256ToIndicesNoSharedKernel<<<numBlocks, 256, 0, stream_->stream>>>(
      flags, indices, sizes, temp);
  CUDA_CHECK(cudaGetLastError());
}

int32_t BlockTestStream::bool256ToIndicesSize() {
  return 80;
}

__global__ void sum64(int64_t* numbers, int64_t* results) {
  extern __shared__ char smem[];
  int32_t idx = blockIdx.x;
  blockSum<256>(
      [&]() { return numbers[idx * 256 + threadIdx.x]; }, smem, results);
  __syncthreads();
}

void BlockTestStream::testSum64(
    int32_t numBlocks,
    int64_t* numbers,
    int64_t* results) {
  auto tempBytes = sizeof(typename cub::BlockReduce<int64_t, 256>::TempStorage);
  sum64<<<numBlocks, 256, tempBytes, stream_->stream>>>(numbers, results);
  CUDA_CHECK(cudaGetLastError());
}

/// Keys and values are n sections of 8K items. The items in each section get
/// sorted on the key.
void __global__ __launch_bounds__(1024)
    testSort(uint16_t** keys, uint16_t** values) {
  extern __shared__ __align__(16) char smem[];
  auto keyBase = keys[blockIdx.x];
  auto valueBase = values[blockIdx.x];
  blockSort<256, 32>(
      [&](auto i) { return keyBase[i]; },
      [&](auto i) { return valueBase[i]; },
      keys[blockIdx.x],
      values[blockIdx.x],
      smem);
  __syncthreads();
}

void __global__ __launch_bounds__(1024)
    testSortNoShared(uint16_t** keys, uint16_t** values, char* smem) {
  auto keyBase = keys[blockIdx.x];
  auto valueBase = values[blockIdx.x];
  char* tbTemp = smem +
      blockIdx.x *
          sizeof(typename cub::BlockRadixSort<uint16_t, 256, 32, uint16_t>::
                     TempStorage);

  blockSort<256, 32>(
      [&](auto i) { return keyBase[i]; },
      [&](auto i) { return valueBase[i]; },
      keys[blockIdx.x],
      values[blockIdx.x],
      tbTemp);
  __syncthreads();
}

int32_t BlockTestStream::sort16SharedSize() {
  return sizeof(
      typename cub::BlockRadixSort<uint16_t, 256, 32, uint16_t>::TempStorage);
}

void BlockTestStream::testSort16(
    int32_t numBlocks,
    uint16_t** keys,
    uint16_t** values) {
  auto tempBytes = sizeof(
      typename cub::BlockRadixSort<uint16_t, 256, 32, uint16_t>::TempStorage);

  testSort<<<numBlocks, 256, tempBytes, stream_->stream>>>(keys, values);
}

void BlockTestStream::testSort16NoShared(
    int32_t numBlocks,
    uint16_t** keys,
    uint16_t** values,
    char* temp) {
  testSortNoShared<<<numBlocks, 256, 0, stream_->stream>>>(keys, values, temp);
}

/// Calls partitionRows on each thread block of 256 threads. The parameters
/// correspond to 'partitionRows'. Each is an array subscripted by blockIdx.x.
void __global__ partitionShortsKernel(
    uint16_t** keys,
    int32_t* numKeys,
    int32_t numPartitions,
    int32_t** ranks,
    int32_t** partitionStarts,
    int32_t** partitionedRows) {
  partitionRows<256>(
      [&](auto i) { return keys[blockIdx.x][i]; },
      numKeys[blockIdx.x],
      numPartitions,
      ranks[blockIdx.x],
      partitionStarts[blockIdx.x],
      partitionedRows[blockIdx.x]);
  __syncthreads();
}

void BlockTestStream::partitionShorts(
    int32_t numBlocks,
    uint16_t** keys,
    int32_t* numKeys,
    int32_t numPartitions,
    int32_t** ranks,
    int32_t** partitionStarts,
    int32_t** partitionedRows) {
  constexpr int32_t kBlockSize = 256;
  auto shared = partitionRowsSharedSize<kBlockSize>(numPartitions);
  partitionShortsKernel<<<numBlocks, kBlockSize, shared, stream_->stream>>>(
      keys, numKeys, numPartitions, ranks, partitionStarts, partitionedRows);
  CUDA_CHECK(cudaGetLastError());
}

/// A mock complex accumulator update function.
ProbeState __device__ arrayAgg64Append(
    ArrayAgg64* accumulator,
    int64_t arg,
    RowAllocator* allocator) {
  auto* last = accumulator->last;
  if (!last || accumulator->numInLast >= sizeof(last->data) / sizeof(int64_t)) {
    auto* next = allocator->allocate<ArrayAgg64::Run>(1);
    if (!next) {
      return ProbeState::kNeedSpace;
    }
    next->next = nullptr;
    if (accumulator->last) {
      accumulator->last->next = next;
      accumulator->last = next;
    } else {
      accumulator->first = accumulator->last = next;
    }
  }
  accumulator->last->data[accumulator->numInLast++] = arg;
  return ProbeState::kDone;
}

/// An mock Ops parameter class to do group by.
class MockGroupByOps {
 public:
  int32_t __device__ blockBase(HashProbe* probe) {
    return probe->numRowsPerThread * blockDim.x * blockIdx.x;
  }

  int32_t __device__ numRowsInBlock(HashProbe* probe) {
    return probe->numRows[blockIdx.x];
  }

  uint64_t __device__ hash(int32_t i, HashProbe* probe) {
    auto key = reinterpret_cast<int64_t**>(probe->keys)[0];
    return hashMix(1, key[i]);
  }

  bool __device__
  compare(GpuHashTable* table, TestingRow* row, int32_t i, HashProbe* probe) {
    return row->key == reinterpret_cast<int64_t**>(probe->keys)[0][i];
  }

  TestingRow* __device__
  newRow(GpuHashTable* table, int32_t partition, int32_t i, HashProbe* probe) {
    auto* allocator = &table->allocators[partition];
    auto row = allocator->allocateRow<TestingRow>();
    if (row) {
      row->key = reinterpret_cast<int64_t**>(probe->keys)[0][i];
      row->flags = 0;
      row->count = 0;
      new (&row->concatenation) ArrayAgg64();
    }
    return row;
  }

  ProbeState __device__ insert(
      GpuHashTable* table,
      int32_t partition,
      GpuBucket* bucket,
      uint32_t misses,
      uint32_t oldTags,
      uint32_t tagWord,
      int32_t i,
      HashProbe* probe,
      TestingRow*& row) {
    if (!row) {
      row = newRow(table, partition, i, probe);
      if (!row) {
        return ProbeState::kNeedSpace;
      }
    }
    auto missShift = __ffs(misses) - 1;
    if (!bucket->addNewTag(tagWord, oldTags, missShift)) {
      return ProbeState::kRetry;
    }
    bucket->store(missShift / 8, row);
    return ProbeState::kDone;
  }

  TestingRow* __device__ getExclusive(
      GpuHashTable* table,
      GpuBucket* bucket,
      TestingRow* row,
      int32_t hitIdx,
      int32_t warp) {
    return row;
    int32_t nanos = 1;
    for (;;) {
      if (atomicTryLock(&row->flags)) {
        return row;
      }
      __nanosleep((nanos + threadIdx.x) & 31);
      nanos += 3;
    }
  }

  void __device__ writeDone(TestingRow* row) {
    // atomicUnlock(&row->flags);
  }

  ProbeState __device__ update(
      GpuHashTable* table,
      GpuBucket* bucket,
      TestingRow* row,
      int32_t i,
      HashProbe* probe) {
    auto* keys = reinterpret_cast<int64_t**>(probe->keys);
    atomicAdd((unsigned long long*)&row->count, (unsigned long long)keys[1][i]);
    return ProbeState::kDone;
    int64_t arg = keys[1][i];
    int32_t part = table->partitionIdx(bucket - table->buckets);
    auto* allocator = &table->allocators[part];
    auto state = arrayAgg64Append(&row->concatenation, arg, allocator);
    row->flags = 0;
    __threadfence();
    return state;
  }
};

void __global__ __launch_bounds__(1024) hashTestKernel(
    GpuHashTable* table,
    HashProbe* probe,
    BlockTestStream::HashCase mode) {
  switch (mode) {
    case BlockTestStream::HashCase::kGroup: {
      table->updatingProbe<TestingRow>(probe, MockGroupByOps());
      break;
    }
    case BlockTestStream::HashCase::kBuild:
    case BlockTestStream::HashCase::kProbe:
      *(long*)0 = 0; // Unimplemented.
  }
  __syncthreads();
}

void BlockTestStream::hashTest(
    GpuHashTableBase* table,
    HashRun& run,
    HashCase mode) {
  int32_t shared = 0;
  if (mode == HashCase::kGroup) {
    shared = GpuHashTable::updatingProbeSharedSize();
  }
  hashTestKernel<<<run.numBlocks, run.blockSize, shared, stream_->stream>>>(
      reinterpret_cast<GpuHashTable*>(table), run.probe, mode);
  CUDA_CHECK(cudaGetLastError());
}

void __global__ allocatorTestKernel(
    int32_t numAlloc,
    int32_t numFree,
    int32_t numStr,
    AllocatorTestResult* allResults) {
  auto* result = allResults + threadIdx.x + blockIdx.x * blockDim.x;
  for (;;) {
    int32_t maxRows = sizeof(result->rows) / sizeof(result->rows[0]);
    int32_t maxStrings = sizeof(result->strings) / sizeof(result->strings[0]);
    for (auto count = 0; count < numAlloc; ++count) {
      if (result->numRows >= maxRows) {
        return;
      }
      auto newRow = result->allocator->allocateRow<int64_t>();
      if (newRow == nullptr) {
        return;
      }
      if (reinterpret_cast<uint64_t>(newRow) == result->allocator->base) {
        printf("");
      }

      result->rows[result->numRows++] = newRow;
    }
    for (auto count = 0; count < numFree; ++count) {
      if (result->numRows == 0) {
        return;
      }
      auto* toFree = result->rows[--result->numRows];
      if (reinterpret_cast<uint64_t>(toFree) == result->allocator->base) {
        printf(""); // GPF();
      }
      if (!result->allocator->inRange(toFree)) {
        GPF();
      }
      result->allocator->freeRow(toFree);
    }
    for (auto count = 0; count < numStr; ++count) {
      if (result->numStrings >= maxStrings) {
        return;
      }
      auto str = result->allocator->allocate<char>(11);
      if (!str) {
        return;
      }
      result->strings[result->numStrings++] = reinterpret_cast<int64_t*>(str);
    }
  }
  __syncthreads();
}

void __global__ initAllocatorKernel(RowAllocator* allocator) {
  if (threadIdx.x == 0) {
    if (allocator->freeSet) {
      reinterpret_cast<FreeSet<uint32_t, 1024>*>(allocator->freeSet)->clear();
    }
  }
  __syncthreads();
}

//  static
int32_t BlockTestStream::freeSetSize() {
  return sizeof(FreeSet<uint32_t, 1024>);
}

void BlockTestStream::initAllocator(HashPartitionAllocator* allocator) {
  initAllocatorKernel<<<1, 1, 0, stream_->stream>>>(
      reinterpret_cast<RowAllocator*>(allocator));
  CUDA_CHECK(cudaGetLastError());
}

void BlockTestStream::rowAllocatorTest(
    int32_t numBlocks,
    int32_t numAlloc,
    int32_t numFree,
    int32_t numStr,
    AllocatorTestResult* results) {
  allocatorTestKernel<<<numBlocks, 64, 0, stream_->stream>>>(
      numAlloc, numFree, numStr, results);
  CUDA_CHECK(cudaGetLastError());
}

#define UPDATE_CASE(name, func, smem)                                      \
  void __global__ name##Kernel(TestingRow* rows, HashProbe* probe) {       \
    func(rows, probe);                                                     \
    __syncthreads();                                                       \
  }                                                                        \
                                                                           \
  void BlockTestStream::name(TestingRow* rows, HashRun& run) {             \
    name##Kernel<<<run.numBlocks, run.blockSize, smem, stream_->stream>>>( \
        rows, run.probe);                                                  \
    CUDA_CHECK(cudaGetLastError());                                        \
  }

UPDATE_CASE(updateSum1NoSync, testSumNoSync, 0);
UPDATE_CASE(updateSum1Mtx, testSumMtx, 0);
UPDATE_CASE(updateSum1MtxCoalesce, testSumMtxCoalesce, 0);
UPDATE_CASE(updateSum1Atomic, testSumAtomic, 0);
UPDATE_CASE(updateSum1AtomicCoalesceShfl, testSumAtomicCoalesceShfl, 0);
UPDATE_CASE(
    updateSum1AtomicCoalesceShmem,
    testSumAtomicCoalesceShmem,
    run.blockSize * sizeof(int64_t));
UPDATE_CASE(updateSum1Exch, testSumExch, sizeof(ProbeShared));
UPDATE_CASE(updateSum1Order, testSumOrder, 0);

void __global__ __launch_bounds__(1024) update1PartitionKernel(
    int32_t numRows,
    int32_t numDistinct,
    int32_t numParts,
    int32_t blockStride,
    HashProbe* probe,
    int32_t* temp) {
  auto blockStart = blockStride * blockIdx.x;
  auto keys = reinterpret_cast<int64_t**>(probe->keys);
  auto indices = keys[0];
  partitionRows<256, int32_t>(
      [&](auto i) -> int32_t { return indices[i + blockStart] % numParts; },
      blockIdx.x == blockDim.x - 1 ? numRows - blockStart : blockStride,
      numParts,
      temp + blockIdx.x * blockStride,
      probe->hostRetries + blockStride * blockIdx.x,
      probe->kernelRetries1 + blockStride * blockIdx.x);
  __syncthreads();
}

void __global__ updateSum1PartKernel(
    TestingRow* rows,
    int32_t numParts,
    HashProbe* probe,
    int32_t numGroups,
    int32_t groupStride) {
  testSumPart(
      rows,
      numParts,
      probe,
      probe->kernelRetries1,
      probe->hostRetries,
      numGroups,
      groupStride);
  __syncthreads();
}

void BlockTestStream::updateSum1Part(TestingRow* rows, HashRun& run) {
  auto numParts = std::min<int32_t>(run.numDistinct, 8192);
  auto groupStride = run.numRows / 32;
  auto numGroups = run.numRows / groupStride;
  auto partSmem = partitionRowsSharedSize<256>(numParts);
  // We use probe->kernelRetries1 as the indices array for partitions. We use
  // probe->hostRetries as the array of partition starts. So, if we have 10
  // partitions, then hostRetries[x..y] is the input rows for partition 1 if x
  // is partitionStarts[0] and y is partitionStarts[1].
  update1PartitionKernel<<<numGroups, 256, partSmem, stream_->stream>>>(
      run.numRows,
      run.numDistinct,
      numParts,
      groupStride,
      run.probe,
      run.partitionTemp);
  CUDA_CHECK(cudaGetLastError());

  int32_t blockSize = roundUp(std::min<int32_t>(256, numParts), 32);
  int32_t numBlocks = numParts / blockSize;
  // There will be one lane per partition. The last blocks may have empty lanes.
  if (numBlocks * blockSize < numParts) {
    ++numBlocks;
  }
  updateSum1PartKernel<<<numBlocks, blockSize, 0, stream_->stream>>>(
      rows, numParts, run.probe, numGroups, groupStride);
  CUDA_CHECK(cudaGetLastError());
}

__global__ void scatterBitsKernel(
    int32_t numSource,
    int32_t numTarget,
    const char* source,
    const uint64_t* targetMask,
    char* target,
    int32_t* temp) {
  if (!temp) {
    extern __shared__ __align__(16) char smem[];
    temp = reinterpret_cast<int32_t*>(smem);
  }
  scatterBitsDevice<4>(numSource, numTarget, source, targetMask, target, temp);
  __syncthreads();
}

//    static
int32_t BlockTestStream::scatterBitsSize(int32_t blockSize) {
  return scatterBitsDeviceSize(blockSize);
}

void BlockTestStream::scatterBits(
    int32_t numSource,
    int32_t numTarget,
    const char* source,
    const uint64_t* targetMask,
    char* target,
    int32_t* temp) {
  scatterBitsKernel<<<
      1,
      256,
      temp ? 0 : scatterBitsDeviceSize(256),
      stream_->stream>>>(
      numSource, numTarget, source, targetMask, target, temp);
}

/// Struct for tracking state between calls to nonNullIndex256 and
/// nonNullIndex256Sparse.
struct NonNullIndexState {
  // Number of non-nulls below 'row'. the null flag at 'row' is not included in
  // the sum.
  int32_t numNonNullBelow;
  int32_t row;
  // Scratch memory with one int32 per warp of 256 wide TB.
  int32_t temp[256 / kWarpThreads];
};

void __global__ nonNullIndexKernel(
    char* nulls,
    int32_t* rows,
    int32_t numRows,
    int32_t* indices,
    int32_t* temp) {
  NonNullState* state = reinterpret_cast<NonNullState*>(temp);
  if (threadIdx.x == 0) {
    state->nonNullsBelow = 0;
    state->nonNullsBelowRow = 0;
  }
  __syncthreads();
  for (auto i = 0; i < numRows; i += blockDim.x) {
    auto last = min(i + 256, numRows);
    if (isDense(rows, i, last)) {
      indices[i + threadIdx.x] =
          nonNullIndex256(nulls, rows[i], last - i, state);
    } else {
      indices[i + threadIdx.x] =
          nonNullIndex256Sparse(nulls, rows + i, last - i, state);
    }
  }
  __syncthreads();
}

void BlockTestStream::nonNullIndex(
    char* nulls,
    int32_t* rows,
    int32_t numRows,
    int32_t* indices,
    int32_t* temp) {
  nonNullIndexKernel<<<1, 256, 0, stream_->stream>>>(
      nulls, rows, numRows, indices, temp);
}

REGISTER_KERNEL("testSort", testSort);
REGISTER_KERNEL("boolToIndices", boolToIndicesKernel);
REGISTER_KERNEL("bool256ToIndices", bool256ToIndicesKernel);
REGISTER_KERNEL("sum64", sum64);
REGISTER_KERNEL("partitionShorts", partitionShortsKernel);
REGISTER_KERNEL("hashTest", hashTestKernel);
REGISTER_KERNEL("allocatorTest", allocatorTestKernel);
REGISTER_KERNEL("sum1atm", updateSum1AtomicKernel);
REGISTER_KERNEL("sum1atmCoaShfl", updateSum1AtomicCoalesceShflKernel);
REGISTER_KERNEL("sum1atmCoaShmem", updateSum1AtomicCoalesceShmemKernel);
REGISTER_KERNEL("sum1Exch", updateSum1ExchKernel);
REGISTER_KERNEL("sum1Part", updateSum1PartKernel);
REGISTER_KERNEL("partSum", update1PartitionKernel);
REGISTER_KERNEL("scatterBits", scatterBitsKernel);

} // namespace facebook::velox::wave
