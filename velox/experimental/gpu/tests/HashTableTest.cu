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

#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <algorithm>
#include <cassert>
#include <cub/cub.cuh> // @manual
#include <random>

#include "velox/experimental/gpu/Common.h"

DEFINE_int32(device, 0, "");
DEFINE_int64(table_size, 8 << 20, "For large table, use 268435456");
DEFINE_bool(use_tags, false, "");
DEFINE_bool(partitioned, false, "");
DEFINE_double(occupancy, 0.5, "");
DEFINE_double(matching_rate, 0.5, "");

namespace facebook::velox::gpu {
namespace {

constexpr int kBlockSize = 256;

[[maybe_unused]] __device__ uint32_t jenkinsRevMix32(uint32_t key) {
  key += (key << 12); // key *= (1 + (1 << 12))
  key ^= (key >> 22);
  key += (key << 4); // key *= (1 + (1 << 4))
  key ^= (key >> 9);
  key += (key << 10); // key *= (1 + (1 << 10))
  key ^= (key >> 2);
  // key *= (1 + (1 << 7)) * (1 + (1 << 12))
  key += (key << 7);
  key += (key << 12);
  return key;
}

__device__ uint64_t twangMix64(uint64_t key) {
  key = (~key) + (key << 21); // key *= (1 << 21) - 1; key -= 1;
  key = key ^ (key >> 24);
  key = key + (key << 3) + (key << 8); // key *= 1 + (1 << 3) + (1 << 8)
  key = key ^ (key >> 14);
  key = key + (key << 2) + (key << 4); // key *= 1 + (1 << 2) + (1 << 4)
  key = key ^ (key >> 28);
  key = key + (key << 31); // key *= 1 + (1 << 31)
  return key;
}

// Must be avalanching to ensure tag performance.
template <typename T>
__device__ uint64_t hashInt(T value) {
  return twangMix64(value);
}

__device__ uint8_t hashTag(uint64_t hash) {
  return static_cast<uint8_t>(hash >> 32) | 0x80;
}

__device__ uint8_t
atomicCASByte(uint8_t* address, uint8_t compare, uint8_t val) {
  using T = uint8_t;
  using T_int = unsigned int;

  T_int shift = ((reinterpret_cast<size_t>(address) & 3) * 8);
  T_int* address_uint32 = reinterpret_cast<T_int*>(
      address - (reinterpret_cast<size_t>(address) & 3));

  // the 'target_value' in `old` can be different from `compare`
  // because other thread may update the value
  // before fetching a value from `address_uint32` in this function
  T_int old = *address_uint32;
  T_int assumed;
  T target_value;

  do {
    assumed = old;
    target_value = T((old >> shift) & 0xff);
    // have to compare `target_value` and `compare` before calling atomicCAS
    // the `target_value` in `old` can be different with `compare`
    if (target_value != compare) {
      break;
    }

    T_int new_value = (old & ~(0x000000ff << shift)) | (T_int(val) << shift);
    old = atomicCAS(address_uint32, assumed, new_value);
  } while (assumed != old);

  return target_value;
}

CudaPtr<uint64_t[]> generateBuildKeys(
    std::default_random_engine& gen,
    std::vector<uint64_t>& keys) {
  std::iota(keys.begin(), keys.end(), 0);
  std::shuffle(keys.begin(), keys.end(), gen);
  auto keysD = allocateDeviceMemory<uint64_t>(keys.size());
  CUDA_CHECK_FATAL(cudaMemcpy(
      keysD.get(),
      keys.data(),
      keys.size() * sizeof(uint64_t),
      cudaMemcpyHostToDevice));
  return keysD;
}

void generateProbeKeys(
    std::default_random_engine& gen,
    std::vector<uint64_t>& keysHost,
    uint64_t* keysDevice) {
  std::shuffle(keysHost.begin(), keysHost.end(), gen);
  std::uniform_int_distribution<uint64_t> dist(keysHost.size());
  for (auto i = keysHost.size() * FLAGS_matching_rate; i < keysHost.size();
       ++i) {
    keysHost[i] = dist(gen);
  }
  std::shuffle(keysHost.begin(), keysHost.end(), gen);
  CUDA_CHECK_FATAL(cudaMemcpy(
      keysDevice,
      keysHost.data(),
      keysHost.size() * sizeof(uint64_t),
      cudaMemcpyHostToDevice));
}

struct HashTable {
  int64_t size;
  uint64_t* keys;
  uint64_t* values;
  uint64_t emptyMarker;
  int hasEmptyValue;
  uint64_t emptyValue;
  uint8_t* tags;
};

template <bool kUseTags>
__global__ void init(HashTable* table) {
  int64_t step = gridDim.x * blockDim.x;
  for (int64_t i = threadIdx.x + blockIdx.x * blockDim.x; i < table->size;
       i += step) {
    table->keys[i] = table->emptyMarker;
    if constexpr (kUseTags) {
      table->tags[i] = 0;
    }
  }
}

template <bool kUseTags>
__global__ void build(
    HashTable* table,
    const uint64_t* keys,
    const uint64_t* values,
    int64_t size);

template <>
__global__ void build<false>(
    HashTable* table,
    const uint64_t* keys,
    const uint64_t* values,
    int64_t size) {
  uint64_t tableSizeMask = table->size - 1;
  int64_t step = gridDim.x * blockDim.x;
  for (int64_t i = threadIdx.x + blockIdx.x * blockDim.x; i < size; i += step) {
    if (__builtin_expect(keys[i] == table->emptyMarker, false)) {
      if (atomicCAS(&table->hasEmptyValue, 0, 1) == 0) {
        table->emptyValue = values[i];
      } else {
        printf("ERROR: Duplicate key %llu\n", keys[i]);
      }
      continue;
    }
    auto hash = hashInt(keys[i]);
    for (auto j = hash & tableSizeMask;; j = (j + 1) & tableSizeMask) {
      if (table->keys[j] == table->emptyMarker &&
          atomicCAS(
              (unsigned long long*)&table->keys[j],
              table->emptyMarker,
              keys[i]) == table->emptyMarker) {
        table->values[j] = values[i];
        break;
      }
      if (table->keys[j] == keys[i]) {
        printf("ERROR: Duplicate key %llu\n", keys[i]);
        break;
      }
    }
  }
}

template <>
__global__ void build<true>(
    HashTable* table,
    const uint64_t* keys,
    const uint64_t* values,
    int64_t size) {
  uint64_t tableSizeMask = table->size - 1;
  int64_t step = gridDim.x * blockDim.x;
  for (int64_t i = threadIdx.x + blockIdx.x * blockDim.x; i < size; i += step) {
    auto hash = hashInt(keys[i]);
    auto tag = hashTag(hash);
    for (auto j = hash & tableSizeMask;; j = (j + 1) & tableSizeMask) {
      if (table->tags[j] == 0 && atomicCASByte(&table->tags[j], 0, tag) == 0) {
        table->keys[j] = keys[i];
        table->values[j] = values[i];
        break;
      }
      if (table->keys[j] == keys[i]) {
        printf("ERROR: Duplicate key %llu\n", keys[i]);
        break;
      }
    }
  }
}

template <bool kUseTags>
__global__ void probe(
    const HashTable* table,
    const uint64_t* keys,
    uint64_t* values,
    bool* hasValue,
    int64_t size);

template <>
__global__ void probe<false>(
    const HashTable* table,
    const uint64_t* keys,
    uint64_t* values,
    bool* hasValue,
    int64_t size) {
  uint64_t tableSizeMask = table->size - 1;
  int64_t step = gridDim.x * blockDim.x;
  for (int64_t i = threadIdx.x + blockIdx.x * blockDim.x; i < size; i += step) {
    if (__builtin_expect(keys[i] == table->emptyMarker, false)) {
      hasValue[i] = table->hasEmptyValue;
      if (table->hasEmptyValue) {
        values[i] = table->emptyValue;
      }
      continue;
    }
    auto hash = hashInt(keys[i]);
    for (auto j = hash & tableSizeMask;; j = (j + 1) & tableSizeMask) {
      if (table->keys[j] == keys[i]) {
        hasValue[i] = true;
        values[i] = table->values[j];
        break;
      }
      if (table->keys[j] == table->emptyMarker) {
        hasValue[i] = false;
        break;
      }
    }
  }
}

template <>
__global__ void probe<true>(
    const HashTable* table,
    const uint64_t* keys,
    uint64_t* values,
    bool* hasValue,
    int64_t size) {
  uint64_t tableSizeMask = table->size - 1;
  int64_t step = gridDim.x * blockDim.x;
  for (int64_t i = threadIdx.x + blockIdx.x * blockDim.x; i < size; i += step) {
    auto hash = hashInt(keys[i]);
    uint32_t tag = hashTag(hash);
    tag = tag | (tag << 8);
    tag = tag | (tag << 16);
    hash &= tableSizeMask;
    auto rem = hash % sizeof(uint32_t);
    int64_t j = hash - rem;
    uint32_t cmpMask = 0xffffffff << (rem * 8);
    for (;;) {
      auto hits = __vcmpeq4(*(uint32_t*)&table->tags[j], tag) & cmpMask;
      while (hits) {
        auto jj = j + (__ffs(hits) - 1) / 8;
        if (table->keys[jj] == keys[i]) {
          hasValue[i] = true;
          values[i] = table->values[jj];
          goto end;
        }
        hits &= hits - 1;
      }
      if (__vcmpeq4(*(uint32_t*)&table->tags[j], 0) & cmpMask) {
        hasValue[i] = false;
        goto end;
      }
      j = (j + sizeof(uint32_t)) & tableSizeMask;
      cmpMask = 0xffffffff;
    }
  end:;
  }
}

__global__ void validate(
    const uint64_t* keys,
    const uint64_t* result,
    const bool* hasResult,
    int64_t size) {
  int64_t step = gridDim.x * blockDim.x;
  for (int64_t i = threadIdx.x + blockIdx.x * blockDim.x; i < size; i += step) {
    if (keys[i] < (uint64_t)size) {
      if (!hasResult[i]) {
        printf("ERROR: Result missing %llu\n", keys[i]);
      } else if (result[i] != keys[i]) {
        printf("ERROR: Result mismatch %llu != %llu\n", result[i], keys[i]);
      }
    } else if (hasResult[i]) {
      printf("ERROR: Unexpected result %llu\n", keys[i]);
    }
  }
}

template <bool kUseTags>
void run() {
  auto tableKeys = allocateDeviceMemory<uint64_t>(FLAGS_table_size);
  auto tableValues = allocateDeviceMemory<uint64_t>(FLAGS_table_size);
  CudaPtr<uint8_t[]> tags;
  auto table = allocateManagedMemory<HashTable>();
  table->size = FLAGS_table_size;
  table->keys = tableKeys.get();
  table->values = tableValues.get();
  table->emptyMarker = 0xdeadbeefbadefeedULL;
  table->hasEmptyValue = 0;
  if constexpr (kUseTags) {
    tags = allocateDeviceMemory<uint8_t>(FLAGS_table_size);
    table->tags = tags.get();
  }

  float time;
  auto startEvent = createCudaEvent();
  auto stopEvent = createCudaEvent();
  int64_t numKeys = FLAGS_table_size * FLAGS_occupancy;
  auto numBlocks = FLAGS_table_size / kBlockSize;
  std::default_random_engine gen(std::random_device{}());

  std::vector<uint64_t> keysHost(numKeys);
  auto keys = generateBuildKeys(gen, keysHost);
  init<kUseTags><<<numBlocks, kBlockSize>>>(table.get());

  CUDA_CHECK_FATAL(cudaEventRecord(startEvent.get()));
  build<kUseTags>
      <<<numBlocks, kBlockSize>>>(table.get(), keys.get(), keys.get(), numKeys);
  CUDA_CHECK_FATAL(cudaEventRecord(stopEvent.get()));
  CUDA_CHECK_FATAL(cudaEventSynchronize(stopEvent.get()));
  CUDA_CHECK_FATAL(
      cudaEventElapsedTime(&time, startEvent.get(), stopEvent.get()));
  printf("Hash build: %.2f billion rows/s\n", numKeys * 1e-6 / time);

  generateProbeKeys(gen, keysHost, keys.get());
  auto result = allocateDeviceMemory<uint64_t>(numKeys);
  auto hasResult = allocateDeviceMemory<bool>(numKeys);

  CUDA_CHECK_FATAL(cudaEventRecord(startEvent.get()));
  probe<kUseTags><<<numBlocks, kBlockSize>>>(
      table.get(), keys.get(), result.get(), hasResult.get(), numKeys);
  CUDA_CHECK_FATAL(cudaEventRecord(stopEvent.get()));
  CUDA_CHECK_FATAL(cudaEventSynchronize(stopEvent.get()));
  CUDA_CHECK_FATAL(
      cudaEventElapsedTime(&time, startEvent.get(), stopEvent.get()));
  printf("Hash probe: %.2f billion rows/s\n", numKeys * 1e-6 / time);

  validate<<<numBlocks, kBlockSize>>>(
      keys.get(), result.get(), hasResult.get(), numKeys);
  CUDA_CHECK_FATAL(cudaGetLastError());
  CUDA_CHECK_FATAL(cudaEventRecord(stopEvent.get()));
  CUDA_CHECK_FATAL(cudaEventSynchronize(stopEvent.get()));
}

__global__ void computeHistogram(
    const uint64_t* keys,
    int64_t size,
    uint64_t mask,
    int64_t* hist) {
  int64_t step = gridDim.x * blockDim.x;
  for (int64_t i = threadIdx.x + blockIdx.x * blockDim.x; i < size; i += step) {
    atomicAdd((unsigned long long*)&hist[hashInt(keys[i]) & mask], 1);
  }
}

__global__ void shuffle(
    const uint64_t* keys,
    int64_t size,
    uint64_t mask,
    int64_t* offsets,
    uint64_t* out) {
  int64_t step = gridDim.x * blockDim.x;
  for (int64_t i = threadIdx.x + blockIdx.x * blockDim.x; i < size; i += step) {
    auto k = keys[i];
    auto j = atomicAdd((unsigned long long*)&offsets[hashInt(k) & mask], 1);
    __stwt(&out[j], k);
  }
}

void partitionKeys(
    const uint64_t* keys,
    int64_t size,
    int64_t numPartitions,
    int64_t* hist,
    int64_t* offsets,
    uint64_t* out,
    char* tmp,
    size_t tmpSize) {
  CUDA_CHECK_FATAL(cudaMemset(hist, 0, numPartitions * sizeof(int64_t)));
  computeHistogram<<<(size + kBlockSize - 1) / kBlockSize, kBlockSize>>>(
      keys, size, numPartitions - 1, hist);
  CUDA_CHECK_FATAL(cudaMemset(offsets, 0, sizeof(int64_t)));
  CUDA_CHECK_FATAL(cub::DeviceScan::InclusiveSum(
      tmp, tmpSize, hist, offsets + 1, numPartitions));
  CUDA_CHECK_FATAL(cudaMemcpy(
      hist,
      offsets,
      numPartitions * sizeof(int64_t),
      cudaMemcpyDeviceToDevice));
  shuffle<<<(size + kBlockSize - 1) / kBlockSize, kBlockSize>>>(
      keys, size, numPartitions - 1, hist, out);
}

// One block per partition.
__global__ void validatePartition(
    int maxPartitionSize,
    int64_t numKeys,
    const uint64_t* keys,
    const int64_t* offsets) {
  if (threadIdx.x == 0) {
    if (offsets[blockIdx.x] >= offsets[blockIdx.x + 1]) {
      printf("ERROR: Bad offsets\n");
      return;
    }
    if (offsets[blockIdx.x + 1] - offsets[blockIdx.x] > maxPartitionSize) {
      printf("ERROR: Partition overflow\n");
      return;
    }
    if (blockIdx.x == gridDim.x - 1 && offsets[gridDim.x] != numKeys) {
      printf("ERROR: Wrong total size\n");
      return;
    }
  }
  uint64_t mask = gridDim.x - 1;
  for (auto i = threadIdx.x + offsets[blockIdx.x]; i < offsets[blockIdx.x + 1];
       i += blockDim.x) {
    if ((hashInt(keys[i]) & mask) != blockIdx.x) {
      printf("ERROR: Key %llu in wrong partition %d\n", keys[i], blockIdx.x);
      return;
    }
  }
}

// One block per partition.
template <bool kUseTags>
__global__ void buildPartitioned(
    HashTable* table,
    int partitionSize,
    int shift,
    const uint64_t* keys,
    const uint64_t* values,
    const int64_t* offsets);

template <>
__global__ void buildPartitioned<false>(
    HashTable* table,
    int partitionSize,
    int shift,
    const uint64_t* keys,
    const uint64_t* values,
    const int64_t* offsets) {
  uint64_t tableSizeMask = partitionSize - 1;
  for (auto i = threadIdx.x + offsets[blockIdx.x]; i < offsets[blockIdx.x + 1];
       i += blockDim.x) {
    if (__builtin_expect(keys[i] == table->emptyMarker, false)) {
      if (atomicCAS_block(&table->hasEmptyValue, 0, 1) == 0) {
        table->emptyValue = values[i];
      } else {
        printf("ERROR: Duplicate key %llu\n", keys[i]);
      }
      continue;
    }
    auto hash = hashInt(keys[i]) >> shift;
    for (auto j = hash & tableSizeMask;; j = (j + 1) & tableSizeMask) {
      auto jj = j + partitionSize * blockIdx.x;
      if (table->keys[jj] == table->emptyMarker &&
          atomicCAS_block(
              (unsigned long long*)&table->keys[jj],
              table->emptyMarker,
              keys[i]) == table->emptyMarker) {
        table->values[jj] = values[i];
        break;
      }
      if (table->keys[jj] == keys[i]) {
        printf("ERROR: Duplicate key %llu\n", keys[i]);
        break;
      }
    }
  }
}

template <>
__global__ void buildPartitioned<true>(
    HashTable* table,
    int partitionSize,
    int shift,
    const uint64_t* keys,
    const uint64_t* values,
    const int64_t* offsets) {
  uint64_t tableSizeMask = partitionSize - 1;
  for (auto i = threadIdx.x + offsets[blockIdx.x]; i < offsets[blockIdx.x + 1];
       i += blockDim.x) {
    auto hash = hashInt(keys[i]) >> shift;
    auto tag = hashTag(hash);
    for (auto j = hash & tableSizeMask;; j = (j + 1) & tableSizeMask) {
      auto jj = j + partitionSize * blockIdx.x;
      if (table->tags[jj] == 0 &&
          atomicCASByte(&table->tags[jj], 0, tag) == 0) {
        table->keys[jj] = keys[i];
        table->values[jj] = values[i];
        break;
      }
      if (table->keys[jj] == keys[i]) {
        printf("ERROR: Duplicate key %llu\n", keys[i]);
        break;
      }
    }
  }
}

// One block per partition.
template <bool kUseTags>
__global__ void probePartitioned(
    const HashTable* table,
    int partitionSize,
    int shift,
    const uint64_t* keys,
    const int64_t* offsets,
    uint64_t* values,
    bool* hasValue);

template <>
__global__ void probePartitioned<false>(
    const HashTable* table,
    int partitionSize,
    int shift,
    const uint64_t* keys,
    const int64_t* offsets,
    uint64_t* values,
    bool* hasValue) {
  extern __shared__ uint64_t tableKeys[];
  for (auto i = threadIdx.x; i < partitionSize; i += blockDim.x) {
    tableKeys[i] = table->keys[i + partitionSize * blockIdx.x];
  }
  __syncthreads();
  uint64_t tableSizeMask = partitionSize - 1;
  for (auto i = threadIdx.x + offsets[blockIdx.x]; i < offsets[blockIdx.x + 1];
       i += blockDim.x) {
    if (__builtin_expect(keys[i] == table->emptyMarker, false)) {
      hasValue[i] = table->hasEmptyValue;
      if (table->hasEmptyValue) {
        values[i] = table->emptyValue;
      }
      continue;
    }
    auto hash = hashInt(keys[i]) >> shift;
    for (auto j = hash & tableSizeMask;; j = (j + 1) & tableSizeMask) {
      auto jj = j + partitionSize * blockIdx.x;
      if (tableKeys[j] == keys[i]) {
        hasValue[i] = true;
        values[i] = table->values[jj];
        break;
      }
      if (tableKeys[j] == table->emptyMarker) {
        hasValue[i] = false;
        break;
      }
    }
  }
}

template <>
__global__ void probePartitioned<true>(
    const HashTable* table,
    int partitionSize,
    int shift,
    const uint64_t* keys,
    const int64_t* offsets,
    uint64_t* values,
    bool* hasValue) {
  extern __shared__ uint8_t tableTags[];
  for (auto i = threadIdx.x; i < partitionSize; i += blockDim.x) {
    tableTags[i] = table->tags[i + partitionSize * blockIdx.x];
  }
  __syncthreads();
  uint64_t tableSizeMask = partitionSize - 1;
  for (auto i = threadIdx.x + offsets[blockIdx.x]; i < offsets[blockIdx.x + 1];
       i += blockDim.x) {
    auto hash = hashInt(keys[i]) >> shift;
    uint32_t tag = hashTag(hash);
    tag = tag | (tag << 8);
    tag = tag | (tag << 16);
    hash &= tableSizeMask;
    auto rem = hash % sizeof(uint32_t);
    int64_t j = hash - rem;
    uint32_t cmpMask = 0xffffffff << (rem * 8);
    for (;;) {
      auto hits = __vcmpeq4(*(uint32_t*)&tableTags[j], tag) & cmpMask;
      while (hits) {
        auto jj = j + (__ffs(hits) - 1) / 8 + partitionSize * blockIdx.x;
        if (table->keys[jj] == keys[i]) {
          hasValue[i] = true;
          values[i] = table->values[jj];
          goto end;
        }
        hits &= hits - 1;
      }
      if (__vcmpeq4(*(uint32_t*)&tableTags[j], 0) & cmpMask) {
        hasValue[i] = false;
        goto end;
      }
      j = (j + sizeof(uint32_t)) & tableSizeMask;
      cmpMask = 0xffffffff;
    }
  end:;
  }
}

template <bool kUseTags>
void runPartitioned() {
  constexpr int kSharedMemorySize = 1 << (kUseTags ? 12 : 16);
  constexpr int kPartitionSize =
      kSharedMemorySize / (kUseTags ? 1 : sizeof(uint64_t));

  float time;
  auto startEvent = createCudaEvent();
  auto stopEvent = createCudaEvent();
  int64_t numKeys = FLAGS_table_size * FLAGS_occupancy;
  int64_t numPartitions = FLAGS_table_size / kPartitionSize;

  auto tableKeys = allocateDeviceMemory<uint64_t>(FLAGS_table_size);
  auto tableValues = allocateDeviceMemory<uint64_t>(FLAGS_table_size);
  auto table = allocateManagedMemory<HashTable>();
  table->size = FLAGS_table_size;
  table->keys = tableKeys.get();
  table->values = tableValues.get();
  table->emptyMarker = 0xdeadbeefbadefeedULL;
  table->hasEmptyValue = 0;
  CudaPtr<uint8_t[]> tags;
  if constexpr (kUseTags) {
    tags = allocateDeviceMemory<uint8_t>(FLAGS_table_size);
    table->tags = tags.get();
  }
  init<kUseTags><<<FLAGS_table_size / kBlockSize, kBlockSize>>>(table.get());

  std::default_random_engine gen(std::random_device{}());
  std::vector<uint64_t> keysHost(numKeys);
  auto keys = generateBuildKeys(gen, keysHost);

  auto hist = allocateDeviceMemory<int64_t>(numPartitions);
  auto offsets = allocateDeviceMemory<int64_t>(numPartitions + 1);
  size_t tmpSize;
  CUDA_CHECK_FATAL(cub::DeviceScan::InclusiveSum(
      nullptr, tmpSize, hist.get(), offsets.get(), numPartitions));
  auto tmp = allocateDeviceMemory<char>(tmpSize);
  auto shuffledKeys = allocateDeviceMemory<uint64_t>(numKeys);

  CUDA_CHECK_FATAL(cudaEventRecord(startEvent.get()));
  partitionKeys(
      keys.get(),
      numKeys,
      numPartitions,
      hist.get(),
      offsets.get(),
      shuffledKeys.get(),
      tmp.get(),
      tmpSize);
  buildPartitioned<kUseTags><<<numPartitions, 1024>>>(
      table.get(),
      kPartitionSize,
      __builtin_ctz(numPartitions),
      shuffledKeys.get(),
      shuffledKeys.get(),
      offsets.get());
  CUDA_CHECK_FATAL(cudaEventRecord(stopEvent.get()));
  CUDA_CHECK_FATAL(cudaEventSynchronize(stopEvent.get()));
  CUDA_CHECK_FATAL(
      cudaEventElapsedTime(&time, startEvent.get(), stopEvent.get()));
  printf("Hash build: %.2f billion rows/s\n", numKeys * 1e-6 / time);

  validatePartition<<<numPartitions, 1024>>>(
      kPartitionSize, numKeys, shuffledKeys.get(), offsets.get());
  CUDA_CHECK_FATAL(cudaGetLastError());
  generateProbeKeys(gen, keysHost, keys.get());
  auto result = allocateDeviceMemory<uint64_t>(numKeys);
  auto hasResult = allocateDeviceMemory<bool>(numKeys);

  CUDA_CHECK_FATAL(cudaEventRecord(startEvent.get()));
  partitionKeys(
      keys.get(),
      numKeys,
      numPartitions,
      hist.get(),
      offsets.get(),
      shuffledKeys.get(),
      tmp.get(),
      tmpSize);
  CUDA_CHECK_FATAL(
      cudaDeviceSetSharedMemConfig(cudaSharedMemBankSizeEightByte));
  CUDA_CHECK_FATAL(cudaFuncSetAttribute(
      probePartitioned<kUseTags>,
      cudaFuncAttributeMaxDynamicSharedMemorySize,
      kSharedMemorySize));
  probePartitioned<kUseTags><<<numPartitions, 1024, kSharedMemorySize>>>(
      table.get(),
      kPartitionSize,
      __builtin_ctz(numPartitions),
      shuffledKeys.get(),
      offsets.get(),
      result.get(),
      hasResult.get());
  CUDA_CHECK_FATAL(cudaEventRecord(stopEvent.get()));
  CUDA_CHECK_FATAL(cudaEventSynchronize(stopEvent.get()));
  CUDA_CHECK_FATAL(
      cudaEventElapsedTime(&time, startEvent.get(), stopEvent.get()));
  printf("Hash probe: %.2f billion rows/s\n", numKeys * 1e-6 / time);

  validatePartition<<<numPartitions, 1024>>>(
      numKeys, numKeys, shuffledKeys.get(), offsets.get());
  CUDA_CHECK_FATAL(cudaGetLastError());
  validate<<<(numKeys + kBlockSize - 1) / kBlockSize, kBlockSize>>>(
      shuffledKeys.get(), result.get(), hasResult.get(), numKeys);
  CUDA_CHECK_FATAL(cudaGetLastError());
  CUDA_CHECK_FATAL(cudaEventRecord(stopEvent.get()));
  CUDA_CHECK_FATAL(cudaEventSynchronize(stopEvent.get()));
}

} // namespace
} // namespace facebook::velox::gpu

int main(int argc, char** argv) {
  using namespace facebook::velox::gpu;
  folly::Init init{&argc, &argv};
  assert(__builtin_popcount(FLAGS_table_size) == 1);
  assert(FLAGS_table_size % kBlockSize == 0);
  CUDA_CHECK_FATAL(cudaSetDevice(FLAGS_device));
  cudaDeviceProp prop;
  CUDA_CHECK_FATAL(cudaGetDeviceProperties(&prop, FLAGS_device));
  printf("Device : %s\n", prop.name);

  if (FLAGS_partitioned) {
    if (FLAGS_use_tags) {
      runPartitioned<true>();
    } else {
      runPartitioned<false>();
    }
  } else {
    if (FLAGS_use_tags) {
      run<true>();
    } else {
      run<false>();
    }
  }
  return 0;
}
