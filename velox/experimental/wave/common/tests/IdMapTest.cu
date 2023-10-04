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
#include <gtest/gtest.h>
#include <random>

#include "velox/experimental/wave/common/Cuda.h"
#include "velox/experimental/wave/common/IdMap.cuh"

DEFINE_bool(benchmark, false, "");

namespace facebook::velox::wave {
namespace {

constexpr int kBlockSize = 256;

std::random_device::result_type randomSeed() {
  auto seed = std::random_device{}();
  LOG(INFO) << "Random seed: " << seed;
  return seed;
}

template <typename T>
struct IdMapHolder {
  GpuAllocator::UniquePtr<T[]> values;
  GpuAllocator::UniquePtr<int32_t[]> ids;
  GpuAllocator::UniquePtr<IdMap<T>> idMap;
};

template <typename T>
__global__ void initTable(IdMap<T>* idMap) {
  idMap->clearTable();
}

template <typename T>
IdMapHolder<T> createIdMap(GpuAllocator* allocator, int capacity) {
  IdMapHolder<T> holder;
  holder.idMap = allocator->allocate<IdMap<T>>();
  holder.values = allocator->allocate<T>(capacity);
  holder.ids = allocator->allocate<int32_t>(capacity);
  holder.idMap->init(capacity, holder.values.get(), holder.ids.get());
  initTable<<<1, kBlockSize>>>(holder.idMap.get());
  EXPECT_EQ(cudaGetLastError(), cudaSuccess);
  return holder;
}

template <typename T>
__global__ void
runMakeIds(IdMap<T>* idMap, const T* values, int size, int32_t* output) {
  int step = gridDim.x * blockDim.x;
  for (int i = threadIdx.x + blockIdx.x * blockDim.x; i < size; i += step) {
    output[i] = idMap->makeId(values[i]);
  }
}

template <typename T>
void makeIds(IdMap<T>* idMap, const T* values, int size, int32_t* output) {
  int numBlocks = (size + kBlockSize - 1) / kBlockSize;
  Stream stream;
  stream.prefetch(getDevice(), const_cast<T*>(values), size * sizeof(T));
  stream.prefetch(getDevice(), output, size * sizeof(int32_t));
  Event start(true), stop(true);
  start.record(stream);
  runMakeIds<<<numBlocks, kBlockSize>>>(idMap, values, size, output);
  ASSERT_EQ(cudaGetLastError(), cudaSuccess);
  stop.record(stream);
  stop.wait();
  if (FLAGS_benchmark) {
    auto ms = stop.elapsedTime(start);
    LOG(INFO) << std::setprecision(3) << size * 1e-6 / ms << " billion rows/s";
  }
}

template <typename T>
void validate(int size, const T* values, const int32_t* ids) {
  std::unordered_map<T, int32_t, Hasher<T, uint32_t>> sofar;
  for (int i = 0; i < size; ++i) {
    ASSERT_GT(ids[i], 0);
    auto it = sofar.find(values[i]);
    if (it == sofar.end()) {
      sofar[values[i]] = ids[i];
    } else {
      ASSERT_EQ(it->second, ids[i]);
    }
  }
  std::vector<bool> haveId(sofar.size());
  for (auto& [_, id] : sofar) {
    ASSERT_FALSE(haveId[id - 1]);
    haveId[id - 1] = true;
  }
}

TEST(IdMapTest, stringView) {
  constexpr int kCapacity = 64;
  constexpr int kUniqueSize = 26;
  const int valueSize = FLAGS_benchmark ? 40'000'000 : 1009;
  auto* allocator = getAllocator(getDevice());
  auto holder = createIdMap<StringView>(allocator, kCapacity);
  auto uniqueValues =
      allocator->allocate<char>(kUniqueSize * (1 + kUniqueSize) / 2);
  for (int i = 0; i < kUniqueSize; ++i) {
    int j = i * (i + 1) / 2;
    memset(&uniqueValues[j], 'a' + i, i + 1);
  }
  auto values = allocator->allocate<StringView>(valueSize);
  std::default_random_engine gen(randomSeed());
  std::uniform_int_distribution<> dist(1, kUniqueSize);
  for (int i = 0; i < valueSize; ++i) {
    int j = dist(gen);
    int k = (j - 1) * j / 2;
    values[i].init(&uniqueValues[k], j);
  }
  auto output = allocator->allocate<int32_t>(valueSize);
  makeIds(holder.idMap.get(), values.get(), valueSize, output.get());
  validate(valueSize, values.get(), output.get());
}

TEST(IdMapTest, int64) {
  constexpr int kCapacity = 8 << 10;
  const int valueSize = FLAGS_benchmark ? 40'000'000 : 40013;
  auto* allocator = getAllocator(getDevice());
  auto holder = createIdMap<int64_t>(allocator, kCapacity);
  auto values = allocator->allocate<int64_t>(valueSize);
  std::default_random_engine gen(randomSeed());
  std::uniform_int_distribution<> dist(0, kCapacity / 2);
  for (int i = 0; i < valueSize; ++i) {
    values[i] = 23 + 37 * dist(gen);
  }
  auto output = allocator->allocate<int32_t>(valueSize);
  makeIds(holder.idMap.get(), values.get(), valueSize, output.get());
  validate(valueSize, values.get(), output.get());
}

void testOverflow(bool withEmptyMarker) {
  constexpr int kCapacity = 32;
  constexpr int kValueSize = 4001;
  auto* allocator = getAllocator(getDevice());
  auto holder = createIdMap<int64_t>(allocator, kCapacity);
  auto values = allocator->allocate<int64_t>(kValueSize);
  for (int i = 0; i < kValueSize; ++i) {
    values[i] = i + !withEmptyMarker;
  }
  auto output = allocator->allocate<int32_t>(kValueSize);
  makeIds(holder.idMap.get(), values.get(), kValueSize, output.get());
  ASSERT_GT(std::count(output.get(), output.get() + kValueSize, -1), 0);
}

TEST(IdMapTest, overflowWithEmptyMarker) {
  testOverflow(true);
}

TEST(IdMapTest, overflowNoEmptyMarker) {
  testOverflow(false);
}

} // namespace
} // namespace facebook::velox::wave

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init follyInit(&argc, &argv);
  if (int device; cudaGetDevice(&device) != cudaSuccess) {
    LOG(WARNING) << "No CUDA detected, skipping all tests";
    return 0;
  }
  return RUN_ALL_TESTS();
}
