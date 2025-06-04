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

#include <gtest/gtest.h>

#include "velox/experimental/wave/common/Atomic.cuh"
#include "velox/experimental/wave/common/Cuda.h"

namespace facebook::velox::wave {
namespace {

template <typename T, typename U>
inline __device__ Atomic<T, MemoryScope::kDevice>* asDeviceAtomic(U* ptr) {
  return reinterpret_cast<Atomic<T, MemoryScope::kDevice>*>(ptr);
}

template <MemoryOrder Order>
__global__ void loadKernel(int* in, int* out) {
  *out = asDeviceAtomic<int>(in)->template load<Order>();
}

template <MemoryOrder Order>
__global__ void storeKernel(int in, int* out) {
  asDeviceAtomic<int>(out)->template store<Order>(in);
}

template <MemoryOrder Order>
__global__ void
compareExchangeKernel(int* expected, int desired, int* out, bool* was_changed) {
  int expected_value = *expected;
  *was_changed = asDeviceAtomic<int>(out)->template compare_exchange<Order>(
      expected_value, desired);
  *expected = expected_value;
}

template <MemoryOrder Order>
struct AtomicTestType {
  static const MemoryOrder memory_order = Order;
};

struct AtomicTestNameGenerator {
  template <typename T>
  static std::string GetName(int) {
    if constexpr (T::memory_order == MemoryOrder::kRelaxed)
      return "relaxed";
    if constexpr (T::memory_order == MemoryOrder::kAcquire)
      return "acquire";
    if constexpr (T::memory_order == MemoryOrder::kRelease)
      return "release";
    return "?";
  }
};

template <typename TypeParam>
class AtomicLoadTest : public testing::Test {};

using AtomicLoadTestTypes = ::testing::Types<
    AtomicTestType<MemoryOrder::kRelaxed>,
    AtomicTestType<MemoryOrder::kAcquire>>;

TYPED_TEST_SUITE(AtomicLoadTest, AtomicLoadTestTypes, AtomicTestNameGenerator);

TYPED_TEST(AtomicLoadTest, load) {
  auto* allocator = getAllocator(getDevice());
  auto input = allocator->allocate<int>();
  *input = 1234;
  auto output = allocator->allocate<int>();
  loadKernel<TypeParam::memory_order><<<1, 1>>>(input.get(), output.get());
  ASSERT_EQ(cudaGetLastError(), cudaSuccess);
  ASSERT_EQ(cudaDeviceSynchronize(), cudaSuccess);
  ASSERT_EQ(*output, 1234);
}

template <typename TypeParam>
class AtomicStoreTest : public testing::Test {};

using AtomicStoreTestTypes = ::testing::Types<
    AtomicTestType<MemoryOrder::kRelaxed>,
    AtomicTestType<MemoryOrder::kRelease>>;

TYPED_TEST_SUITE(
    AtomicStoreTest,
    AtomicStoreTestTypes,
    AtomicTestNameGenerator);

TYPED_TEST(AtomicStoreTest, store) {
  auto* allocator = getAllocator(getDevice());
  auto output = allocator->allocate<int>();
  storeKernel<TypeParam::memory_order><<<1, 1>>>(4321, output.get());
  ASSERT_EQ(cudaGetLastError(), cudaSuccess);
  ASSERT_EQ(cudaDeviceSynchronize(), cudaSuccess);
  ASSERT_EQ(*output, 4321);
}

template <typename TypeParam>
class AtomicCompareExchangeTest : public testing::Test {};

using AtomicCompareExchangeTestTypes = ::testing::Types<
    AtomicTestType<MemoryOrder::kRelaxed>,
    AtomicTestType<MemoryOrder::kAcquire>,
    AtomicTestType<MemoryOrder::kRelease>>;

TYPED_TEST_SUITE(
    AtomicCompareExchangeTest,
    AtomicCompareExchangeTestTypes,
    AtomicTestNameGenerator);

TYPED_TEST(AtomicCompareExchangeTest, compare_exchange) {
  auto* allocator = getAllocator(getDevice());
  auto expected = allocator->allocate<int>();
  *expected = 1234;
  auto output = allocator->allocate<int>();
  *output = 0;
  auto was_changed = allocator->allocate<bool>();
  *was_changed = true;
  compareExchangeKernel<TypeParam::memory_order>
      <<<1, 1>>>(expected.get(), 4321, output.get(), was_changed.get());
  ASSERT_EQ(cudaGetLastError(), cudaSuccess);
  ASSERT_EQ(cudaDeviceSynchronize(), cudaSuccess);
  ASSERT_EQ(*output, 0);
  ASSERT_EQ(*expected, 0);
  ASSERT_EQ(*was_changed, false);
  *output = 1234;
  *expected = 1234;
  compareExchangeKernel<TypeParam::memory_order>
      <<<1, 1>>>(expected.get(), 4321, output.get(), was_changed.get());
  ASSERT_EQ(cudaGetLastError(), cudaSuccess);
  ASSERT_EQ(cudaDeviceSynchronize(), cudaSuccess);
  ASSERT_EQ(*output, 4321);
  ASSERT_EQ(*expected, 1234);
  ASSERT_EQ(*was_changed, true);
}

inline __device__ AtomicMutex<MemoryScope::kDevice>* asDeviceAtomicMutex(
    int* ptr) {
  return reinterpret_cast<AtomicMutex<MemoryScope::kDevice>*>(ptr);
}

__global__ void mutexKernel(int* mtx, int* out) {
  asDeviceAtomicMutex(mtx)->acquire();
  *out += 1;
  asDeviceAtomicMutex(mtx)->release();
}

TEST(AtomicMutexTest, basic) {
  auto* allocator = getAllocator(getDevice());
  auto mutex = allocator->allocate<int>();
  *mutex = 1;
  auto output = allocator->allocate<int>();
  *output = 0;
  mutexKernel<<<4, 8>>>(mutex.get(), output.get());
  ASSERT_EQ(cudaGetLastError(), cudaSuccess);
  ASSERT_EQ(cudaDeviceSynchronize(), cudaSuccess);
  ASSERT_EQ(*mutex, 1);
  ASSERT_EQ(*output, 32);
}

} // namespace
} // namespace facebook::velox::wave
