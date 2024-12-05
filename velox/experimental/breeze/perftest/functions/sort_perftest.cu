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

/*
 * Copyright (c) 2024 by Rivos Inc.
 * Licensed under the Apache License, Version 2.0, see LICENSE for details.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#include <limits>
#include <vector>

#include "breeze/functions/sort.h"
#include "breeze/platforms/platform.h"
#include "breeze/utils/device_vector.h"
#include "breeze/utils/types.h"
#include "perftest/perftest.h"

namespace breeze {

using namespace functions;
using namespace utils;

namespace test {
namespace kernels {

enum { CUDA_WARP_THREADS = 32 };

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, int RADIX_BITS, typename T>
__global__ __launch_bounds__(BLOCK_THREADS) void RadixRank(
    T* out, int indirect_zero = 0) {
  CudaPlatform<BLOCK_THREADS, CUDA_WARP_THREADS> p;
  using BlockRankT = BlockRadixRank<decltype(p), ITEMS_PER_THREAD, RADIX_BITS>;
  __shared__ typename BlockRankT::Scratch scratch;
  T items[ITEMS_PER_THREAD];
  // generate input values
#pragma unroll
  for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
    items[i] = (p.thread_idx() * ITEMS_PER_THREAD + i) % (1 << RADIX_BITS);
  }
  int ranks[ITEMS_PER_THREAD];
  BlockRankT::Rank(p, make_slice<THREAD, WARP_STRIPED>(items),
                   make_slice<THREAD, WARP_STRIPED>(ranks),
                   make_slice(&scratch).template reinterpret<SHARED>());
  if (p.thread_idx() == indirect_zero) {
    out[p.block_idx()] = ranks[indirect_zero];
  }
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, int RADIX_BITS, typename T>
__global__ __launch_bounds__(BLOCK_THREADS) void RadixSort(
    T* out, int indirect_zero = 0) {
  CudaPlatform<BLOCK_THREADS, CUDA_WARP_THREADS> p;
  using BlockSortT =
      BlockRadixSort<decltype(p), ITEMS_PER_THREAD, RADIX_BITS, T, NullType>;
  __shared__ typename BlockSortT::Scratch scratch;
  T items[ITEMS_PER_THREAD];
  // generate input values
#pragma unroll
  for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
    T mask = (p.thread_idx() * ITEMS_PER_THREAD + i) % (1 << RADIX_BITS);
    items[i] = mask | (mask << (sizeof(T) * 8 - RADIX_BITS - 1));
  }
  BlockSortT::Sort(p, make_slice<THREAD, WARP_STRIPED>(items),
                   make_empty_slice(),
                   make_slice(&scratch).template reinterpret<SHARED>());
  if (p.thread_idx() == indirect_zero) {
    out[p.block_idx()] = items[indirect_zero];
  }
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, int RADIX_BITS,
          typename KeyT, typename ValueT>
__global__ __launch_bounds__(BLOCK_THREADS) void RadixSort(
    KeyT* out_keys, ValueT* out_values, int indirect_zero = 0) {
  CudaPlatform<BLOCK_THREADS, CUDA_WARP_THREADS> p;
  using BlockSortT =
      BlockRadixSort<decltype(p), ITEMS_PER_THREAD, RADIX_BITS, KeyT, ValueT>;
  __shared__ typename BlockSortT::Scratch scratch;
  KeyT keys[ITEMS_PER_THREAD];
  ValueT values[ITEMS_PER_THREAD];
  // generate input values
#pragma unroll
  for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
    int index = p.thread_idx() * ITEMS_PER_THREAD + i;
    KeyT mask = index % (1 << RADIX_BITS);
    keys[i] = mask | (mask << (sizeof(KeyT) * 8 - RADIX_BITS - 1));
    values[i] = ITEMS_PER_THREAD * BLOCK_THREADS - index;
  }
  BlockSortT::Sort(p, make_slice<THREAD, WARP_STRIPED>(keys),
                   make_slice<THREAD, WARP_STRIPED>(values),
                   make_slice(&scratch).template reinterpret<SHARED>());
  if (p.thread_idx() == indirect_zero) {
    out_keys[p.block_idx()] = keys[indirect_zero];
    out_values[p.block_idx()] = values[indirect_zero];
  }
}

}  // namespace kernels

using BlockRadixSortConfig = PerfTestArrayConfig<4>;

const BlockRadixSortConfig kConfig = {{
    {"num_items", "540000"},
    {"num_items_short", "50000"},
    {"num_items_grande", "8640000"},
    {"num_items_venti", "500000000"},
}};

template <typename TypeParam>
class BlockSortPerfTest : public PerfTest<BlockRadixSortConfig>,
                          public testing::Test {
 public:
  template <typename T>
  T GetConfigValue(const char* key, T default_value) const {
    return kConfig.get<T>(key, default_value);
  }
  template <typename T>
  T GetSizedConfigValue(const char* key, T default_value) const {
    return kConfig.get_sized<T>(key, default_value);
  }
};

template <int N>
struct RadixBits {
  enum {
    VALUE = N,
  };
};

template <typename LaunchParamsAndItemTypeT, typename RadixBitsT>
struct RadixTestType {
  using launch_params_and_item_type_type = LaunchParamsAndItemTypeT;
  using item_type = typename LaunchParamsAndItemTypeT::item_type;
  using launch_params = typename LaunchParamsAndItemTypeT::launch_params;
  enum {
    RADIX_BITS = RadixBitsT::VALUE,
  };

  static std::string GetName() {
    return launch_params_and_item_type_type::GetName() + ".Bits" +
           std::to_string(RADIX_BITS);
  }
};

using LaunchParamsTypes =
    std::tuple<LaunchParams<256, 8>, LaunchParams<256, 16>>;

using LaunchParamsAndItemTypes =
    CombineLaunchParamsAndTypes<LaunchParamsAndItemType, LaunchParamsTypes, int,
                                unsigned, long long, unsigned long long>;

using RadixTestTypes =
    CombineTestTypes<RadixTestType, LaunchParamsAndItemTypes, RadixBits<8>>;

using TestTypes = MakeTestTypes<RadixTestTypes>::types;

TYPED_TEST_SUITE(BlockSortPerfTest, TestTypes, TestTypeNames);

TYPED_TEST(BlockSortPerfTest, RadixRank) {
  using item_type = typename TypeParam::item_type::type;

  auto check_result = this->GetConfigValue("check_result", true);
  auto num_items = this->GetSizedConfigValue("num_items", 1);

  constexpr int kBlockThreads = TypeParam::launch_params::BLOCK_THREADS;
  constexpr int kItemsPerThread = TypeParam::launch_params::ITEMS_PER_THREAD;
  constexpr int kBlockItems = kBlockThreads * kItemsPerThread;

  int num_blocks = (num_items + kBlockItems - 1) / kBlockItems;
  device_vector<item_type> result(num_blocks);

  // provide throughput information
  this->set_element_count(num_blocks * kBlockItems);
  this->set_element_size(sizeof(item_type));
  this->set_elements_per_thread(kItemsPerThread);

  this->Measure(kConfig, [&]() {
    kernels::RadixRank<kBlockThreads, kItemsPerThread, TypeParam::RADIX_BITS>
        <<<num_blocks, kBlockThreads>>>(result.data());
  });

  if (check_result) {
    std::vector<item_type> actual_result(num_blocks);
    result.copy_to_host(actual_result.data(), actual_result.size());
    std::vector<item_type> expected_result(num_blocks, 0);
    EXPECT_EQ(actual_result, expected_result);
  }
}

TYPED_TEST(BlockSortPerfTest, RadixSort) {
  using item_type = typename TypeParam::item_type::type;

  auto check_result = this->GetConfigValue("check_result", true);
  auto num_items = this->GetSizedConfigValue("num_items", 1);

  constexpr int kBlockThreads = TypeParam::launch_params::BLOCK_THREADS;
  constexpr int kItemsPerThread = TypeParam::launch_params::ITEMS_PER_THREAD;
  constexpr int kBlockItems = kBlockThreads * kItemsPerThread;

  int num_blocks = (num_items + kBlockItems - 1) / kBlockItems;
  device_vector<item_type> result(num_blocks);

  // provide throughput information
  this->set_element_count(num_blocks * kBlockItems);
  this->set_element_size(sizeof(item_type));
  this->set_elements_per_thread(kItemsPerThread);

  this->Measure(kConfig, [&]() {
    kernels::RadixSort<kBlockThreads, kItemsPerThread, TypeParam::RADIX_BITS>
        <<<num_blocks, kBlockThreads>>>(result.data());
  });

  if (check_result) {
    std::vector<item_type> actual_result(num_blocks);
    result.copy_to_host(actual_result.data(), actual_result.size());
    std::vector<item_type> expected_result(num_blocks, 0);
    EXPECT_EQ(actual_result, expected_result);
  }
}

TYPED_TEST(BlockSortPerfTest, RadixSortKeyValues) {
  using key_type = typename TypeParam::item_type::type;
  using value_type = int;

  auto check_result = this->GetConfigValue("check_result", true);
  auto num_items = this->GetSizedConfigValue("num_items", 1);

  constexpr int kBlockThreads = TypeParam::launch_params::BLOCK_THREADS;
  constexpr int kItemsPerThread = TypeParam::launch_params::ITEMS_PER_THREAD;
  constexpr int kBlockItems = kBlockThreads * kItemsPerThread;

  int num_blocks = (num_items + kBlockItems - 1) / kBlockItems;
  device_vector<key_type> result_keys(num_blocks);
  device_vector<value_type> result_values(num_blocks);

  // provide throughput information
  this->set_element_count(num_blocks * kBlockItems);
  this->set_element_size(sizeof(key_type) + sizeof(value_type));
  this->set_elements_per_thread(kItemsPerThread);

  this->Measure(kConfig, [&]() {
    kernels::RadixSort<kBlockThreads, kItemsPerThread, TypeParam::RADIX_BITS>
        <<<num_blocks, kBlockThreads>>>(result_keys.data(),
                                        result_values.data());
  });

  if (check_result) {
    std::vector<key_type> actual_result_keys(num_blocks);
    result_keys.copy_to_host(actual_result_keys.data(),
                             actual_result_keys.size());
    std::vector<key_type> expected_result_keys(num_blocks, 0);
    EXPECT_EQ(actual_result_keys, expected_result_keys);
    std::vector<value_type> actual_result_values(num_blocks);
    result_values.copy_to_host(actual_result_values.data(),
                               actual_result_values.size());
    std::vector<value_type> expected_result_values(
        num_blocks, kBlockThreads * kItemsPerThread);
    EXPECT_EQ(actual_result_values, expected_result_values);
  }
}

}  // namespace test
}  // namespace breeze
