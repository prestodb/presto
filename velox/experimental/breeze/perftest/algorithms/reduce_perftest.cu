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

#include "breeze/algorithms/reduce.h"
#include "breeze/platforms/platform.h"
#include "breeze/utils/device_vector.h"
#include "perftest/perftest.h"

namespace breeze {

using namespace algorithms;
using namespace utils;

namespace test {
namespace kernels {

enum { CUDA_WARP_THREADS = 32 };

template <typename Op, int BLOCK_THREADS, int ITEMS_PER_THREAD, typename T,
          typename U>
__global__ __launch_bounds__(BLOCK_THREADS) void Reduce(const T* in, U* out,
                                                        int num_items) {
  CudaPlatform<BLOCK_THREADS, CUDA_WARP_THREADS> p;
  using DeviceReduceT = DeviceReduce<decltype(p), U>;
  __shared__ typename DeviceReduceT::Scratch scratch;
  DeviceReduceT::template Reduce<Op, ITEMS_PER_THREAD>(
      p, make_slice<GLOBAL>(in), make_slice<GLOBAL>(out),
      make_slice(&scratch).template reinterpret<SHARED>(), num_items);
}

}  // namespace kernels

using ReduceConfig = PerfTestArrayConfig<12>;

const ReduceConfig kConfig = {{
    {"num_input_rows", "1048000"},
    {"num_input_rows_short", "200000"},
    {"num_input_rows_grande", "16768000"},
    {"num_input_rows_venti", "200000000"},
    {"input_generate_method", "FILL"},
    {"input_fill_value", "1"},
    {"expected_sum", "1048000"},
    {"expected_sum_short", "200000"},
    {"expected_sum_grande", "16768000"},
    {"expected_sum_venti", "200000000"},
    {"expected_min", "1"},
    {"expected_max", "1"},
}};

template <typename TypeParam>
class ReducePerfTest : public PerfTest<ReduceConfig>, public testing::Test {
 public:
  template <typename T>
  T GetConfigValue(const char* key, T default_value) const {
    return kConfig.get<T>(key, default_value);
  }
  template <typename T>
  T GetSizedConfigValue(const char* key, T default_value) const {
    return kConfig.get_sized<T>(key, default_value);
  }
  template <typename T>
  std::vector<T> GetConfigColumn(const char* prefix) const {
    return kConfig.get_column<T>(prefix);
  }
};

using LaunchParamsTypes =
    std::tuple<LaunchParams<256, 8>, LaunchParams<256, 16>>;

using TestTypes = MakePerfTestTypes<LaunchParamsTypes, int, long long>::types;

TYPED_TEST_SUITE(ReducePerfTest, TestTypes, TestTypeNames);

TYPED_TEST(ReducePerfTest, Add) {
  using value_type = typename TypeParam::item_type::type;

  auto input = this->template GetConfigColumn<value_type>("input");
  ASSERT_NE(input.size(), 0u);

  auto check_result = this->GetConfigValue("check_result", true);
  auto expected_sum = this->GetSizedConfigValue("expected_sum", 0);

  std::vector<value_type> input_identity(1);

  device_vector<value_type> items(input.size());
  device_vector<value_type> identity(1);
  device_vector<value_type> result(1);

  constexpr int kBlockThreads = TypeParam::launch_params::BLOCK_THREADS;
  constexpr int kItemsPerThread = TypeParam::launch_params::ITEMS_PER_THREAD;
  constexpr int kBlockItems = kBlockThreads * kItemsPerThread;

  int num_blocks = (input.size() + kBlockItems - 1) / kBlockItems;

  // copy input to device memory
  items.copy_from_host(input.data(), input.size());
  identity.copy_from_host(input_identity.data(), input_identity.size());

  // provide throughput information
  this->set_element_count(input.size());
  this->set_element_size(sizeof(value_type));
  this->set_elements_per_thread(kItemsPerThread);
  this->template set_global_memory_loads<value_type>(items.size());

  this->MeasureWithSetup(
      kConfig,
      [&]() {
        cudaMemcpyAsync(result.data(), identity.data(), sizeof(value_type),
                        cudaMemcpyDeviceToDevice);
      },
      [&]() {
        kernels::Reduce<ReduceOpAdd, kBlockThreads, kItemsPerThread>
            <<<num_blocks, kBlockThreads>>>(items.data(), result.data(),
                                            items.size());
      });

  if (check_result) {
    value_type sum = 0;
    result.copy_to_host(&sum, 1);
    EXPECT_EQ(sum, expected_sum);
  }
}

TYPED_TEST(ReducePerfTest, Min) {
  using value_type = typename TypeParam::item_type::type;

  auto input = this->template GetConfigColumn<value_type>("input");
  ASSERT_NE(input.size(), 0u);

  auto check_result = this->GetConfigValue("check_result", true);
  auto expected_min = this->GetConfigValue("expected_min", 0);

  std::vector<value_type> input_identity(
      1, std::numeric_limits<value_type>::max());

  device_vector<value_type> items(input.size());
  device_vector<value_type> identity(1);
  device_vector<value_type> result(1);

  constexpr int kBlockThreads = TypeParam::launch_params::BLOCK_THREADS;
  constexpr int kItemsPerThread = TypeParam::launch_params::ITEMS_PER_THREAD;
  constexpr int kBlockItems = kBlockThreads * kItemsPerThread;

  int num_blocks = (input.size() + kBlockItems - 1) / kBlockItems;

  // copy input to device memory
  items.copy_from_host(input.data(), input.size());
  identity.copy_from_host(input_identity.data(), input_identity.size());

  // provide throughput information
  this->set_element_count(input.size());
  this->set_element_size(sizeof(value_type));
  this->set_elements_per_thread(kItemsPerThread);
  this->template set_global_memory_loads<value_type>(items.size());

  this->MeasureWithSetup(
      kConfig,
      [&]() {
        cudaMemcpyAsync(result.data(), identity.data(), sizeof(value_type),
                        cudaMemcpyDeviceToDevice);
      },
      [&]() {
        kernels::Reduce<ReduceOpMin, kBlockThreads, kItemsPerThread>
            <<<num_blocks, kBlockThreads>>>(items.data(), result.data(),
                                            items.size());
      });

  if (check_result) {
    value_type min = 0;
    result.copy_to_host(&min, 1);
    EXPECT_EQ(min, expected_min);
  }
}

TYPED_TEST(ReducePerfTest, Max) {
  using value_type = typename TypeParam::item_type::type;

  auto input = this->template GetConfigColumn<value_type>("input");
  ASSERT_NE(input.size(), 0u);

  auto check_result = this->GetConfigValue("check_result", true);
  auto expected_max = this->GetConfigValue("expected_max", 0);

  std::vector<value_type> input_identity(
      1, std::numeric_limits<value_type>::min());

  device_vector<value_type> items(input.size());
  device_vector<value_type> identity(1);
  device_vector<value_type> result(1);

  constexpr int kBlockThreads = TypeParam::launch_params::BLOCK_THREADS;
  constexpr int kItemsPerThread = TypeParam::launch_params::ITEMS_PER_THREAD;
  constexpr int kBlockItems = kBlockThreads * kItemsPerThread;

  int num_blocks = (input.size() + kBlockItems - 1) / kBlockItems;

  // copy input to device memory
  items.copy_from_host(input.data(), input.size());
  identity.copy_from_host(input_identity.data(), input_identity.size());

  // provide throughput information
  this->set_element_count(input.size());
  this->set_element_size(sizeof(value_type));
  this->set_elements_per_thread(kItemsPerThread);
  this->template set_global_memory_loads<value_type>(items.size());

  this->MeasureWithSetup(
      kConfig,
      [&]() {
        cudaMemcpyAsync(result.data(), identity.data(), sizeof(value_type),
                        cudaMemcpyDeviceToDevice);
      },
      [&]() {
        kernels::Reduce<ReduceOpMax, kBlockThreads, kItemsPerThread>
            <<<num_blocks, kBlockThreads>>>(items.data(), result.data(),
                                            items.size());
      });

  if (check_result) {
    value_type max = 0;
    result.copy_to_host(&max, 1);
    EXPECT_EQ(max, expected_max);
  }
}

}  // namespace test
}  // namespace breeze
