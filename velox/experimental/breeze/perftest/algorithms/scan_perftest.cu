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

#include <vector>

#include "breeze/algorithms/scan.h"
#include "breeze/functions/load.h"
#include "breeze/functions/scan.h"
#include "breeze/functions/store.h"
#include "breeze/platforms/platform.h"
#include "breeze/utils/block_details.h"
#include "breeze/utils/device_vector.h"
#include "perftest/perftest.h"

namespace breeze {

using namespace algorithms;
using namespace functions;
using namespace utils;

namespace test {
namespace kernels {

enum { CUDA_WARP_THREADS = 32 };

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, int LOOKBACK_DISTANCE,
          typename T, typename U, typename BlockT>
__global__ __launch_bounds__(BLOCK_THREADS) void Scan(const T* in, U* out,
                                                      int* next_block_idx,
                                                      BlockT* blocks,
                                                      int num_items) {
  CudaPlatform<BLOCK_THREADS, CUDA_WARP_THREADS> p;
  using DeviceScanT =
      DeviceScan<decltype(p), U, ITEMS_PER_THREAD, LOOKBACK_DISTANCE>;
  __shared__ typename DeviceScanT::Scratch scratch;
  DeviceScanT::template Scan<ScanOpAdd>(
      p, make_slice<GLOBAL>(in), make_slice<GLOBAL>(out),
      make_slice<GLOBAL>(next_block_idx), make_slice<GLOBAL>(blocks),
      make_slice(&scratch).template reinterpret<SHARED>(), num_items);
}

}  // namespace kernels

using ScanConfig = PerfTestArrayConfig<12>;

const ScanConfig kConfig = {
    {{"num_input_rows", "525000"},
     {"num_input_rows_short", "10000"},
     {"num_input_rows_grande", "8400000"},
     {"num_input_rows_venti", "100000000"},
     {"input_generate_method", "FILL"},
     {"input_fill_value", "1"},
     {"num_expected_prefix_sums_rows", "525000"},
     {"num_expected_prefix_sums_rows_short", "10000"},
     {"num_expected_prefix_sums_rows_grande", "8400000"},
     {"num_expected_prefix_sums_rows_venti", "100000000"},
     {"expected_prefix_sums_generate_method", "SEQUENCE"},
     {"expected_prefix_sums_sequence_start", "1"}}};

template <typename TypeParam>
class ScanPerfTest : public PerfTest<ScanConfig>, public testing::Test {
 public:
  template <typename T>
  T GetConfigValue(const char* key, T default_value) const {
    return kConfig.get<T>(key, default_value);
  }
  template <typename T>
  std::vector<T> GetConfigColumn(const char* prefix) const {
    return kConfig.get_column<T>(prefix);
  }
};

template <int N>
struct LookbackDistance {
  enum { VALUE = N };
};

template <typename LaunchParamsAndItemTypeT, typename LookbackDistanceT>
struct ScanTestType {
  using launch_params_and_item_type_type = LaunchParamsAndItemTypeT;
  using item_type = typename LaunchParamsAndItemTypeT::item_type;
  using launch_params = typename LaunchParamsAndItemTypeT::launch_params;
  enum {
    LOOKBACK_DISTANCE = LookbackDistanceT::VALUE,
  };

  static std::string GetName() {
    return launch_params_and_item_type_type::GetName() + ".LookbackDistance" +
           std::to_string(LOOKBACK_DISTANCE);
  }
};

using LaunchParamsTypes =
    std::tuple<LaunchParams<256, 8>, LaunchParams<256, 16>>;

using LaunchParamsAndItemTypes =
    CombineLaunchParamsAndTypes<LaunchParamsAndItemType, LaunchParamsTypes, int,
                                long long>;

using ScanTestTypes =
    CombineTestTypes<ScanTestType, LaunchParamsAndItemTypes,
                     LookbackDistance<32>, LookbackDistance<64>,
                     LookbackDistance<96>, LookbackDistance<128>>;

using TestTypes = MakeTestTypes<ScanTestTypes>::types;

TYPED_TEST_SUITE(ScanPerfTest, TestTypes, TestTypeNames);

TYPED_TEST(ScanPerfTest, Scan) {
  using value_type = typename TypeParam::item_type::type;
  using sum_type = value_type;
  using block_type = typename try_make_unsigned<value_type>::type;

  auto input = this->template GetConfigColumn<value_type>("input");
  ASSERT_NE(input.size(), 0u);

  auto check_result = this->GetConfigValue("check_result", true);

  device_vector<value_type> items(input.size());
  device_vector<sum_type> prefix_sums(input.size());

  constexpr int kBlockThreads = TypeParam::launch_params::BLOCK_THREADS;
  constexpr int kItemsPerThread = TypeParam::launch_params::ITEMS_PER_THREAD;
  constexpr int kBlockItems = kBlockThreads * kItemsPerThread;
  constexpr int kLookbackDistance = TypeParam::LOOKBACK_DISTANCE;

  int num_blocks = (input.size() + kBlockItems - 1) / kBlockItems;

  device_vector<int> next_block_idx(1);
  device_vector<block_type> blocks(num_blocks);

  // copy input to device memory
  items.copy_from_host(input.data(), input.size());

  // provide throughput information
  this->set_element_count(input.size());
  this->set_element_size(sizeof(value_type));
  this->set_elements_per_thread(kItemsPerThread);
  this->template set_global_memory_loads<value_type>(input.size());
  this->template set_global_memory_stores<sum_type>(input.size());

  this->MeasureWithSetup(
      kConfig,
      [&]() {
        cudaMemsetAsync(next_block_idx.data(), 0, sizeof(int));
        cudaMemsetAsync(blocks.data(), 0, sizeof(block_type) * num_blocks);
      },
      [&]() {
        kernels::Scan<kBlockThreads, kItemsPerThread, kLookbackDistance>
            <<<num_blocks, kBlockThreads>>>(items.data(), prefix_sums.data(),
                                            next_block_idx.data(),
                                            blocks.data(), items.size());
      });

  if (check_result) {
    std::vector<sum_type> actual_prefix_sums(input.size());
    prefix_sums.copy_to_host(actual_prefix_sums.data(),
                             actual_prefix_sums.size());
    auto expected_prefix_sums =
        this->template GetConfigColumn<sum_type>("expected_prefix_sums");
    EXPECT_EQ(expected_prefix_sums, actual_prefix_sums);
  }
}

}  // namespace test
}  // namespace breeze
