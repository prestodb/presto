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

#include "breeze/functions/scan.h"
#include "breeze/platforms/platform.h"
#include "breeze/utils/device_vector.h"
#include "perftest/perftest.h"

namespace breeze {

using namespace functions;
using namespace utils;

namespace test {
namespace kernels {

enum { CUDA_WARP_THREADS = 32 };

template <typename Op, int BLOCK_THREADS, int ITEMS_PER_THREAD, typename T>
__global__ __launch_bounds__(BLOCK_THREADS) void Scan(T* out,
                                                      int indirect_zero = 0) {
  CudaPlatform<BLOCK_THREADS, CUDA_WARP_THREADS> p;
  using BlockScanT = BlockScan<decltype(p), T, ITEMS_PER_THREAD>;
  __shared__ typename BlockScanT::Scratch scratch;
  T items[ITEMS_PER_THREAD];
  // generate input values
#pragma unroll
  for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
    items[i] = 1;
  }
  T result[ITEMS_PER_THREAD];
  BlockScanT::template Scan<Op>(
      p, make_slice(items), make_slice(result),
      make_slice(&scratch).template reinterpret<SHARED>());
  if (p.thread_idx() == (BLOCK_THREADS - 1 - indirect_zero)) {
    out[p.block_idx()] = result[ITEMS_PER_THREAD - 1 - indirect_zero];
  }
}

}  // namespace kernels

using BlockScanConfig = PerfTestArrayConfig<4>;

const BlockScanConfig kConfig = {{
    {"num_items", "1050000"},
    {"num_items_short", "100000"},
    {"num_items_grande", "16800000"},
    {"num_items_venti", "500000000"},
}};

template <typename TypeParam>
class BlockScanPerfTest : public PerfTest<BlockScanConfig>,
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

template <typename LaunchParamsAndItemTypeT, typename ScanOpT>
struct ScanTestType {
  using launch_params_and_item_type_type = LaunchParamsAndItemTypeT;
  using item_type = typename LaunchParamsAndItemTypeT::item_type;
  using launch_params = typename LaunchParamsAndItemTypeT::launch_params;
  using scan_op_type = ScanOpT;

  static typename item_type::type GetExpectedResult() {
    if (std::is_same<scan_op_type, ScanOpAdd>())
      return launch_params::BLOCK_THREADS * launch_params::ITEMS_PER_THREAD;
    return 0;
  }
  static std::string GetScanOpName() {
    if (std::is_same<scan_op_type, ScanOpAdd>()) return "Add";
    return "?";
  }
  static std::string GetName() {
    return launch_params_and_item_type_type::GetName() + "." + GetScanOpName();
  }
};

using LaunchParamsTypes =
    std::tuple<LaunchParams<256, 1>, LaunchParams<256, 4>, LaunchParams<256, 8>,
               LaunchParams<256, 16>>;

using LaunchParamsAndItemTypes =
    CombineLaunchParamsAndTypes<LaunchParamsAndItemType, LaunchParamsTypes, int,
                                unsigned, long long, unsigned long long, float>;

using ScanTestTypes =
    CombineTestTypes<ScanTestType, LaunchParamsAndItemTypes, ScanOpAdd>;

using TestTypes = MakeTestTypes<ScanTestTypes>::types;

TYPED_TEST_SUITE(BlockScanPerfTest, TestTypes, TestTypeNames);

TYPED_TEST(BlockScanPerfTest, Scan) {
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
    kernels::Scan<ScanOpAdd, kBlockThreads, kItemsPerThread>
        <<<num_blocks, kBlockThreads>>>(result.data());
  });

  if (check_result) {
    std::vector<item_type> actual_result(num_blocks);
    result.copy_to_host(actual_result.data(), actual_result.size());
    std::vector<item_type> expected_result(num_blocks,
                                           TypeParam::GetExpectedResult());
    EXPECT_EQ(actual_result, expected_result);
  }
}

}  // namespace test
}  // namespace breeze
