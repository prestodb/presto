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

#include "breeze/functions/reduce.h"
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
__global__ __launch_bounds__(BLOCK_THREADS) void Reduce(T* out) {
  CudaPlatform<BLOCK_THREADS, CUDA_WARP_THREADS> p;
  using BlockReduceT = BlockReduce<decltype(p), T>;
  __shared__ typename BlockReduceT::Scratch scratch;
  T items[ITEMS_PER_THREAD];
  // generate input values
#pragma unroll
  for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
    items[i] = 1;
  }
  T aggregate = BlockReduceT::template Reduce<Op, ITEMS_PER_THREAD>(
      p, make_slice(items),
      make_slice(&scratch).template reinterpret<SHARED>());
  if (p.thread_idx() == 0) {
    out[p.block_idx()] = aggregate;
  }
}

}  // namespace kernels

using BlockReduceConfig = PerfTestArrayConfig<4>;

const BlockReduceConfig kConfig = {{
    {"num_items", "2100000"},
    {"num_items_short", "250000"},
    {"num_items_grande", "33600000"},
    {"num_items_venti", "500000000"},
}};

template <typename TypeParam>
class BlockReducePerfTest : public PerfTest<BlockReduceConfig>,
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

template <typename LaunchParamsAndItemTypeT, typename ReduceOpT>
struct ReduceTestType {
  using launch_params_and_item_type_type = LaunchParamsAndItemTypeT;
  using item_type = typename LaunchParamsAndItemTypeT::item_type;
  using launch_params = typename LaunchParamsAndItemTypeT::launch_params;
  using reduce_op_type = ReduceOpT;

  static typename item_type::type GetExpectedResult() {
    if (std::is_same<reduce_op_type, ReduceOpAdd>())
      return launch_params::BLOCK_THREADS * launch_params::ITEMS_PER_THREAD;
    if (std::is_same<reduce_op_type, ReduceOpMin>()) return 1;
    if (std::is_same<reduce_op_type, ReduceOpMax>()) return 1;
    return 0;
  }
  static std::string GetReduceOpName() {
    if (std::is_same<reduce_op_type, ReduceOpAdd>()) return "Add";
    if (std::is_same<reduce_op_type, ReduceOpMin>()) return "Min";
    if (std::is_same<reduce_op_type, ReduceOpMax>()) return "Max";
    return "?";
  }
  static std::string GetName() {
    return launch_params_and_item_type_type::GetName() + "." +
           GetReduceOpName();
  }
};

using LaunchParamsTypes =
    std::tuple<LaunchParams<256, 8>, LaunchParams<256, 16>>;

using LaunchParamsAndItemTypes =
    CombineLaunchParamsAndTypes<LaunchParamsAndItemType, LaunchParamsTypes, int,
                                unsigned, long long, unsigned long long, float>;

using ReduceTestTypes =
    CombineTestTypes<ReduceTestType, LaunchParamsAndItemTypes, ReduceOpAdd,
                     ReduceOpMin, ReduceOpMax>;

using TestTypes = MakeTestTypes<ReduceTestTypes>::types;

TYPED_TEST_SUITE(BlockReducePerfTest, TestTypes, TestTypeNames);

TYPED_TEST(BlockReducePerfTest, Reduce) {
  using item_type = typename TypeParam::item_type::type;
  using reduce_op_type = typename TypeParam::reduce_op_type;

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
    kernels::Reduce<reduce_op_type, kBlockThreads, kItemsPerThread>
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
