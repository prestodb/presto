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

#include <algorithm>
#include <limits>
#include <numeric>
#include <vector>

#include "algorithm_test.h"

#if defined(PLATFORM_METAL) || defined(PLATFORM_OPENCL)
using TestTypes = ::testing::Types<int, unsigned>;
#elif defined(PLATFORM_SYCL) || defined(PLATFORM_HIP)
using TestTypes = ::testing::Types<unsigned, unsigned long long>;
#else
using TestTypes = ::testing::Types<int, unsigned, long long, unsigned long long,
                                   float, double>;
#endif

TYPED_TEST_SUITE(AlgorithmTest, TestTypes);

TYPED_TEST(AlgorithmTest, ReduceAdd) {
  constexpr int kBlockThreads = 32;
  constexpr int kItemsPerThread = 2;
  constexpr int kBlockItems = kBlockThreads * kItemsPerThread;

  std::vector<TypeParam> in(200, 0);
  std::iota(in.begin(), in.end(), 1);
  TypeParam out = static_cast<TypeParam>(0);
  int num_blocks = (200 + kBlockItems - 1) / kBlockItems;

  this->template Reduce<breeze::algorithms::ReduceOpAdd, kBlockThreads,
                        kItemsPerThread>(in, &out, num_blocks);

  TypeParam expected_result =
      std::accumulate(in.begin(), in.end(), static_cast<TypeParam>(0));
  EXPECT_EQ(expected_result, out);
}

TYPED_TEST(AlgorithmTest, ReduceMin) {
  constexpr int kBlockThreads = 32;
  constexpr int kItemsPerThread = 2;
  constexpr int kBlockItems = kBlockThreads * kItemsPerThread;

  std::vector<TypeParam> in(200, 0);
  std::iota(in.begin(), in.end(), 1);
  TypeParam out = std::numeric_limits<TypeParam>::max();
  int num_blocks = (200 + kBlockItems - 1) / kBlockItems;

  this->template Reduce<breeze::algorithms::ReduceOpMin, kBlockThreads,
                        kItemsPerThread>(in, &out, num_blocks);

  TypeParam expected_result = *std::min_element(in.begin(), in.end());
  EXPECT_EQ(expected_result, out);
}

TYPED_TEST(AlgorithmTest, ReduceMax) {
  constexpr int kBlockThreads = 32;
  constexpr int kItemsPerThread = 2;
  constexpr int kBlockItems = kBlockThreads * kItemsPerThread;

  std::vector<TypeParam> in(200, 0);
  std::iota(in.begin(), in.end(), 1);
  TypeParam out = std::numeric_limits<TypeParam>::min();
  int num_blocks = (200 + kBlockItems - 1) / kBlockItems;

  this->template Reduce<breeze::algorithms::ReduceOpMax, kBlockThreads,
                        kItemsPerThread>(in, &out, num_blocks);

  TypeParam expected_result = *std::max_element(in.begin(), in.end());
  EXPECT_EQ(expected_result, out);
}
