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
#include <numeric>
#include <vector>

#include "algorithm_test.h"

#if defined(PLATFORM_METAL) || defined(PLATFORM_OPENCL)
using TestTypes = ::testing::Types<int, unsigned>;
#else
using TestTypes =
    ::testing::Types<int, unsigned, long long, unsigned long long>;
#endif

TYPED_TEST_SUITE(AlgorithmTest, TestTypes);

template <typename T>
struct identity {
  using type = T;
};

template <typename T>
using try_make_unsigned =
    typename std::conditional<std::is_integral<T>::value, std::make_unsigned<T>,
                              identity<T>>::type;

TYPED_TEST(AlgorithmTest, ScanAdd) {
  constexpr int kBlockThreads = 32;
  constexpr int kItemsPerThread = 2;
  constexpr int kBlockItems = kBlockThreads * kItemsPerThread;
  constexpr int kLookbackDistance = 32;

  typedef typename try_make_unsigned<TypeParam>::type block_type;

  std::vector<TypeParam> in(200, 1);
  std::vector<TypeParam> out(200, 0);

  int num_blocks = (200 + kBlockItems - 1) / kBlockItems;

  int next_block_idx = 0;
  std::vector<block_type> blocks(num_blocks, 0);

  this->template Scan<breeze::algorithms::ScanOpAdd, kBlockThreads,
                      kItemsPerThread, kLookbackDistance>(
      in, out, &next_block_idx, blocks, num_blocks);

  EXPECT_EQ(next_block_idx, 4);

  block_type status_mask = static_cast<block_type>(1)
                           << (sizeof(block_type) * 8 - 1);
  block_type expected_blocks_src[] = {
      status_mask | 64,
      status_mask | 128,
      status_mask | 192,
      status_mask | 200,
  };
  std::vector<block_type> expected_blocks(std::begin(expected_blocks_src),
                                          std::end(expected_blocks_src));
  EXPECT_EQ(expected_blocks, blocks);

  std::vector<TypeParam> expected_result(200, 0);
  std::iota(expected_result.begin(), expected_result.end(), 1);
  EXPECT_EQ(expected_result, out);
}

#if !defined(PLATFORM_OPENMP)

TYPED_TEST(AlgorithmTest, ScanAddWithLookbackDistance64) {
  constexpr int kBlockThreads = 64;
  constexpr int kItemsPerThread = 2;
  constexpr int kBlockItems = kBlockThreads * kItemsPerThread;
  constexpr int kLookbackDistance = 64;

  typedef typename try_make_unsigned<TypeParam>::type block_type;

  std::vector<TypeParam> in(400, 1);
  std::vector<TypeParam> out(400, 0);

  int num_blocks = (400 + kBlockItems - 1) / kBlockItems;

  int next_block_idx = 0;
  std::vector<block_type> blocks(num_blocks, 0);

  this->template Scan<breeze::algorithms::ScanOpAdd, kBlockThreads,
                      kItemsPerThread, kLookbackDistance>(
      in, out, &next_block_idx, blocks, num_blocks);

  EXPECT_EQ(next_block_idx, 4);

  block_type status_mask = static_cast<block_type>(1)
                           << (sizeof(block_type) * 8 - 1);
  block_type expected_blocks_src[] = {
      status_mask | 128,
      status_mask | 256,
      status_mask | 384,
      status_mask | 400,
  };
  std::vector<block_type> expected_blocks(std::begin(expected_blocks_src),
                                          std::end(expected_blocks_src));
  EXPECT_EQ(expected_blocks, blocks);

  std::vector<TypeParam> expected_result(400, 0);
  std::iota(expected_result.begin(), expected_result.end(), 1);
  EXPECT_EQ(expected_result, out);
}

#endif  // !defined(PLATFORM_OPENMP)
