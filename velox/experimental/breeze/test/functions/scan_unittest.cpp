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
#include <vector>

#include "function_test.h"

#if defined(PLATFORM_METAL) || defined(PLATFORM_OPENCL)
using TestTypes = ::testing::Types<int, unsigned, float>;
#else
using TestTypes = ::testing::Types<int, unsigned, long long, unsigned long long,
                                   float, double>;
#endif

TYPED_TEST_SUITE(FunctionTest, TestTypes);

TYPED_TEST(FunctionTest, ScanAdd) {
  TypeParam src[] = {1, 6,  22, 8,  2,  3, 11, 5,  13, 9, 0,  16, 18,
                     7, 10, 4,  14, 0,  4, 21, 4,  7,  5, 10, 8,  20,
                     9, 6,  22, 30, 1,  3, 8,  12, 25, 9, 10, 25, 6,
                     2, 30, 18, 11, 3,  7, 29, 0,  14, 7, 28, 8,  34,
                     6, 2,  16, 8,  26, 1, 9,  12, 7,  3, 27, 5};

  std::vector<TypeParam> in(std::begin(src), std::end(src));
  std::vector<TypeParam> out(in.size(), 0);

  this->template BlockScan<breeze::functions::ScanOpAdd, 32, 2>(in, out);

  TypeParam expected_result[] = {
      1,   7,   29,  37,  39,  42,  53,  58,  71,  80,  80,  96,  114,
      121, 131, 135, 149, 149, 153, 174, 178, 185, 190, 200, 208, 228,
      237, 243, 265, 295, 296, 299, 307, 319, 344, 353, 363, 388, 394,
      396, 426, 444, 455, 458, 465, 494, 494, 508, 515, 543, 551, 585,
      591, 593, 609, 617, 643, 644, 653, 665, 672, 675, 702, 707};
  std::vector<TypeParam> expected_out(std::begin(expected_result),
                                      std::end(expected_result));
  EXPECT_EQ(expected_out, out);
}

TYPED_TEST(FunctionTest, ScanAddOneItemPerThread) {
  TypeParam src[] = {1, 6,  22, 8,  2,  3, 11, 5,  13, 9, 0,  16, 18,
                     7, 10, 4,  14, 0,  4, 21, 4,  7,  5, 10, 8,  20,
                     9, 6,  22, 30, 1,  3, 8,  12, 25, 9, 10, 25, 6,
                     2, 30, 18, 11, 3,  7, 29, 0,  14, 7, 28, 8,  34,
                     6, 2,  16, 8,  26, 1, 9,  12, 7,  3, 27, 5};

  std::vector<TypeParam> in(std::begin(src), std::end(src));
  std::vector<TypeParam> out(in.size(), 0);

  this->template BlockScan<breeze::functions::ScanOpAdd, 64, 1>(in, out);

  TypeParam expected_result[] = {
      1,   7,   29,  37,  39,  42,  53,  58,  71,  80,  80,  96,  114,
      121, 131, 135, 149, 149, 153, 174, 178, 185, 190, 200, 208, 228,
      237, 243, 265, 295, 296, 299, 307, 319, 344, 353, 363, 388, 394,
      396, 426, 444, 455, 458, 465, 494, 494, 508, 515, 543, 551, 585,
      591, 593, 609, 617, 643, 644, 653, 665, 672, 675, 702, 707};
  std::vector<TypeParam> expected_out(std::begin(expected_result),
                                      std::end(expected_result));
  EXPECT_EQ(expected_out, out);
}

TYPED_TEST(FunctionTest, ScanAddHalfBlock) {
  TypeParam src[] = {1,  6, 22, 8,  2, 3, 11, 5,  13, 9,  0, 16, 18, 7,  10, 4,
                     14, 0, 4,  21, 4, 7, 5,  10, 8,  20, 9, 6,  22, 30, 1,  3};

  std::vector<TypeParam> in(std::begin(src), std::end(src));
  std::vector<TypeParam> out(in.size(), 0);

  this->template BlockScan<breeze::functions::ScanOpAdd, 32, 2>(in, out);

  TypeParam expected_result[] = {1,   7,   29,  37,  39,  42,  53,  58,
                                 71,  80,  80,  96,  114, 121, 131, 135,
                                 149, 149, 153, 174, 178, 185, 190, 200,
                                 208, 228, 237, 243, 265, 295, 296, 299};
  std::vector<TypeParam> expected_out(std::begin(expected_result),
                                      std::end(expected_result));
  EXPECT_EQ(expected_out, out);
}

TYPED_TEST(FunctionTest, ScanAddFew) {
  TypeParam src[] = {5, 0, 4, 2, 7, 9};

  std::vector<TypeParam> in(std::begin(src), std::end(src));
  std::vector<TypeParam> out(in.size(), 0);

  this->template BlockScan<breeze::functions::ScanOpAdd, 32, 2>(in, out);

  TypeParam expected_result[] = {5, 5, 9, 11, 18, 27};
  std::vector<TypeParam> expected_out(std::begin(expected_result),
                                      std::end(expected_result));
  EXPECT_EQ(expected_out, out);
}

TYPED_TEST(FunctionTest, ScanAddLowerHalfBitsSet) {
  TypeParam lower_half_bits_set = (1llu << sizeof(TypeParam) * 4) - 1;
  std::vector<TypeParam> in(32, lower_half_bits_set);
  std::vector<TypeParam> out(in.size(), 0);

  this->template BlockScan<breeze::functions::ScanOpAdd, 32, 2>(in, out);

  std::vector<TypeParam> expected_out;
  TypeParam sum = 0;
  for (int i = 0; i < 32; ++i) {
    sum += lower_half_bits_set;
    expected_out.push_back(sum);
  }
  EXPECT_EQ(expected_out, out);
}
