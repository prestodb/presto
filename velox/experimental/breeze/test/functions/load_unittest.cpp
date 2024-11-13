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

#include "function_test.h"

#if defined(PLATFORM_METAL) || defined(PLATFORM_OPENCL)
using TestTypes = ::testing::Types<char, unsigned char, int, unsigned, float>;
#else
using TestTypes =
    ::testing::Types<char, unsigned char, int, unsigned, long long,
                     unsigned long long, float, double>;
#endif

TYPED_TEST_SUITE(FunctionTest, TestTypes);

TYPED_TEST(FunctionTest, Load) {
  TypeParam src[] = {1, 2, 3, 4, 5, 6, 7, 8};

  std::vector<TypeParam> in(std::begin(src), std::end(src));
  std::vector<TypeParam> out(8, 0);

  this->template BlockLoad<4, 2>(in, out);

  EXPECT_EQ(in, out);
}

TYPED_TEST(FunctionTest, LoadFewItems) {
  TypeParam src[] = {1, 2, 3};

  std::vector<TypeParam> in(std::begin(src), std::end(src));
  std::vector<TypeParam> out(in.size(), 0);

  this->template BlockLoad<4, 2>(in, out);

  EXPECT_EQ(in, out);
}

TYPED_TEST(FunctionTest, LoadIf) {
  TypeParam src[] = {1, 2, 3, 4, 5, 6, 7, 8};
  int src_selection_flags[] = {0, 1, 1, 0, 0, 1, 0, 0};

  std::vector<TypeParam> in(std::begin(src), std::end(src));
  std::vector<int> selection_flags(std::begin(src_selection_flags),
                                   std::end(src_selection_flags));
  std::vector<TypeParam> out(8, 0);

  this->template BlockLoadIf<4, 2>(in, selection_flags, out);

  TypeParam expected_result[] = {0, 2, 3, 0, 0, 6, 0, 0};
  std::vector<TypeParam> expected_out(std::begin(expected_result),
                                      std::end(expected_result));
  EXPECT_EQ(expected_out, out);
}

TYPED_TEST(FunctionTest, LoadIfFewItems) {
  TypeParam src[] = {1, 2, 3};
  int src_selection_flags[] = {0, 1, 0};

  std::vector<TypeParam> in(std::begin(src), std::end(src));
  std::vector<int> selection_flags(std::begin(src_selection_flags),
                                   std::end(src_selection_flags));
  std::vector<TypeParam> out(in.size(), 0);

  this->template BlockLoadIf<4, 2>(in, selection_flags, out);

  TypeParam expected_result[] = {0, 2, 0};
  std::vector<TypeParam> expected_out(std::begin(expected_result),
                                      std::end(expected_result));
  EXPECT_EQ(expected_out, out);
}

TYPED_TEST(FunctionTest, LoadFrom) {
  TypeParam src[] = {1, 2, 3, 4, 5, 6, 7, 8};
  int src_offsets[] = {7, 6, 5, 4, 3, 2, 1, 0};

  std::vector<TypeParam> in(std::begin(src), std::end(src));
  std::vector<int> offsets(std::begin(src_offsets), std::end(src_offsets));
  std::vector<TypeParam> out(in.size(), 0);

  this->template BlockLoadFrom<4, 2>(in, offsets, out);

  TypeParam expected_result[] = {8, 7, 6, 5, 4, 3, 2, 1};
  std::vector<TypeParam> expected_out(std::begin(expected_result),
                                      std::end(expected_result));
  EXPECT_EQ(expected_out, out);
}

TYPED_TEST(FunctionTest, LoadFromFewItems) {
  TypeParam src[] = {1, 2, 3};
  int src_offsets[] = {2, 1, 0};

  std::vector<TypeParam> in(std::begin(src), std::end(src));
  std::vector<int> offsets(std::begin(src_offsets), std::end(src_offsets));
  std::vector<TypeParam> out(in.size(), 0);

  this->template BlockLoadFrom<4, 2>(in, offsets, out);

  TypeParam expected_result[] = {3, 2, 1};
  std::vector<TypeParam> expected_out(std::begin(expected_result),
                                      std::end(expected_result));
  EXPECT_EQ(expected_out, out);
}
