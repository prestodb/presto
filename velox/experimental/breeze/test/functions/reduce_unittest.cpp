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
#include <type_traits>

#include "function_test.h"

#if defined(PLATFORM_METAL) || defined(PLATFORM_OPENCL)
using TestTypes = ::testing::Types<int, unsigned, float>;
#else
using TestTypes = ::testing::Types<int, unsigned, long long, unsigned long long,
                                   float, double>;
#endif

TYPED_TEST_SUITE(FunctionTest, TestTypes);

TYPED_TEST(FunctionTest, ReduceAdd) {
  TypeParam src[] = {1, 6,  22, 8,  2,  3, 11, 5,  13, 9, 0,  16, 18,
                     7, 10, 4,  14, 0,  4, 21, 4,  7,  5, 10, 8,  20,
                     9, 6,  22, 30, 1,  3, 8,  12, 25, 9, 10, 25, 6,
                     2, 30, 18, 11, 3,  7, 29, 0,  14, 7, 28, 8,  34,
                     6, 2,  16, 8,  26, 1, 9,  12, 7,  3, 27, 5};

  std::vector<TypeParam> in(std::begin(src), std::end(src));
  TypeParam out = 0;

  this->template BlockReduce<breeze::functions::ReduceOpAdd, 32, 2>(in, &out);

  TypeParam expected_result = 707;
  EXPECT_EQ(expected_result, out);
}

TYPED_TEST(FunctionTest, ReduceAddOneItemPerThread) {
  TypeParam src[] = {1, 6,  22, 8,  2,  3, 11, 5,  13, 9, 0,  16, 18,
                     7, 10, 4,  14, 0,  4, 21, 4,  7,  5, 10, 8,  20,
                     9, 6,  22, 30, 1,  3, 8,  12, 25, 9, 10, 25, 6,
                     2, 30, 18, 11, 3,  7, 29, 0,  14, 7, 28, 8,  34,
                     6, 2,  16, 8,  26, 1, 9,  12, 7,  3, 27, 5};

  std::vector<TypeParam> in(std::begin(src), std::end(src));
  TypeParam out = 0;

  this->template BlockReduce<breeze::functions::ReduceOpAdd, 64, 1>(in, &out);

  TypeParam expected_result = 707;
  EXPECT_EQ(expected_result, out);
}

TYPED_TEST(FunctionTest, ReduceAddFew) {
  TypeParam src[] = {1, 2, 3, 4, 5, 6, 7, 8};

  std::vector<TypeParam> in(std::begin(src), std::end(src));
  TypeParam out = 0;

  this->template BlockReduce<breeze::functions::ReduceOpAdd, 32, 2>(in, &out);

  TypeParam expected_result = 36;
  EXPECT_EQ(expected_result, out);
}

// generate vector with bit patterns to target implementations that
// break down the type into upper and lower half operations, e.g. 64-bit
// add into multiple 32-bit adds.
//
// the upper and lower halves are one of the following bit patterns with
// the appropriate number of bits: 0000, 1111, 0001, 1000, 0111, 1110
template <typename T>
std::vector<T> generate_bit_pattern_vector() {
  enum {
    HALF_NUM_BITS = sizeof(T) * 4,
  };
  std::vector<unsigned long long> bit_patterns = {
      0ull,                               // 00...00
      (1ull << HALF_NUM_BITS) - 1,        // 11...11
      1,                                  // 00...01
      (1ull << (HALF_NUM_BITS - 1)),      // 10...00
      (1ull << (HALF_NUM_BITS - 1)) - 1,  // 01...11
      (1ull << HALF_NUM_BITS) - 2,        // 11...10
  };
  std::vector<T> values;
  for (auto upper_bits : bit_patterns) {
    for (auto lower_bits : bit_patterns) {
      T value = static_cast<T>((upper_bits << HALF_NUM_BITS) | lower_bits);
      values.push_back(value);
    }
  }
  return values;
}

TYPED_TEST(FunctionTest, ReduceAddBitPatterns) {
  auto in = generate_bit_pattern_vector<TypeParam>();
  TypeParam out = 0;

  // Clamp values for signed types to prevent overflow.
  if (std::is_signed<TypeParam>::value) {
    TypeParam in_size = static_cast<TypeParam>(in.size());
    TypeParam min_value = std::numeric_limits<TypeParam>::min() / in_size;
    TypeParam max_value = std::numeric_limits<TypeParam>::max() / in_size;
    for (TypeParam& value : in) {
      value = std::clamp(value, min_value, max_value);
    }
  }

  this->template BlockReduce<breeze::functions::ReduceOpAdd, 32, 2>(in, &out);

  TypeParam expected_result =
      std::accumulate(in.begin(), in.end(), static_cast<TypeParam>(0));
  EXPECT_EQ(expected_result, out);
}

TYPED_TEST(FunctionTest, ReduceMin) {
  TypeParam src[] = {1, 2, 3, 4, 5, 6, 7, 8};

  std::vector<TypeParam> in(std::begin(src), std::end(src));
  TypeParam out = 0;

  this->template BlockReduce<breeze::functions::ReduceOpMin, 32, 2>(in, &out);

  TypeParam expected_result = 1;
  EXPECT_EQ(expected_result, out);
}

TYPED_TEST(FunctionTest, ReduceMinBitPatterns) {
  auto in = generate_bit_pattern_vector<TypeParam>();
  TypeParam out = 0;

  this->template BlockReduce<breeze::functions::ReduceOpMin, 32, 2>(in, &out);

  TypeParam expected_result = *std::min_element(in.begin(), in.end());
  EXPECT_EQ(expected_result, out);
}

TYPED_TEST(FunctionTest, ReduceMax) {
  TypeParam src[] = {1, 2, 3, 4, 5, 6, 7, 8};

  std::vector<TypeParam> in(std::begin(src), std::end(src));
  TypeParam out = 0;

  this->template BlockReduce<breeze::functions::ReduceOpMax, 32, 2>(in, &out);

  TypeParam expected_result = 8;
  EXPECT_EQ(expected_result, out);
}

TYPED_TEST(FunctionTest, ReduceMaxBitPatterns) {
  auto in = generate_bit_pattern_vector<TypeParam>();
  TypeParam out = 0;

  this->template BlockReduce<breeze::functions::ReduceOpMax, 32, 2>(in, &out);

  TypeParam expected_result = *std::max_element(in.begin(), in.end());
  EXPECT_EQ(expected_result, out);
}
