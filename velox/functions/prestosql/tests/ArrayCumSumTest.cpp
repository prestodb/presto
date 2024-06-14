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

#include <optional>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::functions::test;

namespace {
class ArrayCumSumTest : public FunctionBaseTest {
 protected:
  template <typename T>
  void testArrayCumSum(const VectorPtr& expected, const VectorPtr& input) {
    auto result = evaluate("array_cum_sum(c0)", makeRowVector({input}));
    assertEqualVectors(expected, result);
  }
};

TEST_F(ArrayCumSumTest, bigint) {
  auto input = makeNullableArrayVector<int64_t>(
      {{},
       {10000, 1000010000, 100001000010000},
       {-976543210987654321, std::nullopt, std::numeric_limits<int64_t>::max()},
       {std::nullopt, 100001000010000}});
  auto expected = makeNullableArrayVector<int64_t>(
      {{},
       {10000, 1000020000, 100002000030000},
       {-976543210987654321, std::nullopt, std::nullopt},
       {std::nullopt, std::nullopt}});
  testArrayCumSum<int64_t>(expected, input);
}

TEST_F(ArrayCumSumTest, integer) {
  auto input = makeNullableArrayVector<int32_t>(
      {{},
       {1000, 10001000, 1010001000},
       {-976543210, std::nullopt, std::numeric_limits<int32_t>::max()},
       {std::nullopt, 1010001000}});
  auto expected = makeNullableArrayVector<int32_t>(
      {{},
       {1000, 10002000, 1020003000},
       {-976543210, std::nullopt, std::nullopt},
       {std::nullopt, std::nullopt}});
  testArrayCumSum<int32_t>(expected, input);
}

TEST_F(ArrayCumSumTest, smallint) {
  auto input = makeNullableArrayVector<int16_t>(
      {{},
       {10, 100, 10000},
       {-9876, std::nullopt, std::numeric_limits<int16_t>::max()},
       {std::nullopt, 10000}});
  auto expected = makeNullableArrayVector<int16_t>(
      {{},
       {10, 110, 10110},
       {-9876, std::nullopt, std::nullopt},
       {std::nullopt, std::nullopt}});
  testArrayCumSum<int16_t>(expected, input);
}

TEST_F(ArrayCumSumTest, tinyint) {
  auto input = makeNullableArrayVector<int8_t>(
      {{},
       {1, 2, 4},
       {-99, std::nullopt, std::numeric_limits<int8_t>::max()},
       {std::nullopt, 4}});
  auto expected = makeNullableArrayVector<int8_t>(
      {{},
       {1, 3, 7},
       {-99, std::nullopt, std::nullopt},
       {std::nullopt, std::nullopt}});
  testArrayCumSum<int8_t>(expected, input);
}

TEST_F(ArrayCumSumTest, real) {
  auto input = makeNullableArrayVector<float>(
      {{},
       {1, 2, 3},
       {-9, std::nullopt, std::numeric_limits<float>::max()},
       {std::nullopt, 3}});
  auto expected = makeNullableArrayVector<float>(
      {{},
       {1, 3, 6},
       {-9, std::nullopt, std::nullopt},
       {std::nullopt, std::nullopt}});
  testArrayCumSum<float>(expected, input);
}

TEST_F(ArrayCumSumTest, double) {
  auto input = makeNullableArrayVector<double>(
      {{},
       {1, 2, 3},
       {-9, std::nullopt, std::numeric_limits<double>::max()},
       {std::nullopt, 3}});
  auto expected = makeNullableArrayVector<double>(
      {{},
       {1, 3, 6},
       {-9, std::nullopt, std::nullopt},
       {std::nullopt, std::nullopt}});
  testArrayCumSum<double>(expected, input);
}

TEST_F(ArrayCumSumTest, bigintOverflow) {
  constexpr int64_t kMin = std::numeric_limits<int64_t>::min();
  constexpr int64_t kMax = std::numeric_limits<int64_t>::max();

  // Overflow on 1 + kMax.
  auto input = makeNullableArrayVector<int64_t>({{1, kMax, 2, 3}});
  VELOX_ASSERT_THROW(
      evaluate("array_cum_sum(c0)", makeRowVector({input})),
      "integer overflow: 1 + 9223372036854775807");

  // Overflow on -1 + kMin.
  input = makeNullableArrayVector<int64_t>({{-1, kMin, -2, -3}});
  VELOX_ASSERT_THROW(
      evaluate("array_cum_sum(c0)", makeRowVector({input})),
      "integer overflow: -1 + -9223372036854775808");

  // Overflow for array containing consecutive kMin values.
  input = makeNullableArrayVector<int64_t>({{kMax, kMin, kMin}});
  VELOX_ASSERT_THROW(
      evaluate("array_cum_sum(c0)", makeRowVector({input})),
      "integer overflow: -1 + -9223372036854775808");

  // No overflow when function is called on alternating kMax, kMin values.
  input = makeNullableArrayVector<int64_t>(
      {{kMax, kMin, kMax, kMin},
       {kMax, kMin},
       {kMax, kMin, kMax, kMin, kMax, kMin}});
  auto expected = makeNullableArrayVector<int64_t>(
      {{kMax, -1, kMax - 1, -2},
       {kMax, -1},
       {kMax, -1, kMax - 1, -2, kMax - 2, -3}});
  testArrayCumSum<int64_t>(expected, input);
}

TEST_F(ArrayCumSumTest, doubleLimits) {
  constexpr double kInf = std::numeric_limits<double>::infinity();
  constexpr double kNan = std::numeric_limits<double>::quiet_NaN();
  constexpr double kLowest = std::numeric_limits<double>::lowest();
  constexpr double kMax = std::numeric_limits<double>::max();

  auto input = makeNullableArrayVector<double>(
      {{1, kInf, 123.456},
       {1, kNan, 123.456},
       {1, kLowest, 123.456},
       {1, kMax, 123.456},
       {kLowest, kMax, 123.456},
       {kMax, kLowest, 123.456},
       {1, kMax, kInf},
       {1, kLowest, kInf},
       {kLowest, kNan, 5},
       {kMax, kNan, 5}});
  auto expected = makeNullableArrayVector<double>(
      {{1, kInf, kInf},
       {1, kNan, kNan},
       {1, kLowest, kLowest},
       {1, kMax, kMax},
       {kLowest, 0, 123.456},
       {kMax, 0, 123.456},
       {1, kMax, kInf},
       {1, kLowest, kInf},
       {kLowest, kNan, kNan},
       {kMax, kNan, kNan}});
  testArrayCumSum<double>(expected, input);
}
} // namespace
