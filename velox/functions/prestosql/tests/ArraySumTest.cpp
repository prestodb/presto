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
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::functions::test;

namespace {

class ArraySumTest : public FunctionBaseTest {
 protected:
  // Evaluate an expression.
  template <typename T>
  void testExpr(
      const VectorPtr& expected,
      const std::string& expression,
      const std::vector<VectorPtr>& input) {
    auto result = evaluate<FlatVector<T>>(expression, makeRowVector(input));
    assertEqualVectors(expected, result);
  }
};

} // namespace

// Test integer arrays.
TEST_F(ArraySumTest, int64Input) {
  auto input = makeNullableArrayVector<int64_t>(
      {{0, 1, 2}, {std::nullopt, 1, 2}, {std::nullopt}});
  auto expected = makeNullableFlatVector<int64_t>({3, 3, 0});
  testExpr<int64_t>(expected, "array_sum(C0)", {input});
}

TEST_F(ArraySumTest, int32Input) {
  auto input = makeNullableArrayVector<int32_t>(
      {{0, 1, 2}, {std::nullopt, 1, 2}, {std::nullopt}});
  auto expected = makeNullableFlatVector<int64_t>({3, 3, 0});
  testExpr<int64_t>(expected, "array_sum(C0)", {input});
}

TEST_F(ArraySumTest, int16Input) {
  auto input = makeNullableArrayVector<int16_t>(
      {{0, 1, 2}, {std::nullopt, 1, 2}, {std::nullopt}});
  auto expected = makeNullableFlatVector<int64_t>({3, 3, 0});
  testExpr<int64_t>(expected, "array_sum(C0)", {input});
}

TEST_F(ArraySumTest, int8Input) {
  auto input = makeNullableArrayVector<int8_t>(
      {{0, 1, 2}, {std::nullopt, 1, 2}, {std::nullopt}});
  auto expected = makeNullableFlatVector<int64_t>({3, 3, 0});
  testExpr<int64_t>(expected, "array_sum(C0)", {input});
}

TEST_F(ArraySumTest, int64InputLimitsOverflow) {
  auto input = makeNullableArrayVector<int64_t>(
      {{0, std::numeric_limits<int64_t>::max(), 2}});
  auto expected = makeNullableFlatVector<int64_t>(
      {std::numeric_limits<int64_t>::min() + 1});
  EXPECT_THROW(
      testExpr<int64_t>(expected, "array_sum(C0)", {input}),
      facebook::velox::VeloxUserError);
}

// Test floating point arrays
TEST_F(ArraySumTest, realInput) {
  auto input = makeNullableArrayVector<float>(
      {{0, 1, 2}, {std::nullopt, 1, 2}, {std::nullopt}});
  auto expected = makeNullableFlatVector<double>({3, 3, 0});
  testExpr<double>(expected, "array_sum(C0)", {input});
}

TEST_F(ArraySumTest, doubleInput) {
  auto input = makeNullableArrayVector<double>(
      {{0, 1, 2}, {std::nullopt, 1, 2}, {std::nullopt}});
  auto expected = makeNullableFlatVector<double>({3, 3, 0});
  testExpr<double>(expected, "array_sum(C0)", {input});
}

TEST_F(ArraySumTest, doubleInputLimits) {
  auto input = makeNullableArrayVector<double>(
      {{0, std::numeric_limits<double>::infinity(), 2},
       {std::numeric_limits<double>::quiet_NaN(), 1, 2},
       {std::numeric_limits<double>::lowest(), -1},
       {std::numeric_limits<double>::max(), 1.0}});
  auto expected = makeNullableFlatVector<double>(
      {std::numeric_limits<double>::infinity(),
       std::numeric_limits<double>::quiet_NaN(),
       std::numeric_limits<double>::lowest(),
       std::numeric_limits<double>::max()});
  testExpr<double>(expected, "array_sum(C0)", {input});
}
