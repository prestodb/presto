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

class ArraySumTest : public FunctionBaseTest {
 protected:
  // Evaluate an expression.
  template <typename T>
  void testArraySum(const VectorPtr& expected, const VectorPtr& input) {
    auto result = evaluate("array_sum(c0)", makeRowVector({input}));
    assertEqualVectors(expected, result);

    // Test constant input.
    const vector_size_t firstRow = 0;
    result = evaluate(
        "array_sum(c0)",
        makeRowVector({BaseVector::wrapInConstant(10, firstRow, input)}));
    assertEqualVectors(
        BaseVector::wrapInConstant(10, firstRow, expected), result);

    const vector_size_t lastRow = input->size() - 1;
    result = evaluate(
        "array_sum(c0)",
        makeRowVector({BaseVector::wrapInConstant(10, lastRow, input)}));
    assertEqualVectors(
        BaseVector::wrapInConstant(10, lastRow, expected), result);
  }
};

} // namespace

// Test integer arrays.
TEST_F(ArraySumTest, int64Input) {
  auto input = makeNullableArrayVector<int64_t>(
      {{0, 1, 2}, {std::nullopt, 1, 2}, {std::nullopt}});
  auto expected = makeNullableFlatVector<int64_t>({3, 3, 0});
  testArraySum<int64_t>(expected, input);
}

TEST_F(ArraySumTest, int32Input) {
  auto input = makeNullableArrayVector<int32_t>(
      {{0, 1, 2}, {std::nullopt, 1, 2}, {std::nullopt}});
  auto expected = makeNullableFlatVector<int64_t>({3, 3, 0});
  testArraySum<int64_t>(expected, input);
}

TEST_F(ArraySumTest, int16Input) {
  auto input = makeNullableArrayVector<int16_t>(
      {{0, 1, 2}, {std::nullopt, 1, 2}, {std::nullopt}});
  auto expected = makeNullableFlatVector<int64_t>({3, 3, 0});
  testArraySum<int64_t>(expected, input);
}

TEST_F(ArraySumTest, int8Input) {
  auto input = makeNullableArrayVector<int8_t>(
      {{0, 1, 2}, {std::nullopt, 1, 2}, {std::nullopt}});
  auto expected = makeNullableFlatVector<int64_t>({3, 3, 0});
  testArraySum<int64_t>(expected, input);
}

TEST_F(ArraySumTest, overflow) {
  // Flat input.
  auto input = makeRowVector({
      makeNullableArrayVector<int64_t>(
          {{1, 2, 3}, {0, std::numeric_limits<int64_t>::max(), 2, 3}, {}}),
  });
  VELOX_ASSERT_THROW(
      evaluate("array_sum(c0)", input),
      "integer overflow: 9223372036854775807 + 2");

  auto result = evaluate("try(array_sum(c0))", input);
  assertEqualVectors(
      makeNullableFlatVector<int64_t>({6, std::nullopt, 0}), result);

  // Constant input.
  auto emptyInput = makeRowVector(ROW({}), 3);
  VELOX_ASSERT_THROW(
      evaluate(
          "array_sum(array[9223372036854775807, 100::bigint])", emptyInput),
      "integer overflow: 9223372036854775807 + 100");

  result = evaluate(
      "try(array_sum(array[9223372036854775807, 100::bigint]))", emptyInput);
  assertEqualVectors(
      makeNullableFlatVector<int64_t>(
          {std::nullopt, std::nullopt, std::nullopt}),
      result);
}

// Test floating point arrays
TEST_F(ArraySumTest, realInput) {
  auto input = makeNullableArrayVector<float>(
      {{0, 1, 2}, {std::nullopt, 1, 2}, {std::nullopt}});
  auto expected = makeNullableFlatVector<double>({3, 3, 0});
  testArraySum<double>(expected, input);
}

TEST_F(ArraySumTest, doubleInput) {
  auto input = makeNullableArrayVector<double>(
      {{0, 1, 2}, {std::nullopt, 1, 2}, {std::nullopt}});
  auto expected = makeNullableFlatVector<double>({3, 3, 0});
  testArraySum<double>(expected, input);
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
  testArraySum<double>(expected, input);
}
