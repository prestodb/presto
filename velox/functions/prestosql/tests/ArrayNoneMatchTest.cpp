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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

class ArrayNoneMatchTest : public functions::test::FunctionBaseTest {
 protected:
  void testNoneMatchExpr(
      const std::vector<std::optional<bool>>& expected,
      const std::string& lambdaExpr,
      const VectorPtr& input) {
    auto expression = fmt::format("none_match(c0, x -> ({}))", lambdaExpr);
    testNoneMatchExpr(expected, expression, makeRowVector({input}));
  }

  template <typename T>
  void testNoneMatchExpr(
      const std::vector<std::optional<bool>>& expected,
      const std::string& lambdaExpr,
      const std::vector<std::vector<std::optional<T>>>& input) {
    auto expression = fmt::format("none_match(c0, x -> ({}))", lambdaExpr);
    testNoneMatchExpr(
        expected, expression, makeRowVector({makeNullableArrayVector(input)}));
  }

  void testNoneMatchExpr(
      const std::vector<std::optional<bool>>& expected,
      const std::string& expression,
      const RowVectorPtr& input) {
    auto result = evaluate(expression, (input));
    assertEqualVectors(makeNullableFlatVector<bool>(expected), result);
  }
};

TEST_F(ArrayNoneMatchTest, basic) {
  std::vector<std::vector<std::optional<int32_t>>> ints{
      {std::nullopt, 2, 0},
      {-1, 3},
      {-2, -3},
      {},
      {0, std::nullopt},
  };
  std::vector<std::optional<bool>> expectedResult{
      false,
      false,
      true,
      true,
      std::nullopt,
  };
  testNoneMatchExpr(expectedResult, "x > 1", ints);

  expectedResult = {
      false,
      true,
      true,
      true,
      false,
  };
  testNoneMatchExpr(expectedResult, "x is null", ints);

  std::vector<std::vector<std::optional<bool>>> bools{
      {false, true},
      {false, false},
      {std::nullopt, true},
      {std::nullopt, false},
  };
  expectedResult = {
      false,
      true,
      false,
      std::nullopt,
  };
  testNoneMatchExpr(expectedResult, "x", bools);
}

TEST_F(ArrayNoneMatchTest, complexTypes) {
  auto baseVector = makeArrayVector<int64_t>({
      {1, 2, 3},
      {2, 2},
      {3, 3},
      {4, 4},
      {5, 5},
      {},
  });
  // Create an array of array vector using above base vector using offsets.
  // [
  //  [[1, 2, 3]],
  //  [[2, 2], [3, 3], [4, 4], [5, 5]],
  //  [[]]
  // ]
  auto arrayOfArrays = makeArrayVector({0, 1, 5}, baseVector);
  std::vector<std::optional<bool>> expectedResult{false, false, true};
  testNoneMatchExpr(expectedResult, "cardinality(x) > 0", arrayOfArrays);

  // Create an array of array vector using above base vector using offsets.
  // [
  //  [[1, 2, 3]],  cardinalities is 3
  //  [[2, 2], [3, 3], [4, 4], [5, 5]], all cardinalities is 2
  //  [[]],
  //  null
  // ]
  arrayOfArrays = makeArrayVector({0, 1, 5, 6}, baseVector, {3});
  expectedResult = {false, true, true, std::nullopt};
  testNoneMatchExpr(expectedResult, "cardinality(x) > 2", arrayOfArrays);
}

TEST_F(ArrayNoneMatchTest, strings) {
  std::vector<std::vector<std::optional<StringView>>> input{
      {},
      {"abc"},
      {"ab", "abc"},
      {std::nullopt},
  };
  std::vector<std::optional<bool>> expectedResult{
      true,
      false,
      false,
      std::nullopt,
  };
  testNoneMatchExpr(expectedResult, "x = 'abc'", input);
}

TEST_F(ArrayNoneMatchTest, doubles) {
  std::vector<std::vector<std::optional<double>>> input{
      {},
      {0.2},
      {3.0, 0},
      {std::nullopt},
  };
  std::vector<std::optional<bool>> expectedResult{
      true,
      true,
      false,
      std::nullopt,
  };
  testNoneMatchExpr(expectedResult, "x > 1.1", input);
}

TEST_F(ArrayNoneMatchTest, errors) {
  // No throw and return false if there are unmatched elements except nulls
  auto expression = "(10 / x) > 2";
  std::vector<std::vector<std::optional<int8_t>>> input{
      {0, 2, 0, 5, 0},
      {2, 5, std::nullopt, 0},
  };
  testNoneMatchExpr({false, false}, expression, input);

  // Throw error if others are matched or null
  static constexpr std::string_view kErrorMessage{"division by zero"};
  auto errorInput = makeNullableArrayVector<int8_t>({
      {1, 0},
      {2},
      {6},
      {10, 9, 0, std::nullopt},
      {0, std::nullopt, 1},
  });
  VELOX_ASSERT_THROW(
      testNoneMatchExpr({false, false}, expression, errorInput), kErrorMessage);
  // Rerun using TRY to get right results
  auto errorInputRow = makeRowVector({errorInput});
  std::vector<std::optional<bool>> expectedResult = {
      false,
      false,
      true,
      std::nullopt,
      false,
  };
  testNoneMatchExpr(
      expectedResult,
      "TRY(none_match(c0, x -> ((10 / x) > 2)))",
      errorInputRow);
  testNoneMatchExpr(
      expectedResult,
      "none_match(c0, x -> (TRY((10 / x) > 2)))",
      errorInputRow);
}

TEST_F(ArrayNoneMatchTest, conditional) {
  // No throw and return false if there are unmatched elements except nulls
  auto c0 = makeFlatVector<uint32_t>({1, 2, 3, 4, 5});
  auto c1 = makeNullableArrayVector<int32_t>({
      {4, 100, std::nullopt},
      {50, 12},
      {std::nullopt},
      {3, std::nullopt, 0},
      {300, 100},
  });
  auto input = makeRowVector({c0, c1});
  std::vector<std::optional<bool>> expectedResult = {
      std::nullopt,
      true,
      std::nullopt,
      false,
      true,
  };
  testNoneMatchExpr(
      expectedResult,
      "none_match(c1, if (c0 <= 2, x -> (x > 100), x -> (10 / x > 2)))",
      input);
}
