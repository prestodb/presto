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

class ArrayAnyMatchTest : public functions::test::FunctionBaseTest {
 protected:
  void testAnyMatchExpr(
      const std::vector<std::optional<bool>>& expected,
      const std::string& lambdaExpr,
      const VectorPtr& input) {
    auto expression = fmt::format("any_match(c0, x -> ({}))", lambdaExpr);
    testAnyMatchExpr(expected, expression, makeRowVector({input}));
  }

  template <typename T>
  void testAnyMatchExpr(
      const std::vector<std::optional<bool>>& expected,
      const std::string& lambdaExpr,
      const std::vector<std::vector<std::optional<T>>>& input) {
    auto expression = fmt::format("any_match(c0, x -> ({}))", lambdaExpr);
    testAnyMatchExpr(expected, lambdaExpr, makeNullableArrayVector(input));
  }

  void testAnyMatchExpr(
      const std::vector<std::optional<bool>>& expected,
      const std::string& expression,
      const RowVectorPtr& input) {
    auto result = evaluate(expression, (input));
    assertEqualVectors(makeNullableFlatVector<bool>(expected), result);
  }
};

TEST_F(ArrayAnyMatchTest, basic) {
  std::vector<std::vector<std::optional<int32_t>>> ints{
      {std::nullopt, 2, 0},
      {-1, 3},
      {-2, -3},
      {},
      {0, std::nullopt},
  };
  std::vector<std::optional<bool>> expectedResult{
      true,
      true,
      false,
      false,
      std::nullopt,
  };
  testAnyMatchExpr(expectedResult, "x > 1", ints);

  expectedResult = {
      true,
      false,
      false,
      false,
      true,
  };
  testAnyMatchExpr(expectedResult, "x is null", ints);

  std::vector<std::vector<std::optional<bool>>> bools{
      {false, true},
      {false, false},
      {std::nullopt, true},
      {std::nullopt, false},
  };
  expectedResult = {
      true,
      false,
      true,
      std::nullopt,
  };
  testAnyMatchExpr(expectedResult, "x", bools);
}

TEST_F(ArrayAnyMatchTest, complexTypes) {
  auto arrayOfArrays = makeNestedArrayVectorFromJson<int32_t>({
      "[[1, 2, 3]]",
      "[[2, 2], [3, 3], [4, 4], [5, 5]]",
      "[[]]",
  });
  std::vector<std::optional<bool>> expectedResult{
      true,
      true,
      false,
  };
  testAnyMatchExpr(expectedResult, "cardinality(x) > 0", arrayOfArrays);

  arrayOfArrays = makeNestedArrayVectorFromJson<int32_t>({
      "[[1, 2, 3]]",
      "[[2, 2], [3, 3], [4, 4], [5, 5]]",
      "[[]]",
      "null",
  });
  expectedResult = {
      true,
      false,
      false,
      std::nullopt,
  };
  testAnyMatchExpr(expectedResult, "cardinality(x) > 2", arrayOfArrays);
}

TEST_F(ArrayAnyMatchTest, strings) {
  std::vector<std::vector<std::optional<StringView>>> input{
      {},
      {"abc"},
      {"ab", "abc"},
      {std::nullopt},
  };
  std::vector<std::optional<bool>> expectedResult{
      false,
      true,
      true,
      std::nullopt,
  };
  testAnyMatchExpr(expectedResult, "x = 'abc'", input);
}

TEST_F(ArrayAnyMatchTest, doubles) {
  std::vector<std::vector<std::optional<double>>> input{
      {},
      {0.2},
      {3.0, 0},
      {std::nullopt},
  };
  std::vector<std::optional<bool>> expectedResult{
      false,
      false,
      true,
      std::nullopt,
  };
  testAnyMatchExpr(expectedResult, "x > 1.1", input);
}

TEST_F(ArrayAnyMatchTest, errors) {
  // No throw and return false if there are unmatched elements except nulls
  auto expression = "(10 / x) > 2";
  std::vector<std::vector<std::optional<int8_t>>> input{
      {0, 2, 0, 5, 0},
      {2, 5, std::nullopt, 0},
  };
  testAnyMatchExpr({true, true}, expression, input);

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
      testAnyMatchExpr({true, true}, expression, errorInput), kErrorMessage);
  // Rerun using TRY to get right results
  auto errorInputRow = makeRowVector({errorInput});
  std::vector<std::optional<bool>> expectedResult{
      true,
      true,
      false,
      std::nullopt,
      true,
  };
  testAnyMatchExpr(
      expectedResult, "TRY(any_match(c0, x -> ((10 / x) > 2)))", errorInputRow);
  testAnyMatchExpr(
      expectedResult, "any_match(c0, x -> (TRY((10 / x) > 2)))", errorInputRow);
}

TEST_F(ArrayAnyMatchTest, conditional) {
  // No throw and return false if there are unmatched elements except nulls
  auto c0 = makeFlatVector<int32_t>({1, 2, 3, 4, 5});
  auto c1 = makeNullableArrayVector<int32_t>({
      {4, 100, std::nullopt},
      {50, 12},
      {std::nullopt},
      {3, std::nullopt, 0},
      {300, 100},
  });
  auto input = makeRowVector({c0, c1});
  std::vector<std::optional<bool>> expectedResult = {
      std::nullopt, false, std::nullopt, true, false};
  testAnyMatchExpr(
      expectedResult,
      "any_match(c1, if (c0 <= 2, x -> (x > 100), x -> (10 / x > 2)))",
      input);
}

// Test evaluation of lambda functions under different branches of a CASE
// statement. Make sure that first branch covers some rows at the end of the
// vector, so that next branch is evaluated on a subset of rows at the start of
// the vector. In this case, we still need to create 'capture' vectors of the
// size of the original vector. Otherwise, peeling dictionary encoding from
// 'capture' vectors will fail.
TEST_F(ArrayAnyMatchTest, underConditionalWithCapture) {
  auto input = makeRowVector({
      makeFlatVector<int64_t>({1, 2}),
      makeArrayVector<int64_t>({
          {1, 2},
          {1, 2, 3, 4, 5},
      }),
  });

  auto result = evaluate(
      "case "
      "   when any_match(c1, x -> (x > 3)) then 1 "
      "   when any_match(c1, x -> (x < c0 * 2)) then 2 "
      "   else 3 "
      "end",
      input);

  assertEqualVectors(makeFlatVector<int64_t>({2, 1}), result);
}
