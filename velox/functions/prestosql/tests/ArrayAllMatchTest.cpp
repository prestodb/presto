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

class ArrayAllMatchTest : public functions::test::FunctionBaseTest {
 protected:
  void testAllMatchExpr(
      const std::vector<std::optional<bool>>& expected,
      const std::string& lambdaExpr,
      const VectorPtr& input) {
    auto expression = fmt::format("all_match(c0, x -> ({}))", lambdaExpr);
    testAllMatchExpr(expected, expression, makeRowVector({input}));
  }

  template <typename T>
  void testAllMatchExpr(
      const std::vector<std::optional<bool>>& expected,
      const std::string& lambdaExpr,
      const std::vector<std::vector<std::optional<T>>>& input) {
    auto expression = fmt::format("all_match(c0, x -> ({}))", lambdaExpr);
    testAllMatchExpr(
        expected, expression, makeRowVector({makeNullableArrayVector(input)}));
  }

  void testAllMatchExpr(
      const std::vector<std::optional<bool>>& expected,
      const std::string& expression,
      const RowVectorPtr& input) {
    auto result = evaluate(expression, (input));
    assertEqualVectors(makeNullableFlatVector<bool>(expected), result);
  }
};

TEST_F(ArrayAllMatchTest, basic) {
  std::vector<std::vector<std::optional<int32_t>>> ints{
      {std::nullopt, 2, 3},
      {-1, 3},
      {2, 3},
      {},
      {std::nullopt, std::nullopt},
  };
  std::vector<std::optional<bool>> expectedResult{
      std::nullopt,
      false,
      true,
      true,
      std::nullopt,
  };
  testAllMatchExpr(expectedResult, "x > 1", ints);

  expectedResult = {
      false,
      false,
      false,
      true,
      true,
  };
  testAllMatchExpr(expectedResult, "x is null", ints);

  std::vector<std::vector<std::optional<bool>>> bools{
      {false, true},
      {true, true},
      {std::nullopt, true},
      {std::nullopt, false},
  };

  expectedResult = {
      false,
      true,
      std::nullopt,
      false,
  };
  testAllMatchExpr(expectedResult, "x", bools);
}

TEST_F(ArrayAllMatchTest, complexTypes) {
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
  testAllMatchExpr(expectedResult, "cardinality(x) > 0", arrayOfArrays);

  arrayOfArrays = makeNestedArrayVectorFromJson<int32_t>({
      "[[1, 2, 3]]",
      "[[2, 2], [3, 3], [4, 4], [5, 5]]",
      "[[]]",
      "null",
  });
  expectedResult = {true, false, false, std::nullopt};
  testAllMatchExpr(expectedResult, "cardinality(x) > 2", arrayOfArrays);
}

TEST_F(ArrayAllMatchTest, strings) {
  std::vector<std::vector<std::optional<StringView>>> input{
      {},
      {"abc"},
      {"ab", "abc"},
      {std::nullopt},
  };
  std::vector<std::optional<bool>> expectedResult{
      true, true, false, std::nullopt};
  testAllMatchExpr(expectedResult, "x = 'abc'", input);
}

TEST_F(ArrayAllMatchTest, doubles) {
  std::vector<std::vector<std::optional<double>>> input{
      {},
      {1.2},
      {3.0, 0},
      {std::nullopt},
  };
  std::vector<std::optional<bool>> expectedResult{
      true,
      true,
      false,
      std::nullopt,
  };
  testAllMatchExpr(expectedResult, "x > 1.1", input);
}

TEST_F(ArrayAllMatchTest, errors) {
  // No throw and return false if there are unmatched elements except nulls
  auto expression = "(10 / x) > 2";
  std::vector<std::vector<std::optional<int8_t>>> input{
      {0, 2, 0, 5, 0},
      {2, 5, std::nullopt, 0},
  };
  testAllMatchExpr({false, false}, expression, input);

  // Throw error if others are matched or null
  static constexpr std::string_view kErrorMessage{"division by zero"};
  auto errorInput = makeNullableArrayVector<int8_t>({
      {1, 0},
      {2},
      {6},
      {1, 0, std::nullopt},
      {10, std::nullopt},
  });

  VELOX_ASSERT_THROW(
      testAllMatchExpr({false, false}, "(10 / x) > 2", errorInput),
      kErrorMessage);
  // Rerun using TRY to get right results
  auto errorInputRow = makeRowVector({errorInput});
  std::vector<std::optional<bool>> expectedResult{
      std::nullopt,
      true,
      false,
      std::nullopt,
      false,
  };
  testAllMatchExpr(
      expectedResult, "TRY(all_match(c0, x -> ((10 / x) > 2)))", errorInputRow);
  testAllMatchExpr(
      expectedResult, "all_match(c0, x -> (TRY((10 / x) > 2)))", errorInputRow);
}

TEST_F(ArrayAllMatchTest, conditional) {
  // No throw and return false if there are unmatched elements except nulls
  auto c0 = makeFlatVector<int32_t>({1, 2, 3, 4, 5});
  auto c1 = makeNullableArrayVector<int32_t>({
      {100, std::nullopt},
      {500, 120},
      {std::nullopt},
      {5, std::nullopt, 0},
      {3, 1},
  });
  auto input = makeRowVector({c0, c1});
  std::vector<std::optional<bool>> expectedResult{
      false,
      true,
      std::nullopt,
      false,
      true,
  };
  testAllMatchExpr(
      expectedResult,
      "all_match(c1, if (c0 <= 2, x -> (x > 100), x -> (10 / x > 2)))",
      input);
}
