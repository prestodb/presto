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

#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::functions::test;

namespace facebook::velox {

class DecimalArithmeticTest : public FunctionBaseTest {
 public:
  DecimalArithmeticTest() {
    options_.parseDecimalAsDouble = false;
  }

 protected:
  template <typename T>
  void testDecimalExpr(
      const VectorPtr& expected,
      const std::string& expression,
      const std::vector<VectorPtr>& input) {
    auto result = evaluate<SimpleVector<T>>(expression, makeRowVector(input));
    assertEqualVectors(expected, result);
    testOpDictVectors<T>(expression, expected, input);
  }

  template <typename T>
  void testOpDictVectors(
      const std::string& operation,
      const VectorPtr& expected,
      const std::vector<VectorPtr>& flatVector) {
    // Dictionary vectors as arguments.
    auto newSize = flatVector[0]->size() * 2;
    std::vector<VectorPtr> dictVectors;
    for (auto i = 0; i < flatVector.size(); ++i) {
      auto indices = makeIndices(newSize, [&](int row) { return row / 2; });
      dictVectors.push_back(
          VectorTestBase::wrapInDictionary(indices, newSize, flatVector[i]));
    }
    auto resultIndices = makeIndices(newSize, [&](int row) { return row / 2; });
    auto expectedResultDictionary =
        VectorTestBase::wrapInDictionary(resultIndices, newSize, expected);
    auto actual =
        evaluate<SimpleVector<T>>(operation, makeRowVector(dictVectors));
    assertEqualVectors(expectedResultDictionary, actual);
  }
};
} // namespace facebook::velox

TEST_F(DecimalArithmeticTest, add) {
  auto expectedLongFlat =
      makeLongDecimalFlatVector({2000, 4000}, DECIMAL(19, 3));
  auto shortFlat = makeShortDecimalFlatVector({1000, 2000}, DECIMAL(18, 3));

  // Add short and short, returning long.
  testDecimalExpr<UnscaledLongDecimal>(
      expectedLongFlat, "plus(c0, c1)", {shortFlat, shortFlat});

  // Add short and long, returning long.
  auto longFlat = makeLongDecimalFlatVector({1000, 2000}, DECIMAL(19, 3));
  expectedLongFlat = makeLongDecimalFlatVector({2000, 4000}, DECIMAL(20, 3));
  testDecimalExpr<UnscaledLongDecimal>(
      expectedLongFlat, "plus(c0, c1)", {shortFlat, longFlat});

  // Add short and long, returning long.
  testDecimalExpr<UnscaledLongDecimal>(
      expectedLongFlat, "plus(c0, c1)", {longFlat, shortFlat});

  // Add long and long, returning long.
  testDecimalExpr<UnscaledLongDecimal>(
      expectedLongFlat, "c0 + c1", {longFlat, longFlat});

  // Add short and short, returning short.
  shortFlat = makeShortDecimalFlatVector({1000, 2000}, DECIMAL(10, 3));
  auto expectedShortFlat =
      makeShortDecimalFlatVector({2000, 4000}, DECIMAL(11, 3));
  testDecimalExpr<UnscaledShortDecimal>(
      expectedShortFlat, "c0 + c1", {shortFlat, shortFlat});

  auto expectedConstantFlat =
      makeShortDecimalFlatVector({2000, 3000}, DECIMAL(11, 3));

  // Constant and Flat arguments.
  testDecimalExpr<UnscaledShortDecimal>(
      expectedConstantFlat, "plus(1.00, c0)", {shortFlat});

  // Flat and Constant arguments.
  testDecimalExpr<UnscaledShortDecimal>(
      expectedConstantFlat, "plus(c0,1.00)", {shortFlat});

  auto shortWithNullsA = makeNullableShortDecimalFlatVector(
      {1, 2, std::nullopt, 6, std::nullopt}, DECIMAL(10, 3));
  auto shortWithNullsB = makeNullableShortDecimalFlatVector(
      {1, 2, 5, std::nullopt, std::nullopt}, DECIMAL(10, 3));
  auto expectedShortWithNulls = makeNullableShortDecimalFlatVector(
      {2, 4, std::nullopt, std::nullopt, std::nullopt}, DECIMAL(11, 3));
  testDecimalExpr<UnscaledShortDecimal>(
      expectedShortWithNulls,
      "plus(c0, c1)",
      {shortWithNullsA, shortWithNullsB});
}
