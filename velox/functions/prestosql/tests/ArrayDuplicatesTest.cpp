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

// Class to test the array_duplicates operator.
class ArrayDuplicatesTest : public FunctionBaseTest {
 protected:
  // Evaluate an expression.
  void testExpr(
      const VectorPtr& expected,
      const std::string& expression,
      const std::vector<VectorPtr>& input) {
    auto result = evaluate<ArrayVector>(expression, makeRowVector(input));
    assertEqualVectors(expected, result);
  }

  // Execute test for bigint type.
  void testBigint() {
    auto array = makeNullableArrayVector<int64_t>({
        {},
        {1,
         std::numeric_limits<int64_t>::min(),
         std::numeric_limits<int64_t>::max()},
        {std::nullopt},
        {1, 2, 3},
        {2, 1, 1, -2},
        {1, 1, 1},
        {-1, std::nullopt, -1, -1},
        {std::nullopt, std::nullopt, std::nullopt},
        {1, -2, -2, 8, -2, 4, 8, 1},
        {std::numeric_limits<int64_t>::max(),
         std::numeric_limits<int64_t>::max(),
         1,
         std::nullopt,
         0,
         1,
         std::nullopt,
         0},
    });

    auto expected = makeNullableArrayVector<int64_t>({
        {},
        {},
        {},
        {},
        {1},
        {1},
        {-1},
        {std::nullopt},
        {-2, 1, 8},
        {std::nullopt, 0, 1, std::numeric_limits<int64_t>::max()},
    });

    testExpr(expected, "array_duplicates(C0)", {array});
  }
};

} // namespace

// Test integer arrays.
TEST_F(ArrayDuplicatesTest, integerArrays) {
  testBigint();
}

// Test inline (short) strings.
TEST_F(ArrayDuplicatesTest, inlineStringArrays) {
  using S = StringView;

  auto array = makeNullableArrayVector<StringView>({
      {},
      {S("")},
      {std::nullopt},
      {S("a"), S("b")},
      {S("a"), std::nullopt, S("b")},
      {S("a"), S("a")},
      {S("b"), S("a"), S("b"), S("a"), S("a")},
      {std::nullopt, std::nullopt},
      {S("b"), std::nullopt, S("a"), S("a"), std::nullopt, S("b")},
  });

  auto expected = makeNullableArrayVector<StringView>({
      {},
      {},
      {},
      {},
      {},
      {S("a")},
      {S("a"), S("b")},
      {std::nullopt},
      {std::nullopt, S("a"), S("b")},
  });

  testExpr(expected, "array_duplicates(C0)", {array});
}

// Test non-inline (> 12 character length) strings.
TEST_F(ArrayDuplicatesTest, stringArrays) {
  using S = StringView;

  auto array = makeNullableArrayVector<StringView>({
      {S("red shiny car ahead"), S("blue clear sky above")},
      {S("blue clear sky above"),
       S("yellow rose flowers"),
       std::nullopt,
       S("blue clear sky above"),
       S("orange beautiful sunset")},
      {
          S("red shiny car ahead"),
          std::nullopt,
          S("purple is an elegant color"),
          S("red shiny car ahead"),
          S("green plants make us happy"),
          S("purple is an elegant color"),
          std::nullopt,
          S("purple is an elegant color"),
      },
  });

  auto expected = makeNullableArrayVector<StringView>({
      {},
      {S("blue clear sky above")},
      {std::nullopt, S("purple is an elegant color"), S("red shiny car ahead")},
  });

  testExpr(expected, "array_duplicates(C0)", {array});
}

TEST_F(ArrayDuplicatesTest, nonContiguousRows) {
  auto c0 = makeFlatVector<int64_t>(4, [](auto row) { return row; });
  auto c1 = makeArrayVector<int64_t>({
      {1, 1, 2, 3, 3},
      {1, 1, 2, 3, 4, 4},
      {1, 1, 2, 3, 4, 5, 5},
      {1, 1, 2, 3, 3, 4, 5, 6, 6},
  });

  auto c2 = makeArrayVector<int64_t>({
      {0, 0, 1, 1, 2, 3, 3},
      {0, 0, 1, 1, 2, 3, 4, 4},
      {0, 0, 1, 1, 2, 3, 4, 5, 5},
      {0, 0, 1, 1, 2, 3, 4, 5, 6, 6},
  });

  auto expected = makeArrayVector<int64_t>({
      {1, 3},
      {0, 1, 4},
      {1, 5},
      {0, 1, 6},
  });

  auto result = evaluate<ArrayVector>(
      "if(c0 % 2 = 0, array_duplicates(c1), array_duplicates(c2))",
      makeRowVector({c0, c1, c2}));
  assertEqualVectors(expected, result);
}
