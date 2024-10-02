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
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::functions::test;

namespace {

class ArrayHasDuplicatesTest : public FunctionBaseTest {
 protected:
  // Evaluate an expression.
  void testExpr(
      const VectorPtr& expected,
      const std::string& expression,
      const std::vector<VectorPtr>& input) {
    auto result =
        evaluate<SimpleVector<bool>>(expression, makeRowVector(input));
    assertEqualVectors(expected, result);
  }
};

} // namespace

// Test bigint arrays.
TEST_F(ArrayHasDuplicatesTest, bigints) {
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

  auto expected = makeNullableFlatVector<bool>(
      {false, false, false, false, true, true, true, true, true, true});

  testExpr(expected, "array_has_duplicates(C0)", {array});
}

// Test inline (short) strings.
TEST_F(ArrayHasDuplicatesTest, inlineStrings) {
  using S = StringView;

  auto array = makeNullableArrayVector<StringView>({
      {},
      {""_sv},
      {std::nullopt},
      {S("a"), S("b")},
      {S("a"), std::nullopt, S("b")},
      {S("a"), S("a")},
      {S("b"), S("a"), S("b"), S("a"), S("a")},
      {std::nullopt, std::nullopt},
      {S("b"), std::nullopt, S("a"), S("a"), std::nullopt, S("b")},
  });

  auto expected = makeFlatVector<bool>(
      {false, false, false, false, false, true, true, true, true});

  testExpr(expected, "array_has_duplicates(C0)", {array});
}

// Test non-inline (> 12 character length) strings.
TEST_F(ArrayHasDuplicatesTest, longStrings) {
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
  auto expected = makeFlatVector<bool>({false, true, true});
  testExpr(expected, "array_has_duplicates(C0)", {array});
}

TEST_F(ArrayHasDuplicatesTest, nullFreeBigints) {
  auto array = makeArrayVector<int64_t>({
      {1,
       std::numeric_limits<int64_t>::min(),
       std::numeric_limits<int64_t>::max()},
      {2, 1, 1, -2},
      {1, 1, 1},
  });

  auto expected = makeNullableFlatVector<bool>({false, true, true});

  testExpr(expected, "array_has_duplicates(C0)", {array});
}

TEST_F(ArrayHasDuplicatesTest, nullFreeStrings) {
  using S = StringView;

  auto array = makeArrayVector<StringView>(
      {{S("red shiny car ahead"), S("blue clear sky above")},
       {S("red shiny car ahead"),
        S("blue clear sky above"),
        S("blue clear sky above")},
       {S("a"), S("b")},
       {S("a"), S("b"), S("b")}

      });
  auto expected = makeFlatVector<bool>({false, true, false, true});
  testExpr(expected, "array_has_duplicates(C0)", {array});
}
