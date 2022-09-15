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

class ArrayMinTest : public FunctionBaseTest {
 protected:
  template <typename T, typename TExpected = T>
  void testExpr(
      const VectorPtr& expected,
      const std::string& expression,
      const std::vector<VectorPtr>& input) {
    auto result =
        evaluate<SimpleVector<TExpected>>(expression, makeRowVector(input));
    assertEqualVectors(expected, result);
  }

  void testDocExample() {
    auto arrayVector = makeNullableArrayVector<int64_t>(
        {{1, 2, 3}, {-1, -2, -2}, {-1, -2, std::nullopt}, {}});
    auto expected =
        makeNullableFlatVector<int64_t>({1, -2, std::nullopt, std::nullopt});
    testExpr<int64_t>(expected, "array_min(C0)", {arrayVector});
  }

  template <typename T>
  void testIntNullable() {
    auto arrayVector = makeNullableArrayVector<T>(
        {{-1, 0, 1, 2, 3, 4},
         {4, 3, 2, 1, 0, -1, -2},
         {-5, -4, -3, -2, -1},
         {101, 102, 103, 104, std::nullopt},
         {std::nullopt, -1, -2, -3, -4},
         {},
         {std::nullopt}});
    auto expected = makeNullableFlatVector<T>(
        {-1, -2, -5, std::nullopt, std::nullopt, std::nullopt, std::nullopt});
    testExpr<T>(expected, "array_min(C0)", {arrayVector});
  }

  template <typename T>
  void testInt() {
    auto arrayVector = makeArrayVector<T>(
        {{-1, 0, 1, 2, 3, 4},
         {4, 3, 2, 1, 0, -1, -2},
         {-5, -4, -3, -2, -1},
         {101, 102, 103, 104, 105},
         {}});
    auto expected = makeNullableFlatVector<T>({-1, -2, -5, 101, std::nullopt});
    testExpr<T>(expected, "array_min(C0)", {arrayVector});
  }

  void testInLineVarcharNullable() {
    using S = StringView;

    auto arrayVector = makeNullableArrayVector<StringView>({
        {S("red"), S("blue")},
        {std::nullopt, S("blue"), S("yellow"), S("orange")},
        {},
        {S("red"), S("purple"), S("green")},
    });
    auto expected = makeNullableFlatVector<StringView>(
        {S("blue"), std::nullopt, std::nullopt, S("green")});
    testExpr<StringView>(expected, "array_min(C0)", {arrayVector});
  }

  void testVarcharNullable() {
    using S = StringView;
    // use > 12 length string to avoid inlining
    auto arrayVector = makeNullableArrayVector<StringView>({
        {S("red shiny car ahead"), S("blue clear sky above")},
        {std::nullopt,
         S("blue clear sky above"),
         S("yellow rose flowers"),
         S("orange beautiful sunset")},
        {},
        {S("red shiny car ahead"),
         S("purple is an elegant color"),
         S("green plants make us happy")},
    });
    auto expected = makeNullableFlatVector<StringView>(
        {S("blue clear sky above"),
         std::nullopt,
         std::nullopt,
         S("green plants make us happy")});
    testExpr<StringView>(expected, "array_min(C0)", {arrayVector});
  }

  void testBoolNullable() {
    auto arrayVector = makeNullableArrayVector<bool>(
        {{true, false},
         {true},
         {false},
         {},
         {true, false, true, std::nullopt},
         {std::nullopt, true, false, true},
         {false, false, false},
         {true, true, true}});

    auto expected = makeNullableFlatVector<bool>(
        {false,
         true,
         false,
         std::nullopt,
         std::nullopt,
         std::nullopt,
         false,
         true});
    testExpr<bool>(expected, "array_min(C0)", {arrayVector});
  }

  void testBool() {
    auto arrayVector = makeArrayVector<bool>(
        {{true, false},
         {true},
         {false},
         {},
         {false, false, false},
         {true, true, true}});

    auto expected = makeNullableFlatVector<bool>(
        {false, true, false, std::nullopt, false, true});
    testExpr<bool>(expected, "array_min(C0)", {arrayVector});
  }
};

} // namespace
TEST_F(ArrayMinTest, docArrays) {
  testDocExample();
}

TEST_F(ArrayMinTest, intArrays) {
  testIntNullable<int8_t>();
  testIntNullable<int16_t>();
  testIntNullable<int32_t>();
  testIntNullable<int64_t>();
  testInt<int8_t>();
  testInt<int16_t>();
  testInt<int32_t>();
  testInt<int64_t>();
}

TEST_F(ArrayMinTest, varcharArrays) {
  testInLineVarcharNullable();
  testVarcharNullable();
}

TEST_F(ArrayMinTest, boolArrays) {
  testBoolNullable();
  testBool();
}
