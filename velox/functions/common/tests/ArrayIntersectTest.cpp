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
#include "velox/functions/common/tests/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::functions::test;

namespace {

class ArrayIntersectTest : public FunctionBaseTest {
 protected:
  void testExpr(
      const VectorPtr& expected,
      const std::string& expression,
      const std::vector<VectorPtr>& input) {
    auto result = evaluate<ArrayVector>(expression, makeRowVector(input));
    assertEqualVectors(expected, result);
  }

  template <typename T>
  void testInt() {
    auto array1 = makeNullableArrayVector<T>({
        {1, -2, 3, std::nullopt, 4, 5, 6, std::nullopt},
        {1, 2, -2, 1},
        {3, 8, std::nullopt},
        {1, 1, -2, -2, -2, 4, 8},
    });
    auto array2 = makeNullableArrayVector<T>({
        {1, -2, 4},
        {1, -2, 4},
        {1, -2, 4},
        {1, -2, 4},
    });
    auto expected = makeNullableArrayVector<T>({
        {1, -2, 4},
        {1, -2},
        {},
        {1, -2, 4},
    });
    testExpr(expected, "array_intersect(C0, C1)", {array1, array2});
    testExpr(expected, "array_intersect(C1, C0)", {array1, array2});

    // Change C1.
    array2 = makeNullableArrayVector<T>({
        {10, -24, 43},
        {std::nullopt, -2, 2},
        {std::nullopt, std::nullopt, std::nullopt},
        {8, 1, 8, 1},
    });
    expected = makeNullableArrayVector<T>({
        {},
        {2, -2},
        {std::nullopt},
        {1, 8},
    });
    testExpr(expected, "array_intersect(C0, C1)", {array1, array2});
  }
};

} // namespace

TEST_F(ArrayIntersectTest, intArrays) {
  testInt<int8_t>();
  testInt<int16_t>();
  testInt<int32_t>();
  testInt<int64_t>();
}

TEST_F(ArrayIntersectTest, strArrays) {
  using S = StringView;

  auto array1 = makeNullableArrayVector<StringView>({
      {S("a"), std::nullopt, S("b")},
      {S("a"), S("b"), S("a"), S("a")},
      {std::nullopt, S("b"), std::nullopt},
      {S("abc")},
  });
  auto array2 = makeNullableArrayVector<StringView>({
      {S("a")},
      {S("a"), S("b"), S("b")},
      {std::nullopt, std::nullopt, std::nullopt},
      {S("abc"), S("a"), S("b")},
  });
  auto expected = makeNullableArrayVector<StringView>({
      {S("a")},
      {S("a"), S("b")},
      {std::nullopt},
      {S("abc")},
  });
  testExpr(expected, "array_intersect(C0, C1)", {array1, array2});
  testExpr(expected, "array_intersect(C1, C0)", {array1, array2});
}

// When one of the arrays is constant.
TEST_F(ArrayIntersectTest, constant) {
  auto array1 = makeNullableArrayVector<int32_t>({
      {1, -2, 3, std::nullopt, 4, 5, 6, std::nullopt},
      {1, 2, -2, 1},
      {3, 8, std::nullopt},
      {1, 1, -2, -2, -2, 4, 8},
  });
  auto expected = makeNullableArrayVector<int32_t>({
      {1, -2, 4},
      {1, -2},
      {},
      {1, -2, 4},
  });
  testExpr(expected, "array_intersect(C0, ARRAY[1,-2,4])", {array1});
  testExpr(expected, "array_intersect(ARRAY[1,-2,4], C0)", {array1});
  testExpr(
      expected, "array_intersect(ARRAY[1,1,-2,1,-2,4,1,4,4], C0)", {array1});

  // Array containing NULLs.
  expected = makeNullableArrayVector<int32_t>({
      {1, std::nullopt, 4},
      {1},
      {std::nullopt},
      {1, 4},
  });
  testExpr(expected, "array_intersect(C0, ARRAY[1,NULL,4])", {array1});
}

TEST_F(ArrayIntersectTest, wrongTypes) {
  auto expected = makeNullableArrayVector<int32_t>({{1}});
  auto array1 = makeNullableArrayVector<int32_t>({{1}});

  EXPECT_THROW(
      testExpr(expected, "array_intersect(1, 1)", {array1}),
      std::invalid_argument);
  EXPECT_THROW(
      testExpr(expected, "array_intersect(C0, 1)", {array1}),
      std::invalid_argument);
  EXPECT_THROW(
      testExpr(expected, "array_intersect(ARRAY[1], 1)", {array1}),
      std::invalid_argument);
  EXPECT_THROW(
      testExpr(expected, "array_intersect(C0)", {array1}),
      std::invalid_argument);
  EXPECT_THROW(
      testExpr(expected, "array_intersect(C0, C0, C0)", {array1}),
      std::invalid_argument);

  EXPECT_NO_THROW(testExpr(expected, "array_intersect(C0, C0)", {array1}));
}
