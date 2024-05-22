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
using namespace facebook::velox::exec;
using namespace facebook::velox::functions::test;

namespace {

class ArrayUnionTest : public FunctionBaseTest {
 protected:
  void testExpression(
      const std::string& expression,
      const std::vector<VectorPtr>& input,
      const VectorPtr& expected) {
    auto result = evaluate(expression, makeRowVector(input));
    assertEqualVectors(expected, result);
  }

  template <typename T>
  void floatArrayTest() {
    static const T kQuietNaN = std::numeric_limits<T>::quiet_NaN();
    static const T kSignalingNaN = std::numeric_limits<T>::signaling_NaN();
    static const T kInfinity = std::numeric_limits<T>::infinity();
    const auto array1 = makeArrayVector<T>(
        {{1.1, 2.2, 3.3, 4.4},
         {3.3, 4.4},
         {3.3, 4.4, kQuietNaN},
         {3.3, 4.4, kQuietNaN},
         {3.3, 4.4, kQuietNaN},
         {3.3, 4.4, kQuietNaN, kInfinity}});
    const auto array2 = makeArrayVector<T>(
        {{3.3, 4.4},
         {3.3, 5.5},
         {5.5},
         {3.3, kQuietNaN},
         {5.5, kSignalingNaN},
         {5.5, kInfinity}});
    VectorPtr expected;

    expected = makeArrayVector<T>({
        {1.1, 2.2, 3.3, 4.4},
        {3.3, 4.4, 5.5},
        {3.3, 4.4, kQuietNaN, 5.5},
        {3.3, 4.4, kQuietNaN},
        {3.3, 4.4, kQuietNaN, 5.5},
        {3.3, 4.4, kQuietNaN, kInfinity, 5.5},
    });
    testExpression("array_union(c0, c1)", {array1, array2}, expected);
  }
};

/// Union two integer arrays.
TEST_F(ArrayUnionTest, intArray) {
  const auto array1 = makeArrayVector<int64_t>(
      {{1, 2, 3, 4}, {3, 4, 5}, {7, 8, 9}, {10, 20, 30}});
  const auto array2 =
      makeArrayVector<int64_t>({{2, 4, 5}, {3, 4, 5}, {}, {40, 50}});
  VectorPtr expected;

  expected = makeArrayVector<int64_t>({
      {1, 2, 3, 4, 5},
      {3, 4, 5},
      {7, 8, 9},
      {10, 20, 30, 40, 50},
  });
  testExpression("array_union(c0, c1)", {array1, array2}, expected);

  expected = makeArrayVector<int64_t>({
      {2, 4, 5, 1, 3},
      {3, 4, 5},
      {7, 8, 9},
      {40, 50, 10, 20, 30},
  });
  testExpression("array_union(c0, c1)", {array2, array1}, expected);
}

/// Union two string arrays.
TEST_F(ArrayUnionTest, stringArray) {
  const auto array1 =
      makeArrayVector<StringView>({{"foo", "bar"}, {"foo", "baz"}});
  const auto array2 =
      makeArrayVector<StringView>({{"foo", "bar"}, {"bar", "baz"}});
  VectorPtr expected;

  expected = makeArrayVector<StringView>({
      {"foo", "bar"},
      {"foo", "baz", "bar"},
  });
  testExpression("array_union(c0, c1)", {array1, array2}, expected);
}

/// Union two integer arrays with null.
TEST_F(ArrayUnionTest, nullArray) {
  const auto array1 = makeNullableArrayVector<int64_t>({
      {{1, std::nullopt, 3, 4}},
      {7, 8, 9},
      {{10, std::nullopt, std::nullopt}},
  });
  const auto array2 = makeNullableArrayVector<int64_t>({
      {{std::nullopt, std::nullopt, 3, 5}},
      std::nullopt,
      {{1, 10}},
  });
  VectorPtr expected;

  expected = makeNullableArrayVector<int64_t>({
      {{1, std::nullopt, 3, 4, 5}},
      std::nullopt,
      {{10, std::nullopt, 1}},
  });
  testExpression("array_union(c0, c1)", {array1, array2}, expected);

  expected = makeNullableArrayVector<int64_t>({
      {{std::nullopt, 3, 5, 1, 4}},
      std::nullopt,
      {{1, 10, std::nullopt}},
  });
  testExpression("array_union(c0, c1)", {array2, array1}, expected);
}

/// Union complex types.
TEST_F(ArrayUnionTest, complexTypes) {
  auto baseVector = makeArrayVector<int64_t>(
      {{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}});

  // Create arrays of array vector using above base vector.
  // [[1, 1], [2, 2]]
  // [[3, 3], [4, 4]]
  // [[5, 5], [6, 6]]
  auto arrayOfArrays1 = makeArrayVector({0, 2, 4}, baseVector);
  // [[1, 1], [2, 2], [3, 3]]
  // [[4, 4]]
  // [[5, 5], [6, 6]]
  auto arrayOfArrays2 = makeArrayVector({0, 3, 4}, baseVector);

  // [[1, 1], [2, 2], [3, 3]]
  // [[3, 3], [4, 4]]
  // [[5, 5], [6, 6]]
  auto expected = makeArrayVector(
      {0, 3, 5},
      makeArrayVector<int64_t>(
          {{1, 1}, {2, 2}, {3, 3}, {3, 3}, {4, 4}, {5, 5}, {6, 6}}));

  testExpression(
      "array_union(c0, c1)", {arrayOfArrays1, arrayOfArrays2}, expected);
}

/// Union two floating point arrays including extreme values like infinity and
/// NaN.
TEST_F(ArrayUnionTest, floatingPointType) {
  floatArrayTest<float>();
  floatArrayTest<double>();
}
} // namespace
