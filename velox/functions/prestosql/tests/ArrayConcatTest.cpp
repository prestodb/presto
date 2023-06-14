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

class ArrayConcatTest : public FunctionBaseTest {
 protected:
  void testExpression(
      const std::string& expression,
      const std::vector<VectorPtr>& input,
      const VectorPtr& expected) {
    auto result = evaluate(expression, makeRowVector(input));
    assertEqualVectors(expected, result);
  }

  void testExpressionWithError(
      const std::string& expression,
      const std::vector<VectorPtr>& input,
      const std::string& expectedError) {
    VELOX_ASSERT_THROW(
        evaluate(expression, makeRowVector(input)), expectedError);
  }
};

/// Concatenate two integer arrays.
TEST_F(ArrayConcatTest, intArray) {
  const auto intArray = makeArrayVector<int64_t>(
      {{1, 2, 3, 4}, {3, 4, 5}, {7, 8, 9}, {10, 20, 30}});
  const auto emptyArray = makeArrayVector<int64_t>({{9, 8}, {}, {777}, {}});
  VectorPtr expected;

  expected = makeArrayVector<int64_t>({
      {1, 2, 3, 4, 9, 8},
      {3, 4, 5},
      {7, 8, 9, 777},
      {10, 20, 30},
  });
  testExpression("concat(c0, c1)", {intArray, emptyArray}, expected);

  expected = makeArrayVector<int64_t>({
      {9, 8, 1, 2, 3, 4},
      {3, 4, 5},
      {777, 7, 8, 9},
      {10, 20, 30},
  });
  testExpression("concat(c0, c1)", {emptyArray, intArray}, expected);
}

/// Concatenate two integer arrays with null.
TEST_F(ArrayConcatTest, nullArray) {
  const auto intArray =
      makeArrayVector<int64_t>({{1, 2, 3, 4}, {7, 8, 9}, {10, 20, 30}});
  const auto nullArray = makeNullableArrayVector<int64_t>({
      {{std::nullopt, std::nullopt}},
      std::nullopt,
      {{1}},
  });
  VectorPtr expected;

  expected = makeNullableArrayVector<int64_t>({
      {{1, 2, 3, 4, std::nullopt, std::nullopt}},
      std::nullopt,
      {{10, 20, 30, 1}},
  });
  testExpression("concat(c0, c1)", {intArray, nullArray}, expected);

  expected = makeNullableArrayVector<int64_t>({
      {{std::nullopt, std::nullopt, 1, 2, 3, 4}},
      std::nullopt,
      {{1, 10, 20, 30}},
  });
  testExpression("concat(c0, c1)", {nullArray, intArray}, expected);
}

/// Concatenate more than two arrays.
TEST_F(ArrayConcatTest, arity) {
  const auto array1 =
      makeArrayVector<int64_t>({{1, 2, 3, 4}, {3, 4, 5}, {7, 8, 9}});
  const auto array2 = makeArrayVector<int64_t>({{9, 8}, {}, {777}});
  const auto array3 = makeArrayVector<int64_t>({{123, 42}, {55}, {10, 20, 30}});
  const auto array4 = makeNullableArrayVector<int64_t>({
      {{std::nullopt, std::nullopt}},
      std::nullopt,
      {{std::nullopt, std::nullopt, std::nullopt}},
  });
  VectorPtr expected;

  expected = makeArrayVector<int64_t>(
      {{1, 2, 3, 4, 9, 8, 123, 42}, {3, 4, 5, 55}, {7, 8, 9, 777, 10, 20, 30}});
  testExpression("concat(c0, c1, c2)", {array1, array2, array3}, expected);

  expected = makeArrayVector<int64_t>(
      {{123, 42, 9, 8, 1, 2, 3, 4, 9, 8},
       {55, 3, 4, 5},
       {10, 20, 30, 777, 7, 8, 9, 777}});
  testExpression(
      "concat(c0, c1, c2, c3)", {array3, array2, array1, array2}, expected);

  expected = makeNullableArrayVector<int64_t>(
      {{{1, 2, 3, 4, std::nullopt, std::nullopt, 123, 42, 9, 8}},
       std::nullopt,
       {{7, 8, 9, std::nullopt, std::nullopt, std::nullopt, 10, 20, 30, 777}}});
  testExpression(
      "concat(c0, c1, c2, c3)", {array1, array4, array3, array2}, expected);

  testExpressionWithError(
      "concat(c0)", {array1}, "There must be 2 or more arguments to concat");

  std::stringstream expression;
  std::vector<VectorPtr> inputArrayVector;
  inputArrayVector.reserve(255);
  expression << "concat(";
  for (int i = 0; i < 255; i++) {
    expression << ((i > 0) ? ", " : "") << "c" << i;
    inputArrayVector.push_back(array1);
  }
  expression << ")";
  testExpressionWithError(
      expression.str(),
      inputArrayVector,
      "Too many arguments for concat function");
}

/// Concatenate complex types.
TEST_F(ArrayConcatTest, complexTypes) {
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

  // [[1, 1], [2, 2], [1, 1], [2, 2], [3, 3]]
  // [[3, 3], [4, 4], [4, 4]]
  // [[5, 5], [6, 6], [5, 5], [6, 6]]
  auto expected = makeArrayVector(
      {0, 5, 8},
      makeArrayVector<int64_t>(
          {{1, 1},
           {2, 2},
           {1, 1},
           {2, 2},
           {3, 3},
           {3, 3},
           {4, 4},
           {4, 4},
           {5, 5},
           {6, 6},
           {5, 5},
           {6, 6}}));

  testExpression("concat(c0, c1)", {arrayOfArrays1, arrayOfArrays2}, expected);
}
} // namespace
