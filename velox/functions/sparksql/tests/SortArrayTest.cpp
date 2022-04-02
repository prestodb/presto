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

#include <algorithm>
#include <limits>
#include <optional>

#include "velox/functions/prestosql/tests/FunctionBaseTest.h"
#include "velox/functions/sparksql/Register.h"
#include "velox/functions/sparksql/tests/ArraySortTestData.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::functions::sparksql::test {

using namespace facebook::velox::test;
namespace {

using facebook::velox::functions::test::FunctionBaseTest;

class SortArrayTest : public SparkFunctionBaseTest {
 protected:
  void testSortArray(
      const VectorPtr& input,
      const VectorPtr& expectedAsc,
      const VectorPtr& expectedDesc) {
    // Verify that by default array is sorted in ascending order.
    std::string expr = "sort_array(c0)";
    auto result = evaluate<ArrayVector>(expr, makeRowVector({input}));
    assertEqualVectors(expectedAsc, result, expr);

    // Verify sort order with asc flag set to true.
    expr = "sort_array(c0, true)";
    auto resultAsc = evaluate<ArrayVector>(expr, makeRowVector({input}));
    assertEqualVectors(expectedAsc, result, expr);

    // Verify sort order with asc flag set to false.
    expr = "sort_array(c0, false)";
    auto resultDesc = evaluate<ArrayVector>(expr, makeRowVector({input}));
    assertEqualVectors(expectedDesc, resultDesc, expr);
  }

  template <typename T>
  void testInt() {
    auto input = makeNullableArrayVector(intInput<T>());
    auto expected = intAscNullSmallest<T>();
    testSortArray(
        input,
        makeNullableArrayVector(expected),
        makeNullableArrayVector(reverseNested(expected)));
  }

  template <typename T>
  void testFloatingPoint() {
    auto input = makeNullableArrayVector(floatingPointInput<T>());
    auto expected = floatingPointAscNullSmallest<T>();
    testSortArray(
        input,
        makeNullableArrayVector(expected),
        makeNullableArrayVector(reverseNested(expected)));
  }
};

TEST_F(SortArrayTest, invalidInput) {
  auto arg0 = makeNullableArrayVector<int>({{0, 1}});
  std::vector<bool> v = {false};
  auto arg1 = makeFlatVector<bool>(v);
  ASSERT_THROW(
      evaluate<ArrayVector>("sort_array(c0, c1)", makeRowVector({arg0, arg1})),
      VeloxException);
}

TEST_F(SortArrayTest, int8) {
  testInt<int8_t>();
}

TEST_F(SortArrayTest, int16) {
  testInt<int16_t>();
}

TEST_F(SortArrayTest, int32) {
  testInt<int32_t>();
}

TEST_F(SortArrayTest, int64) {
  testInt<int64_t>();
}

TEST_F(SortArrayTest, float) {
  testFloatingPoint<float>();
}

TEST_F(SortArrayTest, double) {
  testFloatingPoint<double>();
}

TEST_F(SortArrayTest, string) {
  auto input = makeNullableArrayVector(stringInput());
  auto expected = stringAscNullSmallest();
  testSortArray(
      input,
      makeNullableArrayVector(expected),
      makeNullableArrayVector(reverseNested(expected)));
}

TEST_F(SortArrayTest, timestamp) {
  auto input = makeNullableArrayVector(timestampInput());
  auto expected = timestampAscNullSmallest();
  testSortArray(
      input,
      makeNullableArrayVector(expected),
      makeNullableArrayVector(reverseNested(expected)));
}

TEST_F(SortArrayTest, date) {
  auto input = makeNullableArrayVector(dateInput());
  auto expected = dateAscNullSmallest();
  testSortArray(
      input,
      makeNullableArrayVector(expected),
      makeNullableArrayVector(reverseNested(expected)));
}

TEST_F(SortArrayTest, bool) {
  auto input = makeNullableArrayVector(boolInput());
  auto expected = boolAscNullSmallest();
  testSortArray(
      input,
      makeNullableArrayVector(expected),
      makeNullableArrayVector(reverseNested(expected)));
}

TEST_F(SortArrayTest, array) {
  auto input = makeNestedArrayVector(arrayInput());
  auto expected = arrayAscNullSmallest();
  testSortArray(
      input,
      makeNestedArrayVector(expected),
      makeNestedArrayVector(reverseNested(expected)));
}

TEST_F(SortArrayTest, map) {
  auto input = makeArrayOfMapVector(mapInput());
  auto expected = mapAscNullSmallest();
  testSortArray(
      input,
      makeArrayOfMapVector(expected),
      makeArrayOfMapVector(reverseNested(expected)));
}

TEST_F(SortArrayTest, row) {
  auto rowType = ROW({INTEGER(), VARCHAR()});
  auto input = makeArrayOfRowVector(rowType, rowInput());
  auto expected = rowAscNullSmallest();
  testSortArray(
      input,
      makeArrayOfRowVector(rowType, expected),
      makeArrayOfRowVector(rowType, reverseNested(expected)));
}
} // namespace
} // namespace facebook::velox::functions::sparksql::test
