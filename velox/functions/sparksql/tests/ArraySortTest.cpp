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
namespace {

using namespace facebook::velox::test;

using facebook::velox::functions::test::FunctionBaseTest;

class ArraySortTest : public SparkFunctionBaseTest {
 protected:
  void testArraySort(const VectorPtr& input, const VectorPtr& expected) {
    auto result =
        evaluate<ArrayVector>("array_sort(c0)", makeRowVector({input}));
    assertEqualVectors(expected, result);
  }

  template <typename T>
  void testInt() {
    auto input = makeNullableArrayVector(intInput<T>());
    auto expected = makeNullableArrayVector(intAscNullLargest<T>());
    testArraySort(input, expected);
  }

  template <typename T>
  void testFloatingPoint() {
    auto input = makeNullableArrayVector(floatingPointInput<T>());
    auto expected = makeNullableArrayVector(floatingPointAscNullLargest<T>());
    testArraySort(input, expected);
  }
};

TEST_F(ArraySortTest, int8) {
  testInt<int8_t>();
}

TEST_F(ArraySortTest, int16) {
  testInt<int16_t>();
}

TEST_F(ArraySortTest, int32) {
  testInt<int32_t>();
}

TEST_F(ArraySortTest, int64) {
  testInt<int64_t>();
}

TEST_F(ArraySortTest, float) {
  testFloatingPoint<float>();
}

TEST_F(ArraySortTest, double) {
  testFloatingPoint<double>();
}

TEST_F(ArraySortTest, string) {
  auto input = makeNullableArrayVector(stringInput());
  auto expected = makeNullableArrayVector(stringAscNullLargest());
  testArraySort(input, expected);
}

TEST_F(ArraySortTest, timestamp) {
  auto input = makeNullableArrayVector(timestampInput());
  auto expected = makeNullableArrayVector(timestampAscNullLargest());
  testArraySort(input, expected);
}

TEST_F(ArraySortTest, date) {
  auto input = makeNullableArrayVector(dateInput());
  auto expected = makeNullableArrayVector(dateAscNullLargest());
  testArraySort(input, expected);
}

TEST_F(ArraySortTest, bool) {
  auto input = makeNullableArrayVector(boolInput());
  auto expected = makeNullableArrayVector(boolAscNullLargest());
  testArraySort(input, expected);
}

TEST_F(ArraySortTest, array) {
  auto input = makeNestedArrayVector(arrayInput());
  auto expected = makeNestedArrayVector(arrayAscNullLargest());
  testArraySort(input, expected);
}

TEST_F(ArraySortTest, map) {
  auto input = makeArrayOfMapVector(mapInput());
  auto expected = makeArrayOfMapVector(mapAscNullLargest());
  testArraySort(input, expected);
}

TEST_F(ArraySortTest, row) {
  auto rowType = ROW({INTEGER(), VARCHAR()});
  auto input = makeArrayOfRowVector(rowType, rowInput());
  auto expected = makeArrayOfRowVector(rowType, rowAscNullLargest());
  testArraySort(input, expected);
}
} // namespace
} // namespace facebook::velox::functions::sparksql::test
