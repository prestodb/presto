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

#include "velox/exec/tests/utils/FunctionUtils.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"
#include "velox/functions/sparksql/Register.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::functions::sparksql::test {
namespace {

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
    auto min = std::numeric_limits<T>::min();
    auto max = std::numeric_limits<T>::max();
    auto input = makeNullableArrayVector<T>({
        {},
        {9, 8, 12},
        {5, 6, 1, std::nullopt, 0, 99, -99},
        {std::nullopt, std::nullopt},
        {max, std::nullopt, min, -1, 1, 0},
    });
    auto expected = makeNullableArrayVector<T>({
        {},
        {8, 9, 12},
        {-99, 0, 1, 5, 6, 99, std::nullopt},
        {std::nullopt, std::nullopt},
        {min, -1, 0, 1, max, std::nullopt},
    });
    testArraySort(input, expected);
  }

  template <typename T>
  void testFloatingPoint() {
    auto lowest = std::numeric_limits<T>::lowest();
    auto max = std::numeric_limits<T>::max();
    auto inf = std::numeric_limits<T>::infinity();
    auto nan = std::numeric_limits<T>::quiet_NaN();
    auto input = makeNullableArrayVector<T>({
        {},
        {1.0001, std::nullopt, 1.0, -2.0, 3.03, std::nullopt},
        {std::nullopt, std::nullopt},
        {max, lowest, nan, inf, -9.009, 9.009, std::nullopt, 0.0},
    });
    auto expected = makeNullableArrayVector<T>({
        {},
        {-2.0, 1.0, 1.0001, 3.03, std::nullopt, std::nullopt},
        {std::nullopt, std::nullopt},
        {lowest, -9.009, 0.0, 9.009, max, inf, nan, std::nullopt},
    });
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
  auto input = makeNullableArrayVector<std::string>({
      {},
      {"spiderman", "captainamerica", "ironman", "hulk", "deadpool", "thor"},
      {"s", "c", "", std::nullopt, "h", "d"},
      {std::nullopt, std::nullopt},
  });
  auto expected = makeNullableArrayVector<std::string>({
      {},
      {"captainamerica", "deadpool", "hulk", "ironman", "spiderman", "thor"},
      {"", "c", "d", "h", "s", std::nullopt},
      {std::nullopt, std::nullopt},
  });
  testArraySort(input, expected);
}

TEST_F(ArraySortTest, timestamp) {
  using T = Timestamp;
  auto input = makeNullableArrayVector<Timestamp>({
      {},
      {T{0, 1}, T{1, 0}, std::nullopt, T{4, 20}, T{3, 30}},
      {std::nullopt, std::nullopt},
  });
  auto expected = makeNullableArrayVector<Timestamp>({
      {},
      {T{0, 1}, T{1, 0}, T{3, 30}, T{4, 20}, std::nullopt},
      {std::nullopt, std::nullopt},
  });
  testArraySort(input, expected);
}

TEST_F(ArraySortTest, date) {
  using D = Date;
  auto input = makeNullableArrayVector<Date>({
      {},
      {D{0}, D{1}, std::nullopt, D{4}, D{3}},
      {std::nullopt, std::nullopt},
  });
  auto expected = makeNullableArrayVector<Date>({
      {},
      {D{0}, D{1}, D{3}, D{4}, std::nullopt},
      {std::nullopt, std::nullopt},
  });
  testArraySort(input, expected);
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
