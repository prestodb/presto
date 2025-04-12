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

#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

using namespace facebook::velox::test;

namespace facebook::velox::functions::sparksql::test {
namespace {

class ArrayPrependTest : public SparkFunctionBaseTest {
 protected:
  void testExpression(
      const std::string& expression,
      const std::vector<VectorPtr>& input,
      const VectorPtr& expected) {
    auto result = evaluate(expression, makeRowVector(input));
    assertEqualVectors(expected, result);
  }

  template <typename T>
  void testFloats() {
    static const T kNaN = std::numeric_limits<T>::quiet_NaN();
    static const T kSNaN = std::numeric_limits<T>::signaling_NaN();
    {
      const auto arrayVector = makeNullableArrayVector<T>(
          {{std::nullopt, 2, 3, std::nullopt, 4},
           {3, 4, 5, kNaN, 3, 4, kNaN},
           {kSNaN, 8, 9}});
      const auto elementVector = makeFlatVector<T>({1, kNaN, kNaN});
      const auto expected = makeNullableArrayVector<T>({
          {1, std::nullopt, 2, 3, std::nullopt, 4},
          {kNaN, 3, 4, 5, kNaN, 3, 4, kNaN},
          {kNaN, kSNaN, 8, 9},
      });
      testExpression(
          "array_prepend(c0, c1)", {arrayVector, elementVector}, expected);
    }
    // Test array_prepend with complex-type elements.
    {
      RowTypePtr rowType;
      if constexpr (std::is_same_v<T, float>) {
        rowType = ROW({REAL(), VARCHAR()});
      } else {
        static_assert(std::is_same_v<T, double>);
        rowType = ROW({DOUBLE(), VARCHAR()});
      }
      using ArrayOfRow = std::vector<std::optional<std::tuple<T, std::string>>>;
      std::vector<ArrayOfRow> data = {
          {{{1, "red"}}, {{2, "black"}}, {{3, "green"}}},
          {{{1, "red"}}, {{2, "black"}}, {{3, "green"}}}};
      auto arrayVector = makeArrayOfRowVector(data, rowType);

      const auto elementVector =
          makeConstantRow(rowType, variant::row({kNaN, "blue"}), 2);

      std::vector<ArrayOfRow> expectedData = {
          {{{kNaN, "blue"}}, {{1, "red"}}, {{2, "black"}}, {{3, "green"}}},
          {{{kNaN, "blue"}}, {{1, "red"}}, {{2, "black"}}, {{3, "green"}}}};
      const auto expected = makeArrayOfRowVector(expectedData, rowType);
      testExpression(
          "array_prepend(c0, c1)", {arrayVector, elementVector}, expected);
    }
  }
};

// Prepend simple-type elements to array.
TEST_F(ArrayPrependTest, intArrays) {
  const auto arrayVector = makeArrayVector<int64_t>(
      {{1, 2, 3, 4}, {3, 4, 5}, {7, 8, 9}, {10, 20, 30}});
  const auto elementVector = makeFlatVector<int64_t>({11, 22, 33, 44});

  VectorPtr expected = makeArrayVector<int64_t>({
      {11, 1, 2, 3, 4},
      {22, 3, 4, 5},
      {33, 7, 8, 9},
      {44, 10, 20, 30},
  });
  testExpression(
      "array_prepend(c0, c1)", {arrayVector, elementVector}, expected);
}

// Prepend simple-type elements to array.
TEST_F(ArrayPrependTest, floatArrays) {
  testFloats<float>();
  testFloats<double>();
}

// Prepend simple-type elements to array.
TEST_F(ArrayPrependTest, stringArrays) {
  const auto arrayVector = makeNullableArrayVector<std::string>(
      {{"foo", std::nullopt, "bar"}, {"bar", "baz"}});
  const auto elementVector =
      makeNullableFlatVector<std::string>({std::nullopt, "foo"});

  VectorPtr expected = makeNullableArrayVector<std::string>(
      {{std::nullopt, "foo", std::nullopt, "bar"}, {"foo", "bar", "baz"}});
  testExpression(
      "array_prepend(c0, c1)", {arrayVector, elementVector}, expected);
}

// Prepend simple-type elements to array.
TEST_F(ArrayPrependTest, boolArrays) {
  auto arrayVector = makeNullableArrayVector<bool>(
      {{true, false, true}, {true, false, std::nullopt}});
  const auto elementVector =
      makeNullableFlatVector<bool>({std::nullopt, false});

  VectorPtr expected = makeNullableArrayVector<bool>(
      {{std::nullopt, true, false, true}, {false, true, false, std::nullopt}});
  testExpression(
      "array_prepend(c0, c1)", {arrayVector, elementVector}, expected);
}

// Prepend null element to array or null array.
TEST_F(ArrayPrependTest, nullArrays) {
  const auto arrayVector = makeArrayVectorFromJson<int64_t>(
      {"[1, 2, 3, null]", "[3, 4, 5]", "null", "[10, 20, null]"});

  const auto elementVector = makeNullableFlatVector<int64_t>(
      {11, std::nullopt, std::nullopt, std::nullopt});

  VectorPtr expected = makeArrayVectorFromJson<int64_t>(
      {"[11, 1, 2, 3, null]",
       "[null, 3, 4, 5]",
       "null",
       "[null, 10, 20, null]"});
  testExpression(
      "array_prepend(c0, c1)", {arrayVector, elementVector}, expected);
}

// Prepend complex-type elements to array.
TEST_F(ArrayPrependTest, complexArray) {
  const auto arrayVector = makeNestedArrayVectorFromJson<int32_t>(
      {"[[1, 2, null], [null], [3, null, 4, null]]",
       "[[1, null, 2], null, [3, null, 4, null]]",
       "[[1, 2], [], [null, 3, 4]]",
       "null",
       "[[], [1, 2]]",
       "[[1, 2], null]",
       "[[1, 2], []]"});

  VectorPtr elementVector = makeArrayVectorFromJson<int32_t>(
      {"[1, 2, 3]",
       "[5]",
       "null",
       "[10, 20]",
       "[6]",
       "[7, null, 8]",
       "[null]"});

  const auto expected = makeNestedArrayVectorFromJson<int32_t>(
      {"[[1, 2, 3], [1, 2, null], [null], [3, null, 4, null]]",
       "[[5], [1, null, 2], null, [3, null, 4, null]]",
       "[null, [1, 2], [], [null, 3, 4]]",
       "null",
       "[[6], [], [1, 2]]",
       "[[7, null, 8], [1, 2], null]",
       "[[null], [1, 2], []]"});
  testExpression(
      "array_prepend(c0, c1)", {arrayVector, elementVector}, expected);
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
