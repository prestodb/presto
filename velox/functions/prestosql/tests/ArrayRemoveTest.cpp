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

class ArrayRemoveTest : public FunctionBaseTest {
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

  template <typename T>
  void testFloats() {
    static const T kNaN = std::numeric_limits<T>::quiet_NaN();
    static const T kSNaN = std::numeric_limits<T>::signaling_NaN();
    {
      const auto arrayVector = makeNullableArrayVector<T>(
          {{1, std::nullopt, 2, 3, std::nullopt, 4},
           {3, 4, 5, kNaN, 3, 4, kNaN},
           {kSNaN, 8, 9}});
      const auto elementVector = makeFlatVector<T>({3, kNaN, kNaN});
      const auto expected = makeNullableArrayVector<T>({
          {1, std::nullopt, 2, std::nullopt, 4},
          {3, 4, 5, 3, 4},
          {8, 9},
      });
      testExpression(
          "array_remove(c0, c1)", {arrayVector, elementVector}, expected);
    }
    // Test array_remove with complex-type elements.
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
          {{{1, "red"}}, {{kNaN, "blue"}}, {{3, "green"}}, {{kNaN, "blue"}}},
          {{{1, "red"}}, {{kSNaN, "blue"}}, {{3, "green"}}, {{kNaN, "blue"}}}};
      auto arrayVector = makeArrayOfRowVector(data, rowType);

      const auto elementVector =
          makeConstantRow(rowType, variant::row({kNaN, "blue"}), 2);

      std::vector<ArrayOfRow> expectedData = {
          {{{1, "red"}}, {{3, "green"}}}, {{{1, "red"}}, {{3, "green"}}}};
      const auto expected = makeArrayOfRowVector(expectedData, rowType);
      testExpression(
          "array_remove(c0, c1)", {arrayVector, elementVector}, expected);
    }
  }
};

//// Remove simple-type elements from array.
TEST_F(ArrayRemoveTest, arrayWithSimpleTypes) {
  const auto arrayVector = makeNullableArrayVector<int64_t>(
      {{1, std::nullopt, 2, 3, std::nullopt, 4},
       {3, 4, 5, std::nullopt, 3, 4, 5},
       {7, 8, 9},
       {10, 20, 30}});
  const auto elementVector = makeFlatVector<int64_t>({3, 3, 33, 30});
  const auto expected = makeNullableArrayVector<int64_t>({
      {1, std::nullopt, 2, std::nullopt, 4},
      {4, 5, std::nullopt, 4, 5},
      {7, 8, 9},
      {10, 20},
  });
  testExpression(
      "array_remove(c0, c1)", {arrayVector, elementVector}, expected);
}

//// Remove simple-type elements from array.
TEST_F(ArrayRemoveTest, arrayWithFloatTypes) {
  testFloats<float>();
  testFloats<double>();
}

//// Remove simple-type elements from array.
TEST_F(ArrayRemoveTest, arrayWithString) {
  const auto arrayVector = makeNullableArrayVector<std::string>(
      {{"a", std::nullopt, "b", "c", std::nullopt, "d"},
       {"c", "d", "e", std::nullopt, "c", "d", "e"},
       {"x", "y", "z"},
       {"aaa", "bbb", "ccc"}});
  const auto elementVector =
      makeFlatVector<std::string>({"c", "c", "i", "ccc"});
  const auto expected = makeNullableArrayVector<std::string>({
      {"a", std::nullopt, "b", std::nullopt, "d"},
      {"d", "e", std::nullopt, "d", "e"},
      {"x", "y", "z"},
      {"aaa", "bbb"},
  });
  testExpression(
      "array_remove(c0, c1)", {arrayVector, elementVector}, expected);
}

//// Remove complex-type elements from array.
TEST_F(ArrayRemoveTest, arrayWithComplexTypes) {
  VectorPtr baseVector, arrayOfArrays;

  baseVector = makeNullableArrayVector<int64_t>(
      {{{1, 1}},
       {{2, 2}},
       {{3, 3}},
       {{4, 4}},
       {{4, 4}},
       {{5, 5}},
       std::nullopt,
       {{6, 6}}});

  // [[1, 1], [2, 2], [3, 3]]
  // [[4, 4], [4, 4]]
  // [[5, 5], null, [6, 6]]
  arrayOfArrays = makeArrayVector({0, 3, 5}, baseVector);

  // [1, 1]
  // [4, 4]
  // [5, 5]
  const auto elementVector = makeArrayVector<int64_t>({{1, 1}, {4, 4}, {5, 5}});

  // [[2, 2], [3, 3]]
  // []
  // [null, [6, 6]]
  const auto expected = makeArrayVector(
      {0, 2, 2},
      makeNullableArrayVector<int64_t>(
          {{{2, 2}}, {{3, 3}}, std::nullopt, {{6, 6}}}));
  testExpression(
      "array_remove(c0, c1)", {arrayOfArrays, elementVector}, expected);

  baseVector = makeNullableArrayVector<int64_t>({
      {1, std::nullopt},
      {2, 2},
      {std::nullopt, 3},
  });

  // [[1, null], [2, 2], [null, 3]]
  arrayOfArrays = makeArrayVector({0}, baseVector);
  testExpressionWithError(
      "array_remove(c0, c1)",
      {arrayOfArrays, elementVector},
      "array_remove does not support arrays with elements that are null or contain null");

  // Test array_remove([[null ,1]], [null, 2]).
  // This does not throw because [null ,1] = [null, 2] is false.
  {
    std::string nestedArray =
        "array_constructor(array_constructor(null::bigint, 1))";
    auto result = evaluate(
        fmt::format(
            "array_remove({}, array_constructor(null::bigint, 2))",
            nestedArray),
        makeRowVector({makeFlatVector<int32_t>(1)}));
    auto expected =
        evaluate(nestedArray, makeRowVector({makeFlatVector<int32_t>(1)}));
    assertEqualVectors(result, expected);
  }
}

////  Remove null from array.
TEST_F(ArrayRemoveTest, arrayWithNull) {
  // [[1, 2], null, [3, 2], [2, 2, 3], [2, 1, 5]]
  const auto arrayVector = makeNullableArrayVector<int64_t>(
      {{1, std::nullopt, 2, 3, std::nullopt, 4},
       {3, 4, 5, std::nullopt, 3, 4, 5},
       {7, 8, 9},
       {10, 20, 30}});
  const auto elementVector = makeNullableFlatVector<std::int64_t>(
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt});
  const auto expected = makeNullableArrayVector<int64_t>(
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt});
  testExpression(
      "array_remove(c0, c1)", {arrayVector, elementVector}, expected);
}
} // namespace
