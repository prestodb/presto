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
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::functions::test;

namespace {

class ArrayMinMaxByTest : public FunctionBaseTest {
 protected:
  template <typename T>
  void testArrayMinMaxBy(
      const std::vector<std::optional<T>>& expected,
      const std::string& expression,
      const RowVectorPtr inputVector) {
    auto result = evaluate(expression, inputVector);
    facebook::velox::test::assertEqualVectors(
        makeNullableFlatVector<T>(expected), result);
  }

  template <typename T>
  void testArrayMinMaxByComplexType(
      ArrayVectorPtr input,
      const std::string& expression,
      T expected) {
    auto inputRow = makeRowVector({input});
    auto result = evaluate(expression, inputRow);
    facebook::velox::test::assertEqualVectors(expected, result);
  }
};

TEST_F(ArrayMinMaxByTest, maxByBasic) {
  // array of integers
  auto intInput = makeRowVector({makeNullableArrayVector<int32_t>({
      {1, 2},
      {-3, 2},
      {-3, 3, 0},
      {3, 3, 3},
      {7},
      {7, std::nullopt},
      {std::nullopt},
  })});

  std::vector<std::optional<int32_t>> intExpected1{
      2,
      2,
      3,
      3,
      7,
      std::nullopt,
      std::nullopt,
  };
  testArrayMinMaxBy<int32_t>(
      intExpected1, "array_max_by(c0, i -> (i))", intInput);
  std::vector<std::optional<int32_t>> intExpected2{
      1,
      -3,
      -3,
      3,
      7,
      std::nullopt,
      std::nullopt,
  };
  testArrayMinMaxBy<int32_t>(
      intExpected2, "array_max_by(c0, i -> ((-1) * i))", intInput);

  // array of doubles
  auto doubleInput = makeRowVector({makeNullableArrayVector<double>({
      {1.0, 2.0},
      {-3.0, 2.0},
      {-3.0, 3.0, 0.0},
      {7.0},
      {7.0, std::nullopt},
      {std::nullopt},
  })});
  std::vector<std::optional<double>> doubleExpected1{
      2.0,
      2.0,
      3.0,
      7.0,
      std::nullopt,
      std::nullopt,
  };
  testArrayMinMaxBy<double>(
      doubleExpected1, "array_max_by(c0, i -> (i))", doubleInput);
  std::vector<std::optional<double>> doubleExpected2{
      2.0,
      -3.0,
      3.0,
      7.0,
      std::nullopt,
      std::nullopt,
  };
  testArrayMinMaxBy<double>(
      doubleExpected2, "array_max_by(c0, i -> (i * i))", doubleInput);
  std::vector<std::optional<double>> doubleExpected3{
      1.0,
      -3.0,
      -3.0,
      7.0,
      std::nullopt,
      std::nullopt,
  };
  testArrayMinMaxBy<double>(
      doubleExpected3, "array_max_by(c0, i -> (1.0 - i))", doubleInput);

  // array of strings
  auto stringInput = makeRowVector({makeNullableArrayVector<StringView>({
      {"1", "2"},
      {"a", "bb", "c"},
      {"abc", std::nullopt},
      {std::nullopt},
  })});
  std::vector<std::optional<StringView>> stringExpected1{
      "2",
      "bb",
      std::nullopt,
      std::nullopt,
  };
  testArrayMinMaxBy<StringView>(
      stringExpected1, "array_max_by(c0, i -> LENGTH(i))", stringInput);
}

TEST_F(ArrayMinMaxByTest, maxByComplexTypes) {
  // array of arrays
  auto intArrayInput = makeNestedArrayVectorFromJson<int64_t>(
      {"[[1, 2, 3]]",
       "[[1, 2, 3], [3, 3], [4, 4], [5, 5]]",
       "[[1, 1], [3, 3], [4, 4], [5, 5]]",
       "[[1, 1], [3, 3], [4, 4], [5, 5], [null]]",
       "[[1, 1], [3, 3], [null, null, null], [4, 4], [5, 5]]",
       "[[], []]"});
  auto intArrayResult = makeArrayVectorFromJson<int64_t>(
      {"[1, 2, 3]",
       "[1, 2, 3]",
       "[5, 5]",
       "[5, 5]",
       "[null, null, null]",
       "[]"});
  testArrayMinMaxByComplexType<ArrayVectorPtr>(
      intArrayInput, "array_max_by(c0, i -> CARDINALITY(i))", intArrayResult);

  // array of maps
  std::vector<std::pair<StringView, std::optional<int64_t>>> a{
      {"one"_sv, 1}, {"two"_sv, 2}};
  std::vector<std::pair<StringView, std::optional<int64_t>>> b{{"one"_sv, 0}};
  std::vector<std::pair<StringView, std::optional<int64_t>>> c{
      {"three"_sv, 4}, {"four"_sv, 5}};
  std::vector<
      std::vector<std::vector<std::pair<StringView, std::optional<int64_t>>>>>
      mapData{{a, b}, {b}, {c}, {c, a}, {a, c}};
  auto arrayVector = makeArrayOfMapVector<StringView, int64_t>(mapData);
  auto mapResult = makeMapVectorFromJson<std::string, int64_t>({
      "{\"one\": 1, \"two\": 2}",
      "{\"one\": 0}",
      "null",
      "null",
      "null",
  });
  testArrayMinMaxByComplexType<MapVectorPtr>(
      arrayVector, "array_max_by(c0,x -> x['one'])", mapResult);

  RowTypePtr rowType = ROW({INTEGER(), VARCHAR()});

  using ArrayOfRow = std::vector<std::optional<std::tuple<int, std::string>>>;
  std::vector<ArrayOfRow> rowData = {
      {{{1, "one"}}, {{2, "two"}}, {{3, "three"}}},
      {{{1, "one"}}, {{4, "three"}}, {{3, "two"}}},
      {{{1, "one"}}}};
  auto ArrayOfRows = makeArrayOfRowVector(rowData, rowType);

  auto c0 = makeNullableFlatVector<int32_t>({3, 4, 1});
  auto c1 = makeNullableFlatVector<std::string>({"three", "three", "one"});
  auto resultVector = makeRowVector({c0, c1});

  testArrayMinMaxByComplexType<RowVectorPtr>(
      ArrayOfRows, "array_max_by(c0,r -> r)", resultVector);
} // namespace

TEST_F(ArrayMinMaxByTest, minByBasic) {
  // array of integers
  auto intInput = makeRowVector({makeNullableArrayVector<int32_t>({
      {1, 2},
      {-3, 2},
      {-3, 3, 0},
      {3, 3, 3},
      {7},
      {7, std::nullopt},
      {std::nullopt},
  })});
  std::vector<std::optional<int32_t>> intExpected1{
      1,
      -3,
      -3,
      3,
      7,
      std::nullopt,
      std::nullopt,
  };
  testArrayMinMaxBy<int32_t>(
      intExpected1, "array_min_by(c0, i -> (i))", intInput);
  std::vector<std::optional<int32_t>> intExpected2{
      2,
      2,
      3,
      3,
      7,
      std::nullopt,
      std::nullopt,
  };
  testArrayMinMaxBy<int32_t>(
      intExpected2, "array_min_by(c0, i -> ((-1) * i))", intInput);

  // array of doubles
  auto doubleInput = makeRowVector({makeNullableArrayVector<double>({
      {1.0, 2.0},
      {-3.0, 2.0},
      {-3.0, 3.0, 0.0},
      {7.0},
      {7.0, std::nullopt},
      {std::nullopt},
  })});
  std::vector<std::optional<double>> doubleExpected1{
      1.0,
      -3.0,
      -3.0,
      7.0,
      std::nullopt,
      std::nullopt,
  };
  testArrayMinMaxBy<double>(
      doubleExpected1, "array_min_by(c0, i -> (i))", doubleInput);
  std::vector<std::optional<double>> doubleExpected2{
      1.0,
      2.0,
      0.0,
      7.0,
      std::nullopt,
      std::nullopt,
  };
  testArrayMinMaxBy<double>(
      doubleExpected2, "array_min_by(c0, i -> (i * i))", doubleInput);
  std::vector<std::optional<double>> doubleExpected3{
      2.0,
      2.0,
      3.0,
      7.0,
      std::nullopt,
      std::nullopt,
  };
  testArrayMinMaxBy<double>(
      doubleExpected3, "array_min_by(c0, i -> (1.0 - i))", doubleInput);

  // array of strings
  auto stringInput = makeRowVector({makeNullableArrayVector<StringView>({
      {"1", "2"},
      {"a", "bb", "c"},
      {"abc", std::nullopt},
      {std::nullopt},
  })});
  std::vector<std::optional<StringView>> stringExpected1{
      "2",
      "c",
      std::nullopt,
      std::nullopt,
  };
  testArrayMinMaxBy<StringView>(
      stringExpected1, "array_min_by(c0, i -> LENGTH(i))", stringInput);
}

TEST_F(ArrayMinMaxByTest, minByComplexTypes) {
  // array of arrays
  auto intArrayInput = makeNestedArrayVectorFromJson<int64_t>(
      {"[[1, 2, 3]]",
       "[[1, 2, 3], [3, 3], [4, 4], [5, 5]]",
       "[[1, 1], [3, 3], [4, 4], [5, 5]]",
       "[[1, 1], [3, 3], [4, 4], [5, 5], [null]]",
       "[[1, 1], [3, 3, 3], [null, null, null], [4, 4, 4], [5, 5, 5]]",
       "[[], []]"});
  auto intArrayResult = makeArrayVectorFromJson<int64_t>(
      {"[1, 2, 3]", "[5, 5]", "[5, 5]", "[null]", "[1, 1]", "[]"});
  testArrayMinMaxByComplexType<ArrayVectorPtr>(
      intArrayInput, "array_min_by(c0, i -> CARDINALITY(i))", intArrayResult);

  // array of maps
  std::vector<std::pair<StringView, std::optional<int64_t>>> a{
      {"one"_sv, 1}, {"two"_sv, 2}};
  std::vector<std::pair<StringView, std::optional<int64_t>>> b{{"one"_sv, 0}};
  std::vector<std::pair<StringView, std::optional<int64_t>>> c{
      {"three"_sv, 4}, {"four"_sv, 5}};
  std::vector<
      std::vector<std::vector<std::pair<StringView, std::optional<int64_t>>>>>
      mapData{{a, b}, {b}, {c}, {c, a}, {a, c}};
  auto arrayVector = makeArrayOfMapVector<StringView, int64_t>(mapData);
  auto mapResult = makeMapVectorFromJson<std::string, int64_t>({
      "{\"one\": 0}",
      "{\"one\": 0}",
      "null",
      "null",
      "null",
  });
  testArrayMinMaxByComplexType<MapVectorPtr>(
      arrayVector, "array_min_by(c0,x -> x['one'])", mapResult);

  RowTypePtr rowType = ROW({INTEGER(), VARCHAR()});

  using ArrayOfRow = std::vector<std::optional<std::tuple<int, std::string>>>;
  std::vector<ArrayOfRow> rowData = {
      {{{1, "one"}}, {{2, "two"}}, {{3, "three"}}},
      {{{1, "one"}}, {{4, "three"}}, {{3, "two"}}},
      {{{1, "one"}}}};
  auto ArrayOfRows = makeArrayOfRowVector(rowData, rowType);

  auto c0 = makeNullableFlatVector<int32_t>({1, 1, 1});
  auto c1 = makeNullableFlatVector<std::string>({"one", "one", "one"});
  auto resultVector = makeRowVector({c0, c1});

  testArrayMinMaxByComplexType<RowVectorPtr>(
      ArrayOfRows, "array_min_by(c0, r -> r)", resultVector);
} // namespace
} // namespace
