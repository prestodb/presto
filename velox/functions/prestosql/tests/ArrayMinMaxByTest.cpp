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
      {"1", "2", "22"},
      {"aa", "bb", "cc"},
      {"abc", std::nullopt},
      {std::nullopt},
  })});
  std::vector<std::optional<StringView>> stringExpected1{
      "22",
      "cc",
      std::nullopt,
      std::nullopt,
  };
  testArrayMinMaxBy<StringView>(
      stringExpected1, "array_max_by(c0, i -> LENGTH(i))", stringInput);

  testArrayMinMaxBy<StringView>(
      stringExpected1, "array_max_by(c0, i -> 2)", stringInput);

  testArrayMinMaxBy<StringView>(
      stringExpected1, "array_max_by(c0, i -> array[2, 1])", stringInput);

  // Throw user error when lambda returns a complex type with null elements
  VELOX_ASSERT_THROW(
      testArrayMinMaxBy<StringView>(
          stringExpected1,
          "array_max_by(c0, i -> array[null, 1])",
          stringInput),
      "Ordering nulls is not supported");

  std::vector<std::optional<StringView>> stringExpected2{
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt,
  };
  testArrayMinMaxBy<StringView>(
      stringExpected2, "array_max_by(c0, i -> null)", stringInput);

  // Different combinations of empty/null inputs
  std::vector<std::optional<int8_t>> emptyExpected{std::nullopt};

  auto arrayVector = makeNullableArrayVector(
      std::vector<std::vector<std::optional<int8_t>>>{{}});
  auto emptyInput = makeRowVector({arrayVector});
  testArrayMinMaxBy<int8_t>(
      emptyExpected, "array_max_by(c0, i ->i * 2)", emptyInput);

  auto arrayVector2 = makeNullableArrayVector(
      std::vector<std::vector<std::optional<int8_t>>>{0});
  auto emptyInput2 = makeRowVector({arrayVector});
  testArrayMinMaxBy<int8_t>(
      emptyExpected, "array_max_by(c0, i ->i / 2)", emptyInput2);

  std::optional<std::vector<std::optional<StringView>>> optionalInputArray = {
      std::vector<std::optional<StringView>>{"abc", std::nullopt}};
  std::vector<std::optional<std::vector<std::optional<StringView>>>>
      optionalInputs = {optionalInputArray, std::nullopt};
  auto optionalArrayVector = makeNullableArrayVector(optionalInputs);
  auto optionalInput = makeRowVector({optionalArrayVector});
  std::vector<std::optional<StringView>> optionalExpected{
      std::nullopt, std::nullopt};
  testArrayMinMaxBy<StringView>(
      optionalExpected, "array_max_by(c0, i -> LENGTH(i))", optionalInput);
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

  // higher order arrays
  auto intArray = makeArrayVector<int64_t>({{1, 5, 2, 4, 3}, {4, 5, 4, 5}});
  auto nestedIntArray = makeArrayVector({0, 1}, intArray);
  auto doubleNestedArray = makeArrayVector({0, 1}, nestedIntArray);
  auto intNestedArrayResult =
      makeArrayVectorFromJson<int64_t>({"[1, 5, 2, 4, 3]", "[4, 5, 4, 5]"});
  testArrayMinMaxByComplexType<ArrayVectorPtr>(
      doubleNestedArray,
      "array_max_by(array_max_by(c0, i -> (i)), i->2)",
      intNestedArrayResult);

  std::vector<std::optional<int64_t>> intExpected2{3, 5};
  auto doubleNestedArrayRow = makeRowVector({doubleNestedArray});
  testArrayMinMaxBy(
      intExpected2,
      "array_max_by(array_max_by(array_max_by(c0, i -> (i)), i->2), i->3)",
      doubleNestedArrayRow);

  auto emptyInnerArray = makeArrayVector<int64_t>({{}});
  auto nestedEmptyArray = makeArrayVector({0}, emptyInnerArray);
  auto doubleNestedEmptyArray = makeArrayVector({0}, nestedEmptyArray);
  auto emptyArrayResult = makeArrayVectorFromJson<int64_t>({"[]"});
  testArrayMinMaxByComplexType<ArrayVectorPtr>(
      doubleNestedEmptyArray,
      "array_max_by(array_max_by(c0, i -> (i)), i -> 3)",
      emptyArrayResult);

  auto nullInnerArray =
      makeNullableArrayVector<int64_t>({std::nullopt, std::nullopt});
  auto nestedNullArray = makeArrayVector({0, 0}, nullInnerArray);
  auto doubleNestedNullArray = makeArrayVector({0, 0}, nestedNullArray);
  auto nullResult = makeArrayVectorFromJson<int64_t>({"null", "null"});
  testArrayMinMaxByComplexType<ArrayVectorPtr>(
      doubleNestedNullArray,
      "array_max_by(array_max_by(c0, i -> (i)), i -> 3)",
      nullResult);
}

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

  std::vector<std::optional<int32_t>> intExpected11{
      1,
      -3,
      -3,
      3,
      7,
      7,
      std::nullopt,
  };
  testArrayMinMaxBy<int32_t>(
      intExpected11, "array_min_by(c0, i -> 3)", intInput);

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
      {"1", "2", "22"},
      {"aa", "bb", "cc"},
      {"abc", std::nullopt},
      {std::nullopt},
  })});
  std::vector<std::optional<StringView>> stringExpected1{
      "1",
      "aa",
      std::nullopt,
      std::nullopt,
  };
  testArrayMinMaxBy<StringView>(
      stringExpected1, "array_min_by(c0, i -> LENGTH(i))", stringInput);
  std::vector<std::optional<StringView>> stringExpected2{
      "1",
      "aa",
      "abc",
      std::nullopt,
  };
  testArrayMinMaxBy<StringView>(
      stringExpected2, "array_min_by(c0, i -> 2)", stringInput);

  testArrayMinMaxBy<StringView>(
      stringExpected2, "array_min_by(c0, i -> array[2, 1])", stringInput);

  // Throw user error when lambda returns a complex type with null elements
  VELOX_ASSERT_THROW(
      testArrayMinMaxBy<StringView>(
          stringExpected1,
          "array_max_by(c0, i -> array[2, null])",
          stringInput),
      "Ordering nulls is not supported");

  std::vector<std::optional<StringView>> stringExpected3{
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt,
  };
  testArrayMinMaxBy<StringView>(
      stringExpected3, "array_min_by(c0, i -> null)", stringInput);

  // Different combinations of empty/null inputs
  std::vector<std::optional<int8_t>> emptyExpected{std::nullopt};

  auto arrayVector = makeNullableArrayVector(
      std::vector<std::vector<std::optional<int8_t>>>{{}});
  auto emptyInput = makeRowVector({arrayVector});
  testArrayMinMaxBy<int8_t>(
      emptyExpected, "array_min_by(c0, i ->i * 2)", emptyInput);

  auto arrayVector2 = makeNullableArrayVector(
      std::vector<std::vector<std::optional<int8_t>>>{0});
  auto emptyInput2 = makeRowVector({arrayVector});
  testArrayMinMaxBy<int8_t>(
      emptyExpected, "array_min_by(c0, i ->i / 2)", emptyInput2);

  std::optional<std::vector<std::optional<StringView>>> optionalInputArray = {
      std::vector<std::optional<StringView>>{"abc", std::nullopt}};
  std::vector<std::optional<std::vector<std::optional<StringView>>>>
      optionalInputs = {optionalInputArray, std::nullopt};
  auto optionalArrayVector = makeNullableArrayVector(optionalInputs);
  auto optionalInput = makeRowVector({optionalArrayVector});
  std::vector<std::optional<StringView>> optionalExpected{
      std::nullopt, std::nullopt};
  testArrayMinMaxBy<StringView>(
      optionalExpected, "array_min_by(c0, i -> LENGTH(i))", optionalInput);
}

TEST_F(ArrayMinMaxByTest, minByComplexTypes) {
  // array of arrays
  auto intArrayInput = makeNestedArrayVectorFromJson<int64_t>(
      {"[[1, 2, 3]]",
       "[[1, 2, 3], [3, 3], [4, 4], [5, 5]]",
       "[[1, 1], [3, 3], [4, 4], [5, 5]]",
       "[[1, 1], [3, 3], [4, 4], [5, 5], [null]]",
       "[[1, 1], [3, 3, 3], [null, null, null], [4, 4, 4], [5, 5, 5]]",
       "[[], []]",
       "[]"});
  auto intArrayResult = makeArrayVectorFromJson<int64_t>(
      {"[1, 2, 3]", "[3, 3]", "[1, 1]", "[null]", "[1, 1]", "[]", "null"});
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

  // higher order arrays
  auto intArray = makeArrayVector<int64_t>({{1, 5, 2, 4, 3}, {4, 5, 4, 5}});
  auto nestedIntArray = makeArrayVector({0, 1}, intArray);
  auto doubleNestedArray = makeArrayVector({0, 1}, nestedIntArray);
  auto intNestedArrayResult =
      makeArrayVectorFromJson<int64_t>({"[1, 5, 2, 4, 3]", "[4, 5, 4, 5]"});
  testArrayMinMaxByComplexType<ArrayVectorPtr>(
      doubleNestedArray,
      "array_min_by(array_min_by(c0, i -> (i)), i->2)",
      intNestedArrayResult);

  std::vector<std::optional<int64_t>> intExpected2{1, 4};
  auto doubleNestedArrayRow = makeRowVector({doubleNestedArray});
  testArrayMinMaxBy(
      intExpected2,
      "array_min_by(array_min_by(array_min_by(c0, i -> (i)), i->2), i->3)",
      doubleNestedArrayRow);

  auto emptyInnerArray = makeArrayVector<int64_t>({{}});
  auto nestedEmptyArray = makeArrayVector({0}, emptyInnerArray);
  auto doubleNestedEmptyArray = makeArrayVector({0}, nestedEmptyArray);
  auto emptyArrayResult = makeArrayVectorFromJson<int64_t>({"[]"});
  testArrayMinMaxByComplexType<ArrayVectorPtr>(
      doubleNestedEmptyArray,
      "array_min_by(array_min_by(c0, i -> (i)), i -> 3)",
      emptyArrayResult);

  auto nullInnerArray =
      makeNullableArrayVector<int64_t>({std::nullopt, std::nullopt});
  auto nestedNullArray = makeArrayVector({0, 0}, nullInnerArray);
  auto doubleNestedNullArray = makeArrayVector({0, 0}, nestedNullArray);
  auto nullResult = makeArrayVectorFromJson<int64_t>({"null", "null"});
  testArrayMinMaxByComplexType<ArrayVectorPtr>(
      doubleNestedNullArray,
      "array_min_by(array_min_by(c0, i -> (i)), i -> 3)",
      nullResult);
}

TEST_F(ArrayMinMaxByTest, differentEncodings) {
  // constant encoding
  auto constantArray = makeConstantArray<int32_t>(1, {-4, 1, 5, 0});
  auto constArrayVector = makeRowVector({constantArray});
  std::vector<std::optional<int32_t>> constantExpected{-4};
  testArrayMinMaxBy<int32_t>(
      constantExpected, "array_max_by(c0, i -> ((-1) * i))", constArrayVector);

  auto constantArray2 = makeConstantArray<int32_t>(3, {-4, 1, 5, 0});
  auto constArrayVector2 = makeRowVector({constantArray});
  std::vector<std::optional<int32_t>> constantExpected2{5};
  testArrayMinMaxBy<int32_t>(
      constantExpected2,
      "array_min_by(c0, i -> ((-1) * i))",
      constArrayVector2);

  // dictionary encoding
  auto values =
      makeNullableFlatVector<int32_t>({1, 2, -3, 3, 0, 7, 7, std::nullopt});
  auto dictIndices = makeIndices({0, 1, 2, 3, 4, 5, 6, 7});
  auto dictElements = wrapInDictionary(dictIndices, values);

  auto dictArray = makeArrayVector({0, 3, 5}, dictElements);
  auto dictInput = makeRowVector({dictArray});
  std::vector<std::optional<int32_t>> dictExpected{-3, 0, std::nullopt};
  testArrayMinMaxBy<int32_t>(
      dictExpected, "array_min_by(c0, i -> (i))", dictInput);

  auto dictArray2 = makeArrayVector({0, 1, 2, 3, 4, 5, 6, 7}, dictElements);
  auto dictInput2 = makeRowVector({dictArray2});
  std::vector<std::optional<int32_t>> dictExpected2{
      1, 2, -3, 3, 0, 7, 7, std::nullopt};
  testArrayMinMaxBy<int32_t>(
      dictExpected2, "array_max_by(c0, i -> (i))", dictInput2);
} // namespace
} // namespace
