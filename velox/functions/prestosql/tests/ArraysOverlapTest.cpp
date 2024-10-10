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
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/vector/tests/TestingDictionaryArrayElementsFunction.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::functions::test;

namespace {
template <typename TKey, typename TValue>
using Pair = std::pair<TKey, std::optional<TValue>>;

class ArraysOverlapTest : public FunctionBaseTest {
 protected:
  void testExpr(
      const VectorPtr& expected,
      const std::string& expression,
      const std::vector<VectorPtr>& input) {
    auto result =
        evaluate<SimpleVector<bool>>(expression, makeRowVector(input));
    assertEqualVectors(expected, result);

    if (input.size() == 2) {
      auto newSize = input[0]->size() * 2;
      auto indices = makeIndices(newSize, [](auto row) { return row / 2; });
      auto firstDict = wrapInDictionary(indices, newSize, input[0]);
      auto secondFlat = flatten(wrapInDictionary(indices, newSize, input[1]));

      auto dictResult = evaluate<SimpleVector<bool>>(
          expression, makeRowVector({firstDict, secondFlat}));
      auto dictExpected = wrapInDictionary(indices, newSize, expected);
      assertEqualVectors(dictExpected, dictResult);
    }
  }

  template <typename T>
  void testInt() {
    auto array1 = makeNullableArrayVector<T>(
        {{1, -2, 3, std::nullopt, 4, 5, 6, std::nullopt},
         {1, 2, -2, 1},
         {3, 8, std::nullopt},
         {1, 1, -2, -2, -2, 4, 8},
         {2, -1},
         {1, 2, 3},
         {std::nullopt},
         {},
         {std::nullopt},
         {},
         {1, 2}});
    auto array2 = makeNullableArrayVector<T>(
        {{1, -2, 4},
         {1, -2, 4},
         {1, -2, 4},
         {1, -2, 4},
         {1, -2, std::nullopt},
         {5, 6, 7},
         {std::nullopt},
         {1, std::nullopt},
         {},
         {std::nullopt},
         {}});
    auto expected = makeNullableFlatVector<bool>(
        {true,
         true,
         std::nullopt,
         true,
         std::nullopt,
         false,
         std::nullopt,
         false,
         false,
         false,
         false});
    testExpr(expected, "arrays_overlap(C0, C1)", {array1, array2});
    testExpr(expected, "arrays_overlap(C1, C0)", {array1, array2});
  }

  template <typename T>
  void testFloatingPoint() {
    auto array1 = makeNullableArrayVector<T>(
        {{1.0001, -2.0, 3.03, std::nullopt, 4.00004},
         {std::numeric_limits<T>::min(), 2.02, -2.001, 1},
         {std::numeric_limits<T>::max(), 8.0001, std::nullopt},
         {9.0009,
          std::numeric_limits<T>::infinity(),
          std::numeric_limits<T>::max()},
         {std::numeric_limits<T>::quiet_NaN(), 9.0009},
         {std::numeric_limits<T>::quiet_NaN(), 3.1},
         // quiet NaN and signaling NaN are treated equal
         {std::numeric_limits<T>::signaling_NaN(), 3.1},
         {std::numeric_limits<T>::quiet_NaN(), 9.0009, std::nullopt}});
    auto array2 = makeNullableArrayVector<T>(
        {{1.0, -2.0, 4.0},
         {std::numeric_limits<T>::min(), 2.0199, 1.000001},
         {1.0001, -2.02, std::numeric_limits<T>::max(), 8.00099},
         {9.0009, std::numeric_limits<T>::infinity()},
         {9.0009, std::numeric_limits<T>::quiet_NaN()},
         {9.0009, std::numeric_limits<T>::quiet_NaN()},
         {9.0009, std::numeric_limits<T>::quiet_NaN()},
         {9.0}});
    auto expected = makeNullableFlatVector<bool>(
        {true, true, true, true, true, true, true, std::nullopt});
    testExpr(expected, "arrays_overlap(C0, C1)", {array1, array2});
    testExpr(expected, "arrays_overlap(C1, C0)", {array1, array2});
  }
}; // class ArraysOverlap
} // namespace

TEST_F(ArraysOverlapTest, intArrays) {
  testInt<int8_t>();
  testInt<int16_t>();
  testInt<int32_t>();
  testInt<int64_t>();
}

TEST_F(ArraysOverlapTest, floatArrays) {
  testFloatingPoint<float>();
  testFloatingPoint<double>();
}

TEST_F(ArraysOverlapTest, boolArrays) {
  auto array1 = makeNullableArrayVector<bool>(
      {{true, false},
       {true, true},
       {false, false},
       {},
       {true, false, true, std::nullopt},
       {std::nullopt, true, false, true},
       {false, true, false},
       {true, false, true},
       {std::nullopt}});

  auto array2 = makeNullableArrayVector<bool>(
      {{true},
       {true, true},
       {false, false},
       {},
       {true, std::nullopt},
       {std::nullopt, false},
       {false, true, false},
       {true, false, true},
       {std::nullopt}});

  auto expected = makeNullableFlatVector<bool>(
      {true, true, true, false, true, true, true, true, std::nullopt});

  testExpr(expected, "arrays_overlap(C0, C1)", {array1, array2});
  testExpr(expected, "arrays_overlap(C1, C0)", {array1, array2});
}

/// Test inline strings.
TEST_F(ArraysOverlapTest, shortStrings) {
  using S = StringView;

  auto array1 = makeNullableArrayVector<StringView>({
      {"a"_sv, std::nullopt, S("b")},
      {S("a"), S("b"), S("a"), S("a")},
      {std::nullopt, S("b"), std::nullopt},
      {"abcd"_sv},
  });
  auto array2 = makeNullableArrayVector<StringView>({
      {S("a")},
      {S("a"), S("b"), S("b")},
      {std::nullopt, std::nullopt, std::nullopt},
      {S("abc"), S("a"), S("b")},
  });
  auto expected =
      makeNullableFlatVector<bool>({true, true, std::nullopt, false});
  testExpr(expected, "arrays_overlap(C0, C1)", {array1, array2});
  testExpr(expected, "arrays_overlap(C1, C0)", {array1, array2});
}

/// Test non-inline (> 12 length) strings.
TEST_F(ArraysOverlapTest, longStrings) {
  using S = StringView;

  auto array1 = makeNullableArrayVector<StringView>({
      {S("red shiny car ahead"), S("blue clear sky above")},
      {std::nullopt,
       S("blue clear sky above"),
       S("yellow rose flowers"),
       S("orange beautiful sunset")},
      {},
      {S("red shiny car ahead"),
       S("purple is an elegant color"),
       S("green plants make us happy")},
  });
  auto array2 = makeNullableArrayVector<StringView>({
      {S("red shiny car ahead")},
      {std::nullopt},
      {},
      {S("red shiny car ahead"), S("green plants make us happy")},
  });
  auto expected =
      makeNullableFlatVector<bool>({true, std::nullopt, false, true});
  testExpr(expected, "arrays_overlap(C0, C1)", {array1, array2});
  testExpr(expected, "arrays_overlap(C1, C0)", {array1, array2});
}

TEST_F(ArraysOverlapTest, complexTypeArray) {
  auto left = makeNestedArrayVectorFromJson<int32_t>({
      "[null, [1, 2, 3], [null, null]]",
      "[[1], [2], []]",
      "[[1, null, 3]]",
      "[[1, null, 3]]",
      "[null]",
  });

  auto right = makeNestedArrayVectorFromJson<int32_t>({
      "[[1, 2, 3]]",
      "[[1]]",
      "[[1, 2]]",
      "[[1, null, 3]]",
      "[[]]",
  });

  auto expected =
      makeNullableFlatVector<bool>({true, true, false, true, std::nullopt});
  testExpr(expected, "arrays_overlap(c0, c1)", {left, right});
}

TEST_F(ArraysOverlapTest, complexTypeMap) {
  std::vector<Pair<StringView, int64_t>> a{{"blue", 1}, {"red", 2}};
  std::vector<Pair<StringView, int64_t>> b{{"green", std::nullopt}};
  std::vector<Pair<StringView, int64_t>> c{{"yellow", 4}, {"purple", 5}};

  std::vector<std::vector<std::vector<Pair<StringView, int64_t>>>> leftData{
      {b, a}, {b}, {c, a}};
  std::vector<std::vector<std::vector<Pair<StringView, int64_t>>>> rightData{
      {a, b}, {}, {b}};

  auto left = makeArrayOfMapVector<StringView, int64_t>(leftData);
  auto right = makeArrayOfMapVector<StringView, int64_t>(rightData);
  auto expected = makeNullableFlatVector<bool>({true, false, false});

  testExpr(expected, "arrays_overlap(c0, c1)", {left, right});
}

TEST_F(ArraysOverlapTest, complexTypeRow) {
  RowTypePtr rowType = ROW({INTEGER(), VARCHAR()});

  using ArrayOfRow = std::vector<std::optional<std::tuple<int, std::string>>>;
  std::vector<ArrayOfRow> leftData = {
      {{{1, "red"}}, {{2, "blue"}}, {{3, "green"}}},
      {{{1, "red"}}, {{2, "blue"}}, std::nullopt},
      {{{1, "red"}}, std::nullopt, std::nullopt},
      {{{1, "red"}}, {{}}, {{}}}};
  std::vector<ArrayOfRow> rightData = {
      {{{2, "blue"}}, {{1, "red"}}},
      {{{1, "green"}}},
      {{{1, "red"}}},
      {{{2, "red"}}}};

  auto left = makeArrayOfRowVector(leftData, rowType);
  auto right = makeArrayOfRowVector(rightData, rowType);
  auto expected =
      makeNullableFlatVector<bool>({true, std::nullopt, true, false});

  testExpr(expected, "arrays_overlap(c0, c1)", {left, right});
}

//// When one of the arrays is constant.
TEST_F(ArraysOverlapTest, constant) {
  auto array1 = makeNullableArrayVector<int32_t>({
      {1, -2, 3, std::nullopt, 4, 5, 6, std::nullopt},
      {1, 2, -2, 1},
      {3, 8, std::nullopt},
      {3, 8, 5},
      {1, 1, -2, -2, -2, 4, 8},
  });
  auto expected =
      makeNullableFlatVector<bool>({true, true, std::nullopt, false, true});
  testExpr(expected, "arrays_overlap(C0, ARRAY[1,-2,4])", {array1});
  testExpr(expected, "arrays_overlap(ARRAY[1,-2,4], C0)", {array1});
  testExpr(
      expected, "arrays_overlap(ARRAY[1,1,-2,1,-2,4,1,4,4], C0)", {array1});

  // Array containing NULLs.
  expected = makeNullableFlatVector<bool>({
      true,
      true,
      std::nullopt,
      std::nullopt,
      true,
  });
  testExpr(expected, "arrays_overlap(C0, ARRAY[1,NULL,4])", {array1});
  testExpr(expected, "arrays_overlap(ARRAY[1,NULL,4], C0)", {array1});
}

TEST_F(ArraysOverlapTest, dictionaryEncodedElementsInConstant) {
  TestingDictionaryArrayElementsFunction::registerFunction();

  auto array = makeArrayVector<int64_t>({{1, 3}, {2, 5}, {0, 6}});
  auto expected = makeNullableFlatVector<bool>({true, true, false});
  testExpr(
      expected,
      "arrays_overlap(testing_dictionary_array_elements(ARRAY [2, 2, 3, 1, 2, 2]), c0)",
      {array});
}

TEST_F(ArraysOverlapTest, TimestampWithTimezone) {
  auto testArraysOverlap =
      [this](
          const std::vector<std::optional<int64_t>>& array1,
          const std::vector<std::optional<int64_t>>& array2,
          std::optional<bool> expected) {
        auto arrayVector1 = makeArrayVector(
            {0}, makeNullableFlatVector(array1, TIMESTAMP_WITH_TIME_ZONE()));
        auto arrayVector2 = makeArrayVector(
            {0}, makeNullableFlatVector(array2, TIMESTAMP_WITH_TIME_ZONE()));
        auto expectedVector = makeNullableFlatVector<bool>({expected});

        testExpr(
            expectedVector,
            "arrays_overlap(C0, C1)",
            {arrayVector1, arrayVector2});
        testExpr(
            expectedVector,
            "arrays_overlap(C1, C0)",
            {arrayVector1, arrayVector2});
      };

  testArraysOverlap(
      {pack(1, 1),
       pack(-2, 2),
       pack(3, 3),
       std::nullopt,
       pack(4, 4),
       pack(5, 5),
       pack(6, 6),
       std::nullopt},
      {pack(1, 10), pack(-2, 11), pack(4, 12)},
      true);
  testArraysOverlap(
      {pack(1, 1), pack(2, 2), pack(-2, 3), pack(1, 4)},
      {pack(1, 10), pack(-2, 11), pack(4, 12)},
      true);
  testArraysOverlap(
      {pack(3, 1), pack(8, 2), std::nullopt},
      {pack(1, 10), pack(-2, 11), pack(4, 12)},
      std::nullopt);
  testArraysOverlap(
      {pack(1, 1),
       pack(1, 2),
       pack(-2, 3),
       pack(-2, 4),
       pack(-2, 5),
       pack(4, 6),
       pack(8, 7)},
      {pack(1, 10), pack(-2, 11), pack(4, 12)},
      true);
  testArraysOverlap(
      {pack(2, 1), pack(-1, 2)},
      {pack(1, 1), pack(-2, 2), std::nullopt},
      std::nullopt);
  testArraysOverlap(
      {pack(1, 1), pack(2, 2), pack(3, 3)},
      {pack(5, 1), pack(6, 2), pack(7, 3)},
      false);
  testArraysOverlap({std::nullopt}, {std::nullopt}, std::nullopt);
  testArraysOverlap({}, {1, std::nullopt}, false);
  testArraysOverlap({std::nullopt}, {}, false);
  testArraysOverlap({}, {std::nullopt}, false);
  testArraysOverlap({pack(1, 1), pack(2, 2)}, {}, false);
}
