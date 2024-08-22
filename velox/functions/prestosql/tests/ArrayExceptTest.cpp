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
#include "velox/vector/tests/TestingDictionaryArrayElementsFunction.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::functions::test;

namespace {

template <typename TKey, typename TValue>
using Pair = std::pair<TKey, std::optional<TValue>>;

class ArrayExceptTest : public FunctionBaseTest {
 protected:
  void testExpr(
      const VectorPtr& expected,
      const std::string& expression,
      const std::vector<VectorPtr>& input) {
    auto result = evaluate<ArrayVector>(expression, makeRowVector(input));
    assertEqualVectors(expected, result);

    // Also test using dictionary encodings.
    if (input.size() == 2) {
      // Wrap first column in a dictionary: repeat each row twice. Wrap second
      // column in the same dictionary, then flatten to prevent peeling of
      // encodings. Wrap the expected result in the same dictionary.
      // The expression evaluation on both dictionary inputs should result in
      // the dictionary of the expected result vector.
      auto newSize = input[0]->size() * 2;
      auto indices = makeIndices(newSize, [](auto row) { return row / 2; });
      auto firstDict = wrapInDictionary(indices, newSize, input[0]);
      auto secondFlat = flatten(wrapInDictionary(indices, newSize, input[1]));

      auto dictResult = evaluate<ArrayVector>(
          expression, makeRowVector({firstDict, secondFlat}));
      auto dictExpected = wrapInDictionary(indices, newSize, expected);
      assertEqualVectors(dictExpected, dictResult);
    }
  }

  template <typename T>
  void testInt() {
    auto array1 = makeNullableArrayVector<T>({
        {1, -2, 3, std::nullopt, 4, 5, 6, std::nullopt},
        {1, 2, -2, 1},
        {3, 8, std::nullopt},
        {1, 1, -2, -2, -2, 4, 8},
    });
    auto array2 = makeNullableArrayVector<T>({
        {1, -2, 4},
        {1, -2, 4},
        {1, -2, 4},
        {1, -2, 4},
    });
    auto expected = makeNullableArrayVector<T>({
        {3, std::nullopt, 5, 6},
        {2},
        {3, 8, std::nullopt},
        {8},
    });
    testExpr(expected, "array_except(C0, C1)", {array1, array2});
    expected = makeNullableArrayVector<T>({
        {},
        {4},
        {1, -2, 4},
        {},
    });
    testExpr(expected, "array_except(C1, C0)", {array1, array2});

    // Change C1.
    array2 = makeNullableArrayVector<T>({
        {10, -24, 43},
        {std::nullopt, -2, 2},
        {std::nullopt, std::nullopt, std::nullopt},
        {8, 1, 8, 1},
    });
    expected = makeNullableArrayVector<T>({
        {1, -2, 3, std::nullopt, 4, 5, 6},
        {1},
        {3, 8},
        {-2, 4},
    });
    testExpr(expected, "array_except(C0, C1)", {array1, array2});
  }

  template <typename T>
  void testFloatingPoint() {
    auto array1 = makeNullableArrayVector<T>({
        {1.0001, -2.0, 3.03, std::nullopt, 4.00004},
        {std::numeric_limits<T>::min(), 2.02, -2.001, 1},
        {std::numeric_limits<T>::max(), 8.0001, std::nullopt},
        {9.0009,
         std::numeric_limits<T>::infinity(),
         std::numeric_limits<T>::max()},
        {std::numeric_limits<T>::quiet_NaN(), 9.0009},
        {std::numeric_limits<T>::quiet_NaN(), 9.0009},
        {std::numeric_limits<T>::quiet_NaN(), 9.0009},
    });
    auto array2 = makeNullableArrayVector<T>({
        {1.0, -2.0, 4.0},
        {std::numeric_limits<T>::min(), 2.0199, -2.001, 1.000001},
        {1.0001, -2.02, std::numeric_limits<T>::max(), 8.00099},
        {9.0009, std::numeric_limits<T>::infinity()},
        {9.0009, std::numeric_limits<T>::quiet_NaN()},
        {std::numeric_limits<T>::quiet_NaN()},
        // quiet NaN and signaling NaN are treated equal
        {std::numeric_limits<T>::signaling_NaN()},
    });

    auto expected = makeNullableArrayVector<T>(
        {{1.0001, 3.03, std::nullopt, 4.00004},
         {2.02, 1},
         {8.0001, std::nullopt},
         {std::numeric_limits<T>::max()},
         {},
         {9.0009},
         {9.0009}});
    testExpr(expected, "array_except(C0, C1)", {array1, array2});

    expected = makeNullableArrayVector<T>(
        {{1.0, 4.0},
         {2.0199, 1.000001},
         {1.0001, -2.02, 8.00099},
         {},
         {},
         {},
         {}});
    testExpr(expected, "array_except(C1, C0)", {array1, array2});
  }
};

} // namespace

TEST_F(ArrayExceptTest, intArrays) {
  testInt<int8_t>();
  testInt<int16_t>();
  testInt<int32_t>();
  testInt<int64_t>();
}

TEST_F(ArrayExceptTest, floatArrays) {
  testFloatingPoint<float>();
  testFloatingPoint<double>();
}

TEST_F(ArrayExceptTest, boolArrays) {
  auto array1 = makeNullableArrayVector<bool>(
      {{true, false},
       {true, true},
       {false, false},
       {},
       {true, false, true, std::nullopt},
       {std::nullopt, true, false, true},
       {false, true, false},
       {true, false, true}});

  auto array2 = makeNullableArrayVector<bool>(
      {{true},
       {true, true},
       {false, false},
       {},
       {true, std::nullopt},
       {std::nullopt, false},
       {false, true, false},
       {true, false, true}});

  auto expected = makeNullableArrayVector<bool>(
      {{false}, {}, {}, {}, {false}, {true}, {}, {}});

  testExpr(expected, "array_except(C0, C1)", {array1, array2});

  expected = makeArrayVector<bool>({{}, {}, {}, {}, {}, {}, {}, {}});
  testExpr(expected, "array_except(C1, C0)", {array1, array2});
}

// Test inline strings.
TEST_F(ArrayExceptTest, strArrays) {
  using S = StringView;

  auto array1 = makeNullableArrayVector<StringView>({
      {S("a"), std::nullopt, S("b")},
      {S("a"), S("b"), S("a"), S("a")},
      {std::nullopt, S("b"), std::nullopt},
      {S("abc")},
  });
  auto array2 = makeNullableArrayVector<StringView>({
      {S("a")},
      {S("a"), S("b"), S("b")},
      {std::nullopt, std::nullopt, std::nullopt},
      {S("abc"), S("a"), S("b")},
  });
  auto expected = makeNullableArrayVector<StringView>({
      {std::nullopt, S("b")},
      {},
      {S("b")},
      {},
  });
  testExpr(expected, "array_except(C0, C1)", {array1, array2});
  expected = makeNullableArrayVector<StringView>({
      {},
      {},
      {},
      {S("a"), S("b")},
  });
  testExpr(expected, "array_except(C1, C0)", {array1, array2});
}

// Test non-inline (> 12 length) strings.
TEST_F(ArrayExceptTest, longStrArrays) {
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
  auto expected = makeNullableArrayVector<StringView>({
      {S("blue clear sky above")},
      {S("blue clear sky above"),
       S("yellow rose flowers"),
       S("orange beautiful sunset")},
      {},
      {S("purple is an elegant color")},
  });
  testExpr(expected, "array_except(C0, C1)", {array1, array2});
  expected = makeArrayVector<StringView>({
      {},
      {},
      {},
      {},
  });
  testExpr(expected, "array_except(C1, C0)", {array1, array2});
}

TEST_F(ArrayExceptTest, varbinary) {
  auto left = makeNullableArrayVector<StringView>(
      {{"a"_sv, "b"_sv, "c"_sv}}, ARRAY(VARBINARY()));
  auto right = makeNullableArrayVector<StringView>(
      {{"b"_sv, "d"_sv}}, ARRAY(VARBINARY()));

  const std::vector<std::vector<std::optional<StringView>>> data = {{}};
  auto expected = makeNullableArrayVector<StringView>(data, ARRAY(VARBINARY()));
  testExpr(expected, "array_except(c0, c1)", {left, left});
  testExpr(expected, "array_except(c0, c1)", {right, right});

  expected = makeNullableArrayVector<StringView>(
      {{"a"_sv, "c"_sv}}, ARRAY(VARBINARY()));
  testExpr(expected, "array_except(c0, c1)", {left, right});

  expected =
      makeNullableArrayVector<StringView>({{"d"_sv}}, ARRAY(VARBINARY()));
  testExpr(expected, "array_except(c0, c1)", {right, left});
}

TEST_F(ArrayExceptTest, complexTypeArray) {
  auto left = makeNestedArrayVectorFromJson<int32_t>({
      "[null, [1, 2, 3], [null, null]]",
      "[[1], [2], []]",
      "[[1, null, 3]]",
      "[[1, null, 3]]",
  });

  auto right = makeNestedArrayVectorFromJson<int32_t>({
      "[[1, 2, 3]]",
      "[[1]]",
      "[[1, null, 3], [1, 2]]",
      "[[1, null, 3, null]]",
  });

  auto expected = makeNestedArrayVectorFromJson<int32_t>({
      "[null, [null, null]]",
      "[[2], []]",
      "[]",
      "[[1, null, 3]]",
  });
  testExpr(expected, "array_except(c0, c1)", {left, right});
}

TEST_F(ArrayExceptTest, complexTypeMap) {
  std::vector<Pair<StringView, int64_t>> a{{"blue", 1}, {"red", 2}};
  std::vector<Pair<StringView, int64_t>> b{{"blue", 2}, {"red", 2}};
  std::vector<Pair<StringView, int64_t>> c{{"green", std::nullopt}};
  std::vector<Pair<StringView, int64_t>> d{{"yellow", 4}, {"purple", 5}};
  std::vector<std::vector<std::vector<Pair<StringView, int64_t>>>> leftData{
      {b, a}, {b}, {c, a}};
  std::vector<std::vector<std::vector<Pair<StringView, int64_t>>>> rightData{
      {a, b}, {}, {a}};
  std::vector<std::vector<std::vector<Pair<StringView, int64_t>>>> expectedData{
      {}, {b}, {c}};

  auto left = makeArrayOfMapVector<StringView, int64_t>(leftData);
  auto right = makeArrayOfMapVector<StringView, int64_t>(rightData);
  auto expected = makeArrayOfMapVector<StringView, int64_t>(expectedData);

  testExpr(expected, "array_except(c0, c1)", {left, right});
}

TEST_F(ArrayExceptTest, complexTypeRow) {
  RowTypePtr rowType = ROW({INTEGER(), VARCHAR()});

  using ArrayOfRow = std::vector<std::optional<std::tuple<int, std::string>>>;
  std::vector<ArrayOfRow> leftData = {
      {{{1, "red"}}, {{2, "blue"}}, {{3, "green"}}},
      {{{1, "red"}}, {{2, "blue"}}, {}},
      {{{1, "red"}}, std::nullopt, std::nullopt}};
  std::vector<ArrayOfRow> rightData = {
      {{{2, "blue"}}, {{1, "red"}}}, {{}, {{1, "green"}}}, {{{1, "red"}}}};
  std::vector<ArrayOfRow> expectedData = {
      {{{3, "green"}}}, {{{1, "red"}}, {{2, "blue"}}}, {std::nullopt}};
  auto left = makeArrayOfRowVector(leftData, rowType);
  auto right = makeArrayOfRowVector(rightData, rowType);
  auto expected = makeArrayOfRowVector(expectedData, rowType);
  testExpr(expected, "array_except(c0, c1)", {left, right});
}

// When one of the arrays is constant.
TEST_F(ArrayExceptTest, constant) {
  auto array1 = makeNullableArrayVector<int32_t>({
      {1, -2, 3, std::nullopt, 4, 5, 6, std::nullopt},
      {1, 2, -2, 1},
      {3, 8, std::nullopt},
      {1, 1, -2, -2, -2, 4, 8},
  });
  auto expected = makeNullableArrayVector<int32_t>({
      {3, std::nullopt, 5, 6},
      {2},
      {3, 8, std::nullopt},
      {8},
  });
  testExpr(expected, "array_except(C0, ARRAY[1,-2,4])", {array1});
  expected = makeNullableArrayVector<int32_t>({
      {},
      {4},
      {1, -2, 4},
      {},
  });
  testExpr(expected, "array_except(ARRAY[1,-2,4], C0)", {array1});
  testExpr(expected, "array_except(ARRAY[1,1,-2,1,-2,4,1,4,4], C0)", {array1});

  // Array containing NULLs.
  expected = makeNullableArrayVector<int32_t>({
      {-2, 3, 5, 6},
      {2, -2},
      {3, 8},
      {-2, 8},
  });
  testExpr(expected, "array_except(C0, ARRAY[1,NULL,4])", {array1});

  expected = makeNullableArrayVector<int32_t>({
      {},
      {std::nullopt, 4},
      {1, 4},
      {std::nullopt},
  });
  testExpr(expected, "array_except(ARRAY[1,NULL,4], C0)", {array1});
}

TEST_F(ArrayExceptTest, dictionaryEncodedElementsInConstant) {
  TestingDictionaryArrayElementsFunction::registerFunction();

  auto array = makeArrayVector<int64_t>({{1, 3}, {2, 5}, {0, 6}});
  auto expected = makeArrayVector<int64_t>({{}, {5}, {6}});
  testExpr(
      expected,
      "array_except(c0, testing_dictionary_array_elements(ARRAY [0, 1, 3, 2, 2, 3, 2]))",
      {array});
}
