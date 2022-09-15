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
using namespace facebook::velox::functions::test;

namespace {

class ArrayCombinationsTest : public FunctionBaseTest {
 protected:
  void testExpr(
      const VectorPtr& expected,
      const std::string& expression,
      const std::vector<VectorPtr>& input) {
    auto result = evaluate<ArrayVector>(expression, makeRowVector(input));
    facebook::velox::test::assertEqualVectors(expected, result);
  }

  template <typename T>
  void testInt() {
    auto arrayVector = makeArrayVector<T>(
        {{0, 1, 2, 3}, {0, 1, 2, 3}, {0, 1, 2, 3}, {0, 1, 2, 3}});
    auto comboLengthVector = makeFlatVector<int32_t>({0, 3, 4, 5});
    auto expected = makeNestedArrayVector<T>(
        {{{std::vector<std::optional<T>>()}},
         {{{0, 1, 2}}, {{0, 1, 3}}, {{0, 2, 3}}, {{1, 2, 3}}},
         {{{0, 1, 2, 3}}},
         {}});
    testExpr(
        expected, "combinations(C0, C1)", {arrayVector, comboLengthVector});
  }

  template <typename T>
  void testIntNullable() {
    auto arrayVector = makeNullableArrayVector<T>(
        {{0, 1, std::nullopt, 3},
         {0, 1, std::nullopt, 3},
         {0, 1, std::nullopt, 3},
         {0, 1, std::nullopt, 3}});
    auto comboLengthVector = makeFlatVector<int32_t>({0, 3, 4, 5});
    auto expected = makeNestedArrayVector<T>({
        {
            {std::vector<std::optional<T>>()},
        },
        {
            {{0, 1, std::nullopt}},
            {{0, 1, 3}},
            {{0, std::nullopt, 3}},
            {{1, std::nullopt, 3}},
        },
        {
            {{0, 1, std::nullopt, 3}},
        },
        {},
    });
    testExpr(
        expected, "combinations(C0, C1)", {arrayVector, comboLengthVector});
  }

  template <typename T>
  void testError(
      const std::vector<std::optional<T>>& inputArray,
      const int combinationLength,
      const std::string& expectedError) {
    auto arrayVector = makeNullableArrayVector<T>({inputArray});
    auto comboLengthVector =
        makeFlatVector<int32_t>(std::vector<int32_t>({combinationLength}));
    VELOX_ASSERT_THROW(
        evaluate<ArrayVector>(
            "combinations(C0, C1)",
            makeRowVector({arrayVector, comboLengthVector})),
        expectedError)
  }
};

} // namespace

TEST_F(ArrayCombinationsTest, intArrays) {
  testIntNullable<int8_t>();
  testIntNullable<int16_t>();
  testIntNullable<int32_t>();
  testIntNullable<int64_t>();
  testIntNullable<float>();
  testIntNullable<double>();
  testInt<int8_t>();
  testInt<int16_t>();
  testInt<int32_t>();
  testInt<int64_t>();
  testInt<float>();
  testInt<double>();
}

TEST_F(ArrayCombinationsTest, errorCases) {
  testError<int32_t>(
      {0, 1, 2, 3}, -1, "combination size must not be negative: -1");
  testError<int32_t>({0, 1, 2, 3}, 8, "combination size must not exceed 5: 8");
  testError<int32_t>(
      std::vector<std::optional<int32_t>>(100001, 1),
      3,
      "combinations exceed max size of 100000");
  testError<int32_t>(
      std::vector<std::optional<int32_t>>(99999, 1),
      3,
      "combinations exceed max size of 100000");
}

TEST_F(ArrayCombinationsTest, inlineVarcharArrays) {
  using S = StringView;
  auto arrayVector = makeNullableArrayVector<S>({
      {},
      {""},
      {std::nullopt},
      {"aa", std::nullopt, "bb"},
      {"bb", "aa", "cc", "aa", "ddd"},
      {"aa", std::nullopt, "bb"},
  });
  auto comboLengthVector = makeFlatVector<int32_t>({0, 1, 1, 2, 4, 5});

  auto expected = makeNestedArrayVector<S>(
      {{{std::vector<std::optional<S>>()}},
       {{{""}}},
       {{{std::optional<S>(std::nullopt)}}},
       {{{"aa", std::nullopt}}, {{"aa", "bb"}}, {{std::nullopt, "bb"}}},
       {{{"bb", "aa", "cc", "aa"}},
        {{"bb", "aa", "cc", "ddd"}},
        {{"bb", "aa", "aa", "ddd"}},
        {{"bb", "cc", "aa", "ddd"}},
        {{"aa", "cc", "aa", "ddd"}}},
       {}});
  testExpr(expected, "combinations(C0, C1)", {arrayVector, comboLengthVector});
}

TEST_F(ArrayCombinationsTest, varcharArrays) {
  using S = StringView;
  auto arrayVector = makeNullableArrayVector<S>({
      {},
      {""},
      {std::nullopt},
      {"red shiny car ahead", std::nullopt, "blue clear sky above"},
      {"blue clear sky above",
       "red shiny car ahead",
       "yellow rose flowers",
       "red shiny car ahead",
       "purple is an elegant color"},
      {"red shiny car ahead", std::nullopt, "blue clear sky above"},
  });
  auto comboLengthVector = makeFlatVector<int32_t>({0, 1, 1, 2, 4, 5});

  auto expected = makeNestedArrayVector<S>(
      {{{std::vector<std::optional<S>>()}},
       {{{""}}},
       {{{std::optional<S>(std::nullopt)}}},
       {{{"red shiny car ahead", std::nullopt}},
        {{"red shiny car ahead", "blue clear sky above"}},
        {{std::nullopt, "blue clear sky above"}}},
       {{{"blue clear sky above",
          "red shiny car ahead",
          "yellow rose flowers",
          "red shiny car ahead"}},
        {{"blue clear sky above",
          "red shiny car ahead",
          "yellow rose flowers",
          "purple is an elegant color"}},
        {{"blue clear sky above",
          "red shiny car ahead",
          "red shiny car ahead",
          "purple is an elegant color"}},
        {{"blue clear sky above",
          "yellow rose flowers",
          "red shiny car ahead",
          "purple is an elegant color"}},
        {{"red shiny car ahead",
          "yellow rose flowers",
          "red shiny car ahead",
          "purple is an elegant color"}}},
       {}});
  testExpr(expected, "combinations(C0, C1)", {arrayVector, comboLengthVector});
}

TEST_F(ArrayCombinationsTest, boolNullableArrays) {
  auto arrayVector = makeNullableArrayVector<bool>({
      {},
      {std::nullopt},
      {true, std::nullopt, false},
      {false, true, false, true, true},
      {true, std::nullopt, false},
  });
  auto comboLengthVector = makeFlatVector<int32_t>({0, 1, 2, 4, 5});

  auto expected = makeNestedArrayVector<bool>(
      {{{std::vector<std::optional<bool>>()}},
       {{{std::optional<bool>(std::nullopt)}}},
       {{{true, std::nullopt}}, {{true, false}}, {{std::nullopt, false}}},
       {{{false, true, false, true}},
        {{false, true, false, true}},
        {{false, true, true, true}},
        {{false, false, true, true}},
        {{true, false, true, true}}},
       {}});
  testExpr(expected, "combinations(C0, C1)", {arrayVector, comboLengthVector});
}

TEST_F(ArrayCombinationsTest, boolArrays) {
  auto arrayVector = makeNullableArrayVector<bool>({
      {},
      {true},
      {true, true, false},
      {false, true, false, true, true},
      {true, true, false},
  });
  auto comboLengthVector = makeFlatVector<int32_t>({0, 1, 2, 4, 5});

  auto expected = makeNestedArrayVector<bool>(
      {{{std::vector<std::optional<bool>>()}},
       {{{true}}},
       {{{true, true}}, {{true, false}}, {{true, false}}},
       {{{false, true, false, true}},
        {{false, true, false, true}},
        {{false, true, true, true}},
        {{false, false, true, true}},
        {{true, false, true, true}}},
       {}});
  testExpr(expected, "combinations(C0, C1)", {arrayVector, comboLengthVector});
}
