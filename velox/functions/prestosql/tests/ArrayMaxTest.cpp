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

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::functions::test;

namespace {

class ArrayMaxTest : public FunctionBaseTest {
 protected:
  void testArrayMax(const VectorPtr& input, const VectorPtr& expected) {
    auto result = evaluate<BaseVector>("array_max(C0)", makeRowVector({input}));
    assertEqualVectors(expected, result);
  }
};

TEST_F(ArrayMaxTest, booleanWithNulls) {
  auto input = makeNullableArrayVector<bool>(
      {{true, false},
       {true},
       {false},
       {},
       {true, false, true, std::nullopt},
       {std::nullopt, true, false, true},
       {false, false, false},
       {true, true, true}});

  auto expected = makeNullableFlatVector<bool>(
      {true,
       true,
       false,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       false,
       true});

  testArrayMax(input, expected);
}

TEST_F(ArrayMaxTest, booleanNoNulls) {
  auto input = makeNullableArrayVector<bool>(
      {{true, false},
       {true},
       {false},
       {},
       {false, false, false},
       {true, true, true}});

  auto expected = makeNullableFlatVector<bool>(
      {true, true, false, std::nullopt, false, true});

  testArrayMax(input, expected);
}

TEST_F(ArrayMaxTest, varcharWithNulls) {
  auto input = makeNullableArrayVector<StringView>({
      {"red"_sv, "blue"_sv},
      {std::nullopt, "blue"_sv, "yellow"_sv, "orange"_sv},
      {},
      {"red"_sv, "purple"_sv, "green"_sv},
  });

  auto expected = makeNullableFlatVector<StringView>(
      {"red"_sv, std::nullopt, std::nullopt, "red"_sv});

  testArrayMax(input, expected);
}

TEST_F(ArrayMaxTest, varcharNoNulls) {
  auto input = makeArrayVector<StringView>({
      {"red"_sv, "blue"_sv},
      {"blue"_sv, "yellow"_sv, "orange"_sv},
      {},
      {"red"_sv, "purple"_sv, "green"_sv},
  });

  auto expected = makeNullableFlatVector<StringView>(
      {"red"_sv, "yellow"_sv, std::nullopt, "red"_sv});

  testArrayMax(input, expected);
}

// Test non-inlined (> 12 length) nullable strings.
TEST_F(ArrayMaxTest, longVarcharWithNulls) {
  auto input = makeNullableArrayVector<StringView>({
      {"red shiny car ahead"_sv, "blue clear sky above"_sv},
      {std::nullopt,
       "blue clear sky above"_sv,
       "yellow rose flowers"_sv,
       "orange beautiful sunset"_sv},
      {},
      {"red shiny car ahead"_sv,
       "purple is an elegant color"_sv,
       "green plants make us happy"_sv},
  });

  auto expected = makeNullableFlatVector<StringView>(
      {"red shiny car ahead"_sv,
       std::nullopt,
       std::nullopt,
       "red shiny car ahead"_sv});

  testArrayMax(input, expected);
}

// Test non-inlined (> 12 length) strings.
TEST_F(ArrayMaxTest, longVarcharNoNulls) {
  auto input = makeNullableArrayVector<StringView>({
      {"red shiny car ahead"_sv, "blue clear sky above"_sv},
      {"blue clear sky above"_sv,
       "yellow rose flowers"_sv,
       "orange beautiful sunset"_sv},
      {},
      {"red shiny car ahead"_sv,
       "purple is an elegant color"_sv,
       "green plants make us happy"_sv},
  });

  auto expected = makeNullableFlatVector<StringView>(
      {"red shiny car ahead"_sv,
       "yellow rose flowers"_sv,
       std::nullopt,
       "red shiny car ahead"_sv});

  testArrayMax(input, expected);
}

// Test documented example.
TEST_F(ArrayMaxTest, docs) {
  auto input = makeNullableArrayVector<int32_t>(
      {{1, 2, 3}, {-1, -2, -2}, {-1, -2, std::nullopt}, {}});
  auto expected =
      makeNullableFlatVector<int32_t>({3, -1, std::nullopt, std::nullopt});
  testArrayMax(input, expected);
}

template <typename Type>
class ArrayMaxIntegralTest : public FunctionBaseTest {
 public:
  using T = typename Type::NativeType::NativeType;

  void testArrayMax(const VectorPtr& input, const VectorPtr& expected) {
    auto result =
        evaluate<SimpleVector<T>>("array_max(C0)", makeRowVector({input}));
    assertEqualVectors(expected, result);
  }

  void testWithNulls() {
    auto input = makeNullableArrayVector<T>(
        {{std::numeric_limits<T>::min(),
          0,
          1,
          2,
          3,
          std::numeric_limits<T>::max()},
         {std::numeric_limits<T>::max(),
          3,
          2,
          1,
          0,
          -1,
          std::numeric_limits<T>::min()},
         {101, 102, 103, std::numeric_limits<T>::max(), std::nullopt},
         {std::nullopt, -1, -2, -3, std::numeric_limits<T>::min()},
         {},
         {std::nullopt}});

    auto expected = makeNullableFlatVector<T>(
        {std::numeric_limits<T>::max(),
         std::numeric_limits<T>::max(),
         std::nullopt,
         std::nullopt,
         std::nullopt,
         std::nullopt});

    testArrayMax(input, expected);
  }

  void testNoNulls() {
    auto input = makeArrayVector<T>(
        {{std::numeric_limits<T>::min(), 0, 1, 2, 3, 4},
         {std::numeric_limits<T>::max(), 3, 2, 1, 0, -1, -2},
         {std::numeric_limits<T>::max(),
          101,
          102,
          103,
          104,
          105,
          std::numeric_limits<T>::max()},
         {}});

    auto expected = makeNullableFlatVector<T>(
        {4,
         std::numeric_limits<T>::max(),
         std::numeric_limits<T>::max(),
         std::nullopt});

    testArrayMax(input, expected);
  }
};

template <typename Type>
class ArrayMaxFloatingPointTest : public FunctionBaseTest {
 public:
  using T = typename Type::NativeType::NativeType;

  void testArrayMax(VectorPtr input, VectorPtr expected) {
    auto result =
        evaluate<SimpleVector<T>>("array_max(C0)", makeRowVector({input}));
    assertEqualVectors(expected, result);
  }

  void testWithNulls() {
    auto input = makeNullableArrayVector<T>(
        {{0.0000, 0.00001},
         {std::nullopt, 1.1, 1.11, -2.2, -1.0, std::numeric_limits<T>::min()},
         {std::numeric_limits<T>::min(),
          -0.0001,
          -0.0002,
          -0.0003,
          std::numeric_limits<T>::max()},
         {},
         {std::numeric_limits<T>::min(), 1.1, 1.22222, 1.33, std::nullopt},
         {-0.00001, -0.0002, 0.0001}});

    auto expected = makeNullableFlatVector<T>(
        {0.00001,
         std::nullopt,
         std::numeric_limits<T>::max(),
         std::nullopt,
         std::nullopt,
         0.0001});
    testArrayMax(input, expected);
  }

  void testNoNulls() {
    auto input = makeArrayVector<T>(
        {{0.0000, 0.00001},
         {std::numeric_limits<T>::max(),
          1.1,
          1.11,
          -2.2,
          -1.0,
          std::numeric_limits<T>::min()},
         {std::numeric_limits<T>::min(),
          -0.0001,
          -0.0002,
          -0.0003,
          std::numeric_limits<T>::max()},
         {},
         {1.1, 1.22222, 1.33},
         {-0.00001, -0.0002, 0.0001}});

    auto expected = makeNullableFlatVector<T>(
        {0.00001,
         std::numeric_limits<T>::max(),
         std::numeric_limits<T>::max(),
         std::nullopt,
         1.33,
         0.0001});
    testArrayMax(input, expected);
  }
};

} // namespace

TYPED_TEST_SUITE(
    ArrayMaxIntegralTest,
    FunctionBaseTest::IntegralTypes,
    FunctionBaseTest::TypeNames);

TYPED_TEST(ArrayMaxIntegralTest, arrayMaxNullable) {
  this->testWithNulls();
}

TYPED_TEST(ArrayMaxIntegralTest, arrayMax) {
  this->testNoNulls();
}

TYPED_TEST_SUITE(
    ArrayMaxFloatingPointTest,
    FunctionBaseTest::FloatingPointTypes,
    FunctionBaseTest::TypeNames);

TYPED_TEST(ArrayMaxFloatingPointTest, arrayMaxNullable) {
  this->testWithNulls();
}

TYPED_TEST(ArrayMaxFloatingPointTest, arrayMax) {
  this->testNoNulls();
}
