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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::functions::test;

namespace {

class ArrayMinTest : public FunctionBaseTest {
 protected:
  template <typename T, typename TExpected = T>
  void testExpr(
      const VectorPtr& expected,
      const std::string& expression,
      const std::vector<VectorPtr>& input) {
    auto result =
        evaluate<SimpleVector<TExpected>>(expression, makeRowVector(input));
    assertEqualVectors(expected, result);
  }

  void testDocExample() {
    auto input1 = makeNullableArrayVector<int64_t>(
        {{1, 2, 3}, {-1, -2, -2}, {-1, -2, std::nullopt}, {}});
    auto expected1 =
        makeNullableFlatVector<int64_t>({1, -2, std::nullopt, std::nullopt});
    testExpr<int64_t>(expected1, "array_min(C0)", {input1});

    static const float kNaN = std::numeric_limits<float>::quiet_NaN();
    static const float kInfinity = std::numeric_limits<float>::infinity();
    auto input2 = makeNullableArrayVector<float>(
        {{-1, kNaN, std::nullopt}, {-1, -2, -3, kNaN}, {kInfinity, kNaN}});
    auto expected2 =
        makeNullableFlatVector<float>({std::nullopt, -3, kInfinity});

    testExpr<float>(expected2, "array_min(C0)", {input2});
  }

  template <typename T>
  void testIntNullable() {
    auto arrayVector = makeNullableArrayVector<T>(
        {{-1, 0, 1, 2, 3, 4},
         {4, 3, 2, 1, 0, -1, -2},
         {-5, -4, -3, -2, -1},
         {101, 102, 103, 104, std::nullopt},
         {std::nullopt, -1, -2, -3, -4},
         {},
         {std::nullopt}});
    auto expected = makeNullableFlatVector<T>(
        {-1, -2, -5, std::nullopt, std::nullopt, std::nullopt, std::nullopt});
    testExpr<T>(expected, "array_min(C0)", {arrayVector});
  }

  template <typename T>
  void testInt() {
    auto arrayVector = makeArrayVector<T>(
        {{-1, 0, 1, 2, 3, 4},
         {4, 3, 2, 1, 0, -1, -2},
         {-5, -4, -3, -2, -1},
         {101, 102, 103, 104, 105},
         {}});
    auto expected = makeNullableFlatVector<T>({-1, -2, -5, 101, std::nullopt});
    testExpr<T>(expected, "array_min(C0)", {arrayVector});
  }

  void testInLineVarcharNullable() {
    using S = StringView;

    auto arrayVector = makeNullableArrayVector<StringView>({
        {S("red"), S("blue")},
        {std::nullopt, S("blue"), S("yellow"), S("orange")},
        {},
        {S("red"), S("purple"), S("green")},
    });
    auto expected = makeNullableFlatVector<StringView>(
        {S("blue"), std::nullopt, std::nullopt, S("green")});
    testExpr<StringView>(expected, "array_min(C0)", {arrayVector});
  }

  void testVarcharNullable() {
    using S = StringView;
    // use > 12 length string to avoid inlining
    auto arrayVector = makeNullableArrayVector<StringView>({
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
    auto expected = makeNullableFlatVector<StringView>(
        {S("blue clear sky above"),
         std::nullopt,
         std::nullopt,
         S("green plants make us happy")});
    testExpr<StringView>(expected, "array_min(C0)", {arrayVector});
  }

  void testBoolNullable() {
    auto arrayVector = makeNullableArrayVector<bool>(
        {{true, false},
         {true},
         {false},
         {},
         {true, false, true, std::nullopt},
         {std::nullopt, true, false, true},
         {false, false, false},
         {true, true, true}});

    auto expected = makeNullableFlatVector<bool>(
        {false,
         true,
         false,
         std::nullopt,
         std::nullopt,
         std::nullopt,
         false,
         true});
    testExpr<bool>(expected, "array_min(C0)", {arrayVector});
  }

  void testBool() {
    auto arrayVector = makeArrayVector<bool>(
        {{true, false},
         {true},
         {false},
         {},
         {false, false, false},
         {true, true, true}});

    auto expected = makeNullableFlatVector<bool>(
        {false, true, false, std::nullopt, false, true});
    testExpr<bool>(expected, "array_min(C0)", {arrayVector});
  }

  template <typename T>
  void testFloatingPoint() {
    static const T kNaN = std::numeric_limits<T>::quiet_NaN();
    static const T kInfinity = std::numeric_limits<T>::infinity();
    static const T kNegativeInfinity = -1 * std::numeric_limits<T>::infinity();
    auto input = makeNullableArrayVector<T>(
        {{-1, std::nullopt, kNaN},
         {-1, std::nullopt, 2},
         {-1, 0, 2},
         {-1, kNegativeInfinity, kNaN},
         {kInfinity, kNaN},
         {kNaN, kNaN}});
    auto expected = makeNullableFlatVector<T>(
        {std::nullopt, std::nullopt, -1, kNegativeInfinity, kInfinity, kNaN});

    testExpr<T>(expected, "array_min(C0)", {input});
  }
};

} // namespace
TEST_F(ArrayMinTest, docs) {
  testDocExample();
}

TEST_F(ArrayMinTest, intArrays) {
  testIntNullable<int8_t>();
  testIntNullable<int16_t>();
  testIntNullable<int32_t>();
  testIntNullable<int64_t>();
  testInt<int8_t>();
  testInt<int16_t>();
  testInt<int32_t>();
  testInt<int64_t>();
}

TEST_F(ArrayMinTest, varcharArrays) {
  testInLineVarcharNullable();
  testVarcharNullable();
}

TEST_F(ArrayMinTest, boolArrays) {
  testBoolNullable();
  testBool();
}

TEST_F(ArrayMinTest, floatArrays) {
  testFloatingPoint<float>();
  testFloatingPoint<double>();
}

TEST_F(ArrayMinTest, complexTypeElements) {
  auto elements = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 3, 2, 1}),
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6}),
  });

  auto arrayVector = makeArrayVector({0}, elements);
  auto result = evaluate("array_min(c0)", makeRowVector({arrayVector}));

  auto expected = makeRowVector({
      makeFlatVector(std::vector<int32_t>{1}),
      makeFlatVector(std::vector<int32_t>{1}),
  });
  assertEqualVectors(expected, result);

  result = evaluate("array_max(c0)", makeRowVector({arrayVector}));

  expected = makeRowVector({
      makeFlatVector(std::vector<int32_t>{3}),
      makeFlatVector(std::vector<int32_t>{4}),
  });
  assertEqualVectors(expected, result);

  // Array with null element.
  elements->setNull(5, true);
  result = evaluate("array_min(c0)", makeRowVector({arrayVector}));

  ASSERT_EQ(1, result->size());
  ASSERT_TRUE(result->isNullAt(0));

  elements->clearAllNulls();
  elements->setNull(0, true);
  result = evaluate("array_min(c0)", makeRowVector({arrayVector}));

  ASSERT_EQ(1, result->size());
  ASSERT_TRUE(result->isNullAt(0));

  // Empty array.
  arrayVector->setOffsetAndSize(0, 0, 0);
  result = evaluate("array_min(c0)", makeRowVector({arrayVector}));

  ASSERT_EQ(1, result->size());
  ASSERT_TRUE(result->isNullAt(0));

  // Arrays with elements that contain nulls may or may not be orderable
  // (depending on whether it would be necessary to compare nulls to decide the
  // order).
  elements = makeRowVector({
      makeNullableFlatVector<int32_t>({1, std::nullopt, 3, 3, 2, 1}),
      makeNullableFlatVector<int32_t>({1, 2, 3, 4, 5, 6}),
  });
  arrayVector = makeArrayVector({0}, elements);

  VELOX_ASSERT_THROW(
      evaluate("array_min(c0)", makeRowVector({arrayVector})),
      "Ordering nulls is not supported");

  VELOX_ASSERT_THROW(
      evaluate("array_max(c0)", makeRowVector({arrayVector})),
      "Ordering nulls is not supported");

  elements = makeRowVector({
      makeNullableFlatVector<int32_t>({1, 2, 3, 3, 2, 1}),
      makeNullableFlatVector<int32_t>({1, std::nullopt, 3, 4, 5, 6}),
  });
  arrayVector = makeArrayVector({0}, elements);

  result = evaluate("array_min(c0)", makeRowVector({arrayVector}));
  expected = makeRowVector({
      makeFlatVector(std::vector<int32_t>{1}),
      makeFlatVector(std::vector<int32_t>{1}),
  });
  assertEqualVectors(expected, result);

  result = evaluate("array_max(c0)", makeRowVector({arrayVector}));
  expected = makeRowVector({
      makeFlatVector(std::vector<int32_t>{3}),
      makeFlatVector(std::vector<int32_t>{4}),
  });
  assertEqualVectors(expected, result);
}
