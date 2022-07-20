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
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::functions::test;

namespace {

// Class to test the array_distinct operator.
class ArrayDistinctTest : public FunctionBaseTest {
 protected:
  // Evaluate an expression.
  void testExpr(
      const VectorPtr& expected,
      const std::string& expression,
      const std::vector<VectorPtr>& input) {
    auto result = evaluate<ArrayVector>(expression, makeRowVector(input));
    assertEqualVectors(expected, result);
  }

  // Execute test for integer types.
  template <typename T>
  void testInt() {
    auto array = makeNullableArrayVector<T>({
        {},
        {0},
        {1},
        {std::numeric_limits<T>::min()},
        {std::numeric_limits<T>::max()},
        {std::nullopt},
        {-1},
        {1, 2, 3},
        {1, 2, 1},
        {1, 1, 1},
        {-1, -2, -3},
        {-1, -2, -1},
        {-1, -1, -1},
        {std::nullopt, std::nullopt, std::nullopt},
        {1, 2, -2, 1},
        {1, 1, -2, -2, -2, 4, 8},
        {3, 8, std::nullopt},
        {1, 2, 3, std::nullopt, 4, 1, 2, std::nullopt},
    });

    auto expected = makeNullableArrayVector<T>({
        {},
        {0},
        {1},
        {std::numeric_limits<T>::min()},
        {std::numeric_limits<T>::max()},
        {std::nullopt},
        {-1},
        {1, 2, 3},
        {1, 2},
        {1},
        {-1, -2, -3},
        {-1, -2},
        {-1},
        {std::nullopt},
        {1, 2, -2},
        {1, -2, 4, 8},
        {3, 8, std::nullopt},
        {1, 2, 3, std::nullopt, 4},
    });

    testExpr(expected, "array_distinct(C0)", {array});
  }

  // Execute test for floating point types.
  template <typename T>
  void testFloatingPoint() {
    auto array = makeNullableArrayVector<T>({
        {},
        {0.0},
        {1.0001},
        {-2.0},
        {3.03},
        {std::numeric_limits<T>::min()},
        {std::numeric_limits<T>::max()},
        {std::numeric_limits<T>::lowest()},
        {std::numeric_limits<T>::infinity()},
        {std::numeric_limits<T>::quiet_NaN()},
        {std::numeric_limits<T>::signaling_NaN()},
        {std::numeric_limits<T>::denorm_min()},
        {std::nullopt},
        {0.0, 0.0},
        {0.0, 10.0},
        {0.0, -10.0},
        {std::numeric_limits<T>::quiet_NaN(),
         std::numeric_limits<T>::quiet_NaN()},
        {std::numeric_limits<T>::signaling_NaN(),
         std::numeric_limits<T>::signaling_NaN()},
        {std::numeric_limits<T>::lowest(), std::numeric_limits<T>::lowest()},
        {std::nullopt, std::nullopt},
        {1.0001, -2.0, 3.03, std::nullopt, 4.00004},
        {std::numeric_limits<T>::min(), 2.02, -2.001, 1},
        {std::numeric_limits<T>::max(), 8.0001, std::nullopt},
        {9.0009,
         std::numeric_limits<T>::infinity(),
         std::numeric_limits<T>::max()},
        {std::numeric_limits<T>::quiet_NaN(), 9.0009},
    });
    auto expected = makeNullableArrayVector<T>({
        {},
        {0.0},
        {1.0001},
        {-2.0},
        {3.03},
        {std::numeric_limits<T>::min()},
        {std::numeric_limits<T>::max()},
        {std::numeric_limits<T>::lowest()},
        {std::numeric_limits<T>::infinity()},
        {std::numeric_limits<T>::quiet_NaN()},
        {std::numeric_limits<T>::signaling_NaN()},
        {std::numeric_limits<T>::denorm_min()},
        {std::nullopt},
        {0.0},
        {0.0, 10.0},
        {0.0, -10.0},
        {std::numeric_limits<T>::quiet_NaN(),
         std::numeric_limits<T>::quiet_NaN()},
        {std::numeric_limits<T>::signaling_NaN(),
         std::numeric_limits<T>::signaling_NaN()},
        {std::numeric_limits<T>::lowest()},
        {std::nullopt},
        {1.0001, -2.0, 3.03, std::nullopt, 4.00004},
        {std::numeric_limits<T>::min(), 2.02, -2.001, 1},
        {std::numeric_limits<T>::max(), 8.0001, std::nullopt},
        {9.0009,
         std::numeric_limits<T>::infinity(),
         std::numeric_limits<T>::max()},
        {std::numeric_limits<T>::quiet_NaN(), 9.0009},
    });

    testExpr(expected, "array_distinct(C0)", {array});
  }
};

} // namespace

// Test boolean arrays.
TEST_F(ArrayDistinctTest, boolArrays) {
  auto array = makeNullableArrayVector<bool>(
      {{},
       {true},
       {false},
       {std::nullopt},
       {true, false},
       {true, std::nullopt},
       {true, true},
       {false, false},
       {std::nullopt, std::nullopt},
       {true, false, true, std::nullopt},
       {std::nullopt, true, false, true},
       {false, true, false},
       {true, false, true}});

  auto expected = makeNullableArrayVector<bool>(
      {{},
       {true},
       {false},
       {std::nullopt},
       {true, false},
       {true, std::nullopt},
       {true},
       {false},
       {std::nullopt},
       {true, false, std::nullopt},
       {std::nullopt, true, false},
       {false, true},
       {true, false}});

  testExpr(expected, "array_distinct(C0)", {array});
}

// Test integer arrays.
TEST_F(ArrayDistinctTest, integerArrays) {
  testInt<int8_t>();
  testInt<int16_t>();
  testInt<int32_t>();
  testInt<int64_t>();
}

// Test floating point arrays.
TEST_F(ArrayDistinctTest, floatArrays) {
  testFloatingPoint<float>();
  testFloatingPoint<double>();
}

// Test inline (short) strings.
TEST_F(ArrayDistinctTest, inlineStringArrays) {
  using S = StringView;

  auto array = makeNullableArrayVector<StringView>({
      {},
      {S("")},
      {S(" ")},
      {S("a")},
      {std::nullopt},
      {S("a"), S("b")},
      {S("a"), S("A")},
      {S("a"), S("a")},
      {std::nullopt, std::nullopt},
      {S("a"), std::nullopt, S("b")},
      {S("a"), S("b"), S("a"), S("a")},
      {std::nullopt, S("b"), std::nullopt},
      {S("abc")},
  });

  auto expected = makeNullableArrayVector<StringView>({
      {},
      {S("")},
      {S(" ")},
      {S("a")},
      {std::nullopt},
      {S("a"), S("b")},
      {S("a"), S("A")},
      {S("a")},
      {std::nullopt},
      {S("a"), std::nullopt, S("b")},
      {S("a"), S("b")},
      {std::nullopt, S("b")},
      {S("abc")},
  });

  testExpr(expected, "array_distinct(C0)", {array});
}

// Test non-inline (> 12 character length) strings.
TEST_F(ArrayDistinctTest, stringArrays) {
  using S = StringView;

  auto array = makeNullableArrayVector<StringView>({
      {S("red shiny car ahead"), S("blue clear sky above")},
      {std::nullopt,
       S("blue clear sky above"),
       S("yellow rose flowers"),
       S("blue clear sky above"),
       S("orange beautiful sunset")},
      {std::nullopt, std::nullopt},
      {},
      {S("red shiny car ahead"),
       S("purple is an elegant color"),
       S("green plants make us happy")},
  });

  auto expected = makeNullableArrayVector<StringView>({
      {S("red shiny car ahead"), S("blue clear sky above")},
      {std::nullopt,
       S("blue clear sky above"),
       S("yellow rose flowers"),
       S("orange beautiful sunset")},
      {std::nullopt},
      {},
      {S("red shiny car ahead"),
       S("purple is an elegant color"),
       S("green plants make us happy")},
  });

  testExpr(expected, "array_distinct(C0)", {array});
}

TEST_F(ArrayDistinctTest, nonContiguousRows) {
  auto c0 = makeFlatVector<int32_t>(4, [](auto row) { return row; });
  auto c1 = makeArrayVector<int32_t>({
      {1, 2, 3, 3},
      {1, 2, 3, 4, 4},
      {1, 2, 3, 4, 5, 5},
      {1, 2, 3, 4, 5, 6, 6},
  });

  auto c2 = makeArrayVector<int32_t>({
      {0, 0, 1, 2, 3, 3},
      {0, 0, 1, 2, 3, 4, 4},
      {0, 0, 1, 2, 3, 4, 5, 5},
      {0, 0, 1, 2, 3, 4, 5, 6, 6},
  });

  auto expected = makeArrayVector<int32_t>({
      {1, 2, 3},
      {0, 1, 2, 3, 4},
      {1, 2, 3, 4, 5},
      {0, 1, 2, 3, 4, 5, 6},
  });

  auto result = evaluate<ArrayVector>(
      "if(c0 % 2 = 0, array_distinct(c1), array_distinct(c2))",
      makeRowVector({c0, c1, c2}));
  assertEqualVectors(expected, result);
}

TEST_F(ArrayDistinctTest, constant) {
  vector_size_t size = 1'000;
  auto data =
      makeArrayVector<int64_t>({{1, 2, 3, 2, 1}, {4, 5, 4, 5}, {6, 6, 6, 6}});

  auto evaluateConstant = [&](vector_size_t row, const VectorPtr& vector) {
    return evaluate(
        "array_distinct(c0)",
        makeRowVector({BaseVector::wrapInConstant(size, row, vector)}));
  };

  auto result = evaluateConstant(0, data);
  auto expected = makeConstantArray<int64_t>(size, {1, 2, 3});
  assertEqualVectors(expected, result);

  result = evaluateConstant(1, data);
  expected = makeConstantArray<int64_t>(size, {4, 5});
  assertEqualVectors(expected, result);

  result = evaluateConstant(2, data);
  expected = makeConstantArray<int64_t>(size, {6});
  assertEqualVectors(expected, result);
}
