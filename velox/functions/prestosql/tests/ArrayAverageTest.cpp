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

class ArrayAverageTest : public FunctionBaseTest {
 protected:
  // Evaluate an expression.
  void testExpr(const VectorPtr& expected, const VectorPtr& input) {
    auto result = evaluate("array_average(C0)", makeRowVector({input}));
    assertEqualVectors(expected, result);
  }
};

} // namespace

// Test floating point arrays.
TEST_F(ArrayAverageTest, doubleInput) {
  auto input = makeNullableArrayVector<double>(
      {{0.0, 1.0, 2.0}, {std::nullopt, 1.0, 2.0}, {std::nullopt}});
  auto expected = makeNullableFlatVector<double>({1.0, 1.5, std::nullopt});
  testExpr(expected, {input});
}

// Test floating point arrays: limits cases.
TEST_F(ArrayAverageTest, doubleLimitsInput) {
  using limits = std::numeric_limits<double>;
  auto input = makeNullableArrayVector<double>(
      {{limits::infinity(), 1.0, 2.0},
       {limits::lowest(), 1.0, 2.0},
       {limits::max(), 1.0, 2.0},
       {limits::quiet_NaN(), 1.0, 2.0}});
  auto expected = makeNullableFlatVector<double>(
      {limits::infinity(),
       -5.992310449541053E307,
       5.992310449541053E307,
       limits::quiet_NaN()});
  testExpr(expected, input);
}

// Test empty array in input.
TEST_F(ArrayAverageTest, emptyInput) {
  auto input = makeArrayVector<double>({{}});
  auto expected = makeNullableFlatVector<double>({std::nullopt});
  testExpr(expected, input);
}

// Test all non-null inputs including empty array.
TEST_F(ArrayAverageTest, nonNullInput) {
  auto input = makeArrayVector<double>({{}, {1.0, 2.0, 3.0}, {4.0}});
  auto expected = makeNullableFlatVector<double>({std::nullopt, 2.0, 4.0});
  testExpr(expected, input);
}
