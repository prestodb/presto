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
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

class ReduceTest : public functions::test::FunctionBaseTest {};

TEST_F(ReduceTest, basic) {
  vector_size_t size = 1'000;
  auto inputArray = makeArrayVector<int64_t>(
      size,
      modN(5),
      [](auto row, auto index) { return row + index; },
      nullEvery(11));
  auto input = makeRowVector({inputArray});
  registerLambda(
      "sum_input",
      rowType("s", BIGINT(), "x", BIGINT()),
      input->type(),
      "s + x");
  registerLambda("sum_output", rowType("s", BIGINT()), input->type(), "s * 3");

  auto result = evaluate<SimpleVector<int64_t>>(
      "reduce(c0, 10, function('sum_input'), function('sum_output'))", input);

  auto expectedResult = makeFlatVector<int64_t>(
      size,
      [](auto row) {
        int64_t sum = 10;
        for (auto i = 0; i < row % 5; i++) {
          sum += row + i;
        }
        return sum * 3;
      },
      nullEvery(11));
  assertEqualVectors(expectedResult, result);
}

// Types of array elements, intermediate results and final results are all
// different: BIGINT vs. DOUBLE vs. BOOLEAN:
//  reduce(a, 100, (s, x) -> s + x * 0.1, s -> s < 101)
TEST_F(ReduceTest, differentResultType) {
  vector_size_t size = 1'000;
  auto inputArray = makeArrayVector<int64_t>(
      size,
      modN(5),
      [](auto row, auto index) { return row + index; },
      nullEvery(11));
  auto input = makeRowVector({inputArray});
  registerLambda(
      "input",
      rowType("s", DOUBLE(), "x", BIGINT()),
      input->type(),
      "s + cast(x as double) * 0.1");
  registerLambda("output", rowType("s", DOUBLE()), input->type(), "s < 101.0");

  auto result = evaluate<SimpleVector<bool>>(
      "reduce(c0, 100.0, function('input'), function('output'))", input);

  auto expectedResult = makeFlatVector<bool>(
      size,
      [](auto row) {
        double sum = 100;
        for (auto i = 0; i < row % 5; i++) {
          sum += (row + i) * 0.1;
        }
        return sum < 101;
      },
      nullEvery(11));
  assertEqualVectors(expectedResult, result);
}

// Test different lambdas applied to different rows
TEST_F(ReduceTest, conditional) {
  vector_size_t size = 1'000;

  // Make 2 columns: the array to transform and a boolean that decided which
  // lambda to use.
  auto inputArray = makeArrayVector<int64_t>(
      size,
      modN(5),
      [](auto row, auto index) { return row + index; },
      nullEvery(11));
  auto condition =
      makeFlatVector<bool>(size, [](auto row) { return row % 3 == 1; });
  auto input = makeRowVector({condition, inputArray});

  auto signature = rowType("s", BIGINT(), "x", BIGINT());
  registerLambda("sum_input", signature, input->type(), "s + x");
  registerLambda("sum_sq_input", signature, input->type(), "s + x * x");

  registerLambda("sum_output", rowType("s", BIGINT()), input->type(), "s");

  auto result = evaluate<SimpleVector<int64_t>>(
      "reduce(c1, 0, if (c0, function('sum_input'), function('sum_sq_input')), function('sum_output'))",
      input);

  auto expectedResult = makeFlatVector<int64_t>(
      size,
      [](auto row) {
        int64_t sum = 0;
        for (auto i = 0; i < row % 5; i++) {
          auto x = row + i;
          sum += (row % 3 == 1) ? x : x * x;
        }
        return sum;
      },
      nullEvery(11));
  assertEqualVectors(expectedResult, result);
}

TEST_F(ReduceTest, finalSelection) {
  vector_size_t size = 1'000;
  auto inputArray = makeArrayVector<int64_t>(
      size,
      modN(5),
      [](auto row, auto index) { return row + index; },
      nullEvery(11));
  auto input = makeRowVector({
      inputArray,
      makeFlatVector<int64_t>(
          size, [](auto row) { return row; }, nullEvery(11)),
  });
  registerLambda(
      "sum_input",
      rowType("s", BIGINT(), "x", BIGINT()),
      input->type(),
      "s + x");
  registerLambda(
      "row_output",
      rowType("s", BIGINT()),
      input->type(),
      "row_constructor(s)");

  auto result = evaluate<RowVector>(
      "if (c1 < 100, row_constructor(c1), "
      "reduce(c0, 10, function('sum_input'), function('row_output')))",
      input);

  auto expectedResult = makeRowVector({makeFlatVector<int64_t>(
      size,
      [](auto row) -> int64_t {
        if (row < 100) {
          return row;
        } else {
          int64_t sum = 10;
          for (auto i = 0; i < row % 5; i++) {
            sum += row + i;
          }
          return sum;
        }
      },
      nullEvery(11))});
  assertEqualVectors(expectedResult, result);
}

TEST_F(ReduceTest, elementIndicesOverwrite) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 2}),
      makeFlatVector<int64_t>({3, 4}),
  });

  registerLambda(
      "input",
      rowType("s", BIGINT(), "x", BIGINT()),
      ROW({ARRAY(BIGINT())}),
      "s + x");
  registerLambda("output", rowType("s", BIGINT()), ROW({ARRAY(BIGINT())}), "s");

  auto result = evaluate(
      "reduce(array[c0, c1], 100, function('input'), function('output'))",
      data);
  assertEqualVectors(makeFlatVector<int64_t>({104, 106}), result);
}
