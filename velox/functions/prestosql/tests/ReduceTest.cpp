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

namespace facebook::velox {
namespace {

using namespace facebook::velox::test;

class ReduceTest : public functions::test::FunctionBaseTest {
 protected:
  void testReduceRewrite(
      const RowVectorPtr& input,
      const std::string& lambdaBody) {
    TestRuntimeStatWriter writer;
    RuntimeStatWriterScopeGuard guard(&writer);
    auto actual = evaluate(
        fmt::format("reduce(c0, 10, (s, x) -> {}, s -> s)", lambdaBody), input);
    ASSERT_EQ(writer.stats().size(), 1);
    ASSERT_EQ(writer.stats()[0].first, "numReduceRewrite");
    ASSERT_EQ(writer.stats()[0].second.value, 1);
    auto expected = evaluate(
        fmt::format("reduce(c0, 10, (s, x) -> {}, s -> 1 * s)", lambdaBody),
        input);
    ASSERT_EQ(writer.stats().size(), 1);
    ASSERT_EQ(writer.stats()[0].first, "numReduceRewrite");
    ASSERT_EQ(writer.stats()[0].second.value, 1);
    assertEqualVectors(expected, actual);
  }
};

TEST_F(ReduceTest, basic) {
  vector_size_t size = 1'000;
  auto inputArray = makeArrayVector<int64_t>(
      size,
      modN(5),
      [](auto row, auto index) { return row + index; },
      nullEvery(11));
  auto input = makeRowVector({inputArray});

  auto result = evaluate<SimpleVector<int64_t>>(
      "reduce(c0, 10, (s, x) -> s + x, s -> s * 3)", input);

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

  auto result = evaluate<SimpleVector<bool>>(
      "reduce(c0, 100.0, (s, x) -> s + cast(x as double) * 0.1, s -> (s < 101.0))",
      input);

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

  auto result = evaluate<SimpleVector<int64_t>>(
      "reduce(c1, 0, if (c0, (s, x) -> s + x, (s, x) -> s + x * x), s -> s)",
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

  auto result = evaluate<RowVector>(
      "if (c1 < 100, row_constructor(c1), "
      "reduce(c0, 10, (s, x) -> s + x, s -> row_constructor(s)))",
      input);

  auto expectedResult = makeRowVector(
      {makeFlatVector<int64_t>(
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
          nullEvery(11))},
      nullEvery(11));
  assertEqualVectors(expectedResult, result);
}

TEST_F(ReduceTest, elementIndicesOverwrite) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 2}),
      makeFlatVector<int64_t>({3, 4}),
  });

  auto result =
      evaluate("reduce(array[c0, c1], 100, (s, x) -> s + x, s -> s)", data);
  assertEqualVectors(makeFlatVector<int64_t>({104, 106}), result);
}

TEST_F(ReduceTest, try) {
  auto input = makeRowVector({
      makeArrayVector<int64_t>({
          {1, 2},
          {3, 4},
          {5, 6},
      }),
  });

  ASSERT_THROW(
      evaluate<SimpleVector<int64_t>>(
          "reduce(c0, 0, (s, x) -> s + x / (x - 3), s -> s)", input),
      std::exception);

  auto result = evaluate<SimpleVector<int64_t>>(
      "try(reduce(c0, 0, (s, x) -> s + x / (x - 3), s -> s))", input);

  auto expectedResult = makeNullableFlatVector<int64_t>({-2, std::nullopt, 4});
  assertEqualVectors(expectedResult, result);

  result = evaluate<SimpleVector<int64_t>>(
      "try(reduce(c0, 0, (s, x) -> s + x / (x - 3), s -> s / (s + 2)))", input);

  expectedResult =
      makeNullableFlatVector<int64_t>({std::nullopt, std::nullopt, 0});
  assertEqualVectors(expectedResult, result);
}

TEST_F(ReduceTest, finalSelectionLargerThanInput) {
  // Verify that final selection is not passed on to lambda. Here, 'reduce' is a
  // CSE, the conjunct ensures that row 2 is only evaluated in the first case,
  // so when row needs to be evaluated the CSE sets the final selection to both
  // rows and calls 'reduce'. The 'slice' inside reduce generates an array of
  // size 1 so if final selection is passed on to the lambda it will fail while
  // attempting to peel which uses final selection to generate peels.
  auto input = makeRowVector({
      makeArrayVector<int64_t>({
          {1, 2, -2, -1, 5},
          {1, 2, -2, -1, 9},
      }),
  });
  auto result = evaluate(
      "case "
      "   when (c0[5] > 6) AND "
      "     (reduce(slice(c0, 1, 4), 0, (s, x) -> s + x, s -> s) = 0) then 1 "
      "   when (reduce(slice(c0, 1, 4), 0, (s, x) -> s + x, s -> s) = 0) then 2 "
      "   else 3 "
      "end",
      input);

  assertEqualVectors(makeFlatVector<int64_t>({2, 1}), result);
}

TEST_F(ReduceTest, nullArray) {
  // Verify that NULL array is not passed on to lambda as it should be handled
  // differently from array with null element.
  // Case 1:  covers leading, middle and trailing nulls (intermediate result can
  // be smaller than input vector)
  auto arrayVector = makeArrayVectorFromJson<int64_t>(
      {"null", "[1, null]", "null", "[null, 2]", "[1, 2, 3]", "null"});
  auto data = makeRowVector({arrayVector});
  auto result = evaluate(
      "reduce(c0, 0, (s, x) -> s + x, s -> coalesce(s, 0) * 10)", data);
  assertEqualVectors(
      makeNullableFlatVector<int64_t>(
          {std::nullopt, 0, std::nullopt, 0, 60, std::nullopt}),
      result);

  // Case 2: Where intermediate result is a constant vector by making the final
  // reduce step output a constant.
  auto singleValueArrayVector =
      makeArrayVectorFromJson<int64_t>({"null", "[1, 2, 3]", "null"});
  result = evaluate(
      "reduce(c0, 0, (s, x) -> s + x, s -> 10)",
      makeRowVector({singleValueArrayVector}));
  assertEqualVectors(
      makeNullableFlatVector<int64_t>({std::nullopt, 10, std::nullopt}),
      result);

  // Case 3: Where intermediate result is null
  auto allNullsArrayVector = makeArrayVectorFromJson<int64_t>({"null", "null"});
  result = evaluate(
      "reduce(c0, 0, (s, x) -> s + x, s -> coalesce(s, 0) * 10)",
      makeRowVector({allNullsArrayVector}));
  assertEqualVectors(
      makeNullableFlatVector<int64_t>({std::nullopt, std::nullopt}), result);
}

// Verify limit on the number of array elements.
TEST_F(ReduceTest, limit) {
  // Make array vector with huge arrays in rows 2 and 4.
  auto data = makeRowVector({makeArrayVector(
      {0, 1'000, 10'000, 100'000, 100'010}, makeConstant(123, 1'000'000))});

  VELOX_ASSERT_THROW(
      evaluate("reduce(c0, 0, (s, x) -> s + x, s -> 1 * s)", data),
      "reduce lambda function doesn't support arrays with more than");

  // Exclude huge arrays.
  SelectivityVector rows(4);
  rows.setValid(2, false);
  rows.updateBounds();
  auto result =
      evaluate("reduce(c0, 0, (s, x) -> s + x, s -> 1 * s)", data, rows);
  auto expected =
      makeFlatVector<int64_t>({123 * 1'000, 123 * 9'000, -1, 123 * 10});
  assertEqualVectors(expected, result, rows);

  // TRY should not mask errors.
  VELOX_ASSERT_THROW(
      evaluate("TRY(reduce(c0, 0, (s, x) -> s + x, s -> 1 * s))", data),
      "reduce lambda function doesn't support arrays with more than");
}

TEST_F(ReduceTest, rewrites) {
  std::vector<std::optional<std::vector<std::optional<int64_t>>>> data;
  for (int i = 0; i < 20; ++i) {
    if (i == 7) {
      data.push_back(std::nullopt);
      continue;
    }
    auto& array = data.emplace_back(std::vector<std::optional<int64_t>>());
    for (int j = 0; j < i % 5; ++j) {
      if (i == 13 && j == 0) {
        array->push_back(std::nullopt);
      } else {
        array->emplace_back(i + j);
      }
    }
  }
  auto input = makeRowVector({makeNullableArrayVector(data)});
  {
    SCOPED_TRACE("plus");
    testReduceRewrite(input, "s + x * 2");
  }
  {
    SCOPED_TRACE("plus minus");
    testReduceRewrite(input, "(s + 1) - x");
  }
  {
    SCOPED_TRACE("if");
    testReduceRewrite(input, "if(x % 2 = 0, s + 1, s)");
  }
}

} // namespace
} // namespace facebook::velox
