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
#include <gtest/gtest-spi.h>

#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/type/Variant.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox::exec::test;
using namespace facebook::velox;

namespace facebook::velox::test {

class QueryAssertionsTest : public OperatorTestBase {};

TEST_F(QueryAssertionsTest, basic) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>(10, [&](auto row) { return row; }),
  });
  createDuckDbTable({data});

  auto plan = PlanBuilder().values({data}).project({"c0"}).planNode();
  assertQuery(plan, "SELECT c0 FROM tmp");

  EXPECT_NONFATAL_FAILURE(
      assertQuery(plan, "SELECT c0 + 1 FROM tmp"),
      "1 extra rows, 1 missing rows");
}

TEST_F(QueryAssertionsTest, noFloatColumn) {
  auto size = 1'000;
  auto expected = makeRowVector({
      makeFlatVector<int64_t>(
          size, [&](auto row) { return row % 3; }, nullEvery(5)),
      makeFlatVector<StringView>(
          size, [&](auto /*row*/) { return "string value"; }, nullEvery(7)),
  });

  // Matched results with the same set of rows as expected but in reverse order.
  auto actual = makeRowVector({
      makeFlatVector<int64_t>(
          size,
          [&](auto row) { return (size - row - 1) % 3; },
          [&](auto row) { return (size - row - 1) % 5 == 0; }),
      makeFlatVector<StringView>(
          size,
          [&](auto /*row*/) { return "string value"; },
          [&](auto row) { return (size - row - 1) % 7 == 0; }),
  });
  EXPECT_TRUE(assertEqualResults({expected}, {actual}));

  // Unmatched results with the last row being different.
  actual = makeRowVector({
      makeFlatVector<int64_t>(
          size,
          [&](auto row) { return row == size - 1 ? 3 : row % 3; },
          nullEvery(5)),
      makeFlatVector<StringView>(
          size, [&](auto /*row*/) { return "string value"; }, nullEvery(7)),
  });
  EXPECT_NONFATAL_FAILURE(
      assertEqualResults({expected}, {actual}), "1 extra rows, 1 missing rows");

  // Unmatched results with different null positions.
  actual = makeRowVector({
      makeFlatVector<int64_t>(
          size, [&](auto row) { return row % 3; }, nullEvery(5)),
      makeFlatVector<StringView>(
          size, [&](auto /*row*/) { return "string value"; }, nullEvery(6)),
  });
  EXPECT_NONFATAL_FAILURE(
      assertEqualResults({expected}, {actual}),
      "176 extra rows, 176 missing rows");

  // Unmatched results with different types.
  actual = makeRowVector({
      makeFlatVector<StringView>(
          size, [&](auto /*row*/) { return "string value"; }),
  });
  EXPECT_NONFATAL_FAILURE(
      assertEqualResults({expected}, {actual}),
      "Types of expected and actual results do not match");

  // Unmatched results with different sizes.
  actual = makeRowVector({
      makeFlatVector<int64_t>(
          size - 1, [&](auto row) { return row % 3; }, nullEvery(5)),
      makeFlatVector<StringView>(
          size - 1, [&](auto /*row*/) { return "string value"; }, nullEvery(7)),
  });
  EXPECT_NONFATAL_FAILURE(
      assertEqualResults({expected}, {actual}), "Expected 1000, got 999");

  // Actual result is empty.
  actual = makeRowVector({
      makeFlatVector<int64_t>(0),
      makeFlatVector<StringView>(0),
  });
  EXPECT_NONFATAL_FAILURE(
      assertEqualResults({expected}, {actual}), "Expected 1000, got 0");
}

TEST_F(QueryAssertionsTest, singleFloatColumn) {
  auto size = 1'000;
  auto expected = makeRowVector({
      makeFlatVector<int32_t>(
          size, [&](auto row) { return row % 4; }, nullEvery(5)),
      makeFlatVector<double>(
          size, [&](auto row) { return row % 6 + 0.01; }, nullEvery(7)),
      makeFlatVector<int64_t>(size, [&](auto row) { return row; }),

  });

  // Matched results in reverse order.
  auto actual = makeRowVector({
      makeFlatVector<int32_t>(
          size,
          [&](auto row) { return (size - row - 1) % 4; },
          [&](auto row) { return (size - row - 1) % 5 == 0; }),
      makeFlatVector<double>(
          size,
          [&](auto row) { return (size - row - 1) % 6 + 0.01; },
          [&](auto row) { return (size - row - 1) % 7 == 0; }),
      makeFlatVector<int64_t>(size, [&](auto row) { return size - row - 1; }),

  });
  EXPECT_TRUE(assertEqualResults({expected}, {actual}));

  // Matched results with epsilon.
  actual = makeRowVector({
      makeFlatVector<int32_t>(
          size, [&](auto row) { return row % 4; }, nullEvery(5)),
      makeFlatVector<double>(
          size,
          [&](auto row) {
            auto value = row % 6 + 0.01;
            return value + value * FLT_EPSILON;
          },
          nullEvery(7)),
      makeFlatVector<int64_t>(size, [&](auto row) { return row; }),

  });
  EXPECT_TRUE(assertEqualResults({expected}, {actual}));

  // Unmatched results with one different row.
  actual = makeRowVector({
      makeFlatVector<int32_t>(
          size, [&](auto row) { return row % 4; }, nullEvery(5)),
      makeFlatVector<double>(
          size,
          [&](auto row) {
            return row == 302
                ? 2.01 + std::max(kEpsilon, double(6 * FLT_EPSILON))
                : row % 6 + 0.01;
          },
          nullEvery(7)),
      makeFlatVector<int64_t>(size, [&](auto row) { return row; }),
  });
  EXPECT_NONFATAL_FAILURE(
      assertEqualResults({expected}, {actual}), "1 extra rows, 1 missing rows");

  // Unmatched results with different null positions.
  actual = makeRowVector({
      makeFlatVector<int32_t>(
          size, [&](auto row) { return row % 4; }, nullEvery(5)),
      makeFlatVector<double>(
          size, [&](auto row) { return row % 6 + 0.01; }, nullEvery(6)),
      makeFlatVector<int64_t>(size, [&](auto row) { return row; }),
  });
  EXPECT_NONFATAL_FAILURE(
      assertEqualResults({expected}, {actual}),
      "262 extra rows, 262 missing rows");

  // Unmatched results with different types.
  actual = makeRowVector({
      makeFlatVector<int64_t>(size, [&](auto row) { return row; }),
  });
  EXPECT_NONFATAL_FAILURE(
      assertEqualResults({expected}, {actual}),
      "Types of expected and actual results do not match");

  // Unmatched results with different sizes.
  actual = makeRowVector({
      makeFlatVector<int32_t>(
          size - 1, [&](auto row) { return row % 4; }, nullEvery(5)),
      makeFlatVector<double>(
          size - 1, [&](auto row) { return row % 6 + 0.01; }, nullEvery(7)),
      makeFlatVector<int64_t>(size - 1, [&](auto row) { return row; }),
  });
  EXPECT_NONFATAL_FAILURE(
      assertEqualResults({expected}, {actual}), "Expected 1000, got 999");

  // Actual result is empty.
  actual = makeRowVector({
      makeFlatVector<int32_t>(0),
      makeFlatVector<double>(0),
      makeFlatVector<int64_t>(0),
  });
  EXPECT_NONFATAL_FAILURE(
      assertEqualResults({expected}, {actual}), "Expected 1000, got 0");
}

TEST_F(QueryAssertionsTest, multiFloatColumnWithUniqueKeys) {
  auto size = 1'000;
  auto expected = makeRowVector({
      makeFlatVector<int64_t>(size, [&](auto row) { return row; }),
      makeFlatVector<float>(
          size, [&](auto row) { return row % 4; }, nullEvery(5)),
      makeFlatVector<double>(
          size, [&](auto row) { return row % 6 + 0.01; }, nullEvery(7)),
  });

  // Matched results with the same set of rows as expected but in reverse order.
  auto actual = makeRowVector({
      makeFlatVector<int64_t>(size, [&](auto row) { return size - row - 1; }),
      makeFlatVector<float>(
          size,
          [&](auto row) { return (size - row - 1) % 4; },
          [&](auto row) { return (size - row - 1) % 5 == 0; }),
      makeFlatVector<double>(
          size,
          [&](auto row) { return (size - row - 1) % 6 + 0.01; },
          [&](auto row) { return (size - row - 1) % 7 == 0; }),
  });
  EXPECT_TRUE(assertEqualResults({expected}, {actual}));

  // Matched results with epsilon.
  actual = makeRowVector({
      makeFlatVector<int64_t>(size, [&](auto row) { return row; }),
      makeFlatVector<float>(
          size,
          [&](auto row) {
            float value = row % 4;
            return value + value * FLT_EPSILON;
          },
          nullEvery(5)),
      makeFlatVector<double>(
          size,
          [&](auto row) {
            double value = row % 6 + 0.01;
            return value - value * FLT_EPSILON;
          },
          nullEvery(7)),
  });
  EXPECT_TRUE(assertEqualResults({expected}, {actual}));

  // Unmatched results with two different row.
  actual = makeRowVector({
      makeFlatVector<int64_t>(size, [&](auto row) { return row; }),
      makeFlatVector<float>(
          size,
          [&](auto row) {
            return row == 6 ? 2 + std::max(float(kEpsilon), 6 * FLT_EPSILON)
                            : row % 4;
          },
          nullEvery(5)),
      makeFlatVector<double>(
          size,
          [&](auto row) {
            return row == 1 ? 1.01 + std::max(kEpsilon, double(3 * FLT_EPSILON))
                            : row % 6 + 0.01;
          },
          nullEvery(7)),
  });
  EXPECT_NONFATAL_FAILURE(
      assertEqualResults({expected}, {actual}), "2 extra rows, 2 missing rows");

  // Unmatched results with different null positions.
  actual = makeRowVector({
      makeFlatVector<int64_t>(size, [&](auto row) { return row; }),
      makeFlatVector<float>(
          size, [&](auto row) { return row % 4; }, nullEvery(5)),
      makeFlatVector<double>(
          size, [&](auto row) { return row % 6 + 0.01; }, nullEvery(6)),
  });
  EXPECT_NONFATAL_FAILURE(
      assertEqualResults({expected}, {actual}),
      "262 extra rows, 262 missing rows");
}

TEST_F(QueryAssertionsTest, multiFloatColumnWithNonUniqueKeys) {
  auto size = 1'000;
  auto expected = makeRowVector({
      makeFlatVector<int64_t>(size, [&](auto row) { return row % 2; }),
      makeFlatVector<float>(
          size, [&](auto row) { return row % 4 - 0.01; }, nullEvery(5)),
      makeFlatVector<double>(
          size, [&](auto row) { return row % 6 + 0.01; }, nullEvery(7)),
  });

  // Matched results in reverse order.
  auto actual = makeRowVector({
      makeFlatVector<int64_t>(
          size, [&](auto row) { return (size - row - 1) % 2; }),
      makeFlatVector<float>(
          size,
          [&](auto row) { return (size - row - 1) % 4 - 0.01; },
          [&](auto row) { return (size - row - 1) % 5 == 0; }),
      makeFlatVector<double>(
          size,
          [&](auto row) { return (size - row - 1) % 6 + 0.01; },
          [&](auto row) { return (size - row - 1) % 7 == 0; }),
  });
  EXPECT_TRUE(assertEqualResults({expected}, {actual}));

  // Rows with epsilon are expected to be considered unmatch because result sets
  // with float columns and non-unique values at non-floating-point columns
  // would be compared directly without epsilon.
  actual = makeRowVector({
      makeFlatVector<int64_t>(size, [&](auto row) { return row % 2; }),
      makeFlatVector<float>(
          size,
          [&](auto row) {
            auto value = row % 4 - 0.01;
            return value + value * FLT_EPSILON;
          },
          nullEvery(5)),
      makeFlatVector<double>(
          size,
          [&](auto row) {
            auto value = row % 6 + 0.01;
            return value - value * FLT_EPSILON;
          },
          nullEvery(7)),
  });
  EXPECT_NONFATAL_FAILURE(
      assertEqualResults({expected}, {actual}),
      "971 extra rows, 971 missing rows");
}

TEST_F(QueryAssertionsTest, nullDecimalValue) {
  auto shortDecimal = makeRowVector({makeNullableShortDecimalFlatVector(
      {std::nullopt}, SHORT_DECIMAL(5, 2))});
  EXPECT_TRUE(assertEqualResults({shortDecimal}, {shortDecimal}));

  auto longDecimal = makeRowVector(
      {makeNullableLongDecimalFlatVector({std::nullopt}, LONG_DECIMAL(20, 2))});
  EXPECT_TRUE(assertEqualResults({longDecimal}, {longDecimal}));

  EXPECT_NONFATAL_FAILURE(
      assertEqualResults({shortDecimal}, {longDecimal}),
      "Types of expected and actual results do not match");
}

} // namespace facebook::velox::test
