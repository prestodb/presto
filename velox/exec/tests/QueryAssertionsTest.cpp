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
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox::exec::test;
using namespace facebook::velox;

namespace facebook::velox::test {

class QueryAssertionsTest : public OperatorTestBase {
 public:
  void assertQueryWithThreadingConfigs(
      const core::PlanNodePtr& plan,
      const std::string& duckDbSql) {
    CursorParameters multiThreadedParams{};
    multiThreadedParams.planNode = plan;
    multiThreadedParams.singleThreaded = false;
    assertQuery(multiThreadedParams, duckDbSql);

    CursorParameters singleThreadedParams{};
    singleThreadedParams.planNode = plan;
    singleThreadedParams.singleThreaded = true;
    assertQuery(singleThreadedParams, duckDbSql);
  }
};

TEST_F(QueryAssertionsTest, basic) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>(10, [&](auto row) { return row; }),
  });
  createDuckDbTable({data});

  auto plan = PlanBuilder().values({data}).project({"c0"}).planNode();
  assertQueryWithThreadingConfigs(plan, "SELECT c0 FROM tmp");

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
  auto shortDecimal = makeRowVector(
      {makeNullableFlatVector<int64_t>({std::nullopt}, DECIMAL(5, 2))});
  EXPECT_TRUE(assertEqualResults({shortDecimal}, {shortDecimal}));

  createDuckDbTable({shortDecimal});
  auto plan = PlanBuilder().values({shortDecimal}).planNode();
  assertQueryWithThreadingConfigs(plan, "SELECT c0 FROM tmp");

  auto longDecimal = makeRowVector(
      {makeNullableFlatVector<int128_t>({std::nullopt}, DECIMAL(20, 2))});
  EXPECT_TRUE(assertEqualResults({longDecimal}, {longDecimal}));

  createDuckDbTable({longDecimal});
  plan = PlanBuilder().values({longDecimal}).planNode();
  assertQueryWithThreadingConfigs(plan, "SELECT c0 FROM tmp");

  EXPECT_NONFATAL_FAILURE(
      assertEqualResults({shortDecimal}, {longDecimal}),
      "Types of expected and actual results do not match");
}

TEST_F(QueryAssertionsTest, valuesMismatch) {
  auto test = makeRowVector({
      makeFlatVector<int64_t>({10, 20}, DECIMAL(12, 2)),
  });
  createDuckDbTable({test});

  // The expected values should be off by 1.
  auto plan = PlanBuilder().values({test}).project({"c0"}).planNode();
  EXPECT_NONFATAL_FAILURE(
      assertQuery(plan, "SELECT c0 + 1 FROM tmp"),
      "Expected 2, got 2\n"
      "2 extra rows, 2 missing rows\n"
      "2 of extra rows:\n\t"
      "0.10\n\t"
      "0.20\n\n"
      "2 of missing rows:\n\t"
      "1.10\n\t"
      "1.20");
}

TEST_F(QueryAssertionsTest, noExpectedRows) {
  auto actual = makeRowVector({
      makeFlatVector(std::vector<int64_t>{4000}, DECIMAL(6, 2)),
  });
  createDuckDbTable({actual});

  auto expected = makeRowVector({
      makeFlatVector<int64_t>({}, BIGINT()),
  });
  auto plan = PlanBuilder().values({actual}).project({"c0"}).planNode();
  EXPECT_NONFATAL_FAILURE(
      assertQuery(plan, expected),
      "Expected 0, got 1\n"
      "1 extra rows, 0 missing rows\n"
      "1 of extra rows:\n\t"
      "40.00\n\n"
      "0 of missing rows:");
}

TEST_F(QueryAssertionsTest, noActualRows) {
  auto actual = makeRowVector({
      makeFlatVector<int64_t>({}, BIGINT()),
  });
  createDuckDbTable({actual});

  auto expected = makeRowVector({
      makeFlatVector(std::vector<int64_t>{1000}, DECIMAL(6, 2)),
  });
  auto plan = PlanBuilder().values({actual}).project({"c0"}).planNode();
  EXPECT_NONFATAL_FAILURE(
      assertQuery(plan, expected),
      "Expected 1, got 0\n"
      "0 extra rows, 1 missing rows\n"
      "0 of extra rows:\n\n"
      "1 of missing rows:\n\t"
      "10.00");
}

TEST_F(QueryAssertionsTest, nullVariant) {
  auto input = makeRowVector(
      {makeNullableArrayVector<int64_t>(
           {{std::nullopt, 1}, {2, 3, 4}, {std::nullopt}}),
       makeNullableArrayVector<double>(
           {{std::nullopt, 1.1}, {2.2, 3.3, 4.4}, {std::nullopt}})});
  createDuckDbTable({input});
  auto plan = PlanBuilder().values({input}).planNode();
  assertQueryWithThreadingConfigs(plan, "SELECT * FROM tmp");

  input = makeRowVector({makeNullableMapVector<int64_t, double>(
      {std::nullopt,
       {{{1, 1.1}, {2, std::nullopt}}},
       {},
       {{{3, 3.3}, {4, 4.4}, {5, 5.5}}},
       {std::nullopt},
       {{{6, std::nullopt}}}})});
  createDuckDbTable({input});
  plan = PlanBuilder().values({input}).planNode();
  assertQueryWithThreadingConfigs(plan, "SELECT * FROM tmp");

  input = makeRowVector({makeRowVector(
      {makeNullConstant(TypeKind::BIGINT, 10),
       makeNullConstant(TypeKind::DOUBLE, 10)})});
  createDuckDbTable({input});
  plan = PlanBuilder().values({input}).planNode();
  assertQueryWithThreadingConfigs(plan, "SELECT * FROM tmp");
}

TEST_F(QueryAssertionsTest, varbinary) {
  auto data = makeRowVector({makeFlatVector<std::string>(
      {"Short string", "Longer strings...", "abc"}, VARBINARY())});

  auto rowType = asRowType(data->type());

  createDuckDbTable({data});

  auto duckResult = duckDbQueryRunner_.execute("SELECT * FROM tmp", rowType);
  ASSERT_EQ(duckResult.size(), data->size());
  ASSERT_EQ(duckResult.begin()->begin()->kind(), TypeKind::VARBINARY);
  ASSERT_TRUE(assertEqualResults(duckResult, rowType, {data}));

  auto plan = PlanBuilder().values({data}).planNode();
  assertQueryWithThreadingConfigs(plan, "SELECT * FROM tmp");
}

TEST_F(QueryAssertionsTest, intervalDayTime) {
  // INTERVAL_DAY_TIME needs special handling as it is a logical (vs physical)
  // type mapping to BIGINT. Tests its use as a FLAT vector and in a MAP
  // serialized to DuckDB to cover the specialized code-paths.
  auto data = makeRowVector(
      {makeFlatVector<int64_t>({5, 10, 15, 0}, INTERVAL_DAY_TIME())});

  createDuckDbTable({data});
  auto plan = PlanBuilder().values({data}).planNode();
  assertQueryWithThreadingConfigs(plan, "SELECT * FROM tmp");

  data = makeRowVector({makeMapVectorFromJson<int64_t, double>(
      {"null",
       "{1: 1.1, 2: null}",
       "{}",
       "{3: 3.3, 4: 4.4, 5: 5.5}",
       "null",
       "{6: null}"},
      MAP(INTERVAL_DAY_TIME(), DOUBLE()))});
  createDuckDbTable({data});
  plan = PlanBuilder().values({data}).planNode();
  assertQueryWithThreadingConfigs(plan, "SELECT * FROM tmp");
}

TEST_F(QueryAssertionsTest, plansWithEqualResults) {
  VectorFuzzer::Options opts;
  VectorFuzzer fuzzer(opts, pool());

  auto input = fuzzer.fuzzInputRow(ROW({"c0"}, {INTEGER()}));
  auto plan1 = PlanBuilder().values({input}).orderBy({"c0"}, false).planNode();

  // The input plan has 100 rows. So the limit has no effect here.
  auto plan2 = PlanBuilder()
                   .values({input})
                   .orderBy({"c0"}, false)
                   .limit(0, 100, false)
                   .planNode();
  assertEqualResults(plan1, plan2);

  // The limit drops 50 result rows from the original plan.
  plan2 = PlanBuilder()
              .values({input})
              .orderBy({"c0"}, false)
              .limit(0, 50, false)
              .planNode();
  EXPECT_NONFATAL_FAILURE(
      assertEqualResults(plan1, plan2), "Expected 100, got 50");
}

} // namespace facebook::velox::test
