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
#include "velox/functions/lib/window/tests/WindowTestBase.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::window::test {

namespace {

static const std::vector<std::string> kAggregateFunctions = {
    std::string("sum(c2)"),
    std::string("min(c2)"),
    std::string("max(c2)"),
    std::string("count(c2)"),
    std::string("avg(c2)"),
    std::string("sum(1)")};

// This AggregateWindowTestBase class is used to instantiate parameterized
// aggregate window function tests. The parameters are (function, over clause).
// The window function is tested for the over clause and all combinations of
// frame clauses. Doing so helps to construct input vectors and DuckDB table
// only once for the (function, over clause) combination over all frame clauses.
struct AggregateWindowTestParam {
  const std::string function;
  const std::string overClause;
};

class AggregateWindowTest : public WindowTestBase {
 protected:
  explicit AggregateWindowTest(const AggregateWindowTestParam& testParam)
      : function_(testParam.function), overClause_(testParam.overClause) {}

  explicit AggregateWindowTest() : function_(""), overClause_("") {}

  void SetUp() override {
    WindowTestBase::SetUp();
    window::prestosql::registerAllWindowFunctions();
  }

  void testWindowFunction(const std::vector<RowVectorPtr>& vectors) {
    testWindowFunction(vectors, kFrameClauses);
  }

  void testWindowFunction(
      const std::vector<RowVectorPtr>& vectors,
      const std::vector<std::string>& frameClauses) {
    WindowTestBase::testWindowFunction(
        vectors, function_, {overClause_}, frameClauses);
  }

  const std::string function_;
  const std::string overClause_;
};

std::vector<AggregateWindowTestParam> getAggregateTestParams() {
  std::vector<AggregateWindowTestParam> params;
  for (auto function : kAggregateFunctions) {
    for (auto overClause : kOverClauses) {
      params.push_back({function, overClause});
    }
  }
  return params;
}

class MultiAggregateWindowTest
    : public AggregateWindowTest,
      public testing::WithParamInterface<AggregateWindowTestParam> {
 public:
  MultiAggregateWindowTest() : AggregateWindowTest(GetParam()) {}
};

// Tests function with a dataset with uniform partitions.
TEST_P(MultiAggregateWindowTest, basic) {
  testWindowFunction({makeSimpleVector(10)});
}

// Tests function with a dataset with a single partition but 2 input row
// vectors.
TEST_P(MultiAggregateWindowTest, singlePartition) {
  auto input = {makeSinglePartitionVector(50), makeSinglePartitionVector(40)};
  testWindowFunction(input);
}

// Tests function with a dataset where all partitions have a single row.
TEST_P(MultiAggregateWindowTest, singleRowPartitions) {
  testWindowFunction({makeSingleRowPartitionsVector(40)});
}

// Tests function with a randomly generated input dataset.
TEST_P(MultiAggregateWindowTest, randomInput) {
  testWindowFunction({makeRandomInputVector(25)});
}

// Instantiate all the above tests for each combination of aggregate function
// and over clause.
VELOX_INSTANTIATE_TEST_SUITE_P(
    AggregatesTestInstantiation,
    MultiAggregateWindowTest,
    testing::ValuesIn(getAggregateTestParams()));

// Test for an aggregate function with strings that needs out of line storage.
TEST_F(AggregateWindowTest, variableWidthAggregate) {
  auto size = 10;
  auto input = {makeRowVector({
      makeRandomInputVector(BIGINT(), size, 0.2),
      makeRandomInputVector(SMALLINT(), size, 0.2),
      makeRandomInputVector(VARCHAR(), size, 0.3),
      makeRandomInputVector(VARCHAR(), size, 0.3),
  })};

  WindowTestBase::testWindowFunction(input, "min(c2)", kOverClauses);
  WindowTestBase::testWindowFunction(
      input, "max(c2)", kOverClauses, {""}, false);
}

// Tests function with k RANGE PRECEDING (FOLLOWING) frames.
TEST_F(AggregateWindowTest, rangeFrames) {
  auto aggregateFunctions = kAggregateFunctions;
  aggregateFunctions.push_back("array_agg(c2)");
  for (const auto& function : aggregateFunctions) {
    // count function is skipped as DuckDB returns inconsistent results
    // with Velox for rows with empty frames. Velox expects empty frames to
    // return 0, but DuckDB returns null.
    if (function != "count(c2)") {
      testKRangeFrames(function);
    }
  }
}

TEST_F(AggregateWindowTest, rangeNullsOrder) {
  auto c0 = makeNullableFlatVector<int64_t>({1, 2, 1, std::nullopt});
  auto input = makeRowVector({c0});

  std::string overClause = "order by c0 asc nulls last";
  // This frame corresponds to range between 0 preceding and 0 following
  // (since c0 is used for both the ORDER BY and range frame column).
  // So for each row the frame corresponds to all rows with the same value.

  // This test validates that the null value doesn't mix in the range of
  // adjacent rows.
  std::string frameClause = "range between c0 preceding and c0 following";
  auto arr =
      makeNullableArrayVector<int64_t>({{1, 1}, {2}, {1, 1}, {std::nullopt}});
  auto expected = makeRowVector({c0, arr});

  WindowTestBase::testWindowFunction(
      {input}, "array_agg(c0)", overClause, frameClause, expected);

  overClause = "order by c0 asc nulls first";
  WindowTestBase::testWindowFunction(
      {input}, "array_agg(c0)", overClause, frameClause, expected);

  overClause = "order by c0 desc nulls last";
  WindowTestBase::testWindowFunction(
      {input}, "array_agg(c0)", overClause, frameClause, expected);

  overClause = "order by c0 desc nulls first";
  WindowTestBase::testWindowFunction(
      {input}, "array_agg(c0)", overClause, frameClause, expected);
}

// Test for aggregates that return NULL as the default value for empty frames
// against DuckDb.
TEST_F(AggregateWindowTest, nullEmptyResult) {
  auto input = {makeSinglePartitionVector(50), makeSinglePartitionVector(40)};
  auto aggregateFunctions = kAggregateFunctions;
  aggregateFunctions.erase(
      std::remove(
          aggregateFunctions.begin(), aggregateFunctions.end(), "count(c2)"),
      aggregateFunctions.end());
  for (const auto& function : aggregateFunctions) {
    WindowTestBase::testWindowFunction(
        input, function, kOverClauses, kEmptyFrameClauses);
  }
}

// Test for count aggregate with empty frames against expectedResult and not
// DuckDb, since DuckDb returns NULL instead of 0 for such queries.
TEST_F(AggregateWindowTest, nonNullEmptyResult) {
  auto c0 = makeFlatVector<int64_t>({-1, -1, -1, -1, -1, -1, 2, 2, 2, 2});
  auto c1 = makeFlatVector<double>({-1, -2, -3, -4, -5, -6, -7, -8, -9, -10});
  auto input = makeRowVector({c0, c1});

  auto expected = makeRowVector(
      {c0, c1, makeFlatVector<int64_t>({0, 0, 0, 1, 2, 3, 0, 0, 0, 1})});
  std::string overClause = "partition by c0 order by c1 desc";
  std::string frameClause = "rows between 6 preceding and 3 preceding";
  WindowTestBase::testWindowFunction(
      {input}, "count(c1)", overClause, frameClause, expected);

  expected = makeRowVector({c0, c1, makeConstant<int64_t>(0, c0->size())});
  frameClause = "rows between 6 following and unbounded following";
  WindowTestBase::testWindowFunction(
      {input}, "count(c1)", overClause, frameClause, expected);
}

TEST_F(AggregateWindowTest, testDecimal) {
  auto size = 30;
  auto testAggregate = [&](const TypePtr& type) {
    auto input = {makeRowVector({
        makeRandomInputVector(BIGINT(), size, 0.2),
        makeRandomInputVector(type, size, 0.2),
        makeFlatVector<int64_t>(size, [](auto row) { return row % 11 + 1; }),
        makeFlatVector<int64_t>(size, [](auto row) { return row % 13 + 1; }),
    })};

    WindowTestBase::testWindowFunction(input, "min(c1)", kOverClauses);
    WindowTestBase::testWindowFunction(
        input, "max(c1)", kOverClauses, {""}, false);
    WindowTestBase::testWindowFunction(
        input, "sum(c1)", kOverClauses, {""}, false);
    WindowTestBase::testWindowFunction(
        input, "count(c1)", kOverClauses, {""}, false);
  };

  testAggregate(DECIMAL(5, 2));
  testAggregate(DECIMAL(20, 5));
}

TEST_F(AggregateWindowTest, integerOverflowRowsFrame) {
  auto c0 = makeFlatVector<int64_t>({-1, -1, -1, -1, -1, -1, 2, 2, 2, 2});
  auto c1 = makeFlatVector<double>({-1, -2, -3, -4, -5, -6, -7, -8, -9, -10});
  // INT32_MAX: 2147483647
  auto c2 = makeFlatVector<int32_t>(
      {1,
       2147483647,
       2147483646,
       2147483645,
       1,
       10,
       1,
       2147483647,
       2147483646,
       2147483645});
  auto c3 = makeFlatVector<int64_t>(
      {2147483651,
       1,
       2147483650,
       10,
       2147483648,
       2147483647,
       2,
       2147483646,
       2147483650,
       2147483648});
  auto input = makeRowVector({c0, c1, c2, c3});
  std::string overClause = "partition by c0 order by c1 desc";

  // Constant following larger than INT32_MAX (2147483647).
  std::string frameClause = "rows between 0 preceding and 2147483650 following";
  auto expected = makeRowVector(
      {c0,
       c1,
       c2,
       c3,
       makeFlatVector<int64_t>({6, 5, 4, 3, 2, 1, 4, 3, 2, 1})});
  WindowTestBase::testWindowFunction(
      {input}, "count(c1)", overClause, frameClause, expected);

  // Overflow starts happening from middle of the partition.
  frameClause = "rows between 0 preceding and 2147483645 following";
  expected = makeRowVector(
      {c0,
       c1,
       c2,
       c3,
       makeFlatVector<int64_t>({6, 5, 4, 3, 2, 1, 4, 3, 2, 1})});
  WindowTestBase::testWindowFunction(
      {input}, "count(c1)", overClause, frameClause, expected);

  // Column-specified following (int32).
  frameClause = "rows between 0 preceding and c2 following";
  expected = makeRowVector(
      {c0,
       c1,
       c2,
       c3,
       makeFlatVector<int64_t>({2, 5, 4, 3, 2, 1, 2, 3, 2, 1})});
  WindowTestBase::testWindowFunction(
      {input}, "count(c1)", overClause, frameClause, expected);

  // Column-specified following (int64).
  frameClause = "rows between 0 preceding and c3 following";
  expected = makeRowVector(
      {c0,
       c1,
       c2,
       c3,
       makeFlatVector<int64_t>({6, 2, 4, 3, 2, 1, 3, 3, 2, 1})});
  WindowTestBase::testWindowFunction(
      {input}, "count(c1)", overClause, frameClause, expected);

  // Constant preceding larger than INT32_MAX.
  frameClause = "rows between 2147483650 preceding and 0 following";
  expected = makeRowVector(
      {c0,
       c1,
       c2,
       c3,
       makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6, 1, 2, 3, 4})});
  WindowTestBase::testWindowFunction(
      {input}, "count(c1)", overClause, frameClause, expected);

  // Column-specified preceding (int32).
  frameClause = "rows between c2 preceding and 0 following";
  expected = makeRowVector(
      {c0,
       c1,
       c2,
       c3,
       makeFlatVector<int64_t>({1, 2, 3, 4, 2, 6, 1, 2, 3, 4})});
  WindowTestBase::testWindowFunction(
      {input}, "count(c1)", overClause, frameClause, expected);

  // Column-specified preceding (int64).
  frameClause = "rows between c3 preceding and 0 following";
  expected = makeRowVector(
      {c0,
       c1,
       c2,
       c3,
       makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6, 1, 2, 3, 4})});
  WindowTestBase::testWindowFunction(
      {input}, "count(c1)", overClause, frameClause, expected);

  // Constant preceding & following both larger than INT32_MAX.
  frameClause = "rows between 2147483650 preceding and 2147483651 following";
  expected = makeRowVector(
      {c0,
       c1,
       c2,
       c3,
       makeFlatVector<int64_t>({6, 6, 6, 6, 6, 6, 4, 4, 4, 4})});
  WindowTestBase::testWindowFunction(
      {input}, "count(c1)", overClause, frameClause, expected);
}

}; // namespace
}; // namespace facebook::velox::window::test
