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
  for (const auto& function : kAggregateFunctions) {
    // count function is skipped as DuckDB returns inconsistent results
    // with Velox for rows with empty frames. Velox expects empty frames to
    // return 0, but DuckDB returns null.
    if (function != "count(c2)") {
      testKRangeFrames(function);
    }
  }
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
}; // namespace
}; // namespace facebook::velox::window::test
