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
#include "velox/common/file/FileSystems.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
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

class AggregateWindowTestBase : public WindowTestBase {
 protected:
  explicit AggregateWindowTestBase(const AggregateWindowTestParam& testParam)
      : function_(testParam.function), overClause_(testParam.overClause) {}

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

class SimpleAggregatesTest
    : public AggregateWindowTestBase,
      public testing::WithParamInterface<AggregateWindowTestParam> {
 public:
  SimpleAggregatesTest() : AggregateWindowTestBase(GetParam()) {}
};

// Tests function with a dataset with uniform partitions.
TEST_P(SimpleAggregatesTest, basic) {
  testWindowFunction({makeSimpleVector(10)});
}

// Tests function with a dataset with a single partition but 2 input row
// vectors.
TEST_P(SimpleAggregatesTest, singlePartition) {
  auto input = {makeSinglePartitionVector(50), makeSinglePartitionVector(40)};
  testWindowFunction(input);
}

// Tests function with a dataset where all partitions have a single row.
TEST_P(SimpleAggregatesTest, singleRowPartitions) {
  testWindowFunction({makeSingleRowPartitionsVector(40)});
}

// Tests function with a randomly generated input dataset.
TEST_P(SimpleAggregatesTest, randomInput) {
  testWindowFunction({makeRandomInputVector(25)});
}

// Instantiate all the above tests for each combination of aggregate function
// and over clause.
VELOX_INSTANTIATE_TEST_SUITE_P(
    AggregatesTestInstantiation,
    SimpleAggregatesTest,
    testing::ValuesIn(getAggregateTestParams()));

class WindowTest : public WindowTestBase {
 protected:
  void SetUp() override {
    WindowTestBase::SetUp();
    window::prestosql::registerAllWindowFunctions();
    filesystems::registerLocalFileSystem();
  }
};

// Test for an aggregate function with strings that needs out of line storage.
TEST_F(WindowTest, variableWidthAggregate) {
  auto size = 10;
  auto input = {makeRowVector({
      makeRandomInputVector(BIGINT(), size, 0.2),
      makeRandomInputVector(SMALLINT(), size, 0.2),
      makeRandomInputVector(VARCHAR(), size, 0.3),
      makeRandomInputVector(VARCHAR(), size, 0.3),
  })};

  testWindowFunction(input, "min(c2)", kOverClauses);
  testWindowFunction(input, "max(c2)", kOverClauses);
}

// Tests function with k RANGE PRECEDING (FOLLOWING) frames.
TEST_F(WindowTest, rangeFrames) {
  for (const auto& function : kAggregateFunctions) {
    // count function is skipped as DuckDB returns inconsistent results
    // with Velox for rows with empty frames. Velox expects empty frames to
    // return 0, but DuckDB returns null.
    if (function != "count(c2)") {
      testKRangeFrames(function);
    }
  }
}

TEST_F(WindowTest, spill) {
  const vector_size_t size = 1'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          // Payload.
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          // Partition key.
          makeFlatVector<int16_t>(size, [](auto row) { return row % 11; }),
          // Sorting key.
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  auto plan = PlanBuilder()
                  .values(split(data, 10))
                  .window({"row_number() over (partition by p order by s)"})
                  .capturePlanNodeId(windowId)
                  .planNode();

  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto task =
      AssertQueryBuilder(plan, duckDbQueryRunner_)
          .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
          .config(core::QueryConfig::kTestingSpillPct, "100")
          .config(core::QueryConfig::kSpillEnabled, "true")
          .config(core::QueryConfig::kWindowSpillEnabled, "true")
          .spillDirectory(spillDirectory->path)
          .assertResults(
              "SELECT *, row_number() over (partition by p order by s) FROM tmp");

  auto taskStats = exec::toPlanStats(task->taskStats());
  const auto& stats = taskStats.at(windowId);

  ASSERT_GT(stats.spilledBytes, 0);
  ASSERT_GT(stats.spilledRows, 0);
  ASSERT_GT(stats.spilledFiles, 0);
  ASSERT_GT(stats.spilledPartitions, 0);
}

TEST_F(WindowTest, missingFunctionSignature) {
  auto input = {makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3}),
      makeFlatVector<std::string>({"A", "B", "C"}),
      makeFlatVector<int64_t>({10, 20, 30}),
  })};

  auto runWindow = [&](const core::CallTypedExprPtr& callExpr) {
    core::WindowNode::Frame frame{
        core::WindowNode::WindowType::kRows,
        core::WindowNode::BoundType::kUnboundedPreceding,
        nullptr,
        core::WindowNode::BoundType::kUnboundedFollowing,
        nullptr};

    core::WindowNode::Function windowFunction{callExpr, frame, false};

    CursorParameters params;
    params.planNode =
        PlanBuilder()
            .values(input)
            .addNode([&](auto nodeId, auto source) -> core::PlanNodePtr {
              return std::make_shared<core::WindowNode>(
                  nodeId,
                  std::vector<core::FieldAccessTypedExprPtr>{
                      std::make_shared<core::FieldAccessTypedExpr>(
                          BIGINT(), "c0")},
                  std::vector<core::FieldAccessTypedExprPtr>{}, // sortingKeys
                  std::vector<core::SortOrder>{}, // sortingOrders
                  std::vector<std::string>{"w"},
                  std::vector<core::WindowNode::Function>{windowFunction},
                  false,
                  source);
            })
            .planNode();

    readCursor(params, [](auto*) {});
  };

  auto callExpr = std::make_shared<core::CallTypedExpr>(
      BIGINT(),
      std::vector<core::TypedExprPtr>{
          std::make_shared<core::FieldAccessTypedExpr>(VARCHAR(), "c1")},
      "sum");

  VELOX_ASSERT_THROW(
      runWindow(callExpr),
      "Window function signature is not supported: sum(VARCHAR). Supported signatures:");

  callExpr = std::make_shared<core::CallTypedExpr>(
      VARCHAR(),
      std::vector<core::TypedExprPtr>{
          std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "c2")},
      "sum");

  VELOX_ASSERT_THROW(
      runWindow(callExpr),
      "Unexpected return type for window function sum(BIGINT). Expected BIGINT. Got VARCHAR.");
}

TEST_F(WindowTest, duplicateOrOverlappingKeys) {
  auto data = makeRowVector(
      ROW({"a", "b", "c", "d", "e"},
          {
              BIGINT(),
              BIGINT(),
              BIGINT(),
              BIGINT(),
              BIGINT(),
          }),
      10);

  auto plan = [&](const std::vector<std::string>& partitionKeys,
                  const std::vector<std::string>& sortingKeys) {
    std::ostringstream sql;
    sql << "row_number() over (";
    if (!partitionKeys.empty()) {
      sql << " partition by ";
      sql << folly::join(", ", partitionKeys);
    }
    if (!sortingKeys.empty()) {
      sql << " order by ";
      sql << folly::join(", ", sortingKeys);
    }
    sql << ")";

    PlanBuilder().values({data}).window({sql.str()}).planNode();
  };

  VELOX_ASSERT_THROW(
      plan({"a", "a"}, {"b"}),
      "Partitioning keys must be unique. Found duplicate key: a");

  VELOX_ASSERT_THROW(
      plan({"a", "b"}, {"c", "d", "c"}),
      "Sorting keys must be unique and not overlap with partitioning keys. Found duplicate key: c");

  VELOX_ASSERT_THROW(
      plan({"a", "b"}, {"c", "b"}),
      "Sorting keys must be unique and not overlap with partitioning keys. Found duplicate key: b");
}

class AggregateEmptyFramesTest : public WindowTestBase {};

// Test for aggregates that return NULL as the default value for empty frames
// against DuckDb.
TEST_F(AggregateEmptyFramesTest, nullEmptyResult) {
  auto input = {makeSinglePartitionVector(50), makeSinglePartitionVector(40)};
  auto aggregateFunctions = kAggregateFunctions;
  aggregateFunctions.erase(
      std::remove(
          aggregateFunctions.begin(), aggregateFunctions.end(), "count(c2)"),
      aggregateFunctions.end());
  for (const auto& function : aggregateFunctions) {
    testWindowFunction(input, function, kOverClauses, kEmptyFrameClauses);
  }
}

// Test for count aggregate with empty frames against expectedResult and not
// DuckDb, since DuckDb returns NULL instead of 0 for such queries.
TEST_F(AggregateEmptyFramesTest, nonNullEmptyResult) {
  auto c0 = makeFlatVector<int64_t>({-1, -1, -1, -1, -1, -1, 2, 2, 2, 2});
  auto c1 = makeFlatVector<double>({-1, -2, -3, -4, -5, -6, -7, -8, -9, -10});
  auto input = makeRowVector({c0, c1});

  auto expected = makeRowVector(
      {c0, c1, makeFlatVector<int64_t>({0, 0, 0, 1, 2, 3, 0, 0, 0, 1})});
  std::string overClause = "partition by c0 order by c1 desc";
  std::string frameClause = "rows between 6 preceding and 3 preceding";
  testWindowFunction({input}, "count(c1)", overClause, frameClause, expected);

  expected = makeRowVector({c0, c1, makeConstant<int64_t>(0, c0->size())});
  frameClause = "rows between 6 following and unbounded following";
  testWindowFunction({input}, "count(c1)", overClause, frameClause, expected);
}
}; // namespace
}; // namespace facebook::velox::window::test
