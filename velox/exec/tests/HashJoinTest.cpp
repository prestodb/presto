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

#include "velox/dwio/dwrf/test/utils/BatchMaker.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/Cursor.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/type/tests/FilterBuilder.h"
#include "velox/type/tests/SubfieldFiltersBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

using facebook::velox::test::BatchMaker;

static const std::string kWriter = "HashJoinTest.Writer";

class HashJoinTest : public HiveConnectorTestBase {
 protected:
  void SetUp() override {
    HiveConnectorTestBase::SetUp();
  }

  static std::vector<std::string> concat(
      const std::vector<std::string>& a,
      const std::vector<std::string>& b) {
    std::vector<std::string> result;
    result.insert(result.end(), a.begin(), a.end());
    result.insert(result.end(), b.begin(), b.end());
    return result;
  }

  void testJoin(
      const std::vector<TypePtr>& keyTypes,
      int32_t numThreads,
      int32_t leftSize,
      int32_t rightSize,
      const std::string& referenceQuery,
      const std::string& filter = "") {
    auto leftType = makeRowType(keyTypes, "t_");
    auto rightType = makeRowType(keyTypes, "u_");

    auto leftBatch = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(leftType, leftSize, *pool_));
    auto rightBatch = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rightType, rightSize, *pool_));

    CursorParameters params;
    auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
    params.planNode = PlanBuilder(planNodeIdGenerator)
                          .values({leftBatch}, true)
                          .hashJoin(
                              makeKeyNames(keyTypes.size(), "t_"),
                              makeKeyNames(keyTypes.size(), "u_"),
                              PlanBuilder(planNodeIdGenerator)
                                  .values({rightBatch}, true)
                                  .planNode(),
                              filter,
                              concat(leftType->names(), rightType->names()))
                          .planNode();
    params.maxDrivers = numThreads;

    createDuckDbTable("t", {leftBatch});
    createDuckDbTable("u", {rightBatch});
    auto task = ::assertQuery(
        params, [](auto*) {}, referenceQuery, duckDbQueryRunner_);

    // A quick sanity check for memory usage reporting. Check that peak total
    // memory usage for the hash join node is > 0.
    auto planStats = toPlanStats(task->taskStats());
    auto joinNodeId = params.planNode->id();
    ASSERT_TRUE(planStats.at(joinNodeId).peakMemoryBytes > 0);
  }

  static std::vector<std::string> makeKeyNames(
      int cnt,
      const std::string& prefix) {
    std::vector<std::string> names;
    for (int i = 0; i < cnt; ++i) {
      names.push_back(fmt::format("{}k{}", prefix, i));
    }
    return names;
  }

  static RowTypePtr makeRowType(
      const std::vector<TypePtr>& keyTypes,
      const std::string& namePrefix) {
    std::vector<std::string> names = makeKeyNames(keyTypes.size(), namePrefix);
    names.push_back(fmt::format("{}data", namePrefix));

    std::vector<TypePtr> types = keyTypes;
    types.push_back(VARCHAR());

    return ROW(std::move(names), std::move(types));
  }

  static RuntimeMetric getFiltersProduced(
      const std::shared_ptr<Task>& task,
      int operatorIndex) {
    auto stats = task->taskStats().pipelineStats.front().operatorStats;
    return stats[operatorIndex].runtimeStats["dynamicFiltersProduced"];
  }

  static RuntimeMetric getFiltersAccepted(
      const std::shared_ptr<Task>& task,
      int operatorIndex) {
    auto stats = task->taskStats().pipelineStats.front().operatorStats;
    return stats[operatorIndex].runtimeStats["dynamicFiltersAccepted"];
  }

  static RuntimeMetric getReplacedWithFilterRows(
      const std::shared_ptr<Task>& task,
      int operatorIndex) {
    auto stats = task->taskStats().pipelineStats.front().operatorStats;
    return stats[operatorIndex].runtimeStats["replacedWithDynamicFilterRows"];
  }

  static uint64_t getInputPositions(
      const std::shared_ptr<Task>& task,
      int operatorIndex) {
    auto stats = task->taskStats().pipelineStats.front().operatorStats;
    return stats[operatorIndex].inputPositions;
  }
};

TEST_F(HashJoinTest, bigintArray) {
  testJoin(
      {BIGINT()},
      1,
      16000,
      15000,
      "SELECT t_k0, t_data, u_k0, u_data FROM "
      "  t, u "
      "  WHERE t_k0 = u_k0");
}

TEST_F(HashJoinTest, bigintArrayParallel) {
  testJoin(
      {BIGINT()},
      2,
      16000,
      15000,
      "SELECT t_k0, t_data, u_k0, u_data FROM "
      "  t, u "
      "  WHERE t_k0 = u_k0 "
      "UNION ALL SELECT t_k0, t_data, u_k0, u_data FROM "
      "  t, u "
      "  WHERE t_k0 = u_k0 "
      "UNION ALL SELECT t_k0, t_data, u_k0, u_data FROM "
      "  t, u "
      "  WHERE t_k0 = u_k0 "
      "UNION ALL SELECT t_k0, t_data, u_k0, u_data FROM "
      "  t, u "
      "  WHERE t_k0 = u_k0");
}

TEST_F(HashJoinTest, emptyBuild) {
  testJoin(
      {BIGINT()},
      1,
      16000,
      0,
      "SELECT t_k0, t_data, u_k0, u_data FROM "
      "  t, u "
      "  WHERE t_k0 = u_k0");
}

TEST_F(HashJoinTest, normalizedKey) {
  testJoin(
      {BIGINT(), VARCHAR()},
      1,
      16000,
      15000,
      "SELECT t_k0, t_k1, t_data, u_k0, u_k1, u_data FROM "
      "  t, u "
      "  WHERE t_k0 = u_k0 AND t_k1 = u_k1");
}

TEST_F(HashJoinTest, normalizedKeyOverflow) {
  testJoin(
      {BIGINT(), VARCHAR(), BIGINT(), BIGINT(), BIGINT(), BIGINT()},
      1,
      16000,
      15000,
      "SELECT t_k0, t_k1, t_k2, t_k3, t_k4, t_k5, t_data, u_k0, u_k1, u_k2, u_k3, u_k4, u_k5, u_data FROM "
      "  t, u "
      "  WHERE t_k0 = u_k0 AND t_k1 = u_k1 AND t_k2 = u_k2 AND t_k3 = u_k3 AND t_k4 = u_k4 AND t_k5 = u_k5  ");
}

TEST_F(HashJoinTest, allTypes) {
  testJoin(
      {BIGINT(), VARCHAR(), REAL(), DOUBLE(), INTEGER(), SMALLINT(), TINYINT()},
      1,
      16000,
      15000,
      "SELECT t_k0, t_k1, t_k2, t_k3, t_k4, t_k5, t_k6, t_data, u_k0, u_k1, u_k2, u_k3, u_k4, u_k5, u_k6, u_data FROM "
      "  t, u "
      "  WHERE t_k0 = u_k0 AND t_k1 = u_k1 AND t_k2 = u_k2 AND t_k3 = u_k3 AND t_k4 = u_k4 AND t_k5 = u_k5 AND t_k6 = u_k6 ");
}

TEST_F(HashJoinTest, filter) {
  testJoin(
      {BIGINT()},
      1,
      16000,
      15000,
      "SELECT t_k0, t_data, u_k0, u_data FROM "
      "  t, u "
      "  WHERE t_k0 = u_k0 AND ((t_k0 % 100) + (u_k0 % 100)) % 40 < 20",
      "((t_k0 % 100) + (u_k0 % 100)) % 40 < 20");
}

TEST_F(HashJoinTest, joinSidesDifferentSchema) {
  // In this join, the tables have different schema. LHS table t has schema
  // {INTEGER, VARCHAR, INTEGER}. RHS table u has schema {INTEGER, REAL,
  // INTEGER}. The filter predicate uses
  // a column from the right table  before the left and the corresponding
  // columns at the same channel number(1) have different types. This has been
  // a source of crashes in the join logic.

  size_t batchSize = 100;

  std::vector<std::string> stringVector = {"aaa", "bbb", "ccc", "ddd", "eee"};
  auto leftVectors = makeRowVector({
      makeFlatVector<int32_t>(batchSize, [](auto row) { return row; }),
      makeFlatVector<StringView>(
          batchSize,
          [&](auto row) {
            return StringView(stringVector[row % stringVector.size()]);
          }),
      makeFlatVector<int32_t>(batchSize, [](auto row) { return row; }),
  });
  auto rightVectors = makeRowVector({
      makeFlatVector<int32_t>(batchSize, [](auto row) { return row; }),
      makeFlatVector<double>(batchSize, [](auto row) { return row * 5.0; }),
      makeFlatVector<int32_t>(batchSize, [](auto row) { return row; }),
  });
  createDuckDbTable("t", {leftVectors});
  createDuckDbTable("u", {rightVectors});

  std::string referenceQuery =
      "SELECT t.c0 * t.c2/2 FROM "
      "  t, u "
      "  WHERE t.c0 = u.c0 AND "
      "  u.c2 > 10 AND ltrim(t.c1) = 'a%'";
  // In this hash join the 2 tables have a common key which is the
  // first channel in both tables.
  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
  auto planNode =
      PlanBuilder(planNodeIdGenerator)
          .values({leftVectors})
          .project({"c0 AS t_c0", "c1 AS t_c1", "c2 AS t_c2"})
          .hashJoin(
              {"t_c0"},
              {"u_c0"},
              PlanBuilder(planNodeIdGenerator)
                  .values({rightVectors})
                  .project({"c0 AS u_c0", "c1 AS u_c1", "c2 AS u_c2"})
                  .planNode(),
              "u_c2 > 10 AND ltrim(t_c1) = 'a%'",
              {"t_c0", "t_c2"})
          .project({"t_c0 * t_c2/2"})
          .planNode();

  ::assertQuery(planNode, referenceQuery, duckDbQueryRunner_);
}

TEST_F(HashJoinTest, memory) {
  // Measures memory allocation in a 1:n hash join followed by
  // projection and aggregation. We expect vectors to be mostly
  // reused, except for t_k0 + 1, which is a dictionary after the
  // join.
  std::vector<TypePtr> keyTypes = {BIGINT()};
  auto leftType = makeRowType(keyTypes, "t_");
  auto rightType = makeRowType(keyTypes, "u_");

  std::vector<RowVectorPtr> leftBatches;
  std::vector<RowVectorPtr> rightBatches;
  for (auto i = 0; i < 100; ++i) {
    leftBatches.push_back(std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(leftType, 1000, *pool_)));
  }
  for (auto i = 0; i < 10; ++i) {
    rightBatches.push_back(std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rightType, 800, *pool_)));
  }

  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();

  CursorParameters params;
  params.planNode = PlanBuilder(planNodeIdGenerator)
                        .values(leftBatches, true)
                        .hashJoin(
                            makeKeyNames(keyTypes.size(), "t_"),
                            makeKeyNames(keyTypes.size(), "u_"),
                            PlanBuilder(planNodeIdGenerator)
                                .values(rightBatches, true)
                                .planNode(),
                            "",
                            concat(leftType->names(), rightType->names()))
                        .project({"t_k0 % 1000 AS k1", "u_k0 % 1000 AS k2"})
                        .singleAggregation({}, {"sum(k1)", "sum(k2)"})
                        .planNode();
  params.queryCtx = core::QueryCtx::createForTest();
  auto tracker = memory::MemoryUsageTracker::create();
  params.queryCtx->pool()->setMemoryUsageTracker(tracker);
  auto [taskCursor, rows] = readCursor(params, [](Task*) {});
  EXPECT_GT(2500, tracker->getNumAllocs());
  EXPECT_GT(7'500'000, tracker->getCumulativeBytes());
}

TEST_F(HashJoinTest, lazyVectors) {
  // a dataset of multiple row groups with multiple columns. We create
  // different dictionary wrappings for different columns and load the
  // rows in scope at different times.
  auto leftVectors = makeRowVector(
      {makeFlatVector<int32_t>(30'000, [](auto row) { return row; }),
       makeFlatVector<int64_t>(30'000, [](auto row) { return row % 23; }),
       makeFlatVector<int32_t>(30'000, [](auto row) { return row % 31; }),
       makeFlatVector<StringView>(30'000, [](auto row) {
         return StringView(fmt::format("{}   string", row % 43));
       })});

  auto rightVectors = makeRowVector(
      {makeFlatVector<int32_t>(10'000, [](auto row) { return row * 3; }),
       makeFlatVector<int64_t>(10'000, [](auto row) { return row % 31; })});

  auto leftFile = TempFilePath::create();
  writeToFile(leftFile->path, kWriter, leftVectors);
  createDuckDbTable("t", {leftVectors});

  auto rightFile = TempFilePath::create();
  writeToFile(rightFile->path, kWriter, rightVectors);
  createDuckDbTable("u", {rightVectors});

  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
  core::PlanNodeId leftScanId;
  core::PlanNodeId rightScanId;
  auto op = PlanBuilder(planNodeIdGenerator)
                .tableScan(ROW({"c0", "c1"}, {INTEGER(), BIGINT()}))
                .capturePlanNodeId(leftScanId)
                .hashJoin(
                    {"c0"},
                    {"c0"},
                    PlanBuilder(planNodeIdGenerator)
                        .tableScan(ROW({"c0"}, {INTEGER()}))
                        .capturePlanNodeId(rightScanId)
                        .planNode(),
                    "",
                    {"c1"})
                .project({"c1 + 1"})
                .planNode();

  assertQuery(
      op,
      {{rightScanId, {rightFile}}, {leftScanId, {leftFile}}},
      "SELECT t.c1 + 1 FROM t, u WHERE t.c0 = u.c0");

  planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
  op = PlanBuilder(planNodeIdGenerator)
           .tableScan(
               ROW({"c0", "c1", "c2", "c3"},
                   {INTEGER(), BIGINT(), INTEGER(), VARCHAR()}))
           .capturePlanNodeId(leftScanId)
           .filter("c2 < 29")
           .hashJoin(
               {"c0"},
               {"bc0"},
               PlanBuilder(planNodeIdGenerator)
                   .tableScan(ROW({"c0", "c1"}, {INTEGER(), BIGINT()}))
                   .capturePlanNodeId(rightScanId)
                   .project({"c0 as bc0", "c1 as bc1"})
                   .planNode(),
               "(c1 + bc1) % 33 < 27",
               {"c1", "bc1", "c3"})
           .project({"c1 + 1", "bc1", "length(c3)"})
           .planNode();

  assertQuery(
      op,
      {{rightScanId, {rightFile}}, {leftScanId, {leftFile}}},
      "SELECT t.c1 + 1, U.c1, length(t.c3) FROM t, u "
      "WHERE t.c0 = u.c0 and t.c2 < 29 and (t.c1 + u.c1) % 33 < 27");
}

/// Test hash join where build-side keys come from a small range and allow for
/// array-based lookup instead of a hash table.
TEST_F(HashJoinTest, arrayBasedLookup) {
  auto oddIndices = makeIndices(500, [](auto i) { return 2 * i + 1; });

  auto leftVectors = {
      // Join key vector is flat.
      makeRowVector({
          makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
          makeFlatVector<int64_t>(1'000, [](auto row) { return row; }),
      }),
      // Join key vector is constant. There is a match in the build side.
      makeRowVector({
          BaseVector::createConstant(4, 2'000, pool_.get()),
          makeFlatVector<int64_t>(2'000, [](auto row) { return row; }),
      }),
      // Join key vector is constant. There is no match.
      makeRowVector({
          BaseVector::createConstant(5, 2'000, pool_.get()),
          makeFlatVector<int64_t>(2'000, [](auto row) { return row; }),
      }),
      // Join key vector is a dictionary.
      makeRowVector({
          wrapInDictionary(
              oddIndices,
              500,
              makeFlatVector<int32_t>(1'000, [](auto row) { return row * 4; })),
          makeFlatVector<int64_t>(1'000, [](auto row) { return row; }),
      })};

  // 100 key values in [0, 198] range.
  auto rightVectors = {makeRowVector(
      {makeFlatVector<int32_t>(100, [](auto row) { return row * 2; })})};

  createDuckDbTable("t", {leftVectors});
  createDuckDbTable("u", {rightVectors});

  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
  auto op =
      PlanBuilder(planNodeIdGenerator)
          .values(leftVectors)
          .hashJoin(
              {"c0"},
              {"c0"},
              PlanBuilder(planNodeIdGenerator).values(rightVectors).planNode(),
              "",
              {"c1"})
          .project({"c1 + 1"})
          .planNode();

  auto task = assertQuery(op, "SELECT t.c1 + 1 FROM t, u WHERE t.c0 = u.c0");
  auto joinStats =
      task->taskStats().pipelineStats.back().operatorStats.back().runtimeStats;
  EXPECT_EQ(101, joinStats["distinctKey0"].sum);
  EXPECT_EQ(200, joinStats["rangeKey0"].sum);
}

TEST_F(HashJoinTest, innerJoinWithEmptyBuild) {
  auto leftVectors = makeRowVector({
      makeFlatVector<int32_t>(
          1'234, [](auto row) { return row % 11; }, nullEvery(13)),
      makeFlatVector<int32_t>(1'234, [](auto row) { return row; }),
  });

  auto rightVectors = makeRowVector({makeFlatVector<int32_t>(
      123, [](auto row) { return row % 5; }, nullEvery(7))});

  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
  auto op = PlanBuilder(planNodeIdGenerator)
                .values({leftVectors})
                .hashJoin(
                    {"c0"},
                    {"c0"},
                    PlanBuilder(planNodeIdGenerator)
                        .values({rightVectors})
                        .filter("c0 < 0")
                        .planNode(),
                    "",
                    {"c1"},
                    core::JoinType::kInner)
                .planNode();

  assertQueryReturnsEmptyResult(op);
}

TEST_F(HashJoinTest, semiJoin) {
  auto leftVectors = makeRowVector({
      makeFlatVector<int32_t>(
          1'234, [](auto row) { return row % 11; }, nullEvery(13)),
      makeFlatVector<int32_t>(1'234, [](auto row) { return row; }),
  });

  auto rightVectors = makeRowVector({
      makeFlatVector<int32_t>(
          123, [](auto row) { return row % 5; }, nullEvery(7)),
  });

  createDuckDbTable("t", {leftVectors});
  createDuckDbTable("u", {rightVectors});

  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
  auto op = PlanBuilder(planNodeIdGenerator)
                .values({leftVectors})
                .hashJoin(
                    {"c0"},
                    {"c0"},
                    PlanBuilder(planNodeIdGenerator)
                        .values({rightVectors})
                        .planNode(),
                    "",
                    {"c1"},
                    core::JoinType::kSemi)
                .planNode();

  assertQuery(op, "SELECT t.c1 FROM t WHERE t.c0 IN (SELECT c0 FROM u)");

  // Empty build side.
  planNodeIdGenerator->reset();
  op = PlanBuilder(planNodeIdGenerator)
           .values({leftVectors})
           .hashJoin(
               {"c0"},
               {"c0"},
               PlanBuilder(planNodeIdGenerator)
                   .values({rightVectors})
                   .filter("c0 < 0")
                   .planNode(),
               "",
               {"c1"},
               core::JoinType::kSemi)
           .planNode();

  assertQuery(
      op, "SELECT t.c1 FROM t WHERE t.c0 IN (SELECT c0 FROM u WHERE c0 < 0)");
}

TEST_F(HashJoinTest, antiJoin) {
  auto leftVectors = makeRowVector({
      makeFlatVector<int32_t>(
          1'000, [](auto row) { return row % 11; }, nullEvery(13)),
      makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
  });

  auto rightVectors = makeRowVector({
      makeFlatVector<int32_t>(
          1'234, [](auto row) { return row % 5; }, nullEvery(7)),
  });

  createDuckDbTable("t", {leftVectors});
  createDuckDbTable("u", {rightVectors});

  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
  auto op = PlanBuilder(planNodeIdGenerator)
                .values({leftVectors})
                .hashJoin(
                    {"c0"},
                    {"c0"},
                    PlanBuilder(planNodeIdGenerator)
                        .values({rightVectors})
                        .filter("c0 IS NOT NULL")
                        .planNode(),
                    "",
                    {"c1"},
                    core::JoinType::kAnti)
                .planNode();

  assertQuery(
      op,
      "SELECT t.c1 FROM t WHERE t.c0 NOT IN (SELECT c0 FROM u WHERE c0 IS NOT NULL)");

  // Empty build side.
  planNodeIdGenerator->reset();
  op = PlanBuilder(planNodeIdGenerator)
           .values({leftVectors})
           .hashJoin(
               {"c0"},
               {"c0"},
               PlanBuilder(planNodeIdGenerator)
                   .values({rightVectors})
                   .filter("c0 < 0")
                   .planNode(),
               "",
               {"c1"},
               core::JoinType::kAnti)
           .planNode();

  assertQuery(
      op,
      "SELECT t.c1 FROM t WHERE t.c0 NOT IN (SELECT c0 FROM u WHERE c0 < 0)");

  // Build side with nulls. Anti join always returns nothing.
  planNodeIdGenerator->reset();
  op = PlanBuilder(planNodeIdGenerator)
           .values({leftVectors})
           .hashJoin(
               {"c0"},
               {"c0"},
               PlanBuilder(planNodeIdGenerator)
                   .values({rightVectors})
                   .planNode(),
               "",
               {"c1"},
               core::JoinType::kAnti)
           .planNode();

  assertQuery(op, "SELECT t.c1 FROM t WHERE t.c0 NOT IN (SELECT c0 FROM u)");
}

TEST_F(HashJoinTest, dynamicFilters) {
  std::vector<RowVectorPtr> leftVectors;
  leftVectors.reserve(20);

  auto leftFiles = makeFilePaths(20);

  for (int i = 0; i < 20; i++) {
    auto rowVector = makeRowVector({
        makeFlatVector<int32_t>(1'024, [&](auto row) { return row - i * 10; }),
        makeFlatVector<int64_t>(1'024, [](auto row) { return row; }),
    });
    leftVectors.push_back(rowVector);
    writeToFile(leftFiles[i]->path, kWriter, rowVector);
  }

  // 100 key values in [35, 233] range.
  auto rightKey =
      makeFlatVector<int32_t>(100, [](auto row) { return 35 + row * 2; });
  auto rightVectors = {makeRowVector({
      rightKey,
      makeFlatVector<int64_t>(100, [](auto row) { return row; }),
  })};

  createDuckDbTable("t", {leftVectors});
  createDuckDbTable("u", {rightVectors});

  auto probeType = ROW({"c0", "c1"}, {INTEGER(), BIGINT()});

  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();

  auto buildSide = PlanBuilder(planNodeIdGenerator)
                       .values(rightVectors)
                       .project({"c0 AS u_c0", "c1 AS u_c1"})
                       .planNode();
  auto keyOnlyBuildSide = PlanBuilder(planNodeIdGenerator)
                              .values({makeRowVector({rightKey})})
                              .project({"c0 AS u_c0"})
                              .planNode();

  // Basic push-down.
  {
    // Inner join.
    core::PlanNodeId leftScanId;
    auto op = PlanBuilder(planNodeIdGenerator)
                  .tableScan(probeType)
                  .capturePlanNodeId(leftScanId)
                  .hashJoin(
                      {"c0"},
                      {"u_c0"},
                      buildSide,
                      "",
                      {"c0", "c1", "u_c1"},
                      core::JoinType::kInner)
                  .project({"c0", "c1 + 1", "c1 + u_c1"})
                  .planNode();

    auto task = assertQuery(
        op,
        {{leftScanId, leftFiles}},
        "SELECT t.c0, t.c1 + 1, t.c1 + u.c1 FROM t, u WHERE t.c0 = u.c0");
    EXPECT_EQ(1, getFiltersProduced(task, 1).sum);
    EXPECT_EQ(1, getFiltersAccepted(task, 0).sum);
    EXPECT_EQ(0, getReplacedWithFilterRows(task, 1).sum);
    EXPECT_LT(getInputPositions(task, 1), 1024 * 20);

    // Semi join.
    op = PlanBuilder(planNodeIdGenerator)
             .tableScan(probeType)
             .capturePlanNodeId(leftScanId)
             .hashJoin(
                 {"c0"},
                 {"u_c0"},
                 buildSide,
                 "",
                 {"c0", "c1"},
                 core::JoinType::kSemi)
             .project({"c0", "c1 + 1"})
             .planNode();

    task = assertQuery(
        op,
        {{leftScanId, leftFiles}},
        "SELECT t.c0, t.c1 + 1 FROM t WHERE t.c0 IN (SELECT c0 FROM u)");
    EXPECT_EQ(1, getFiltersProduced(task, 1).sum);
    EXPECT_EQ(1, getFiltersAccepted(task, 0).sum);
    EXPECT_GT(getReplacedWithFilterRows(task, 1).sum, 0);
    EXPECT_LT(getInputPositions(task, 1), 1024 * 20);
  }

  // Basic push-down with column names projected out of the table scan having
  // different names than column names in the files.
  {
    auto scanOutputType = ROW({"a", "b"}, {INTEGER(), BIGINT()});
    ColumnHandleMap assignments;
    assignments["a"] = regularColumn("c0", INTEGER());
    assignments["b"] = regularColumn("c1", BIGINT());

    core::PlanNodeId leftScanId;
    auto op =
        PlanBuilder(planNodeIdGenerator)
            .tableScan(
                scanOutputType,
                makeTableHandle(common::test::SubfieldFiltersBuilder().build()),
                assignments)
            .capturePlanNodeId(leftScanId)
            .hashJoin({"a"}, {"u_c0"}, buildSide, "", {"a", "b", "u_c1"})
            .project({"a", "b + 1", "b + u_c1"})
            .planNode();

    auto task = assertQuery(
        op,
        {{leftScanId, leftFiles}},
        "SELECT t.c0, t.c1 + 1, t.c1 + u.c1 FROM t, u WHERE t.c0 = u.c0");
    EXPECT_EQ(1, getFiltersProduced(task, 1).sum);
    EXPECT_EQ(1, getFiltersAccepted(task, 0).sum);
    EXPECT_EQ(0, getReplacedWithFilterRows(task, 1).sum);
    EXPECT_LT(getInputPositions(task, 1), 1024 * 20);
  }

  // Push-down that requires merging filters.
  {
    auto filters =
        common::test::singleSubfieldFilter("c0", common::test::lessThan(500));
    core::PlanNodeId leftScanId;
    auto op = PlanBuilder(planNodeIdGenerator)
                  .tableScan(
                      probeType,
                      makeTableHandle(std::move(filters)),
                      allRegularColumns(probeType))
                  .capturePlanNodeId(leftScanId)
                  .hashJoin({"c0"}, {"u_c0"}, buildSide, "", {"c1", "u_c1"})
                  .project({"c1 + u_c1"})
                  .planNode();

    auto task = assertQuery(
        op,
        {{leftScanId, leftFiles}},
        "SELECT t.c1 + u.c1 FROM t, u WHERE t.c0 = u.c0 AND t.c0 < 500");
    EXPECT_EQ(1, getFiltersProduced(task, 1).sum);
    EXPECT_EQ(1, getFiltersAccepted(task, 0).sum);
    EXPECT_EQ(0, getReplacedWithFilterRows(task, 1).sum);
  }

  // Push-down that turns join into a no-op.
  {
    core::PlanNodeId leftScanId;
    auto op =
        PlanBuilder(planNodeIdGenerator)
            .tableScan(probeType)
            .capturePlanNodeId(leftScanId)
            .hashJoin({"c0"}, {"u_c0"}, keyOnlyBuildSide, "", {"c0", "c1"})
            .project({"c0", "c1 + 1"})
            .planNode();

    auto task = assertQuery(
        op,
        {{leftScanId, leftFiles}},
        "SELECT t.c0, t.c1 + 1 FROM t, u WHERE t.c0 = u.c0");
    EXPECT_EQ(1, getFiltersProduced(task, 1).sum);
    EXPECT_EQ(1, getFiltersAccepted(task, 0).sum);
    EXPECT_GT(getReplacedWithFilterRows(task, 1).sum, 0);
    EXPECT_LT(getInputPositions(task, 1), 1024 * 20);
  }

  // Push-down that requires merging filters and turns join into a no-op.
  {
    auto filters =
        common::test::singleSubfieldFilter("c0", common::test::lessThan(500));
    core::PlanNodeId leftScanId;
    auto op = PlanBuilder(planNodeIdGenerator)
                  .tableScan(
                      probeType,
                      makeTableHandle(std::move(filters)),
                      allRegularColumns(probeType))
                  .capturePlanNodeId(leftScanId)
                  .hashJoin({"c0"}, {"u_c0"}, keyOnlyBuildSide, "", {"c1"})
                  .project({"c1 + 1"})
                  .planNode();

    auto task = assertQuery(
        op,
        {{leftScanId, leftFiles}},
        "SELECT t.c1 + 1 FROM t, u WHERE t.c0 = u.c0 AND t.c0 < 500");
    EXPECT_EQ(1, getFiltersProduced(task, 1).sum);
    EXPECT_EQ(1, getFiltersAccepted(task, 0).sum);
    EXPECT_GT(getReplacedWithFilterRows(task, 1).sum, 0);
  }

  // Disable filter push-down by using highly selective filter in the scan.
  {
    // Inner join.
    auto filters =
        common::test::singleSubfieldFilter("c0", common::test::lessThan(200));
    auto probeTableHandle = makeTableHandle(std::move(filters));
    core::PlanNodeId leftScanId;
    auto op =
        PlanBuilder(planNodeIdGenerator)
            .tableScan(
                probeType, probeTableHandle, allRegularColumns(probeType))
            .capturePlanNodeId(leftScanId)
            .hashJoin(
                {"c0"}, {"u_c0"}, buildSide, "", {"c1"}, core::JoinType::kInner)
            .project({"c1 + 1"})
            .planNode();

    auto task = assertQuery(
        op,
        {{leftScanId, leftFiles}},
        "SELECT t.c1 + 1 FROM t, u WHERE t.c0 = u.c0 AND t.c0 < 200");
    EXPECT_EQ(0, getFiltersProduced(task, 1).sum);
    EXPECT_EQ(0, getFiltersAccepted(task, 0).sum);
    EXPECT_EQ(0, getReplacedWithFilterRows(task, 1).sum);

    // Semi join.
    op = PlanBuilder(planNodeIdGenerator)
             .tableScan(
                 probeType, probeTableHandle, allRegularColumns(probeType))
             .capturePlanNodeId(leftScanId)
             .hashJoin(
                 {"c0"}, {"u_c0"}, buildSide, "", {"c1"}, core::JoinType::kSemi)
             .project({"c1 + 1"})
             .planNode();

    task = assertQuery(
        op,
        {{leftScanId, leftFiles}},
        "SELECT t.c1 + 1 FROM t WHERE t.c0 IN (SELECT c0 FROM u) AND t.c0 < 200");
    EXPECT_EQ(0, getFiltersProduced(task, 1).sum);
    EXPECT_EQ(0, getFiltersAccepted(task, 0).sum);
    EXPECT_EQ(0, getReplacedWithFilterRows(task, 1).sum);
  }

  // Disable filter push-down by using values in place of scan.
  {
    auto op = PlanBuilder(planNodeIdGenerator)
                  .values(leftVectors)
                  .hashJoin({"c0"}, {"u_c0"}, buildSide, "", {"c1"})
                  .project({"c1 + 1"})
                  .planNode();

    auto task = assertQuery(op, "SELECT t.c1 + 1 FROM t, u WHERE t.c0 = u.c0");
    EXPECT_EQ(0, getFiltersProduced(task, 1).sum);
    EXPECT_EQ(0, getFiltersAccepted(task, 0).sum);
    EXPECT_EQ(getInputPositions(task, 1), 1024 * 20);
  }

  // Disable filter push-down by using an expression as the join key on the
  // probe side.
  {
    core::PlanNodeId leftScanId;
    auto op = PlanBuilder(planNodeIdGenerator)
                  .tableScan(probeType)
                  .capturePlanNodeId(leftScanId)
                  .project({"cast(c0 + 1 as integer) AS t_key", "c1"})
                  .hashJoin({"t_key"}, {"u_c0"}, buildSide, "", {"c1"})
                  .project({"c1 + 1"})
                  .planNode();

    auto task = assertQuery(
        op,
        {{leftScanId, leftFiles}},
        "SELECT t.c1 + 1 FROM t, u WHERE (t.c0 + 1) = u.c0");
    EXPECT_EQ(0, getFiltersProduced(task, 1).sum);
    EXPECT_EQ(0, getFiltersAccepted(task, 0).sum);
    EXPECT_EQ(getInputPositions(task, 1), 1024 * 20);
  }
}

TEST_F(HashJoinTest, leftJoin) {
  // Left side keys are [0, 1, 2,..10].
  // Use 3-rd column as row number to allow for asserting the order of results.
  auto leftVectors = {
      makeRowVector(
          {"c0", "c1", "row_number"},
          {
              makeFlatVector<int32_t>(
                  1'234, [](auto row) { return row % 11; }, nullEvery(13)),
              makeFlatVector<int32_t>(1'234, [](auto row) { return row; }),
              makeFlatVector<int32_t>(1'234, [](auto row) { return row; }),
          }),
      makeRowVector(
          {"c0", "c1", "row_number"},
          {
              makeFlatVector<int32_t>(
                  2'222,
                  [](auto row) { return (row + 3) % 11; },
                  nullEvery(13)),
              makeFlatVector<int32_t>(2'222, [](auto row) { return row; }),
              makeFlatVector<int32_t>(
                  2'222, [](auto row) { return 1'234 + row; }),
          }),
  };

  // Right side keys are [0, 1, 2, 3, 4].
  auto rightVectors = makeRowVector({
      makeFlatVector<int32_t>(
          123, [](auto row) { return row % 5; }, nullEvery(7)),
      makeFlatVector<int32_t>(
          123, [](auto row) { return -111 + row * 2; }, nullEvery(7)),
  });

  createDuckDbTable("t", leftVectors);
  createDuckDbTable("u", {rightVectors});

  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
  auto buildSide = PlanBuilder(planNodeIdGenerator)
                       .values({rightVectors})
                       .project({"c0 AS u_c0", "c1 AS u_c1"})
                       .planNode();

  auto op = PlanBuilder(planNodeIdGenerator)
                .values(leftVectors)
                .hashJoin(
                    {"c0"},
                    {"u_c0"},
                    buildSide,
                    "",
                    {"row_number", "c0", "c1", "u_c1"},
                    core::JoinType::kLeft)
                .planNode();

  assertQueryOrdered(
      op,
      "SELECT t.row_number, t.c0, t.c1, u.c1 FROM t LEFT JOIN u ON t.c0 = u.c0 ORDER BY 1",
      {0});

  // Empty build side.
  auto emptyBuildSide = PlanBuilder(planNodeIdGenerator)
                            .values({rightVectors})
                            .filter("c0 < 0")
                            .project({"c0 AS u_c0", "c1 AS u_c1"})
                            .planNode();

  op = PlanBuilder(planNodeIdGenerator)
           .values(leftVectors)
           .hashJoin(
               {"c0"},
               {"u_c0"},
               emptyBuildSide,
               "",
               {"row_number", "c1"},
               core::JoinType::kLeft)
           .planNode();

  assertQueryOrdered(
      op,
      "SELECT t.row_number, t.c1 FROM t LEFT JOIN (SELECT c0 FROM u WHERE c0 < 0) u ON t.c0 = u.c0 ORDER BY 1",
      {0});

  // All left-side rows have a match on the build side.
  op = PlanBuilder(planNodeIdGenerator)
           .values(leftVectors)
           .filter("c0 < 5")
           .hashJoin(
               {"c0"},
               {"u_c0"},
               buildSide,
               "",
               {"row_number", "c0", "c1", "u_c1"},
               core::JoinType::kLeft)
           .planNode();

  assertQueryOrdered(
      op,
      "SELECT t.row_number, t.c0, t.c1, u.c1 FROM (SELECT * FROM t WHERE c0 < 5) t"
      " LEFT JOIN u ON t.c0 = u.c0 ORDER BY t.row_number",
      {0});

  // Additional filter.
  op = PlanBuilder(planNodeIdGenerator)
           .values(leftVectors)
           .hashJoin(
               {"c0"},
               {"u_c0"},
               buildSide,
               "(c1 + u_c1) % 2 = 1",
               {"row_number", "c0", "c1", "u_c1"},
               core::JoinType::kLeft)
           .planNode();

  assertQueryOrdered(
      op,
      "SELECT t.row_number, t.c0, t.c1, u.c1 FROM t LEFT JOIN u ON t.c0 = u.c0 AND (t.c1 + u.c1) % 2 = 1 ORDER BY t.row_number",
      {0});

  // No rows pass the additional filter.
  op = PlanBuilder(planNodeIdGenerator)
           .values(leftVectors)
           .hashJoin(
               {"c0"},
               {"u_c0"},
               buildSide,
               "(c1 + u_c1) % 2  = 3",
               {"row_number", "c0", "c1", "u_c1"},
               core::JoinType::kLeft)
           .planNode();

  assertQueryOrdered(
      op,
      "SELECT t.row_number, t.c0, t.c1, u.c1 FROM t LEFT JOIN u ON t.c0 = u.c0 AND (t.c1 + u.c1) % 2 = 3 ORDER BY t.row_number",
      {0});
}

/// Tests left join with a filter that may evalute to true, false or null. Makes
/// sure that null filter results are handled correctly, e.g. as if the filter
/// returned false.
TEST_F(HashJoinTest, leftJoinWithNullableFilter) {
  auto leftVectors = {
      makeRowVector({
          makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
          makeNullableFlatVector<int32_t>(
              {10, std::nullopt, 30, std::nullopt, 50}),
      }),
      makeRowVector({
          makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
          makeNullableFlatVector<int32_t>(
              {std::nullopt, 20, 30, std::nullopt, 50}),
      })};
  auto rightVectors = {
      makeRowVector({makeFlatVector<int32_t>({1, 2, 10, 30, 40})}),
  };

  createDuckDbTable("t", leftVectors);
  createDuckDbTable("u", rightVectors);

  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();

  auto buildSide = PlanBuilder(planNodeIdGenerator)
                       .values(rightVectors)
                       .project({"c0 AS u_c0"})
                       .planNode();

  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(leftVectors)
                  .hashJoin(
                      {"c0"},
                      {"u_c0"},
                      buildSide,
                      "c1 + u_c0 > 0",
                      {"c0", "c1", "u_c0"},
                      core::JoinType::kLeft)
                  .planNode();

  assertQuery(
      plan, "SELECT * FROM t LEFT JOIN u ON (t.c0 = u.c0 AND t.c1 + u.c0 > 0)");
}

TEST_F(HashJoinTest, rightJoin) {
  // Left side keys are [0, 1, 2,..10].
  auto leftVectors = {
      makeRowVector({
          makeFlatVector<int32_t>(
              1'234, [](auto row) { return row % 11; }, nullEvery(13)),
          makeFlatVector<int32_t>(1'234, [](auto row) { return row; }),
      }),
      makeRowVector({
          makeFlatVector<int32_t>(
              2'222, [](auto row) { return (row + 3) % 11; }, nullEvery(13)),
          makeFlatVector<int32_t>(2'222, [](auto row) { return row; }),
      }),
  };

  // Right side keys are [-3, -2, -1, 0, 1, 2, 3].
  auto rightVectors = makeRowVector({
      makeFlatVector<int32_t>(
          123, [](auto row) { return -3 + row % 7; }, nullEvery(11)),
      makeFlatVector<int32_t>(
          123, [](auto row) { return -111 + row * 2; }, nullEvery(13)),
  });

  createDuckDbTable("t", leftVectors);
  createDuckDbTable("u", {rightVectors});

  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();

  auto buildSide = PlanBuilder(planNodeIdGenerator)
                       .values({rightVectors})
                       .project({"c0 AS u_c0", "c1 AS u_c1"})
                       .planNode();

  auto op = PlanBuilder(planNodeIdGenerator)
                .values(leftVectors)
                .hashJoin(
                    {"c0"},
                    {"u_c0"},
                    buildSide,
                    "",
                    {"c0", "c1", "u_c1"},
                    core::JoinType::kRight)
                .planNode();

  assertQuery(op, "SELECT t.c0, t.c1, u.c1 FROM t RIGHT JOIN u ON t.c0 = u.c0");

  // Empty build side.
  auto emptyBuildSide = PlanBuilder(planNodeIdGenerator)
                            .values({rightVectors})
                            .filter("c0 > 100")
                            .project({"c0 AS u_c0", "c1 AS u_c1"})
                            .planNode();
  op = PlanBuilder(planNodeIdGenerator)
           .values(leftVectors)
           .hashJoin(
               {"c0"},
               {"u_c0"},
               emptyBuildSide,
               "",
               {"c1"},
               core::JoinType::kRight)
           .planNode();

  assertQueryReturnsEmptyResult(op);

  // All right-side rows have a match on the left side.
  planNodeIdGenerator->reset();
  op = PlanBuilder(planNodeIdGenerator)
           .values(leftVectors)
           .hashJoin(
               {"c0"},
               {"u_c0"},
               PlanBuilder(planNodeIdGenerator)
                   .values({rightVectors})
                   .filter("c0 >= 0")
                   .project({"c0 AS u_c0", "c1 AS u_c1"})
                   .planNode(),
               "",
               {"c0", "c1", "u_c1"},
               core::JoinType::kRight)
           .planNode();

  assertQuery(
      op,
      "SELECT t.c0, t.c1, u.c1 FROM t"
      " RIGHT JOIN (SELECT * FROM u WHERE c0 >= 0) u ON t.c0 = u.c0");

  // Additional filter.
  op = PlanBuilder(planNodeIdGenerator)
           .values(leftVectors)
           .hashJoin(
               {"c0"},
               {"u_c0"},
               buildSide,
               "(c1 + u_c1) % 2 = 1",
               {"c0", "c1", "u_c1"},
               core::JoinType::kRight)
           .planNode();

  assertQuery(
      op,
      "SELECT t.c0, t.c1, u.c1 FROM t RIGHT JOIN u ON t.c0 = u.c0 AND (t.c1 + u.c1) % 2 = 1");

  // No rows pass the additional filter.
  op = PlanBuilder(planNodeIdGenerator)
           .values(leftVectors)
           .hashJoin(
               {"c0"},
               {"u_c0"},
               buildSide,
               "(c1 + u_c1) % 2 = 3",
               {"c0", "c1", "u_c1"},
               core::JoinType::kRight)
           .planNode();

  assertQuery(
      op,
      "SELECT t.c0, t.c1, u.c1 FROM t RIGHT JOIN u ON t.c0 = u.c0 AND (t.c1 + u.c1) % 2 = 3");
}

TEST_F(HashJoinTest, fullJoin) {
  // Left side keys are [0, 1, 2,..10].
  auto leftVectors = {
      makeRowVector({
          makeFlatVector<int32_t>(
              1'234, [](auto row) { return row % 11; }, nullEvery(13)),
          makeFlatVector<int32_t>(1'234, [](auto row) { return row; }),
      }),
      makeRowVector({
          makeFlatVector<int32_t>(
              2'222, [](auto row) { return (row + 3) % 11; }, nullEvery(13)),
          makeFlatVector<int32_t>(2'222, [](auto row) { return row; }),
      }),
  };

  // Right side keys are [-3, -2, -1, 0, 1, 2, 3].
  auto rightVectors = makeRowVector({
      makeFlatVector<int32_t>(
          123, [](auto row) { return -3 + row % 7; }, nullEvery(11)),
      makeFlatVector<int32_t>(
          123, [](auto row) { return -111 + row * 2; }, nullEvery(13)),
  });

  createDuckDbTable("t", leftVectors);
  createDuckDbTable("u", {rightVectors});

  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();

  auto buildSide = PlanBuilder(planNodeIdGenerator)
                       .values({rightVectors})
                       .project({"c0 AS u_c0", "c1 AS u_c1"})
                       .planNode();

  auto op = PlanBuilder(planNodeIdGenerator)
                .values(leftVectors)
                .hashJoin(
                    {"c0"},
                    {"u_c0"},
                    buildSide,
                    "",
                    {"c0", "c1", "u_c1"},
                    core::JoinType::kFull)
                .planNode();

  assertQuery(
      op, "SELECT t.c0, t.c1, u.c1 FROM t FULL OUTER JOIN u ON t.c0 = u.c0");

  // Empty build side.
  planNodeIdGenerator->reset();
  auto emptyBuildSide = PlanBuilder(planNodeIdGenerator)
                            .values({rightVectors})
                            .filter("c0 > 100")
                            .project({"c0 AS u_c0", "c1 AS u_c1"})
                            .planNode();
  op = PlanBuilder(planNodeIdGenerator)
           .values(leftVectors)
           .hashJoin(
               {"c0"},
               {"u_c0"},
               emptyBuildSide,
               "",
               {"c1"},
               core::JoinType::kFull)
           .planNode();

  assertQuery(
      op,
      "SELECT t.c1 FROM t FULL OUTER JOIN (SELECT * FROM u WHERE c0 > 100) u ON t.c0 = u.c0");

  // Additional filter.
  op = PlanBuilder(planNodeIdGenerator)
           .values(leftVectors)
           .hashJoin(
               {"c0"},
               {"u_c0"},
               buildSide,
               "(c1 + u_c1) % 2 = 1",
               {"c0", "c1", "u_c1"},
               core::JoinType::kFull)
           .planNode();

  assertQuery(
      op,
      "SELECT t.c0, t.c1, u.c1 FROM t FULL OUTER JOIN u ON t.c0 = u.c0 AND (t.c1 + u.c1) % 2 = 1");

  // No rows pass the additional filter.
  op = PlanBuilder(planNodeIdGenerator)
           .values(leftVectors)
           .hashJoin(
               {"c0"},
               {"u_c0"},
               buildSide,
               "(c1 + u_c1) % 2 = 3",
               {"c0", "c1", "u_c1"},
               core::JoinType::kFull)
           .planNode();

  assertQuery(
      op,
      "SELECT t.c0, t.c1, u.c1 FROM t FULL OUTER JOIN u ON t.c0 = u.c0 AND (t.c1 + u.c1) % 2 = 3");
}

// Verify the size of the join output vectors when projecting build-side
// variable-width column.
TEST_F(HashJoinTest, memoryUsage) {
  std::vector<RowVectorPtr> probeData;
  probeData.reserve(10);
  for (auto i = 0; i < 10; i++) {
    probeData.push_back(makeRowVector(
        {makeFlatVector<int32_t>(1'000, [](auto row) { return row % 5; })}));
  }
  auto buildData = makeRowVector(
      {"u_c0", "u_c1"},
      {makeFlatVector<int32_t>({0, 1, 2}),
       makeFlatVector<std::string>({
           std::string(40, 'a'),
           std::string(50, 'b'),
           std::string(30, 'c'),
       })});

  core::PlanNodeId joinNodeId;

  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .values(probeData)
          .hashJoin(
              {"c0"},
              {"u_c0"},
              PlanBuilder(planNodeIdGenerator).values({buildData}).planNode(),
              "",
              {"c0", "u_c1"})
          .capturePlanNodeId(joinNodeId)
          .singleAggregation({}, {"count(1)"})
          .planNode();

  auto task = assertQuery(plan, "SELECT 6000");

  auto planStats = toPlanStats(task->taskStats());
  auto outputBytes = planStats.at(joinNodeId).outputBytes;
  ASSERT_LT(outputBytes, 512 * 1024);
}
