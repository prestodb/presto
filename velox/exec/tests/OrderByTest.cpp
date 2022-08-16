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
#include "velox/common/file/FileSystems.h"
#include "velox/core/QueryConfig.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/Spiller.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/tests/VectorMaker.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::core;
using namespace facebook::velox::exec::test;

namespace {
// Returns aggregated spilled stats by 'task'.
Spiller::Stats spilledStats(const exec::Task& task) {
  Spiller::Stats spilledStats;
  auto stats = task.taskStats();
  for (auto& pipeline : stats.pipelineStats) {
    for (auto op : pipeline.operatorStats) {
      spilledStats.spilledBytes += op.spilledBytes;
      spilledStats.spilledRows += op.spilledRows;
      spilledStats.spilledPartitions += op.spilledPartitions;
    }
  }
  return spilledStats;
}
} // namespace

class OrderByTest : public OperatorTestBase {
 protected:
  void SetUp() override {
    filesystems::registerLocalFileSystem();
  }

  void testSingleKey(
      const std::vector<RowVectorPtr>& input,
      const std::string& key) {
    core::PlanNodeId orderById;
    auto keyIndex = input[0]->type()->asRow().getChildIdx(key);
    auto plan = PlanBuilder()
                    .values(input)
                    .orderBy({fmt::format("{} ASC NULLS LAST", key)}, false)
                    .capturePlanNodeId(orderById)
                    .planNode();
    runTest(
        plan,
        orderById,
        fmt::format("SELECT * FROM tmp ORDER BY {} NULLS LAST", key),
        {keyIndex});

    plan = PlanBuilder()
               .values(input)
               .orderBy({fmt::format("{} DESC NULLS FIRST", key)}, false)
               .planNode();
    runTest(
        plan,
        orderById,
        fmt::format("SELECT * FROM tmp ORDER BY {} DESC NULLS FIRST", key),
        {keyIndex});
  }

  void testSingleKey(
      const std::vector<RowVectorPtr>& input,
      const std::string& key,
      const std::string& filter) {
    core::PlanNodeId orderById;
    auto keyIndex = input[0]->type()->asRow().getChildIdx(key);
    auto plan = PlanBuilder()
                    .values(input)
                    .filter(filter)
                    .orderBy({fmt::format("{} ASC NULLS LAST", key)}, false)
                    .capturePlanNodeId(orderById)
                    .planNode();
    runTest(
        plan,
        orderById,
        fmt::format(
            "SELECT * FROM tmp WHERE {} ORDER BY {} NULLS LAST", filter, key),
        {keyIndex});

    plan = PlanBuilder()
               .values(input)
               .filter(filter)
               .orderBy({fmt::format("{} DESC NULLS FIRST", key)}, false)
               .capturePlanNodeId(orderById)
               .planNode();
    runTest(
        plan,
        orderById,
        fmt::format(
            "SELECT * FROM tmp WHERE {} ORDER BY {} DESC NULLS FIRST",
            filter,
            key),
        {keyIndex});
  }

  void testTwoKeys(
      const std::vector<RowVectorPtr>& input,
      const std::string& key1,
      const std::string& key2) {
    auto rowType = input[0]->type()->asRow();
    auto keyIndices = {rowType.getChildIdx(key1), rowType.getChildIdx(key2)};

    std::vector<core::SortOrder> sortOrders = {
        core::kAscNullsLast, core::kDescNullsFirst};
    std::vector<std::string> sortOrderSqls = {"NULLS LAST", "DESC NULLS FIRST"};

    for (int i = 0; i < sortOrders.size(); i++) {
      for (int j = 0; j < sortOrders.size(); j++) {
        core::PlanNodeId orderById;
        auto plan = PlanBuilder()
                        .values(input)
                        .orderBy(
                            {fmt::format("{} {}", key1, sortOrderSqls[i]),
                             fmt::format("{} {}", key2, sortOrderSqls[j])},
                            false)
                        .capturePlanNodeId(orderById)
                        .planNode();
        runTest(
            plan,
            orderById,
            fmt::format(
                "SELECT * FROM tmp ORDER BY {} {}, {} {}",
                key1,
                sortOrderSqls[i],
                key2,
                sortOrderSqls[j]),
            keyIndices);
      }
    }
  }

  void runTest(
      core::PlanNodePtr planNode,
      const core::PlanNodeId& orderById,
      const std::string& duckDbSql,
      const std::vector<uint32_t>& sortingKeys) {
    {
      SCOPED_TRACE("run without spilling");
      assertQueryOrdered(planNode, duckDbSql, sortingKeys);
    }
    {
      SCOPED_TRACE("run with spilling");
      auto spillDirectory = exec::test::TempDirectoryPath::create();
      auto queryCtx = core::QueryCtx::createForTest();
      queryCtx->setConfigOverridesUnsafe(
          {{core::QueryConfig::kTestingSpillPct, "100"},
           {core::QueryConfig::kSpillPath, spillDirectory->path}});
      CursorParameters params;
      params.planNode = planNode;
      params.queryCtx = queryCtx;
      auto task = assertQueryOrdered(params, duckDbSql, sortingKeys);
      auto inputRows = toPlanStats(task->taskStats()).at(orderById).inputRows;
      if (inputRows > 0) {
        EXPECT_LT(0, spilledStats(*task).spilledBytes);
        EXPECT_EQ(1, spilledStats(*task).spilledPartitions);
        // NOTE: the last input batch won't go spilling.
        EXPECT_GT(inputRows, spilledStats(*task).spilledRows);
      } else {
        EXPECT_EQ(0, spilledStats(*task).spilledBytes);
      }
    }
  }
};

TEST_F(OrderByTest, selectiveFilter) {
  vector_size_t batchSize = 1000;
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 3; ++i) {
    auto c0 = makeFlatVector<int64_t>(
        batchSize,
        [&](vector_size_t row) { return batchSize * i + row; },
        nullEvery(5));
    auto c1 = makeFlatVector<int64_t>(
        batchSize, [&](vector_size_t row) { return row; }, nullEvery(5));
    auto c2 = makeFlatVector<double>(
        batchSize, [](vector_size_t row) { return row * 0.1; }, nullEvery(11));
    vectors.push_back(makeRowVector({c0, c1, c2}));
  }
  createDuckDbTable(vectors);

  // c0 values are unique across batches
  testSingleKey(vectors, "c0", "c0 % 333 = 0");

  // c1 values are unique only within a batch
  testSingleKey(vectors, "c1", "c1 % 333 = 0");
}

TEST_F(OrderByTest, singleKey) {
  vector_size_t batchSize = 1000;
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 2; ++i) {
    auto c0 = makeFlatVector<int64_t>(
        batchSize, [&](vector_size_t row) { return row; }, nullEvery(5));
    auto c1 = makeFlatVector<double>(
        batchSize, [](vector_size_t row) { return row * 0.1; }, nullEvery(11));
    vectors.push_back(makeRowVector({c0, c1}));
  }
  createDuckDbTable(vectors);

  testSingleKey(vectors, "c0");

  // parser doesn't support "is not null" expression, hence, using c0 % 2 >= 0
  testSingleKey(vectors, "c0", "c0 % 2 >= 0");

  core::PlanNodeId orderById;
  auto plan = PlanBuilder()
                  .values(vectors)
                  .orderBy({"c0 DESC NULLS LAST"}, false)
                  .capturePlanNodeId(orderById)
                  .planNode();
  runTest(
      plan, orderById, "SELECT * FROM tmp ORDER BY c0 DESC NULLS LAST", {0});

  plan = PlanBuilder()
             .values(vectors)
             .orderBy({"c0 ASC NULLS FIRST"}, false)
             .capturePlanNodeId(orderById)
             .planNode();
  runTest(plan, orderById, "SELECT * FROM tmp ORDER BY c0 NULLS FIRST", {0});
}

TEST_F(OrderByTest, multipleKeys) {
  vector_size_t batchSize = 1000;
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 2; ++i) {
    // c0: half of rows are null, a quarter is 0 and remaining quarter is 1
    auto c0 = makeFlatVector<int64_t>(
        batchSize, [](vector_size_t row) { return row % 4; }, nullEvery(2, 1));
    auto c1 = makeFlatVector<int32_t>(
        batchSize, [](vector_size_t row) { return row; }, nullEvery(7));
    auto c2 = makeFlatVector<double>(
        batchSize, [](vector_size_t row) { return row * 0.1; }, nullEvery(11));
    vectors.push_back(makeRowVector({c0, c1, c2}));
  }
  createDuckDbTable(vectors);

  testTwoKeys(vectors, "c0", "c1");

  core::PlanNodeId orderById;
  auto plan = PlanBuilder()
                  .values(vectors)
                  .orderBy({"c0 ASC NULLS FIRST", "c1 ASC NULLS LAST"}, false)
                  .capturePlanNodeId(orderById)
                  .planNode();
  runTest(
      plan,
      orderById,
      "SELECT * FROM tmp ORDER BY c0 NULLS FIRST, c1 NULLS LAST",
      {0, 1});

  plan = PlanBuilder()
             .values(vectors)
             .orderBy({"c0 DESC NULLS LAST", "c1 DESC NULLS FIRST"}, false)
             .capturePlanNodeId(orderById)
             .planNode();
  runTest(
      plan,
      orderById,
      "SELECT * FROM tmp ORDER BY c0 DESC NULLS LAST, c1 DESC NULLS FIRST",
      {0, 1});
}

TEST_F(OrderByTest, multiBatchResult) {
  vector_size_t batchSize = 5000;
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 10; ++i) {
    auto c0 = makeFlatVector<int64_t>(
        batchSize,
        [&](vector_size_t row) { return batchSize * i + row; },
        nullEvery(5));
    auto c1 = makeFlatVector<double>(
        batchSize, [](vector_size_t row) { return row * 0.1; }, nullEvery(11));
    vectors.push_back(makeRowVector({c0, c1, c1, c1, c1, c1}));
  }
  createDuckDbTable(vectors);

  testSingleKey(vectors, "c0");
}

TEST_F(OrderByTest, varfields) {
  vector_size_t batchSize = 1000;
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 5; ++i) {
    auto c0 = makeFlatVector<int64_t>(
        batchSize,
        [&](vector_size_t row) { return batchSize * i + row; },
        nullEvery(5));
    auto c1 = makeFlatVector<double>(
        batchSize, [](vector_size_t row) { return row * 0.1; }, nullEvery(11));
    auto c2 = makeFlatVector<StringView>(
        batchSize,
        [](vector_size_t row) { return StringView(std::to_string(row)); },
        nullEvery(17));
    // TODO: Add support for array/map in createDuckDbTable and verify
    // that we can sort by array/map as well.
    vectors.push_back(makeRowVector({c0, c1, c2}));
  }
  createDuckDbTable(vectors);

  testSingleKey(vectors, "c2");
}

TEST_F(OrderByTest, unknown) {
  vector_size_t size = 1'000;
  auto vector = makeRowVector(
      {makeFlatVector<int64_t>(size, [](auto row) { return row % 7; }),
       BaseVector::createConstant(
           variant(TypeKind::UNKNOWN), size, pool_.get())});

  // Exclude "UNKNOWN" column as DuckDB doesn't understand UNKNOWN type
  createDuckDbTable(
      {makeRowVector({vector->childAt(0)}),
       makeRowVector({vector->childAt(0)})});

  core::PlanNodeId orderById;
  auto plan = PlanBuilder()
                  .values({vector, vector})
                  .orderBy({"c0 DESC NULLS LAST"}, false)
                  .capturePlanNodeId(orderById)
                  .planNode();
  runTest(
      plan,
      orderById,
      "SELECT *, null FROM tmp ORDER BY c0 DESC NULLS LAST",
      {0});
}

/// Verifies that Order By output batch sizes correspond to
/// preferredOutputBatchSize.
TEST_F(OrderByTest, outputBatchSize) {
  struct {
    int numRowsPerBatch;
    int preferredOutBatchSize;
    int expectedOutputVectors;

    std::string debugString() const {
      return fmt::format(
          "numRowsPerBatch:{}, preferredOutBatchSize:{}, expectedOutputVectors:{}",
          numRowsPerBatch,
          preferredOutBatchSize,
          expectedOutputVectors);
    }
  } testSettings[] = {{1024, 1024 * 1024 * 10, 1}, {1024, 1, 2}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    const vector_size_t batchSize = testData.numRowsPerBatch;
    std::vector<RowVectorPtr> rowVectors;
    auto c0 = makeFlatVector<int64_t>(
        batchSize, [&](vector_size_t row) { return row; }, nullEvery(5));
    auto c1 = makeFlatVector<double>(
        batchSize, [&](vector_size_t row) { return row; }, nullEvery(11));
    std::vector<VectorPtr> vectors;
    vectors.push_back(c0);
    for (int i = 0; i < 256; ++i) {
      vectors.push_back(c1);
    }
    rowVectors.push_back(makeRowVector(vectors));
    createDuckDbTable(rowVectors);

    core::PlanNodeId orderById;
    auto plan = PlanBuilder()
                    .values(rowVectors)
                    .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                    .capturePlanNodeId(orderById)
                    .planNode();
    auto queryCtx = core::QueryCtx::createForTest();
    queryCtx->setConfigOverridesUnsafe(
        {{core::QueryConfig::kPreferredOutputBatchSize,
          std::to_string(testData.preferredOutBatchSize)}});
    CursorParameters params;
    params.planNode = plan;
    params.queryCtx = queryCtx;
    auto task = assertQueryOrdered(
        params, "SELECT * FROM tmp ORDER BY c0 ASC NULLS LAST", {0});
    EXPECT_EQ(
        testData.expectedOutputVectors,
        toPlanStats(task->taskStats()).at(orderById).outputVectors);
  }
}

TEST_F(OrderByTest, spill) {
  const int kNumBatches = 3;
  const int kNumRows = 100'000;
  std::vector<RowVectorPtr> batches;
  for (int i = 0; i < kNumBatches; ++i) {
    batches.push_back(makeRowVector(
        {makeFlatVector<int64_t>(kNumRows, [](auto row) { return row * 3; }),
         makeFlatVector<StringView>(kNumRows, [](auto row) {
           return StringView(std::to_string(row * 3));
         })}));
  }
  createDuckDbTable(batches);

  auto plan = PlanBuilder()
                  .values(batches)
                  .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                  .planNode();
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto queryCtx = core::QueryCtx::createForTest();
  constexpr int64_t kMaxBytes = 20LL << 20; // 20 MB
  queryCtx->pool()->setMemoryUsageTracker(
      memory::MemoryUsageTracker::create(kMaxBytes, 0, kMaxBytes));
  // Set 'kSpillableReservationGrowthPct' to an extreme large value to trigger
  // disk spilling by failed memory growth reservation.
  queryCtx->setConfigOverridesUnsafe(
      {{core::QueryConfig::kSpillPath, spillDirectory->path},
       {core::QueryConfig::kSpillableReservationGrowthPct, "1000"}});
  CursorParameters params;
  params.planNode = plan;
  params.queryCtx = queryCtx;
  auto task = assertQueryOrdered(
      params, "SELECT * FROM tmp ORDER BY c0 ASC NULLS LAST", {0});
  auto stats = task->taskStats().pipelineStats;
  EXPECT_LT(0, stats[0].operatorStats[1].spilledRows);
  EXPECT_GT(kNumBatches * kNumRows, stats[0].operatorStats[1].spilledRows);
  EXPECT_LT(0, stats[0].operatorStats[1].spilledBytes);
  EXPECT_EQ(1, stats[0].operatorStats[1].spilledPartitions);
}
