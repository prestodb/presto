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
#include <re2/re2.h>

#include <fmt/format.h>
#include "folly/experimental/EventCount.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/core/QueryConfig.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/Spiller.h"
#include "velox/exec/tests/utils/ArbitratorTestUtil.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::common::testutil;
using namespace facebook::velox::core;
using namespace facebook::velox::exec::test;

namespace facebook::velox::exec::test {
namespace {
// Returns aggregated spilled stats by 'task'.
common::SpillStats spilledStats(const exec::Task& task) {
  common::SpillStats spilledStats;
  auto stats = task.taskStats();
  for (auto& pipeline : stats.pipelineStats) {
    for (auto op : pipeline.operatorStats) {
      spilledStats.spilledInputBytes += op.spilledInputBytes;
      spilledStats.spilledBytes += op.spilledBytes;
      spilledStats.spilledRows += op.spilledRows;
      spilledStats.spilledPartitions += op.spilledPartitions;
      spilledStats.spilledFiles += op.spilledFiles;
    }
  }
  return spilledStats;
}

void abortPool(memory::MemoryPool* pool) {
  try {
    VELOX_FAIL("Manual MemoryPool Abortion");
  } catch (const VeloxException&) {
    pool->abort(std::current_exception());
  }
}
} // namespace

class OrderByTest : public OperatorTestBase {
 protected:
  void SetUp() override {
    filesystems::registerLocalFileSystem();
    if (!isRegisteredVectorSerde()) {
      this->registerVectorSerde();
    }
    rng_.seed(123);

    rowType_ = ROW(
        {{"c0", INTEGER()},
         {"c1", INTEGER()},
         {"c2", VARCHAR()},
         {"c3", VARCHAR()}});
    fuzzerOpts_.vectorSize = 1024;
    fuzzerOpts_.nullRatio = 0;
    fuzzerOpts_.stringVariableLength = false;
    fuzzerOpts_.stringLength = 1024;
    fuzzerOpts_.allowLazyVector = false;
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
      auto queryCtx = core::QueryCtx::create(executor_.get());
      TestScopedSpillInjection scopedSpillInjection(100);
      queryCtx->testingOverrideConfigUnsafe({
          {core::QueryConfig::kSpillEnabled, "true"},
          {core::QueryConfig::kOrderBySpillEnabled, "true"},
      });
      CursorParameters params;
      params.planNode = planNode;
      params.queryCtx = queryCtx;
      params.spillDirectory = spillDirectory->getPath();
      auto task = assertQueryOrdered(params, duckDbSql, sortingKeys);
      auto inputRows = toPlanStats(task->taskStats()).at(orderById).inputRows;
      const uint64_t peakSpillMemoryUsage =
          memory::spillMemoryPool()->stats().peakBytes;
      ASSERT_EQ(memory::spillMemoryPool()->stats().usedBytes, 0);
      if (inputRows > 0) {
        EXPECT_LT(0, spilledStats(*task).spilledInputBytes);
        EXPECT_LT(0, spilledStats(*task).spilledBytes);
        EXPECT_EQ(1, spilledStats(*task).spilledPartitions);
        EXPECT_LT(0, spilledStats(*task).spilledFiles);
        EXPECT_EQ(inputRows, spilledStats(*task).spilledRows);
        ASSERT_EQ(memory::spillMemoryPool()->stats().usedBytes, 0);
        if (memory::spillMemoryPool()->trackUsage()) {
          ASSERT_GT(memory::spillMemoryPool()->stats().peakBytes, 0);
          ASSERT_GE(
              memory::spillMemoryPool()->stats().peakBytes,
              peakSpillMemoryUsage);
        }
      } else {
        EXPECT_EQ(0, spilledStats(*task).spilledInputBytes);
        EXPECT_EQ(0, spilledStats(*task).spilledBytes);
      }
      OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
    }
  }

  std::vector<RowVectorPtr> makeVectors(
      const RowTypePtr& rowType,
      int32_t numVectors,
      int32_t rowsPerVector) {
    std::vector<RowVectorPtr> vectors;
    for (int32_t i = 0; i < numVectors; ++i) {
      auto vector = std::dynamic_pointer_cast<RowVector>(
          velox::test::BatchMaker::createBatch(rowType, rowsPerVector, *pool_));
      vectors.push_back(vector);
    }
    return vectors;
  }

  folly::Random::DefaultGenerator rng_;
  memory::MemoryReclaimer::Stats reclaimerStats_;
  RowTypePtr rowType_;
  VectorFuzzer::Options fuzzerOpts_;
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
        [](vector_size_t row) {
          return StringView::makeInline(std::to_string(row));
        },
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
  auto vector = makeRowVector({
      makeFlatVector<int64_t>(size, [](auto row) { return row % 7; }),
      BaseVector::createNullConstant(UNKNOWN(), size, pool()),
  });

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

/// Verifies output batch rows of OrderBy
TEST_F(OrderByTest, outputBatchRows) {
  struct {
    int numRowsPerBatch;
    int preferredOutBatchBytes;
    int maxOutBatchRows;
    int expectedOutputVectors;

    // TODO: add output size check with spilling enabled
    std::string debugString() const {
      return fmt::format(
          "numRowsPerBatch:{}, preferredOutBatchBytes:{}, maxOutBatchRows:{}, expectedOutputVectors:{}",
          numRowsPerBatch,
          preferredOutBatchBytes,
          maxOutBatchRows,
          expectedOutputVectors);
    }
  } testSettings[] = {
      {1024, 1, 100, 1024},
      // estimated size per row is ~2092, set preferredOutBatchBytes to 20920,
      // so each batch has 10 rows, so it would return 100 batches
      {1000, 20920, 100, 100},
      // same as above, but maxOutBatchRows is 1, so it would return 1000
      // batches
      {1000, 20920, 1, 1000}};

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
    auto queryCtx = core::QueryCtx::create(executor_.get());
    queryCtx->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kPreferredOutputBatchBytes,
          std::to_string(testData.preferredOutBatchBytes)},
         {core::QueryConfig::kMaxOutputBatchRows,
          std::to_string(testData.maxOutBatchRows)}});
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
  const auto rowType =
      ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), VARCHAR()});
  const auto vectors = createVectors(rowType, 1024, 48 << 20);
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId orderNodeId;
  const auto plan =
      PlanBuilder(planNodeIdGenerator)
          .values(vectors)
          .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
          .capturePlanNodeId(orderNodeId)
          .planNode();

  const auto expectedResult = AssertQueryBuilder(plan).copyResults(pool_.get());

  auto spillDirectory = exec::test::TempDirectoryPath::create();
  TestScopedSpillInjection scopedSpillInjection(100);
  auto task = AssertQueryBuilder(plan)
                  .spillDirectory(spillDirectory->getPath())
                  .config(core::QueryConfig::kSpillEnabled, true)
                  .config(core::QueryConfig::kOrderBySpillEnabled, true)
                  .assertResults(expectedResult);
  auto taskStats = exec::toPlanStats(task->taskStats());
  auto& planStats = taskStats.at(orderNodeId);
  ASSERT_GT(planStats.spilledBytes, 0);
  ASSERT_GT(planStats.spilledRows, 0);
  ASSERT_GT(planStats.spilledBytes, 0);
  ASSERT_GT(planStats.spilledInputBytes, 0);
  ASSERT_EQ(planStats.spilledPartitions, 1);
  ASSERT_GT(planStats.spilledFiles, 0);
  ASSERT_GT(planStats.customStats[Operator::kSpillRuns].count, 0);
  ASSERT_GT(planStats.customStats[Operator::kSpillFillTime].sum, 0);
  ASSERT_GT(planStats.customStats[Operator::kSpillSortTime].sum, 0);
  ASSERT_GT(planStats.customStats[Operator::kSpillSerializationTime].sum, 0);
  ASSERT_GT(planStats.customStats[Operator::kSpillFlushTime].sum, 0);
  ASSERT_EQ(
      planStats.customStats[Operator::kSpillSerializationTime].count,
      planStats.customStats[Operator::kSpillFlushTime].count);
  ASSERT_GT(planStats.customStats[Operator::kSpillWrites].sum, 0);
  ASSERT_GT(planStats.customStats[Operator::kSpillWriteTime].sum, 0);
  ASSERT_EQ(
      planStats.customStats[Operator::kSpillWrites].count,
      planStats.customStats[Operator::kSpillWriteTime].count);
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
}

DEBUG_ONLY_TEST_F(OrderByTest, reclaimDuringInputProcessing) {
  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  auto rowType = ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), INTEGER()});
  VectorFuzzer fuzzer({.vectorSize = 1000}, pool());
  const int32_t numBatches = 10;
  std::vector<RowVectorPtr> batches;
  for (int32_t i = 0; i < numBatches; ++i) {
    batches.push_back(fuzzer.fuzzRow(rowType));
  }

  struct {
    // 0: trigger reclaim with some input processed.
    // 1: trigger reclaim after all the inputs processed.
    int triggerCondition;
    bool spillEnabled;
    bool expectedReclaimable;

    std::string debugString() const {
      return fmt::format(
          "triggerCondition {}, spillEnabled {}, expectedReclaimable {}",
          triggerCondition,
          spillEnabled,
          expectedReclaimable);
    }
  } testSettings[] = {
      {0, true, true}, {1, true, true}, {0, false, false}, {1, false, false}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    auto spillDirectory = exec::test::TempDirectoryPath::create();
    auto queryCtx = core::QueryCtx::create(executor_.get());
    queryCtx->testingOverrideMemoryPool(memory::memoryManager()->addRootPool(
        queryCtx->queryId(), kMaxBytes, memory::MemoryReclaimer::create()));
    auto expectedResult =
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                .planNode())
            .queryCtx(queryCtx)
            .copyResults(pool_.get());

    folly::EventCount driverWait;
    auto driverWaitKey = driverWait.prepareWait();
    folly::EventCount testWait;
    auto testWaitKey = testWait.prepareWait();

    std::atomic<int> numInputs{0};
    Operator* op;
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::addInput",
        std::function<void(Operator*)>(([&](Operator* testOp) {
          if (testOp->operatorType() != "OrderBy") {
            ASSERT_FALSE(testOp->canReclaim());
            return;
          }
          op = testOp;
          ++numInputs;
          if (testData.triggerCondition == 0) {
            if (numInputs != 2) {
              return;
            }
          }
          if (testData.triggerCondition == 1) {
            if (numInputs != numBatches) {
              return;
            }
          }
          ASSERT_EQ(op->canReclaim(), testData.expectedReclaimable);
          uint64_t reclaimableBytes{0};
          const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
          ASSERT_EQ(reclaimable, testData.expectedReclaimable);
          if (testData.expectedReclaimable) {
            ASSERT_GT(reclaimableBytes, 0);
          } else {
            ASSERT_EQ(reclaimableBytes, 0);
          }
          testWait.notify();
          driverWait.wait(driverWaitKey);
        })));

    std::thread taskThread([&]() {
      if (testData.spillEnabled) {
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                .planNode())
            .queryCtx(queryCtx)
            .spillDirectory(spillDirectory->getPath())
            .config(core::QueryConfig::kSpillEnabled, true)
            .config(core::QueryConfig::kOrderBySpillEnabled, true)
            .maxDrivers(1)
            .assertResults(expectedResult);
      } else {
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                .planNode())
            .queryCtx(queryCtx)
            .maxDrivers(1)
            .assertResults(expectedResult);
      }
    });

    testWait.wait(testWaitKey);
    ASSERT_TRUE(op != nullptr);
    auto task = op->testingOperatorCtx()->task();
    auto taskPauseWait = task->requestPause();
    driverWait.notify();
    taskPauseWait.wait();

    uint64_t reclaimableBytes{0};
    const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
    ASSERT_EQ(op->canReclaim(), testData.expectedReclaimable);
    ASSERT_EQ(reclaimable, testData.expectedReclaimable);
    if (testData.expectedReclaimable) {
      ASSERT_GT(reclaimableBytes, 0);
    } else {
      ASSERT_EQ(reclaimableBytes, 0);
    }

    if (testData.expectedReclaimable) {
      op->pool()->reclaim(
          folly::Random::oneIn(2) ? 0 : folly::Random::rand32(rng_),
          0,
          reclaimerStats_);
      ASSERT_GT(reclaimerStats_.reclaimedBytes, 0);
      ASSERT_GT(reclaimerStats_.reclaimExecTimeUs, 0);
      reclaimerStats_.reset();
      ASSERT_EQ(op->pool()->usedBytes(), 0);
    } else {
      VELOX_ASSERT_THROW(
          op->reclaim(
              folly::Random::oneIn(2) ? 0 : folly::Random::rand32(rng_),
              reclaimerStats_),
          "");
    }

    Task::resume(task);

    taskThread.join();

    auto stats = task->taskStats().pipelineStats;
    if (testData.expectedReclaimable) {
      ASSERT_GT(stats[0].operatorStats[1].spilledBytes, 0);
      ASSERT_EQ(stats[0].operatorStats[1].spilledPartitions, 1);
    } else {
      ASSERT_EQ(stats[0].operatorStats[1].spilledBytes, 0);
      ASSERT_EQ(stats[0].operatorStats[1].spilledPartitions, 0);
    }
    OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
  }
  ASSERT_EQ(reclaimerStats_, memory::MemoryReclaimer::Stats{});
}

DEBUG_ONLY_TEST_F(OrderByTest, reclaimDuringReserve) {
  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  auto rowType = ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), INTEGER()});
  const int32_t numBatches = 10;
  std::vector<RowVectorPtr> batches;
  for (int32_t i = 0; i < numBatches; ++i) {
    const size_t size = i == 0 ? 100 : 40000;
    VectorFuzzer fuzzer({.vectorSize = size}, pool());
    batches.push_back(fuzzer.fuzzRow(rowType));
  }

  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto queryCtx = core::QueryCtx::create(executor_.get());
  queryCtx->testingOverrideMemoryPool(memory::memoryManager()->addRootPool(
      queryCtx->queryId(), kMaxBytes, memory::MemoryReclaimer::create()));
  auto expectedResult =
      AssertQueryBuilder(
          PlanBuilder()
              .values(batches)
              .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
              .planNode())
          .queryCtx(queryCtx)
          .copyResults(pool_.get());

  folly::EventCount driverWait;
  auto driverWaitKey = driverWait.prepareWait();
  folly::EventCount testWait;
  auto testWaitKey = testWait.prepareWait();

  Operator* op;
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal::addInput",
      std::function<void(Operator*)>(([&](Operator* testOp) {
        if (testOp->operatorType() != "OrderBy") {
          ASSERT_FALSE(testOp->canReclaim());
          return;
        }
        op = testOp;
      })));

  std::atomic<bool> injectOnce{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::common::memory::MemoryPoolImpl::maybeReserve",
      std::function<void(memory::MemoryPoolImpl*)>(
          ([&](memory::MemoryPoolImpl* pool) {
            ASSERT_TRUE(op != nullptr);
            const std::string re(".*OrderBy");
            if (!RE2::FullMatch(pool->name(), re)) {
              return;
            }
            if (!injectOnce.exchange(false)) {
              return;
            }
            ASSERT_TRUE(op->canReclaim());
            uint64_t reclaimableBytes{0};
            const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
            ASSERT_TRUE(reclaimable);
            ASSERT_GT(reclaimableBytes, 0);
            auto* driver = op->testingOperatorCtx()->driver();
            SuspendedSection suspendedSection(driver);
            testWait.notify();
            driverWait.wait(driverWaitKey);
          })));

  std::thread taskThread([&]() {
    AssertQueryBuilder(
        PlanBuilder()
            .values(batches)
            .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
            .planNode())
        .queryCtx(queryCtx)
        .spillDirectory(spillDirectory->getPath())
        .config(core::QueryConfig::kSpillEnabled, true)
        .config(core::QueryConfig::kOrderBySpillEnabled, true)
        .maxDrivers(1)
        .assertResults(expectedResult);
  });

  testWait.wait(testWaitKey);
  ASSERT_TRUE(op != nullptr);
  auto task = op->testingOperatorCtx()->task();
  auto taskPauseWait = task->requestPause();
  taskPauseWait.wait();

  uint64_t reclaimableBytes{0};
  const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
  ASSERT_TRUE(op->canReclaim());
  ASSERT_TRUE(reclaimable);
  ASSERT_GT(reclaimableBytes, 0);

  op->pool()->reclaim(
      folly::Random::oneIn(2) ? 0 : folly::Random::rand32(rng_),
      0,
      reclaimerStats_);
  ASSERT_GT(reclaimerStats_.reclaimedBytes, 0);
  ASSERT_GT(reclaimerStats_.reclaimExecTimeUs, 0);
  reclaimerStats_.reset();
  ASSERT_EQ(op->pool()->usedBytes(), 0);

  driverWait.notify();
  Task::resume(task);

  taskThread.join();

  auto stats = task->taskStats().pipelineStats;
  ASSERT_GT(stats[0].operatorStats[1].spilledBytes, 0);
  ASSERT_EQ(stats[0].operatorStats[1].spilledPartitions, 1);
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
  ASSERT_EQ(reclaimerStats_, memory::MemoryReclaimer::Stats{});
}

DEBUG_ONLY_TEST_F(OrderByTest, reclaimDuringAllocation) {
  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  auto rowType = ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), INTEGER()});
  VectorFuzzer fuzzer({.vectorSize = 1000}, pool());
  const int32_t numBatches = 10;
  std::vector<RowVectorPtr> batches;
  for (int32_t i = 0; i < numBatches; ++i) {
    batches.push_back(fuzzer.fuzzRow(rowType));
  }

  const std::vector<bool> enableSpillings = {false, true};
  for (const auto enableSpilling : enableSpillings) {
    SCOPED_TRACE(fmt::format("enableSpilling {}", enableSpilling));
    auto spillDirectory = exec::test::TempDirectoryPath::create();
    auto queryCtx = core::QueryCtx::create(executor_.get());
    queryCtx->testingOverrideMemoryPool(
        memory::memoryManager()->addRootPool(queryCtx->queryId(), kMaxBytes));
    auto expectedResult =
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                .planNode())
            .queryCtx(queryCtx)
            .copyResults(pool_.get());

    folly::EventCount driverWait;
    auto driverWaitKey = driverWait.prepareWait();
    folly::EventCount testWait;
    auto testWaitKey = testWait.prepareWait();

    Operator* op;
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::addInput",
        std::function<void(Operator*)>(([&](Operator* testOp) {
          if (testOp->operatorType() != "OrderBy") {
            ASSERT_FALSE(testOp->canReclaim());
            return;
          }
          op = testOp;
        })));

    std::atomic<bool> injectOnce{true};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::common::memory::MemoryPoolImpl::allocateNonContiguous",
        std::function<void(memory::MemoryPoolImpl*)>(
            ([&](memory::MemoryPoolImpl* pool) {
              ASSERT_TRUE(op != nullptr);
              const std::string re(".*OrderBy");
              if (!RE2::FullMatch(pool->name(), re)) {
                return;
              }
              if (!injectOnce.exchange(false)) {
                return;
              }
              ASSERT_EQ(op->canReclaim(), enableSpilling);
              uint64_t reclaimableBytes{0};
              const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
              ASSERT_EQ(reclaimable, enableSpilling);
              if (enableSpilling) {
                ASSERT_GE(reclaimableBytes, 0);
              } else {
                ASSERT_EQ(reclaimableBytes, 0);
              }
              auto* driver = op->testingOperatorCtx()->driver();
              SuspendedSection suspendedSection(driver);
              testWait.notify();
              driverWait.wait(driverWaitKey);
            })));

    std::thread taskThread([&]() {
      if (enableSpilling) {
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                .planNode())
            .queryCtx(queryCtx)
            .spillDirectory(spillDirectory->getPath())
            .config(core::QueryConfig::kSpillEnabled, true)
            .config(core::QueryConfig::kOrderBySpillEnabled, true)
            .maxDrivers(1)
            .assertResults(expectedResult);
      } else {
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                .planNode())
            .queryCtx(queryCtx)
            .maxDrivers(1)
            .assertResults(expectedResult);
      }
    });

    testWait.wait(testWaitKey);
    ASSERT_TRUE(op != nullptr);
    auto task = op->testingOperatorCtx()->task();
    auto taskPauseWait = task->requestPause();
    taskPauseWait.wait();

    uint64_t reclaimableBytes{0};
    const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
    ASSERT_EQ(op->canReclaim(), enableSpilling);
    ASSERT_EQ(reclaimable, enableSpilling);
    if (enableSpilling) {
      ASSERT_GE(reclaimableBytes, 0);
    } else {
      ASSERT_EQ(reclaimableBytes, 0);
    }

    VELOX_ASSERT_THROW(
        op->reclaim(
            folly::Random::oneIn(2) ? 0 : folly::Random::rand32(rng_),
            reclaimerStats_),
        "");

    driverWait.notify();
    Task::resume(task);

    taskThread.join();

    auto stats = task->taskStats().pipelineStats;
    ASSERT_EQ(stats[0].operatorStats[1].spilledBytes, 0);
    ASSERT_EQ(stats[0].operatorStats[1].spilledPartitions, 0);
    OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
  }
  ASSERT_EQ(reclaimerStats_, memory::MemoryReclaimer::Stats{0});
}

DEBUG_ONLY_TEST_F(OrderByTest, reclaimDuringOutputProcessing) {
  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  auto rowType = ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), INTEGER()});
  VectorFuzzer fuzzer({.vectorSize = 1000}, pool());
  const int32_t numBatches = 200;
  std::vector<RowVectorPtr> batches;
  for (int32_t i = 0; i < numBatches; ++i) {
    batches.push_back(fuzzer.fuzzRow(rowType));
  }

  const std::vector<bool> enableSpillings = {false, true};
  for (const auto enableSpilling : enableSpillings) {
    SCOPED_TRACE(fmt::format("enableSpilling {}", enableSpilling));
    auto spillDirectory = exec::test::TempDirectoryPath::create();
    auto queryCtx = core::QueryCtx::create(executor_.get());
    queryCtx->testingOverrideMemoryPool(memory::memoryManager()->addRootPool(
        queryCtx->queryId(), kMaxBytes, memory::MemoryReclaimer::create()));
    auto expectedResult =
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                .planNode())
            .queryCtx(queryCtx)
            .copyResults(pool_.get());

    folly::EventCount driverWait;
    auto driverWaitKey = driverWait.prepareWait();
    folly::EventCount testWait;
    auto testWaitKey = testWait.prepareWait();

    std::atomic<bool> injectOnce{true};
    Operator* op;
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::noMoreInput",
        std::function<void(Operator*)>(([&](Operator* testOp) {
          if (testOp->operatorType() != "OrderBy") {
            ASSERT_FALSE(testOp->canReclaim());
            return;
          }
          op = testOp;
          if (!injectOnce.exchange(false)) {
            return;
          }
          ASSERT_EQ(op->canReclaim(), enableSpilling);
          uint64_t reclaimableBytes{0};
          const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
          ASSERT_EQ(reclaimable, enableSpilling);
          if (enableSpilling) {
            ASSERT_GT(reclaimableBytes, 0);
          } else {
            ASSERT_EQ(reclaimableBytes, 0);
          }
          testWait.notify();
          driverWait.wait(driverWaitKey);
        })));

    std::thread taskThread([&]() {
      if (enableSpilling) {
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                .planNode())
            .queryCtx(queryCtx)
            .spillDirectory(spillDirectory->getPath())
            .config(core::QueryConfig::kSpillEnabled, true)
            .config(core::QueryConfig::kOrderBySpillEnabled, true)
            .maxDrivers(1)
            .assertResults(expectedResult);
      } else {
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                .planNode())
            .queryCtx(queryCtx)
            .maxDrivers(1)
            .assertResults(expectedResult);
      }
    });

    testWait.wait(testWaitKey);
    ASSERT_TRUE(op != nullptr);
    auto task = op->testingOperatorCtx()->task();
    auto taskPauseWait = task->requestPause();
    driverWait.notify();
    taskPauseWait.wait();

    uint64_t reclaimableBytes{0};
    const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
    ASSERT_EQ(op->canReclaim(), enableSpilling);
    ASSERT_EQ(reclaimable, enableSpilling);

    if (enableSpilling) {
      ASSERT_GT(reclaimableBytes, 0);
      reclaimerStats_.reset();
      op->pool()->reclaim(reclaimableBytes, 0, reclaimerStats_);
      ASSERT_GT(reclaimerStats_.reclaimedBytes, 0);
      ASSERT_LE(reclaimerStats_.reclaimedBytes, reclaimableBytes);
      ASSERT_GT(reclaimerStats_.reclaimExecTimeUs, 0);
    } else {
      ASSERT_EQ(reclaimableBytes, 0);
      VELOX_ASSERT_THROW(
          op->reclaim(
              folly::Random::oneIn(2) ? 0 : folly::Random::rand32(rng_),
              reclaimerStats_),
          "");
    }

    Task::resume(task);
    taskThread.join();

    auto stats = task->taskStats().pipelineStats;
    ASSERT_TRUE(!enableSpilling || stats[0].operatorStats[1].spilledBytes > 0);
    ASSERT_TRUE(
        !enableSpilling || stats[0].operatorStats[1].spilledPartitions > 0);
    OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
  }
  ASSERT_EQ(reclaimerStats_.numNonReclaimableAttempts, 0);
}

DEBUG_ONLY_TEST_F(OrderByTest, abortDuringOutputProcessing) {
  auto rowType = ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), INTEGER()});
  const auto batches = makeVectors(rowType, 10, 128);

  struct {
    bool abortFromRootMemoryPool;
    int numDrivers;

    std::string debugString() const {
      return fmt::format(
          "abortFromRootMemoryPool {} numDrivers {}",
          abortFromRootMemoryPool,
          numDrivers);
    }
  } testSettings[] = {{true, 1}, {false, 1}, {true, 4}, {false, 4}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto expectedResult =
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                .planNode())
            .copyResults(pool_.get());

    std::atomic_bool injectOnce{true};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::noMoreInput",
        std::function<void(Operator*)>(([&](Operator* op) {
          if (op->operatorType() != "OrderBy") {
            return;
          }
          if (!injectOnce.exchange(false)) {
            return;
          }
          ASSERT_GT(op->pool()->usedBytes(), 0);
          auto* driver = op->testingOperatorCtx()->driver();
          ASSERT_EQ(
              driver->task()->enterSuspended(driver->state()),
              StopReason::kNone);
          testData.abortFromRootMemoryPool ? abortPool(op->pool()->root())
                                           : abortPool(op->pool());
          // We can't directly reclaim memory from this hash build operator as
          // its driver thread is running and in suspension state.
          ASSERT_GT(op->pool()->root()->usedBytes(), 0);
          ASSERT_EQ(
              driver->task()->leaveSuspended(driver->state()),
              StopReason::kAlreadyTerminated);
          ASSERT_TRUE(op->pool()->aborted());
          ASSERT_TRUE(op->pool()->root()->aborted());
          VELOX_MEM_POOL_ABORTED("Memory pool aborted");
        })));

    VELOX_ASSERT_THROW(
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                .planNode())
            .maxDrivers(1)
            .assertResults(expectedResult),
        "Manual MemoryPool Abortion");
    waitForAllTasksToBeDeleted();
  }
}

DEBUG_ONLY_TEST_F(OrderByTest, abortDuringInputgProcessing) {
  auto rowType = ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), INTEGER()});
  const auto batches = makeVectors(rowType, 10, 128);

  struct {
    bool abortFromRootMemoryPool;
    int numDrivers;

    std::string debugString() const {
      return fmt::format(
          "abortFromRootMemoryPool {} numDrivers {}",
          abortFromRootMemoryPool,
          numDrivers);
    }
  } testSettings[] = {{true, 1}, {false, 1}, {true, 4}, {false, 4}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto expectedResult =
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                .planNode())
            .copyResults(pool_.get());

    std::atomic_int numInputs{0};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::addInput",
        std::function<void(Operator*)>(([&](Operator* op) {
          if (op->operatorType() != "OrderBy") {
            return;
          }
          if (++numInputs != 2) {
            return;
          }
          auto* driver = op->testingOperatorCtx()->driver();
          ASSERT_EQ(
              driver->task()->enterSuspended(driver->state()),
              StopReason::kNone);
          testData.abortFromRootMemoryPool ? abortPool(op->pool()->root())
                                           : abortPool(op->pool());
          // We can't directly reclaim memory from this hash build operator as
          // its driver thread is running and in suspension state.
          ASSERT_GT(op->pool()->root()->usedBytes(), 0);
          ASSERT_EQ(
              driver->task()->leaveSuspended(driver->state()),
              StopReason::kAlreadyTerminated);
          ASSERT_TRUE(op->pool()->aborted());
          ASSERT_TRUE(op->pool()->root()->aborted());
          // Simulate the memory abort by memory arbitrator.
          VELOX_MEM_POOL_ABORTED("Memory pool aborted");
        })));

    VELOX_ASSERT_THROW(
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
                .planNode())
            .maxDrivers(1)
            .assertResults(expectedResult),
        "Manual MemoryPool Abortion");
    waitForAllTasksToBeDeleted();
  }
}

DEBUG_ONLY_TEST_F(OrderByTest, spillWithNoMoreOutput) {
  const auto rowType =
      ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), VARCHAR()});
  const auto vectors = createVectors(rowType, 1024, 4 << 20);
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId orderNodeId;
  const auto plan =
      PlanBuilder(planNodeIdGenerator)
          .values(vectors)
          .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
          .capturePlanNodeId(orderNodeId)
          .planNode();

  const auto expectedResult = AssertQueryBuilder(plan).copyResults(pool_.get());

  std::atomic_int numOutputs{0};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal::getOutput",
      std::function<void(Operator*)>(([&](Operator* op) {
        if (op->operatorType() != "OrderBy") {
          return;
        }
        if (!op->testingNoMoreInput()) {
          return;
        }
        if (++numOutputs != 2) {
          return;
        }
        ASSERT_TRUE(!op->isFinished());
        op->reclaim(1'000'000'000, reclaimerStats_);
        ASSERT_EQ(reclaimerStats_.reclaimedBytes, 0);
      })));

  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto task =
      AssertQueryBuilder(plan)
          .spillDirectory(spillDirectory->getPath())
          .config(core::QueryConfig::kSpillEnabled, true)
          .config(core::QueryConfig::kOrderBySpillEnabled, true)
          // Set output buffer size to extreme large to read all the
          // output rows in one vector.
          .config(QueryConfig::kPreferredOutputBatchRows, 1'000'000'000)
          .config(QueryConfig::kMaxOutputBatchRows, 1'000'000'000)
          .config(QueryConfig::kPreferredOutputBatchBytes, 1'000'000'000)
          .config(QueryConfig::kMaxSpillBytes, 1)
          .maxDrivers(1)
          .assertResults(expectedResult);
  auto taskStats = exec::toPlanStats(task->taskStats());
  auto& planStats = taskStats.at(orderNodeId);
  ASSERT_EQ(planStats.spilledBytes, 0);
  ASSERT_EQ(planStats.spilledRows, 0);
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
}

TEST_F(OrderByTest, maxSpillBytes) {
  const auto rowType =
      ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), VARCHAR()});
  const auto vectors = createVectors(rowType, 1024, 15 << 20);
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId orderNodeId;
  const auto plan =
      PlanBuilder(planNodeIdGenerator)
          .values(vectors)
          .orderBy({fmt::format("{} ASC NULLS LAST", "c0")}, false)
          .capturePlanNodeId(orderNodeId)
          .planNode();
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto queryCtx = core::QueryCtx::create(executor_.get());

  struct {
    int32_t maxSpilledBytes;
    bool expectedExceedLimit;
    std::string debugString() const {
      return fmt::format("maxSpilledBytes {}", maxSpilledBytes);
    }
  } testSettings[] = {{1 << 30, false}, {16 << 20, true}, {0, false}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    try {
      TestScopedSpillInjection scopedSpillInjection(100);
      AssertQueryBuilder(plan)
          .spillDirectory(spillDirectory->getPath())
          .queryCtx(queryCtx)
          .config(core::QueryConfig::kSpillEnabled, true)
          .config(core::QueryConfig::kOrderBySpillEnabled, true)
          .config(QueryConfig::kMaxSpillBytes, testData.maxSpilledBytes)
          .copyResults(pool_.get());
      ASSERT_FALSE(testData.expectedExceedLimit);
    } catch (const VeloxRuntimeError& e) {
      ASSERT_TRUE(testData.expectedExceedLimit);
      ASSERT_NE(
          e.message().find(
              "Query exceeded per-query local spill limit of 16.00MB"),
          std::string::npos);
      ASSERT_EQ(
          e.errorCode(), facebook::velox::error_code::kSpillLimitExceeded);
    }
  }
}

DEBUG_ONLY_TEST_F(OrderByTest, reclaimFromOrderBy) {
  std::vector<RowVectorPtr> vectors = createVectors(8, rowType_, fuzzerOpts_);
  createDuckDbTable(vectors);
  std::atomic_int numInputs{0};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal::addInput",
      std::function<void(Operator*)>(([&](Operator* op) {
        if (op->operatorType() != "OrderBy") {
          return;
        }
        if (++numInputs != 5) {
          return;
        }
        auto* driver = op->testingOperatorCtx()->driver();
        SuspendedSection suspendedSection(driver);
        memory::testingRunArbitration();
      })));

  const auto spillDirectory = exec::test::TempDirectoryPath::create();
  core::PlanNodeId orderById;
  auto task =
      AssertQueryBuilder(duckDbQueryRunner_)
          .spillDirectory(spillDirectory->getPath())
          .config(core::QueryConfig::kSpillEnabled, true)
          .config(core::QueryConfig::kOrderBySpillEnabled, true)
          .plan(PlanBuilder()
                    .values(vectors)
                    .orderBy({"c0 ASC NULLS LAST"}, false)
                    .capturePlanNodeId(orderById)
                    .planNode())
          .assertResults("SELECT * FROM tmp ORDER BY c0 ASC NULLS LAST");
  auto taskStats = exec::toPlanStats(task->taskStats());
  auto& planStats = taskStats.at(orderById);
  ASSERT_GT(planStats.spilledBytes, 0);
  task.reset();
  waitForAllTasksToBeDeleted();
}

DEBUG_ONLY_TEST_F(OrderByTest, reclaimFromEmptyOrderBy) {
  const std::vector<RowVectorPtr> vectors =
      createVectors(8, rowType_, fuzzerOpts_);
  createDuckDbTable(vectors);

  std::atomic<bool> injectOnce{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal::addInput",
      std::function<void(Operator*)>(([&](Operator* op) {
        if (op->operatorType() != "OrderBy") {
          return;
        }

        if (!injectOnce.exchange(false)) {
          return;
        }
        testingRunArbitration(op->pool());
      })));

  const auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto task =
      AssertQueryBuilder(duckDbQueryRunner_)
          .spillDirectory(spillDirectory->getPath())
          .config(core::QueryConfig::kSpillEnabled, true)
          .config(core::QueryConfig::kOrderBySpillEnabled, true)
          .plan(PlanBuilder()
                    .values(vectors)
                    .orderBy({"c0 ASC NULLS LAST"}, false)
                    .planNode())
          .assertResults("SELECT * FROM tmp ORDER BY c0 ASC NULLS LAST");
  // Verify no spill has been triggered.
  const auto stats = task->taskStats().pipelineStats;
  ASSERT_EQ(stats[0].operatorStats[1].spilledBytes, 0);
  ASSERT_EQ(stats[0].operatorStats[1].spilledPartitions, 0);
}
} // namespace facebook::velox::exec::test
