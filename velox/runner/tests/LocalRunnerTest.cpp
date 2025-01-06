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

#include "velox/exec/tests/utils/DistributedPlanBuilder.h"
#include "velox/exec/tests/utils/LocalRunnerTestBase.h"
#include "velox/exec/tests/utils/QueryAssertions.h"

namespace facebook::velox::runner {
namespace {

using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

constexpr int kWaitTimeoutUs = 500'000;

class LocalRunnerTest : public LocalRunnerTestBase {
 protected:
  static constexpr int32_t kNumFiles = 5;
  static constexpr int32_t kNumVectors = 5;
  static constexpr int32_t kRowsPerVector = 10000;
  static constexpr int32_t kNumRows = kNumFiles * kNumVectors * kRowsPerVector;

  static void SetUpTestCase() {
    // The lambdas will be run after this scope returns, so make captures
    // static.
    static int32_t counter1;
    // Clear 'counter1' so that --gtest_repeat runs get the same data.
    counter1 = 0;
    auto customize1 = [&](const RowVectorPtr& rows) {
      makeAscending(rows, counter1);
    };

    static int32_t counter2;
    counter2 = kNumRows - 1;
    auto customize2 = [&](const RowVectorPtr& rows) {
      makeDescending(rows, counter2);
    };

    rowType_ = ROW({"c0"}, {BIGINT()});
    testTables_ = {
        TableSpec{
            .name = "T",
            .columns = rowType_,
            .rowsPerVector = kRowsPerVector,
            .numVectorsPerFile = kNumVectors,
            .numFiles = kNumFiles,
            .customizeData = customize1},
        TableSpec{
            .name = "U",
            .columns = rowType_,
            .rowsPerVector = kRowsPerVector,
            .numVectorsPerFile = kNumVectors,
            .numFiles = kNumFiles,
            .customizeData = customize2}};

    // Creates the data and schema from 'testTables_'. These are created on the
    // first test fixture initialization.
    LocalRunnerTestBase::SetUpTestCase();
  }

  std::shared_ptr<memory::MemoryPool> makeRootPool(const std::string& queryId) {
    static std::atomic_uint64_t poolId{0};
    return memory::memoryManager()->addRootPool(
        fmt::format("{}_{}", queryId, poolId++));
  }

  // Returns a plan with a table scan. This is a single stage if 'numWorkers' is
  // 1, otherwise this is a scan stage plus shuffle to a stage that gathers the
  // scan results.
  MultiFragmentPlanPtr makeScanPlan(const std::string& id, int32_t numWorkers) {
    MultiFragmentPlan::Options options = {
        .queryId = id, .numWorkers = numWorkers, .numDrivers = 2};

    DistributedPlanBuilder rootBuilder(options, idGenerator_, pool_.get());
    rootBuilder.tableScan("T", rowType_);
    if (numWorkers > 1) {
      rootBuilder.shufflePartitioned({}, 1, false);
    }
    return std::make_shared<MultiFragmentPlan>(
        rootBuilder.fragments(), std::move(options));
  }

  MultiFragmentPlanPtr makeJoinPlan(
      std::string project = "c0",
      bool broadcastBuild = false) {
    MultiFragmentPlan::Options options = {
        .queryId = "test.", .numWorkers = 4, .numDrivers = 2};
    const int32_t width = 3;

    DistributedPlanBuilder rootBuilder(options, idGenerator_, pool_.get());
    rootBuilder.tableScan("T", rowType_)
        .project({project})
        .shufflePartitioned({"c0"}, 3, false)
        .hashJoin(
            {"c0"},
            {"b0"},
            broadcastBuild
                ? DistributedPlanBuilder(rootBuilder)
                      .tableScan("U", rowType_)
                      .project({"c0 as b0"})
                      .shuffleBroadcastResult()
                : DistributedPlanBuilder(rootBuilder)
                      .tableScan("U", rowType_)
                      .project({"c0 as b0"})
                      .shufflePartitionedResult({"b0"}, width, false),
            "",
            {"c0", "b0"})
        .shufflePartitioned({}, 1, false)
        .finalAggregation({}, {"count(1)"}, {{BIGINT()}});
    return std::make_shared<MultiFragmentPlan>(
        rootBuilder.fragments(), std::move(options));
  }

  static void makeAscending(const RowVectorPtr& rows, int32_t& counter) {
    auto ints = rows->childAt(0)->as<FlatVector<int64_t>>();
    for (auto i = 0; i < ints->size(); ++i) {
      ints->set(i, counter + i);
    }
    counter += ints->size();
  }

  static void makeDescending(const RowVectorPtr& rows, int32_t& counter) {
    auto ints = rows->childAt(0)->as<FlatVector<int64_t>>();
    for (auto i = 0; i < ints->size(); ++i) {
      ints->set(i, counter - i);
    }
    counter -= ints->size();
  }

  void checkScanCount(const std::string& id, int32_t numWorkers) {
    auto scan = makeScanPlan(id, numWorkers);
    auto rootPool = makeRootPool(id);
    auto splitSourceFactory = makeSimpleSplitSourceFactory(scan);
    auto localRunner = std::make_shared<LocalRunner>(
        std::move(scan), makeQueryCtx(id, rootPool.get()), splitSourceFactory);
    auto results = readCursor(localRunner);

    int32_t count = 0;
    for (auto& rows : results) {
      count += rows->size();
    }
    localRunner->waitForCompletion(kWaitTimeoutUs);
    EXPECT_EQ(250'000, count);
  }

  std::shared_ptr<core::PlanNodeIdGenerator> idGenerator_{
      std::make_shared<core::PlanNodeIdGenerator>()};
  // The below are declared static to be scoped to TestCase so as to reuse the
  // dataset between tests.

  inline static RowTypePtr rowType_;
};

TEST_F(LocalRunnerTest, count) {
  auto join = makeJoinPlan();
  const std::string id = "q1";
  auto rootPool = makeRootPool(id);
  auto splitSourceFactory = makeSimpleSplitSourceFactory(join);
  auto localRunner = std::make_shared<LocalRunner>(
      std::move(join), makeQueryCtx(id, rootPool.get()), splitSourceFactory);
  auto results = readCursor(localRunner);
  auto stats = localRunner->stats();
  EXPECT_EQ(1, results.size());
  EXPECT_EQ(1, results[0]->size());
  EXPECT_EQ(
      kNumRows, results[0]->childAt(0)->as<FlatVector<int64_t>>()->valueAt(0));
  results.clear();
  EXPECT_EQ(Runner::State::kFinished, localRunner->state());
  localRunner->waitForCompletion(kWaitTimeoutUs);
}

TEST_F(LocalRunnerTest, error) {
  auto join = makeJoinPlan("if (c0 = 111, c0 / 0, c0 + 1) as c0");
  const std::string id = "q1";
  auto rootPool = makeRootPool(id);
  auto splitSourceFactory = makeSimpleSplitSourceFactory(join);
  auto localRunner = std::make_shared<LocalRunner>(
      std::move(join), makeQueryCtx(id, rootPool.get()), splitSourceFactory);
  EXPECT_THROW(readCursor(localRunner), VeloxUserError);
  EXPECT_EQ(Runner::State::kError, localRunner->state());
  localRunner->waitForCompletion(kWaitTimeoutUs);
}

TEST_F(LocalRunnerTest, scan) {
  checkScanCount("s1", 1);
  checkScanCount("s2", 3);
}

TEST_F(LocalRunnerTest, broadcast) {
  auto plan = makeJoinPlan("c0", true);
  const std::string id = "q1";
  auto rootPool = makeRootPool(id);
  auto splitSourceFactory = makeSimpleSplitSourceFactory(plan);
  auto localRunner = std::make_shared<LocalRunner>(
      std::move(plan), makeQueryCtx(id, rootPool.get()), splitSourceFactory);
  auto results = readCursor(localRunner);
  auto stats = localRunner->stats();
  EXPECT_EQ(1, results.size());
  EXPECT_EQ(1, results[0]->size());
  EXPECT_EQ(
      kNumRows, results[0]->childAt(0)->as<FlatVector<int64_t>>()->valueAt(0));
  results.clear();
  EXPECT_EQ(Runner::State::kFinished, localRunner->state());
  localRunner->waitForCompletion(kWaitTimeoutUs);
}

} // namespace
} // namespace facebook::velox::runner
