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
#include "folly/experimental/EventCount.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/common/DataSink.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/PartitionedOutputBufferManager.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::common::testutil;
using namespace facebook::velox::memory;

using facebook::velox::test::BatchMaker;

class MultiFragmentTest : public HiveConnectorTestBase {
 protected:
  static std::string makeTaskId(const std::string& prefix, int num) {
    return fmt::format("local://{}-{}", prefix, num);
  }

  std::shared_ptr<Task> makeTask(
      const std::string& taskId,
      std::shared_ptr<const core::PlanNode> planNode,
      int destination,
      Consumer consumer = nullptr,
      int64_t maxMemory = kMaxMemory) {
    auto queryCtx = std::make_shared<core::QueryCtx>(
        executor_.get(), std::make_shared<core::MemConfig>(configSettings_));
    queryCtx->testingOverrideMemoryPool(
        memory::defaultMemoryManager().addRootPool(
            queryCtx->queryId(), maxMemory));
    core::PlanFragment planFragment{planNode};
    return std::make_shared<Task>(
        taskId,
        std::move(planFragment),
        destination,
        std::move(queryCtx),
        std::move(consumer));
  }

  std::vector<RowVectorPtr> makeVectors(int count, int rowsPerVector) {
    std::vector<RowVectorPtr> vectors;
    for (int i = 0; i < count; ++i) {
      auto vector = std::dynamic_pointer_cast<RowVector>(
          BatchMaker::createBatch(rowType_, rowsPerVector, *pool_));
      vectors.push_back(vector);
    }
    return vectors;
  }

  void addHiveSplits(
      std::shared_ptr<Task> task,
      const std::vector<std::shared_ptr<TempFilePath>>& filePaths) {
    for (auto& filePath : filePaths) {
      auto split = exec::Split(
          std::make_shared<HiveConnectorSplit>(
              kHiveConnectorId,
              "file:" + filePath->path,
              facebook::velox::dwio::common::FileFormat::DWRF),
          -1);
      task->addSplit("0", std::move(split));
      VLOG(1) << filePath->path << "\n";
    }
    task->noMoreSplits("0");
  }

  void addRemoteSplits(
      std::shared_ptr<Task> task,
      const std::vector<std::string>& remoteTaskIds) {
    for (auto& taskId : remoteTaskIds) {
      auto split =
          exec::Split(std::make_shared<RemoteConnectorSplit>(taskId), -1);
      task->addSplit("0", std::move(split));
    }
    task->noMoreSplits("0");
  }

  std::shared_ptr<Task> assertQuery(
      const std::shared_ptr<const core::PlanNode>& plan,
      const std::vector<std::string>& remoteTaskIds,
      const std::string& duckDbSql,
      std::optional<std::vector<uint32_t>> sortingKeys = std::nullopt) {
    std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;
    for (auto& taskId : remoteTaskIds) {
      splits.push_back(std::make_shared<RemoteConnectorSplit>(taskId));
    }
    return OperatorTestBase::assertQuery(plan, splits, duckDbSql, sortingKeys);
  }

  void assertQueryOrdered(
      const std::shared_ptr<const core::PlanNode>& plan,
      const std::vector<std::string>& remoteTaskIds,
      const std::string& duckDbSql,
      const std::vector<uint32_t>& sortingKeys) {
    assertQuery(plan, remoteTaskIds, duckDbSql, sortingKeys);
  }

  void setupSources(int filePathCount, int rowsPerVector) {
    filePaths_ = makeFilePaths(filePathCount);
    vectors_ = makeVectors(filePaths_.size(), rowsPerVector);
    for (int i = 0; i < filePaths_.size(); i++) {
      writeToFile(filePaths_[i]->path, vectors_[i]);
    }
    createDuckDbTable(vectors_);
  }

  void verifyExchangeStats(
      const std::shared_ptr<Task>& task,
      int32_t expectedCount) const {
    auto exchangeStats =
        task->taskStats().pipelineStats[0].operatorStats[0].runtimeStats;
    ASSERT_EQ(1, exchangeStats.count("localExchangeSource.numPages"));
    ASSERT_EQ(
        expectedCount, exchangeStats.at("localExchangeSource.numPages").count);
  }

  RowTypePtr rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5"},
          {BIGINT(), INTEGER(), SMALLINT(), REAL(), DOUBLE(), VARCHAR()})};
  std::unordered_map<std::string, std::string> configSettings_;
  std::vector<std::shared_ptr<TempFilePath>> filePaths_;
  std::vector<RowVectorPtr> vectors_;
};

TEST_F(MultiFragmentTest, aggregationSingleKey) {
  setupSources(10, 1000);
  std::vector<std::shared_ptr<Task>> tasks;
  auto leafTaskId = makeTaskId("leaf", 0);
  core::PlanNodePtr partialAggPlan;
  {
    partialAggPlan = PlanBuilder()
                         .tableScan(rowType_)
                         .project({"c0 % 10 AS c0", "c1"})
                         .partialAggregation({"c0"}, {"sum(c1)"})
                         .partitionedOutput({"c0"}, 3)
                         .planNode();

    auto leafTask = makeTask(leafTaskId, partialAggPlan, 0);
    tasks.push_back(leafTask);
    Task::start(leafTask, 4);
    addHiveSplits(leafTask, filePaths_);
  }

  core::PlanNodePtr finalAggPlan;
  std::vector<std::string> finalAggTaskIds;
  for (int i = 0; i < 3; i++) {
    finalAggPlan = PlanBuilder()
                       .exchange(partialAggPlan->outputType())
                       .finalAggregation({"c0"}, {"sum(a0)"}, {BIGINT()})
                       .partitionedOutput({}, 1)
                       .planNode();

    finalAggTaskIds.push_back(makeTaskId("final-agg", i));
    auto task = makeTask(finalAggTaskIds.back(), finalAggPlan, i);
    tasks.push_back(task);
    Task::start(task, 1);
    addRemoteSplits(task, {leafTaskId});
  }

  auto op = PlanBuilder().exchange(finalAggPlan->outputType()).planNode();

  assertQuery(
      op, finalAggTaskIds, "SELECT c0 % 10, sum(c1) FROM tmp GROUP BY 1");

  for (auto& task : tasks) {
    ASSERT_TRUE(waitForTaskCompletion(task.get())) << task->taskId();
  }

  // Verify the created memory pools.
  for (int i = 0; i < tasks.size(); ++i) {
    SCOPED_TRACE(fmt::format("task {}", tasks[i]->taskId()));
    int32_t numPools = 0;
    std::unordered_map<std::string, memory::MemoryPool*> poolsByName;
    std::vector<memory::MemoryPool*> pools;
    pools.push_back(tasks[i]->pool());
    poolsByName[tasks[i]->pool()->name()] = tasks[i]->pool();
    while (!pools.empty()) {
      numPools += pools.size();
      std::vector<memory::MemoryPool*> childPools;
      for (auto pool : pools) {
        pool->visitChildren([&](memory::MemoryPool* childPool) -> bool {
          EXPECT_EQ(poolsByName.count(childPool->name()), 0)
              << childPool->name();
          poolsByName[childPool->name()] = childPool;
          if (childPool->parent() != nullptr) {
            EXPECT_EQ(poolsByName.count(childPool->parent()->name()), 1);
            EXPECT_EQ(
                poolsByName[childPool->parent()->name()], childPool->parent());
          }
          childPools.push_back(childPool);
          return true;
        });
      }
      pools.swap(childPools);
    }
    if (i == 0) {
      // For leaf task, it has total 21 memory pools: task pool + 4 plan node
      // pools (TableScan, FilterProject, PartialAggregation, PartitionedOutput)
      // + 16 operator pools (4 drivers * number of plan nodes) + 4 connector
      // pools for TableScan.
      ASSERT_EQ(numPools, 25);
    } else {
      // For root task, it has total 8 memory pools: task pool + 3 plan node
      // pools (Exchange, Aggregation, PartitionedOutput) and 4 leaf pools: 3
      // operator pools (1 driver * number of plan nodes) + 1 exchange client
      // pool.
      ASSERT_EQ(numPools, 8);
    }
  }
}

TEST_F(MultiFragmentTest, aggregationMultiKey) {
  setupSources(10, 1'000);
  std::vector<std::shared_ptr<Task>> tasks;
  auto leafTaskId = makeTaskId("leaf", 0);
  core::PlanNodePtr partialAggPlan;
  {
    partialAggPlan = PlanBuilder()
                         .tableScan(rowType_)
                         .project({"c0 % 10 AS c0", "c1 % 2 AS c1", "c2"})
                         .partialAggregation({"c0", "c1"}, {"sum(c2)"})
                         .partitionedOutput({"c0", "c1"}, 3)
                         .planNode();

    auto leafTask = makeTask(leafTaskId, partialAggPlan, 0);
    tasks.push_back(leafTask);
    Task::start(leafTask, 4);
    addHiveSplits(leafTask, filePaths_);
  }

  core::PlanNodePtr finalAggPlan;
  std::vector<std::string> finalAggTaskIds;
  for (int i = 0; i < 3; i++) {
    finalAggPlan = PlanBuilder()
                       .exchange(partialAggPlan->outputType())
                       .finalAggregation({"c0", "c1"}, {"sum(a0)"}, {BIGINT()})
                       .partitionedOutput({}, 1)
                       .planNode();

    finalAggTaskIds.push_back(makeTaskId("final-agg", i));
    auto task = makeTask(finalAggTaskIds.back(), finalAggPlan, i);
    tasks.push_back(task);
    Task::start(task, 1);
    addRemoteSplits(task, {leafTaskId});
  }

  auto op = PlanBuilder().exchange(finalAggPlan->outputType()).planNode();

  assertQuery(
      op,
      finalAggTaskIds,
      "SELECT c0 % 10, c1 % 2, sum(c2) FROM tmp GROUP BY 1, 2");

  for (auto& task : tasks) {
    ASSERT_TRUE(waitForTaskCompletion(task.get())) << task->taskId();
  }
}

TEST_F(MultiFragmentTest, distributedTableScan) {
  setupSources(10, 1000);
  // Run the table scan several times to test the caching.
  for (int i = 0; i < 3; ++i) {
    auto leafTaskId = makeTaskId("leaf", 0);

    auto leafPlan = PlanBuilder()
                        .tableScan(rowType_)
                        .project({"c0 % 10", "c1 % 2", "c2"})
                        .partitionedOutput({}, 1, {"c2", "p1", "p0"})
                        .planNode();

    auto leafTask = makeTask(leafTaskId, leafPlan, 0);
    Task::start(leafTask, 4);
    addHiveSplits(leafTask, filePaths_);

    auto op = PlanBuilder().exchange(leafPlan->outputType()).planNode();
    auto task =
        assertQuery(op, {leafTaskId}, "SELECT c2, c1 % 2, c0 % 10 FROM tmp");

    verifyExchangeStats(task, 1);

    ASSERT_TRUE(waitForTaskCompletion(leafTask.get())) << leafTask->taskId();
  }
}

TEST_F(MultiFragmentTest, mergeExchange) {
  setupSources(20, 1000);

  static const core::SortOrder kAscNullsLast(true, false);
  std::vector<std::shared_ptr<Task>> tasks;

  std::vector<std::shared_ptr<TempFilePath>> filePaths0(
      filePaths_.begin(), filePaths_.begin() + 10);
  std::vector<std::shared_ptr<TempFilePath>> filePaths1(
      filePaths_.begin() + 10, filePaths_.end());

  std::vector<std::vector<std::shared_ptr<TempFilePath>>> filePathsList = {
      filePaths0, filePaths1};

  std::vector<std::string> partialSortTaskIds;
  RowTypePtr outputType;

  for (int i = 0; i < 2; ++i) {
    auto sortTaskId = makeTaskId("orderby", tasks.size());
    partialSortTaskIds.push_back(sortTaskId);
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto partialSortPlan = PlanBuilder(planNodeIdGenerator)
                               .localMerge(
                                   {"c0"},
                                   {PlanBuilder(planNodeIdGenerator)
                                        .tableScan(rowType_)
                                        .orderBy({"c0"}, true)
                                        .planNode()})
                               .partitionedOutput({}, 1)
                               .planNode();

    auto sortTask = makeTask(sortTaskId, partialSortPlan, tasks.size());
    tasks.push_back(sortTask);
    Task::start(sortTask, 4);
    addHiveSplits(sortTask, filePathsList[i]);
    outputType = partialSortPlan->outputType();
  }

  auto finalSortTaskId = makeTaskId("orderby", tasks.size());
  auto finalSortPlan = PlanBuilder()
                           .mergeExchange(outputType, {"c0"})
                           .partitionedOutput({}, 1)
                           .planNode();

  auto task = makeTask(finalSortTaskId, finalSortPlan, 0);
  tasks.push_back(task);
  Task::start(task, 1);
  addRemoteSplits(task, partialSortTaskIds);

  auto op = PlanBuilder().exchange(outputType).planNode();
  assertQueryOrdered(
      op, {finalSortTaskId}, "SELECT * FROM tmp ORDER BY 1 NULLS LAST", {0});

  for (auto& task : tasks) {
    ASSERT_TRUE(waitForTaskCompletion(task.get())) << task->taskId();
  }
}

// Test reordering and dropping columns in PartitionedOutput operator.
TEST_F(MultiFragmentTest, partitionedOutput) {
  setupSources(10, 1000);

  // Test dropping columns only
  {
    auto leafTaskId = makeTaskId("leaf", 0);
    auto leafPlan = PlanBuilder()
                        .values(vectors_)
                        .partitionedOutput({}, 1, {"c0", "c1"})
                        .planNode();
    auto leafTask = makeTask(leafTaskId, leafPlan, 0);
    Task::start(leafTask, 4);
    auto op = PlanBuilder().exchange(leafPlan->outputType()).planNode();

    assertQuery(op, {leafTaskId}, "SELECT c0, c1 FROM tmp");

    ASSERT_TRUE(waitForTaskCompletion(leafTask.get())) << leafTask->taskId();
  }

  // Test reordering and dropping at the same time
  {
    auto leafTaskId = makeTaskId("leaf", 0);
    auto leafPlan = PlanBuilder()
                        .values(vectors_)
                        .partitionedOutput({}, 1, {"c3", "c0", "c2"})
                        .planNode();
    auto leafTask = makeTask(leafTaskId, leafPlan, 0);
    Task::start(leafTask, 4);
    auto op = PlanBuilder().exchange(leafPlan->outputType()).planNode();

    assertQuery(op, {leafTaskId}, "SELECT c3, c0, c2 FROM tmp");

    ASSERT_TRUE(waitForTaskCompletion(leafTask.get())) << leafTask->taskId();
  }

  // Test producing duplicate columns
  {
    auto leafTaskId = makeTaskId("leaf", 0);
    auto leafPlan =
        PlanBuilder()
            .values(vectors_)
            .partitionedOutput(
                {}, 1, {"c0", "c1", "c2", "c3", "c4", "c3", "c2", "c1", "c0"})
            .planNode();
    auto leafTask = makeTask(leafTaskId, leafPlan, 0);
    Task::start(leafTask, 4);
    auto op = PlanBuilder().exchange(leafPlan->outputType()).planNode();

    assertQuery(
        op, {leafTaskId}, "SELECT c0, c1, c2, c3, c4, c3, c2, c1, c0 FROM tmp");

    ASSERT_TRUE(waitForTaskCompletion(leafTask.get())) << leafTask->taskId();
  }

  // Test dropping the partitioning key
  {
    constexpr int32_t kFanout = 4;
    auto leafTaskId = makeTaskId("leaf", 0);
    auto leafPlan = PlanBuilder()
                        .values(vectors_)
                        .partitionedOutput({"c5"}, kFanout, {"c2", "c0", "c3"})
                        .planNode();
    auto leafTask = makeTask(leafTaskId, leafPlan, 0);
    Task::start(leafTask, 4);

    auto intermediatePlan = PlanBuilder()
                                .exchange(leafPlan->outputType())
                                .partitionedOutput({}, 1, {"c3", "c0", "c2"})
                                .planNode();
    std::vector<std::string> intermediateTaskIds;
    for (auto i = 0; i < kFanout; ++i) {
      intermediateTaskIds.push_back(makeTaskId("intermediate", i));
      auto intermediateTask =
          makeTask(intermediateTaskIds.back(), intermediatePlan, i);
      Task::start(intermediateTask, 1);
      addRemoteSplits(intermediateTask, {leafTaskId});
    }

    auto op = PlanBuilder().exchange(intermediatePlan->outputType()).planNode();

    auto task =
        assertQuery(op, intermediateTaskIds, "SELECT c3, c0, c2 FROM tmp");

    verifyExchangeStats(task, kFanout);

    ASSERT_TRUE(waitForTaskCompletion(leafTask.get())) << leafTask->taskId();
  }

  // Test asynchronously deleting task buffer (due to abort from downstream).
  {
    auto leafTaskId = makeTaskId("leaf", 0);
    auto leafPlan = PlanBuilder()
                        .values(vectors_)
                        .partitionedOutput({}, 1, {"c0", "c1"})
                        .planNode();
    auto leafTask = makeTask(leafTaskId, leafPlan, 0);
    Task::start(leafTask, 4);
    auto bufferMgr = PartitionedOutputBufferManager::getInstance().lock();
    // Delete the results asynchronously to simulate abort from downstream.
    bufferMgr->deleteResults(leafTaskId, 0);

    ASSERT_TRUE(waitForTaskCompletion(leafTask.get())) << leafTask->taskId();
  }
}

TEST_F(MultiFragmentTest, broadcast) {
  auto data = makeRowVector(
      {makeFlatVector<int32_t>(1'000, [](auto row) { return row; })});

  // Make leaf task: Values -> Repartitioning (broadcast)
  std::vector<std::shared_ptr<Task>> tasks;
  auto leafTaskId = makeTaskId("leaf", 0);
  auto leafPlan =
      PlanBuilder().values({data}).partitionedOutputBroadcast().planNode();
  auto leafTask = makeTask(leafTaskId, leafPlan, 0);
  tasks.emplace_back(leafTask);
  Task::start(leafTask, 1);

  // Make next stage tasks.
  core::PlanNodePtr finalAggPlan;
  std::vector<std::string> finalAggTaskIds;
  for (int i = 0; i < 3; i++) {
    finalAggPlan = PlanBuilder()
                       .exchange(leafPlan->outputType())
                       .singleAggregation({}, {"count(1)"})
                       .partitionedOutput({}, 1)
                       .planNode();

    finalAggTaskIds.push_back(makeTaskId("final-agg", i));
    auto task = makeTask(finalAggTaskIds.back(), finalAggPlan, i);
    tasks.emplace_back(task);
    Task::start(task, 1);
    leafTask->updateBroadcastOutputBuffers(i + 1, false);
    addRemoteSplits(task, {leafTaskId});
  }
  leafTask->updateBroadcastOutputBuffers(finalAggTaskIds.size(), true);

  // Collect results from multiple tasks.
  auto op = PlanBuilder().exchange(finalAggPlan->outputType()).planNode();

  assertQuery(op, finalAggTaskIds, "SELECT UNNEST(array[1000, 1000, 1000])");

  for (auto& task : tasks) {
    ASSERT_TRUE(waitForTaskCompletion(task.get())) << task->taskId();
  }

  // Make sure duplicate 'updateBroadcastOutputBuffers' message after task
  // completion doesn't cause an error.
  leafTask->updateBroadcastOutputBuffers(finalAggTaskIds.size(), true);
}

TEST_F(MultiFragmentTest, replicateNullsAndAny) {
  auto data = makeRowVector({makeFlatVector<int32_t>(
      1'000, [](auto row) { return row; }, nullEvery(7))});

  std::vector<std::shared_ptr<Task>> tasks;
  auto addTask = [&](std::shared_ptr<Task> task,
                     const std::vector<std::string>& remoteTaskIds) {
    tasks.emplace_back(task);
    Task::start(task, 1);
    if (!remoteTaskIds.empty()) {
      addRemoteSplits(task, remoteTaskIds);
    }
  };

  // Make leaf task: Values -> Repartitioning (3-way)
  auto leafTaskId = makeTaskId("leaf", 0);
  auto leafPlan = PlanBuilder()
                      .values({data})
                      .partitionedOutput({"c0"}, 3, true)
                      .planNode();
  auto leafTask = makeTask(leafTaskId, leafPlan, 0);
  addTask(leafTask, {});

  // Make next stage tasks to count nulls.
  core::PlanNodePtr finalAggPlan;
  std::vector<std::string> finalAggTaskIds;
  for (int i = 0; i < 3; i++) {
    finalAggPlan =
        PlanBuilder()
            .exchange(leafPlan->outputType())
            .project({"c0 is null AS co_is_null"})
            .partialAggregation({}, {"count_if(co_is_null)", "count(1)"})
            .partitionedOutput({}, 1)
            .planNode();

    finalAggTaskIds.push_back(makeTaskId("final-agg", i));
    auto task = makeTask(finalAggTaskIds.back(), finalAggPlan, i);
    addTask(task, {leafTaskId});
  }

  // Collect results and verify number of nulls is 3 times larger than in the
  // original data.
  auto op =
      PlanBuilder()
          .exchange(finalAggPlan->outputType())
          .finalAggregation({}, {"sum(a0)", "sum(a1)"}, {BIGINT(), BIGINT()})
          .planNode();

  assertQuery(
      op,
      finalAggTaskIds,
      "SELECT 3 * ceil(1000.0 / 7) /* number of null rows */, 1000 + 2 * ceil(1000.0 / 7) /* total number of rows */");

  for (auto& task : tasks) {
    ASSERT_TRUE(waitForTaskCompletion(task.get())) << task->taskId();
  }
}

// Test query finishing before all splits have been scheduled.
TEST_F(MultiFragmentTest, limit) {
  auto data = makeRowVector({makeFlatVector<int32_t>(
      1'000, [](auto row) { return row; }, nullEvery(7))});

  auto file = TempFilePath::create();
  writeToFile(file->path, {data});

  // Make leaf task: Values -> PartialLimit(10) -> Repartitioning(0).
  auto leafTaskId = makeTaskId("leaf", 0);
  auto leafPlan =
      PlanBuilder()
          .tableScan(std::dynamic_pointer_cast<const RowType>(data->type()))
          .limit(0, 10, true)
          .partitionedOutput({}, 1)
          .planNode();
  auto leafTask = makeTask(leafTaskId, leafPlan, 0);
  Task::start(leafTask, 1);

  leafTask.get()->addSplit(
      "0", exec::Split(makeHiveConnectorSplit(file->path)));

  // Make final task: Exchange -> FinalLimit(10).
  auto plan = PlanBuilder()
                  .exchange(leafPlan->outputType())
                  .localPartition({})
                  .limit(0, 10, false)
                  .planNode();

  auto split =
      exec::Split(std::make_shared<RemoteConnectorSplit>(leafTaskId), -1);

  // Expect the task to produce results before receiving no-more-splits message.
  bool splitAdded = false;
  auto task = ::assertQuery(
      plan,
      [&](Task* task) {
        if (splitAdded) {
          return;
        }
        task->addSplit("0", std::move(split));
        splitAdded = true;
      },
      "VALUES (null), (1), (2), (3), (4), (5), (6), (null), (8), (9)",
      duckDbQueryRunner_);

  ASSERT_TRUE(waitForTaskCompletion(task.get())) << task->taskId();
  ASSERT_TRUE(waitForTaskCompletion(leafTask.get())) << leafTask->taskId();
}

TEST_F(MultiFragmentTest, mergeExchangeOverEmptySources) {
  std::vector<std::shared_ptr<Task>> tasks;
  std::vector<std::string> leafTaskIds;

  auto data = makeRowVector(rowType_, 0);

  for (int i = 0; i < 2; ++i) {
    auto taskId = makeTaskId("leaf-", i);
    leafTaskIds.push_back(taskId);
    auto plan =
        PlanBuilder().values({data}).partitionedOutput({}, 1).planNode();

    auto task = makeTask(taskId, plan, tasks.size());
    tasks.push_back(task);
    Task::start(task, 4);
  }

  auto exchangeTaskId = makeTaskId("exchange-", 0);
  auto plan = PlanBuilder()
                  .mergeExchange(rowType_, {"c0"})
                  .singleAggregation({"c0"}, {"count(1)"})
                  .planNode();

  assertQuery(plan, leafTaskIds, "");

  for (auto& task : tasks) {
    ASSERT_TRUE(waitForTaskCompletion(task.get())) << task->taskId();
  }
}

namespace {
core::PlanNodePtr makeJoinOverExchangePlan(
    const RowTypePtr& exchangeType,
    const RowVectorPtr& buildData) {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  return PlanBuilder(planNodeIdGenerator)
      .exchange(exchangeType)
      .hashJoin(
          {"c0"},
          {"u_c0"},
          PlanBuilder(planNodeIdGenerator).values({buildData}).planNode(),
          "",
          {"c0"})
      .partitionedOutput({}, 1)
      .planNode();
}
} // namespace

TEST_F(MultiFragmentTest, earlyCompletion) {
  // Setup a distributed query with 4 tasks:
  // - 1 leaf task with results partitioned 2 ways;
  // - 2 intermediate tasks reading from 2 partitions produced by the leaf task.
  // Each task reads from one partition.
  // - 1 output task that collects the results from the intermediate tasks.
  //
  // Make it so that leaf task fills up output buffers and blocks waiting for
  // upstream tasks to read the data. Make it then so that one of the upstream
  // tasks finishes early without fetching any data from the leaf task. Use join
  // with an empty build side.
  //
  // Verify all tasks finish successfully without hanging.

  std::vector<std::shared_ptr<Task>> tasks;

  // Set a very low limit for maximum amount of data to accumulate in the output
  // buffers. PartitionedOutput operator has a hard-coded limit of 60 KB, hence,
  // can't go lower than that.
  configSettings_[core::QueryConfig::kMaxPartitionedOutputBufferSize] = "100";

  // Create leaf task.
  auto data = makeRowVector(
      {makeFlatVector<int64_t>(10'000, [](auto row) { return row; })});

  auto leafTaskId = makeTaskId("leaf", 0);
  auto plan = PlanBuilder()
                  .values({data, data, data, data})
                  .partitionedOutput({"c0"}, 2)
                  .planNode();

  auto task = makeTask(leafTaskId, plan, tasks.size());
  tasks.push_back(task);
  Task::start(task, 1);

  // Create intermediate tasks.
  std::vector<std::string> joinTaskIds;
  RowTypePtr joinOutputType;
  for (int i = 0; i < 2; ++i) {
    RowVectorPtr buildData;
    if (i == 0) {
      buildData = makeRowVector(ROW({"u_c0"}, {BIGINT()}), 0);
    } else {
      buildData = makeRowVector(
          {"u_c0"}, {makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6})});
    }

    auto joinPlan =
        makeJoinOverExchangePlan(asRowType(data->type()), buildData);

    joinOutputType = joinPlan->outputType();

    auto taskId = makeTaskId("join", i);
    joinTaskIds.push_back(taskId);

    auto task = makeTask(taskId, joinPlan, i);
    tasks.push_back(task);
    Task::start(task, 4);

    addRemoteSplits(task, {leafTaskId});
  }

  // Create output task.
  auto outputPlan = PlanBuilder().exchange(joinOutputType).planNode();

  assertQuery(
      outputPlan, joinTaskIds, "SELECT UNNEST([3, 3, 3, 3, 4, 4, 4, 4])");

  for (auto& task : tasks) {
    ASSERT_TRUE(waitForTaskCompletion(task.get())) << task->taskId();
  }
}

TEST_F(MultiFragmentTest, earlyCompletionBroadcast) {
  // Same as 'earlyCompletion' test, but broadcasts leaf task results to all
  // intermediate tasks.

  std::vector<std::shared_ptr<Task>> tasks;

  // Set a very low limit for maximum amount of data to accumulate in the output
  // buffers. PartitionedOutput operator has a hard-coded limit of 60 KB, hence,
  // can't go lower than that.
  configSettings_[core::QueryConfig::kMaxPartitionedOutputBufferSize] = "100";

  // Create leaf task.
  auto data = makeRowVector(
      {makeFlatVector<int64_t>(10'000, [](auto row) { return row; })});

  auto leafTaskId = makeTaskId("leaf", 0);
  auto plan = PlanBuilder()
                  .values({data, data, data, data})
                  .partitionedOutputBroadcast()
                  .planNode();

  auto leafTask = makeTask(leafTaskId, plan, tasks.size());
  tasks.push_back(leafTask);
  Task::start(leafTask, 1);

  // Create intermediate tasks.
  std::vector<std::string> joinTaskIds;
  RowTypePtr joinOutputType;
  for (int i = 0; i < 2; ++i) {
    RowVectorPtr buildData;
    if (i == 0) {
      buildData = makeRowVector(ROW({"u_c0"}, {BIGINT()}), 0);
    } else {
      buildData = makeRowVector(
          {"u_c0"}, {makeFlatVector<int64_t>({-7, 10, 12345678})});
    }

    auto joinPlan =
        makeJoinOverExchangePlan(asRowType(data->type()), buildData);

    joinOutputType = joinPlan->outputType();

    auto taskId = makeTaskId("join", i);
    joinTaskIds.push_back(taskId);

    auto task = makeTask(taskId, joinPlan, i);
    tasks.push_back(task);
    Task::start(task, 4);

    leafTask->updateBroadcastOutputBuffers(i + 1, false);

    addRemoteSplits(task, {leafTaskId});
  }
  leafTask->updateBroadcastOutputBuffers(joinTaskIds.size(), true);

  // Create output task.
  auto outputPlan = PlanBuilder().exchange(joinOutputType).planNode();

  assertQuery(outputPlan, joinTaskIds, "SELECT UNNEST([10, 10, 10, 10])");

  for (auto& task : tasks) {
    ASSERT_TRUE(waitForTaskCompletion(task.get())) << task->taskId();
  }
}

TEST_F(MultiFragmentTest, earlyCompletionMerge) {
  // Same as 'earlyCompletion' test, but uses MergeExchange instead of Exchange.

  std::vector<std::shared_ptr<Task>> tasks;

  // Set a very low limit for maximum amount of data to accumulate in the output
  // buffers. PartitionedOutput operator has a hard-coded limit of 60 KB, hence,
  // can't go lower than that.
  configSettings_[core::QueryConfig::kMaxPartitionedOutputBufferSize] = "100";

  // Create leaf task.
  auto data = makeRowVector(
      {makeFlatVector<int64_t>(10'000, [](auto row) { return row; })});

  auto leafTaskId = makeTaskId("leaf", 0);
  auto plan = PlanBuilder()
                  .values({data, data, data, data})
                  .partitionedOutput({"c0"}, 2)
                  .planNode();

  auto task = makeTask(leafTaskId, plan, tasks.size());
  tasks.push_back(task);
  Task::start(task, 1);

  // Create intermediate tasks.
  std::vector<std::string> joinTaskIds;
  RowTypePtr joinOutputType;
  for (int i = 0; i < 2; ++i) {
    RowVectorPtr buildData;
    if (i == 0) {
      buildData = makeRowVector(ROW({"u_c0"}, {BIGINT()}), 0);
    } else {
      buildData = makeRowVector(
          {"u_c0"}, {makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6})});
    }

    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto joinPlan =
        PlanBuilder(planNodeIdGenerator)
            .mergeExchange(asRowType(data->type()), {"c0"})
            .hashJoin(
                {"c0"},
                {"u_c0"},
                PlanBuilder(planNodeIdGenerator).values({buildData}).planNode(),
                "",
                {"c0"})
            .partitionedOutput({}, 1)
            .planNode();

    joinOutputType = joinPlan->outputType();

    auto taskId = makeTaskId("join", i);
    joinTaskIds.push_back(taskId);

    auto task = makeTask(taskId, joinPlan, i);
    tasks.push_back(task);
    Task::start(task, 4);

    addRemoteSplits(task, {leafTaskId});
  }

  // Create output task.
  auto outputPlan = PlanBuilder().exchange(joinOutputType).planNode();

  assertQuery(
      outputPlan, joinTaskIds, "SELECT UNNEST([3, 3, 3, 3, 4, 4, 4, 4])");

  for (auto& task : tasks) {
    ASSERT_TRUE(waitForTaskCompletion(task.get())) << task->taskId();
  }
}

class SlowNode : public core::PlanNode {
 public:
  SlowNode(const core::PlanNodeId& id, core::PlanNodePtr source)
      : PlanNode(id), sources_{std::move(source)} {}

  const RowTypePtr& outputType() const override {
    return sources_[0]->outputType();
  }

  const std::vector<core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  std::string_view name() const override {
    return "slow";
  }

 private:
  void addDetails(std::stringstream& /* stream */) const override {}

  std::vector<core::PlanNodePtr> sources_;
};

class SlowOperator : public Operator {
 public:
  SlowOperator(
      int32_t operatorId,
      DriverCtx* driverCtx,
      std::shared_ptr<const SlowNode> slowNode)
      : Operator(
            driverCtx,
            slowNode->outputType(),
            operatorId,
            slowNode->id(),
            "SlowOperator") {}

  void addInput(RowVectorPtr input) override {
    input_ = std::move(input);
  }

  bool needsInput() const override {
    return input_ == nullptr;
  }

  RowVectorPtr getOutput() override {
    if (!input_ || consumedOneBatch_) {
      return nullptr;
    }
    consumedOneBatch_ = true;
    return std::move(input_);
  }

  void noMoreInput() override {
    Operator::noMoreInput();
  }

  BlockingReason isBlocked(ContinueFuture* future) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return false;
  }

 private:
  bool consumedOneBatch_{false};
};

class SlowOperatorTranslator : public Operator::PlanNodeTranslator {
  std::unique_ptr<Operator>
  toOperator(DriverCtx* ctx, int32_t id, const core::PlanNodePtr& node) {
    if (auto slowNode = std::dynamic_pointer_cast<const SlowNode>(node)) {
      return std::make_unique<SlowOperator>(id, ctx, slowNode);
    }
    return nullptr;
  }
};

TEST_F(MultiFragmentTest, exchangeDestruction) {
  // This unit test tests the proper destruction of ExchangeClient upon
  // task destruction.
  Operator::registerOperator(std::make_unique<SlowOperatorTranslator>());

  // Set small size so that ExchangeSource will conduct multiple rounds of
  // pulls of data while SlowOperator will only consume one batch (data
  // from a single pull of ExchangeSource). This makes sure we always have
  // data buffered at ExchangeQueue in Exchange operator, which this test
  // requires.
  configSettings_[core::QueryConfig::kMaxPartitionedOutputBufferSize] = "2048";
  setupSources(10, 1000);
  auto leafTaskId = makeTaskId("leaf", 0);
  core::PlanNodePtr leafPlan;

  leafPlan = PlanBuilder()
                 .tableScan(rowType_)
                 .project({"c0 % 10 AS c0", "c1"})
                 .partitionedOutput({}, 1)
                 .planNode();

  auto leafTask = makeTask(leafTaskId, leafPlan, 0);
  Task::start(leafTask, 1);
  addHiveSplits(leafTask, filePaths_);

  auto rootPlan =
      PlanBuilder()
          .exchange(leafPlan->outputType())
          .addNode([&leafPlan](std::string id, core::PlanNodePtr node) {
            return std::make_shared<SlowNode>(id, std::move(node));
          })
          .partitionedOutput({}, 1)
          .planNode();

  auto rootTask = makeTask("root-task", rootPlan, 0);
  Task::start(rootTask, 1);
  addRemoteSplits(rootTask, {leafTaskId});

  ASSERT_FALSE(waitForTaskCompletion(rootTask.get(), 1'000'000));
  leafTask->requestAbort().get();
  rootTask->requestAbort().get();

  // Destructors of root Task should be called without issues. Unprocessed
  // SerializedPages in MemoryPool tracked LocalExchangeSource should be freed
  // properly with no crash.
  leafTask = nullptr;
  rootTask = nullptr;
}

TEST_F(MultiFragmentTest, cancelledExchange) {
  // Create a source fragment borrow the output type from it.
  auto planFragment = exec::test::PlanBuilder()
                          .tableScan(rowType_)
                          .filter("c0 % 5 = 1")
                          .partitionedOutput({}, 1, {"c0", "c1"})
                          .planFragment();

  // Create task with exchange.
  auto planFragmentWithExchange =
      exec::test::PlanBuilder()
          .exchange(planFragment.planNode->outputType())
          .partitionedOutput({}, 1)
          .planFragment();
  auto exchangeTask =
      makeTask("output.0.0.1", planFragmentWithExchange.planNode, 0);
  // Start the task and abort it straight away.
  Task::start(exchangeTask, 2);
  exchangeTask->requestAbort();

  /* sleep override */
  // Wait till all the terminations, closures and destructions are done.
  std::this_thread::sleep_for(std::chrono::seconds(1));
  // We expect no references left except for ours.
  EXPECT_EQ(1, exchangeTask.use_count());
}

class TestCustomExchangeNode : public core::PlanNode {
 public:
  TestCustomExchangeNode(const core::PlanNodeId& id, const RowTypePtr type)
      : PlanNode(id), outputType_(type) {}

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<core::PlanNodePtr>& sources() const override {
    static std::vector<core::PlanNodePtr> kEmptySources;
    return kEmptySources;
  }

  bool requiresExchangeClient() const override {
    return true;
  }

  bool requiresSplits() const override {
    return true;
  }

  std::string_view name() const override {
    return "CustomExchange";
  }

 private:
  void addDetails(std::stringstream& stream) const override {
    // Nothing to add
  }

  RowTypePtr outputType_;
};

class TestCustomExchange : public exec::Exchange {
 public:
  TestCustomExchange(
      int32_t operatorId,
      DriverCtx* FOLLY_NONNULL ctx,
      const std::shared_ptr<const TestCustomExchangeNode>& customExchangeNode,
      std::shared_ptr<ExchangeClient> exchangeClient)
      : exec::Exchange(
            operatorId,
            ctx,
            std::make_shared<core::ExchangeNode>(
                customExchangeNode->id(),
                customExchangeNode->outputType()),
            std::move(exchangeClient)) {}

  RowVectorPtr getOutput() override {
    stats_.wlock()->addRuntimeStat("testCustomExchangeStat", RuntimeCounter(1));
    return exec::Exchange::getOutput();
  }
};

class TestCustomExchangeTranslator : public exec::Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<exec::Operator> toOperator(
      exec::DriverCtx* ctx,
      int32_t id,
      const core::PlanNodePtr& node,
      std::shared_ptr<ExchangeClient> exchangeClient) override {
    if (auto customExchangeNode =
            std::dynamic_pointer_cast<const TestCustomExchangeNode>(node)) {
      return std::make_unique<TestCustomExchange>(
          id, ctx, customExchangeNode, std::move(exchangeClient));
    }
    return nullptr;
  }
};

TEST_F(MultiFragmentTest, customPlanNodeWithExchangeClient) {
  setupSources(5, 100);
  Operator::registerOperator(std::make_unique<TestCustomExchangeTranslator>());
  auto leafTaskId = makeTaskId("leaf", 0);
  auto leafPlan =
      PlanBuilder().values(vectors_).partitionedOutput({}, 1).planNode();
  auto leafTask = makeTask(leafTaskId, leafPlan, 0);
  Task::start(leafTask, 1);

  CursorParameters params;
  core::PlanNodeId testNodeId;
  params.maxDrivers = 1;
  params.planNode =
      PlanBuilder()
          .addNode([&leafPlan](std::string id, core::PlanNodePtr /* input */) {
            return std::make_shared<TestCustomExchangeNode>(
                id, leafPlan->outputType());
          })
          .capturePlanNodeId(testNodeId)
          .planNode();

  auto cursor = std::make_unique<TaskCursor>(params);
  auto task = cursor->task();
  addRemoteSplits(task, {leafTaskId});
  while (cursor->moveNext()) {
  }
  EXPECT_NE(
      toPlanStats(task->taskStats())
          .at(testNodeId)
          .customStats.count("testCustomExchangeStat"),
      0);
  ASSERT_TRUE(waitForTaskCompletion(leafTask.get(), 3'000'000))
      << leafTask->taskId();
  ASSERT_TRUE(waitForTaskCompletion(task.get(), 3'000'000)) << task->taskId();
}

// This test is to reproduce the race condition between task terminate and no
// more split call:
// T1: task terminate triggered by task error.
// T2: task terminate collects all the pending remote splits under the task
// lock.
// T3: task terminate release the lock to handle the pending remote splits.
// T4: no more split call get invoked and erase the exchange client since the
//     task is not running.
// T5: task terminate processes the pending remote splits by accessing the
//     associated exchange client and run into segment fault.
DEBUG_ONLY_TEST_F(
    MultiFragmentTest,
    raceBetweenTaskTerminateAndTaskNoMoreSplits) {
  setupSources(10, 1000);
  auto leafTaskId = makeTaskId("leaf", 0);
  core::PlanNodePtr leafPlan = PlanBuilder()
                                   .tableScan(rowType_)
                                   .project({"c0 % 10 AS c0", "c1"})
                                   .partitionedOutput({}, 1)
                                   .planNode();
  auto leafTask = makeTask(leafTaskId, leafPlan, 0);
  Task::start(leafTask, 1);
  addHiveSplits(leafTask, filePaths_);

  const std::string kRootTaskId("root-task");
  folly::EventCount blockTerminate;
  folly::EventCount blockNoMoreSplits;
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Task::setError",
      std::function<void(Task*)>(([&](Task* task) {
        if (task->taskId() != kRootTaskId) {
          return;
        }
        // Trigger to add more split after the task has run into error.
        auto split =
            exec::Split(std::make_shared<RemoteConnectorSplit>(leafTaskId), -1);
        task->addSplit("0", std::move(split));
      })));

  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Task::terminate",
      std::function<void(Task*)>(([&](Task* task) {
        if (task->taskId() != kRootTaskId) {
          return;
        }
        // Unblock no more split call in the middle of task terminate execution.
        blockNoMoreSplits.notify();
        auto waitKey = blockTerminate.prepareWait();
        // Block to wait for the no more split call to finish.
        blockTerminate.wait(waitKey);
      })));
  auto rootPlan = PlanBuilder()
                      .exchange(leafPlan->outputType())
                      .finalAggregation({"c0"}, {"count(c1)"}, {BIGINT()})
                      .planNode();

  const int64_t kRootMemoryLimit = 1 << 20;
  auto rootTask = makeTask(
      kRootTaskId,
      rootPlan,
      0,
      [](RowVectorPtr /*unused*/, ContinueFuture* /*unused*/)
          -> BlockingReason { return BlockingReason::kNotBlocked; },
      kRootMemoryLimit);
  Task::start(rootTask, 1);
  {
    auto split =
        exec::Split(std::make_shared<RemoteConnectorSplit>(leafTaskId), -1);
    rootTask->addSplit("0", std::move(split));
  }
  auto waitKey = blockNoMoreSplits.prepareWait();
  // Block to wait for task terminate execution.
  blockNoMoreSplits.wait(waitKey);
  rootTask->noMoreSplits("0");
  // Unblock task terminate execution after no more split call finishes.
  blockTerminate.notify();
  ASSERT_TRUE(waitForTaskFailure(rootTask.get(), 1'000'000'000));
}

TEST_F(MultiFragmentTest, taskTerminateWithPendingOutputBuffers) {
  setupSources(8, 1000);
  auto taskId = makeTaskId("task", 0);
  core::PlanNodePtr leafPlan;
  leafPlan =
      PlanBuilder().tableScan(rowType_).partitionedOutput({}, 1).planNode();

  auto task = makeTask(taskId, leafPlan, 0);
  Task::start(task, 1);
  addHiveSplits(task, filePaths_);

  auto bufferManager = PartitionedOutputBufferManager::getInstance().lock();
  const uint64_t maxBytes = std::numeric_limits<uint64_t>::max();
  const int destination = 0;
  std::vector<std::unique_ptr<folly::IOBuf>> receivedIobufs;
  int64_t sequence = 0;
  for (;;) {
    auto dataPromise = ContinuePromise("WaitForOutput");
    bool complete{false};
    ASSERT_TRUE(bufferManager->getData(
        taskId,
        destination,
        maxBytes,
        sequence,
        [&](std::vector<std::unique_ptr<folly::IOBuf>> iobufs,
            int64_t inSequence) {
          for (auto& iobuf : iobufs) {
            if (iobuf != nullptr) {
              ++inSequence;
              receivedIobufs.push_back(std::move(iobuf));
            } else {
              complete = true;
            }
          }
          sequence = inSequence;
          dataPromise.setValue();
        }));
    dataPromise.getSemiFuture().wait();
    if (complete) {
      break;
    }
  }

  // Abort the task to terminate after get buffers from the partitioned output
  // buffer manager.
  task->requestAbort();
  ASSERT_TRUE(waitForTaskAborted(task.get(), 5'000'000));

  // Wait for 1 second to let async driver activities to finish and
  // 'receivedIobufs' should be the last ones to hold the reference on the task.
  std::this_thread::sleep_for(std::chrono::seconds(1));
  int expectedTaskRefCounts{0};
  for (const auto& iobuf : receivedIobufs) {
    auto nextIoBuf = iobuf->next();
    while (nextIoBuf != iobuf.get()) {
      ++expectedTaskRefCounts;
      nextIoBuf = nextIoBuf->next();
    }
    ++expectedTaskRefCounts;
  }
  ASSERT_EQ(task.use_count(), 1 + expectedTaskRefCounts);
  receivedIobufs.clear();
  ASSERT_EQ(task.use_count(), 1);
  task.reset();
}
