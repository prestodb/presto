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
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/common/DataSink.h"
#include "velox/dwio/dwrf/test/utils/BatchMaker.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::connector::hive;

using facebook::velox::test::BatchMaker;

class MultiFragmentTest : public HiveConnectorTestBase {
 protected:
  static std::string makeTaskId(const std::string& prefix, int num) {
    return fmt::format("local://{}-{}", prefix, num);
  }

  std::shared_ptr<Task> makeTask(
      const std::string& taskId,
      std::shared_ptr<const core::PlanNode> planNode,
      int destination) {
    auto queryCtx = core::QueryCtx::createForTest(
        std::make_shared<core::MemConfig>(configSettings_));
    core::PlanFragment planFragment{planNode};
    return std::make_shared<Task>(
        taskId, std::move(planFragment), destination, std::move(queryCtx));
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

  void assertQuery(
      const std::shared_ptr<const core::PlanNode>& plan,
      const std::vector<std::string>& remoteTaskIds,
      const std::string& duckDbSql,
      std::optional<std::vector<uint32_t>> sortingKeys = std::nullopt) {
    std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;
    for (auto& taskId : remoteTaskIds) {
      splits.push_back(std::make_shared<RemoteConnectorSplit>(taskId));
    }
    OperatorTestBase::assertQuery(plan, splits, duckDbSql, sortingKeys);
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
  std::shared_ptr<core::PlanNode> partialAggPlan;
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

  std::shared_ptr<core::PlanNode> finalAggPlan;
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
}

TEST_F(MultiFragmentTest, aggregationMultiKey) {
  setupSources(10, 1'000);
  std::vector<std::shared_ptr<Task>> tasks;
  auto leafTaskId = makeTaskId("leaf", 0);
  std::shared_ptr<core::PlanNode> partialAggPlan;
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

  std::shared_ptr<core::PlanNode> finalAggPlan;
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
}

TEST_F(MultiFragmentTest, distributedTableScan) {
  setupSources(10, 1000);
  // Run the table scan several times to test the caching.
  for (int i = 0; i < 3; ++i) {
    std::vector<std::shared_ptr<Task>> tasks;
    auto leafTaskId = makeTaskId("leaf", 0);
    std::shared_ptr<core::PlanNode> leafPlan;
    {
      PlanBuilder builder;
      leafPlan = builder.tableScan(rowType_)
                     .project({"c0 % 10", "c1 % 2", "c2"})
                     .partitionedOutput({}, 1, {"c2", "p1", "p0"})
                     .planNode();

      auto leafTask = makeTask(leafTaskId, leafPlan, 0);
      tasks.push_back(leafTask);
      Task::start(leafTask, 4);
      addHiveSplits(leafTask, filePaths_);
    }
    auto op = PlanBuilder().exchange(leafPlan->outputType()).planNode();

    assertQuery(op, {leafTaskId}, "SELECT c2, c1 % 2, c0 % 10 FROM tmp");
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
    auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
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

  auto task = makeTask(finalSortTaskId, finalSortPlan, tasks.size());
  tasks.push_back(task);
  Task::start(task, 1);
  addRemoteSplits(task, partialSortTaskIds);

  auto op = PlanBuilder().exchange(outputType).planNode();
  assertQueryOrdered(
      op, {finalSortTaskId}, "SELECT * FROM tmp ORDER BY 1 NULLS LAST", {0});
}

// Test reordering and dropping columns in PartitionedOutput operator
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

    assertQuery(op, intermediateTaskIds, "SELECT c3, c0, c2 FROM tmp");
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
  std::shared_ptr<core::PlanNode> finalAggPlan;
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
  std::shared_ptr<core::PlanNode> finalAggPlan;
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
}

// Test query finishing before all splits have been scheduled.
TEST_F(MultiFragmentTest, limit) {
  auto data = makeRowVector({makeFlatVector<int32_t>(
      1'000, [](auto row) { return row; }, nullEvery(7))});

  auto file = TempFilePath::create();
  writeToFile(file->path, {data});

  // Make leaf task: Values -> PartialLimit(1) -> Repartitioning(0).
  auto leafTaskId = makeTaskId("leaf", 0);
  auto leafPlan =
      PlanBuilder()
          .tableScan(std::dynamic_pointer_cast<const RowType>(data->type()))
          .limit(0, 10, false)
          .partitionedOutput({}, 1)
          .planNode();
  auto leafTask = makeTask(leafTaskId, leafPlan, 0);
  Task::start(leafTask, 1);

  leafTask.get()->addSplit(
      "0", exec::Split(makeHiveConnectorSplit(file->path)));

  // Make final task: Exchange -> FinalLimit(1).
  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .localPartition(
                      {},
                      {PlanBuilder(planNodeIdGenerator)
                           .exchange(leafPlan->outputType())
                           .planNode()})
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
      "VALUES (null), (1), (2), (3), (4), (5), (6), (7), (8), (9)",
      duckDbQueryRunner_);

  ASSERT_TRUE(waitForTaskCompletion(task.get()));
  ASSERT_TRUE(waitForTaskCompletion(leafTask.get()));
}
