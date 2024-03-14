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
#include <regex>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/MemoryArbitrator.h"
#include "velox/exec/OutputBufferManager.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/Cursor.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/type/Type.h"

namespace facebook::velox::exec::test {

class GroupedExecutionTest : public virtual HiveConnectorTestBase {
 protected:
  void SetUp() override {
    HiveConnectorTestBase::SetUp();
  }

  static void SetUpTestCase() {
    FLAGS_velox_testing_enable_arbitration = true;
    HiveConnectorTestBase::SetUpTestCase();
  }

  std::vector<RowVectorPtr> makeVectors(
      int32_t count,
      int32_t rowsPerVector,
      const RowTypePtr& rowType = nullptr) {
    auto inputType = rowType ? rowType : rowType_;
    return HiveConnectorTestBase::makeVectors(inputType, count, rowsPerVector);
  }

  exec::Split makeHiveSplitWithGroup(std::string path, int32_t group) {
    return exec::Split(makeHiveConnectorSplit(std::move(path)), group);
  }

  exec::Split makeHiveSplit(std::string path) {
    return exec::Split(makeHiveConnectorSplit(std::move(path)));
  }

  static core::PlanNodePtr tableScanNode(const RowTypePtr& outputType) {
    return PlanBuilder().tableScan(outputType).planNode();
  }

  static std::unordered_set<int32_t> getCompletedSplitGroups(
      const std::shared_ptr<exec::Task>& task) {
    return task->taskStats().completedSplitGroups;
  }

  static void waitForFinishedDrivers(
      const std::shared_ptr<exec::Task>& task,
      uint32_t n) {
    // Limit wait to 10 seconds.
    size_t iteration{0};
    while (task->numFinishedDrivers() < n and iteration < 100) {
      /* sleep override */
      usleep(100'000); // 0.1 second.
      ++iteration;
    }
    ASSERT_EQ(n, task->numFinishedDrivers());
  }

  RowTypePtr rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6"},
          {BIGINT(),
           INTEGER(),
           SMALLINT(),
           REAL(),
           DOUBLE(),
           VARCHAR(),
           TINYINT()})};
};

// Here we test the grouped execution sanity checks.
TEST_F(GroupedExecutionTest, groupedExecutionErrors) {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId tableScanNodeId;
  core::PlanNodeId projectNodeId;
  core::PlanNodeId localPartitionNodeId;
  core::PlanNodeId tableScanNodeId2;
  auto planFragment =
      PlanBuilder(planNodeIdGenerator)
          .localPartitionRoundRobin(
              {PlanBuilder(planNodeIdGenerator)
                   .tableScan(rowType_)
                   .capturePlanNodeId(tableScanNodeId)
                   .project({"c0", "c1", "c2", "c3", "c4", "c5"})
                   .capturePlanNodeId(projectNodeId)
                   .planNode(),
               PlanBuilder(planNodeIdGenerator)
                   .tableScan(rowType_)
                   .capturePlanNodeId(tableScanNodeId2)
                   .project({"c0", "c1", "c2", "c3", "c4", "c5"})
                   .planNode()})
          .capturePlanNodeId(localPartitionNodeId)
          .partitionedOutput({}, 1, {"c0", "c1", "c2", "c3", "c4", "c5"})
          .planFragment();

  std::shared_ptr<core::QueryCtx> queryCtx;
  std::shared_ptr<exec::Task> task;
  planFragment.numSplitGroups = 10;

  // Check ungrouped execution with supplied leaf node ids.
  planFragment.executionStrategy = core::ExecutionStrategy::kUngrouped;
  planFragment.groupedExecutionLeafNodeIds.clear();
  planFragment.groupedExecutionLeafNodeIds.emplace(tableScanNodeId);
  queryCtx = std::make_shared<core::QueryCtx>(executor_.get());
  task = exec::Task::create("0", planFragment, 0, std::move(queryCtx));
  VELOX_ASSERT_THROW(
      task->start(3, 1),
      "groupedExecutionLeafNodeIds must be empty in ungrouped execution mode");

  // Check grouped execution without supplied leaf node ids.
  planFragment.executionStrategy = core::ExecutionStrategy::kGrouped;
  planFragment.groupedExecutionLeafNodeIds.clear();
  queryCtx = std::make_shared<core::QueryCtx>(executor_.get());
  task = exec::Task::create("0", planFragment, 0, std::move(queryCtx));
  VELOX_ASSERT_THROW(
      task->start(3, 1),
      "groupedExecutionLeafNodeIds must not be empty in "
      "grouped execution mode");

  // Check grouped execution with supplied non-leaf node id.
  planFragment.executionStrategy = core::ExecutionStrategy::kGrouped;
  planFragment.groupedExecutionLeafNodeIds.clear();
  planFragment.groupedExecutionLeafNodeIds.emplace(projectNodeId);
  queryCtx = std::make_shared<core::QueryCtx>(executor_.get());
  task = exec::Task::create("0", planFragment, 0, std::move(queryCtx));
  VELOX_ASSERT_THROW(
      task->start(3, 1),
      fmt::format(
          "Grouped execution leaf node {} is not a leaf node in any pipeline",
          projectNodeId));

  // Check grouped execution with supplied leaf and non-leaf node ids.
  planFragment.executionStrategy = core::ExecutionStrategy::kGrouped;
  planFragment.groupedExecutionLeafNodeIds.clear();
  planFragment.groupedExecutionLeafNodeIds.emplace(tableScanNodeId);
  planFragment.groupedExecutionLeafNodeIds.emplace(projectNodeId);
  queryCtx = std::make_shared<core::QueryCtx>(executor_.get());
  task = exec::Task::create("0", planFragment, 0, std::move(queryCtx));
  VELOX_ASSERT_THROW(
      task->start(3, 1),
      fmt::format(
          "Grouped execution leaf node {} is not a leaf node in any pipeline",
          projectNodeId));

  // Check grouped execution with supplied leaf node id for a non-source
  // pipeline.
  planFragment.executionStrategy = core::ExecutionStrategy::kGrouped;
  planFragment.groupedExecutionLeafNodeIds.clear();
  planFragment.groupedExecutionLeafNodeIds.emplace(localPartitionNodeId);
  queryCtx = std::make_shared<core::QueryCtx>(executor_.get());
  task = exec::Task::create("0", planFragment, 0, std::move(queryCtx));
  VELOX_ASSERT_THROW(
      task->start(3, 1),
      fmt::format(
          "Grouped execution leaf node {} not found or it is not a leaf node",
          localPartitionNodeId));
}

// Here we test various aspects of grouped/bucketed execution involving
// output buffer and 3 pipelines.
TEST_F(GroupedExecutionTest, groupedExecutionWithOutputBuffer) {
  // Create source file - we will read from it in 6 splits.
  auto vectors = makeVectors(10, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);

  // A chain of three pipelines separated by local exchange with the leaf one
  // having scan running grouped execution - this will make all three pipelines
  // running grouped execution.
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId tableScanNodeId;

  auto planFragment =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(rowType_)
          .capturePlanNodeId(tableScanNodeId)
          .project({"c3 as x", "c2 as y", "c1 as z", "c0 as w", "c4", "c5"})
          .localPartitionRoundRobinRow()
          .project({"w as c0", "z as c1", "y as c2", "x as c3", "c4", "c5"})
          .localPartitionRoundRobinRow()
          .partitionedOutput({}, 1, {"c0", "c1", "c2", "c3", "c4", "c5"})
          .planFragment();
  planFragment.executionStrategy = core::ExecutionStrategy::kGrouped;
  planFragment.groupedExecutionLeafNodeIds.emplace(tableScanNodeId);
  planFragment.numSplitGroups = 10;
  auto queryCtx = std::make_shared<core::QueryCtx>(executor_.get());
  auto task =
      exec::Task::create("0", std::move(planFragment), 0, std::move(queryCtx));
  // 3 drivers max and 1 concurrent split group.
  task->start(3, 1);

  // All pipelines run grouped execution, so no drivers should be running.
  EXPECT_EQ(0, task->numRunningDrivers());

  // Add one split for group (8).
  task->addSplit("0", makeHiveSplitWithGroup(filePath->path, 8));

  // Only one split group should be in the processing mode, so 9 drivers (3 per
  // pipeline).
  EXPECT_EQ(9, task->numRunningDrivers());
  EXPECT_EQ(std::unordered_set<int32_t>{}, getCompletedSplitGroups(task));

  // Add the rest of splits
  task->addSplit("0", makeHiveSplitWithGroup(filePath->path, 1));
  task->addSplit("0", makeHiveSplitWithGroup(filePath->path, 5));
  task->addSplit("0", makeHiveSplitWithGroup(filePath->path, 8));
  task->addSplit("0", makeHiveSplitWithGroup(filePath->path, 5));
  task->addSplit("0", makeHiveSplitWithGroup(filePath->path, 8));

  // One split group should be in the processing mode, so 9 drivers.
  EXPECT_EQ(9, task->numRunningDrivers());
  EXPECT_EQ(std::unordered_set<int32_t>{}, getCompletedSplitGroups(task));

  // Finalize one split group (8) and wait until 3 drivers are finished.
  task->noMoreSplitsForGroup("0", 8);
  waitForFinishedDrivers(task, 9);
  // As one split group is finished, another one should kick in, so 3 drivers.
  EXPECT_EQ(9, task->numRunningDrivers());
  EXPECT_EQ(std::unordered_set<int32_t>({8}), getCompletedSplitGroups(task));

  // Finalize the second split group (1) and wait until 18 drivers are finished.
  task->noMoreSplitsForGroup("0", 1);
  waitForFinishedDrivers(task, 18);

  // As one split group is finished, another one should kick in, so 3 drivers.
  EXPECT_EQ(9, task->numRunningDrivers());
  EXPECT_EQ(std::unordered_set<int32_t>({1, 8}), getCompletedSplitGroups(task));

  // Finalize the third split group (5) and wait until 27 drivers are finished.
  task->noMoreSplitsForGroup("0", 5);
  waitForFinishedDrivers(task, 27);

  // No split groups should be processed at the moment, so 0 drivers.
  EXPECT_EQ(0, task->numRunningDrivers());
  EXPECT_EQ(
      std::unordered_set<int32_t>({1, 5, 8}), getCompletedSplitGroups(task));

  // Flag that we would have no more split groups.
  task->noMoreSplits("0");

  // 'Delete results' from output buffer triggers 'set all output consumed',
  // which should finish the task.
  auto outputBufferManager = exec::OutputBufferManager::getInstance().lock();
  outputBufferManager->deleteResults(task->taskId(), 0);

  // Task must be finished at this stage.
  EXPECT_EQ(exec::TaskState::kFinished, task->state());
  EXPECT_EQ(
      std::unordered_set<int32_t>({1, 5, 8}), getCompletedSplitGroups(task));

  // Check that stats are properly assembled.
  auto taskStats = task->taskStats();

  // Expect 9 drivers in total: 3 drivers x 3 groups.
  for (const auto& pipeStats : taskStats.pipelineStats) {
    for (const auto& opStats : pipeStats.operatorStats) {
      EXPECT_EQ(
          9, opStats.runtimeStats.find("runningFinishWallNanos")->second.count);
    }
  }

  // Check TableScan for total number of splits.
  EXPECT_EQ(6, taskStats.pipelineStats[2].operatorStats[0].numSplits);
  // Check FilterProject for total number of vectors/batches.
  EXPECT_EQ(18, taskStats.pipelineStats[1].operatorStats[1].inputVectors);
}

DEBUG_ONLY_TEST_F(
    GroupedExecutionTest,
    groupedExecutionWithHashJoinSpillCheck) {
  // Create source file to read as split input.
  auto vectors = makeVectors(24, 20);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);

  struct {
    bool enableSpill;
    bool mixedExecutionMode;
    int expectedNumDrivers;
    bool expectedSpill;

    std::string debugString() const {
      return fmt::format(
          "enableSpill {}, mixedExecutionMode {}, expectedNumDrivers {}, expectedSpill {}",
          enableSpill,
          mixedExecutionMode,
          expectedNumDrivers,
          expectedSpill);
    }
  } testSettings[] = {
      {false, false, 12, false},
      {false, true, 9, false},
      {true, false, 12, true},
      {true, true, 9, false}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    core::PlanNodeId probeScanNodeId;
    core::PlanNodeId buildScanNodeId;

    PlanBuilder planBuilder(planNodeIdGenerator, pool_.get());
    planBuilder.tableScan(rowType_)
        .capturePlanNodeId(probeScanNodeId)
        .project({"c0 as x"});
    // Hash join.
    core::PlanNodeId joinNodeId;
    auto planFragment = planBuilder
                            .hashJoin(
                                {"x"},
                                {"y"},
                                PlanBuilder(planNodeIdGenerator, pool_.get())
                                    .tableScan(rowType_, {"c0 > 0"})
                                    .capturePlanNodeId(buildScanNodeId)
                                    .project({"c0 as y"})
                                    .planNode(),
                                "",
                                {"x", "y"})
                            .capturePlanNodeId(joinNodeId)
                            .partitionedOutput({}, 1, {"x", "y"})
                            .planFragment();

    planFragment.executionStrategy = core::ExecutionStrategy::kGrouped;
    planFragment.groupedExecutionLeafNodeIds.emplace(probeScanNodeId);
    if (!testData.mixedExecutionMode) {
      planFragment.groupedExecutionLeafNodeIds.emplace(buildScanNodeId);
    }
    planFragment.numSplitGroups = 2;

    auto queryCtx = std::make_shared<core::QueryCtx>(executor_.get());
    if (testData.enableSpill) {
      std::unordered_map<std::string, std::string> configs;
      configs.emplace(core::QueryConfig::kSpillEnabled, "true");
      configs.emplace(core::QueryConfig::kJoinSpillEnabled, "true");
      queryCtx->testingOverrideConfigUnsafe(std::move(configs));
    }

    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::noMoreInput",
        std::function<void(Operator*)>([&](Operator* op) {
          if (op->operatorType() != "HashBuild") {
            return;
          }
          ASSERT_EQ(op->canReclaim(), testData.expectedSpill);
          if (testData.enableSpill) {
            memory::testingRunArbitration(op->pool());
          }
        }));

    auto task = exec::Task::create(
        "0", std::move(planFragment), 0, std::move(queryCtx));
    const auto spillDirectory = exec::test::TempDirectoryPath::create();
    if (testData.enableSpill) {
      task->setSpillDirectory(spillDirectory->path);
    }

    // 3 drivers max and 1 concurrent split group to execute one group at a
    // time.
    task->start(3, 1);
    ASSERT_EQ(task->hasMixedExecutionGroup(), testData.mixedExecutionMode);

    // Add split(s) to the build scan.
    if (testData.mixedExecutionMode) {
      task->addSplit(buildScanNodeId, makeHiveSplit(filePath->path));
    } else {
      task->addSplit(
          buildScanNodeId, makeHiveSplitWithGroup(filePath->path, 0));
      task->addSplit(
          buildScanNodeId, makeHiveSplitWithGroup(filePath->path, 1));
    }
    // Add one split for probe split group (0).
    task->addSplit(probeScanNodeId, makeHiveSplitWithGroup(filePath->path, 0));
    // Add one split for probe split group (1).
    task->addSplit(probeScanNodeId, makeHiveSplitWithGroup(filePath->path, 1));

    // Finalize the build split(s).
    if (testData.mixedExecutionMode) {
      task->noMoreSplits(buildScanNodeId);
    } else {
      task->noMoreSplitsForGroup(buildScanNodeId, 0);
      task->noMoreSplitsForGroup(buildScanNodeId, 1);
    }
    // Finalize probe split groups.
    task->noMoreSplitsForGroup(probeScanNodeId, 0);
    task->noMoreSplitsForGroup(probeScanNodeId, 1);

    waitForFinishedDrivers(task, testData.expectedNumDrivers);

    // 'Delete results' from output buffer triggers 'set all output consumed',
    // which should finish the task.
    auto outputBufferManager = exec::OutputBufferManager::getInstance().lock();
    outputBufferManager->deleteResults(task->taskId(), 0);

    // Task must be finished at this stage.
    ASSERT_EQ(task->state(), exec::TaskState::kFinished);

    auto taskStats = exec::toPlanStats(task->taskStats());
    auto& planStats = taskStats.at(joinNodeId);
    if (testData.expectedSpill) {
      ASSERT_GT(planStats.spilledBytes, 0);
    } else {
      ASSERT_EQ(planStats.spilledBytes, 0);
    }
  }
}

// Here we test various aspects of grouped/bucketed execution involving
// output buffer and 3 pipelines.
TEST_F(GroupedExecutionTest, groupedExecutionWithHashAndNestedLoopJoin) {
  // Create source file - we will read from it in 6 splits.
  auto vectors = makeVectors(4, 20);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);

  // Run the test twice - for Hash and Cross Join.
  for (size_t i = 0; i < 2; ++i) {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    core::PlanNodeId probeScanNodeId;
    core::PlanNodeId buildScanNodeId;

    PlanBuilder planBuilder(planNodeIdGenerator, pool_.get());
    planBuilder.tableScan(rowType_)
        .capturePlanNodeId(probeScanNodeId)
        .project({"c3 as x", "c2 as y", "c1 as z", "c0 as w", "c4", "c5"});
    // Hash or Nested Loop join.
    core::PlanNodeId joinNodeId;
    if (i == 0) {
      planBuilder
          .hashJoin(
              {"w"},
              {"r"},
              PlanBuilder(planNodeIdGenerator, pool_.get())
                  .tableScan(rowType_, {"c0 > 0"})
                  .capturePlanNodeId(buildScanNodeId)
                  .project({"c0 as r"})
                  .planNode(),
              "",
              {"x", "y", "z", "w", "c4", "c5"})
          .capturePlanNodeId(joinNodeId)
          .localPartitionRoundRobinRow()
          .project({"w as c0", "z as c1", "y as c2", "x as c3", "c4", "c5"})
          .planNode();
    } else {
      planBuilder
          .nestedLoopJoin(
              PlanBuilder(planNodeIdGenerator, pool_.get())
                  .tableScan(rowType_, {"c0 > 0"})
                  .capturePlanNodeId(buildScanNodeId)
                  .project({"c0 as r"})
                  .planNode(),
              {"x", "y", "z", "r", "c4", "c5"})
          .localPartitionRoundRobinRow()
          .project({"r as c0", "z as c1", "y as c2", "x as c3", "c4", "c5"})
          .planNode();
    }
    auto planFragment =
        planBuilder.localPartitionRoundRobinRow()
            .partitionedOutput({}, 1, {"c0", "c1", "c2", "c3", "c4", "c5"})
            .planFragment();

    planFragment.executionStrategy = core::ExecutionStrategy::kGrouped;
    planFragment.groupedExecutionLeafNodeIds.emplace(probeScanNodeId);
    planFragment.numSplitGroups = 10;
    auto queryCtx = std::make_shared<core::QueryCtx>(executor_.get());
    auto task = exec::Task::create(
        "0", std::move(planFragment), 0, std::move(queryCtx));
    // 3 drivers max and 1 concurrent split group.
    task->start(3, 1);

    // Build pipeline runs ungrouped execution, so it should have drivers
    // running.
    EXPECT_EQ(3, task->numRunningDrivers());

    // Add single split to the build scan.
    task->addSplit(buildScanNodeId, makeHiveSplit(filePath->path));

    // Add one split for group (8).
    task->addSplit(probeScanNodeId, makeHiveSplitWithGroup(filePath->path, 8));

    // Only one split group should be in the processing mode, so 9 drivers (3
    // per pipeline) grouped + 3 ungrouped.
    EXPECT_EQ(3 + 9, task->numRunningDrivers());
    EXPECT_EQ(std::unordered_set<int32_t>{}, getCompletedSplitGroups(task));

    // Add the rest of splits
    task->addSplit(probeScanNodeId, makeHiveSplitWithGroup(filePath->path, 1));
    task->addSplit(probeScanNodeId, makeHiveSplitWithGroup(filePath->path, 5));
    task->addSplit(probeScanNodeId, makeHiveSplitWithGroup(filePath->path, 8));
    task->addSplit(probeScanNodeId, makeHiveSplitWithGroup(filePath->path, 5));
    task->addSplit(probeScanNodeId, makeHiveSplitWithGroup(filePath->path, 8));

    // One split group should be in the processing mode, so 9 drivers (3 per
    // pipeline) grouped + 3 ungrouped.
    EXPECT_EQ(3 + 9, task->numRunningDrivers());
    EXPECT_EQ(std::unordered_set<int32_t>{}, getCompletedSplitGroups(task));

    // Finalize the build splits.
    task->noMoreSplits(buildScanNodeId);
    // Wait till the build is finished and check drivers and splits again.
    waitForFinishedDrivers(task, 3);
    // One split group should be in the processing mode, so 9 drivers.
    EXPECT_EQ(9, task->numRunningDrivers());
    EXPECT_EQ(std::unordered_set<int32_t>{}, getCompletedSplitGroups(task));

    // Finalize one split group (8) and wait until 3 drivers are finished.
    task->noMoreSplitsForGroup(probeScanNodeId, 8);
    waitForFinishedDrivers(task, 3 + 9);
    // As one split group is finished, another one should kick in, so 9 drivers.
    EXPECT_EQ(9, task->numRunningDrivers());
    EXPECT_EQ(std::unordered_set<int32_t>({8}), getCompletedSplitGroups(task));

    // Finalize the second split group (1) and wait until 18 drivers are
    // finished.
    task->noMoreSplitsForGroup(probeScanNodeId, 1);
    waitForFinishedDrivers(task, 3 + 18);

    // As one split group is finished, another one should kick in, so 9 drivers.
    EXPECT_EQ(9, task->numRunningDrivers());
    EXPECT_EQ(
        std::unordered_set<int32_t>({1, 8}), getCompletedSplitGroups(task));

    // Finalize the third split group (5) and wait until 27 drivers are
    // finished.
    task->noMoreSplitsForGroup(probeScanNodeId, 5);
    waitForFinishedDrivers(task, 3 + 27);

    // No split groups should be processed at the moment, so 0 drivers.
    EXPECT_EQ(0, task->numRunningDrivers());
    EXPECT_EQ(
        std::unordered_set<int32_t>({1, 5, 8}), getCompletedSplitGroups(task));

    // Flag that we would have no more split groups.
    task->noMoreSplits(probeScanNodeId);

    // 'Delete results' from output buffer triggers 'set all output consumed',
    // which should finish the task.
    auto outputBufferManager = exec::OutputBufferManager::getInstance().lock();
    outputBufferManager->deleteResults(task->taskId(), 0);

    // Task must be finished at this stage.
    EXPECT_EQ(exec::TaskState::kFinished, task->state());
    EXPECT_EQ(
        std::unordered_set<int32_t>({1, 5, 8}), getCompletedSplitGroups(task));
    if (i == 0) {
      // Check each split group has a separate hash join node pool.
      const std::unordered_set<int32_t> expectedSplitGroupIds({1, 5, 8});
      int numSplitGroupJoinNodes{0};
      task->pool()->visitChildren([&](memory::MemoryPool* childPool) -> bool {
        if (folly::StringPiece(childPool->name())
                .startsWith(fmt::format("node.{}[", joinNodeId))) {
          ++numSplitGroupJoinNodes;
          std::vector<std::string> parts;
          folly::split(".", childPool->name(), parts);
          const std::string name = parts[1];
          parts.clear();
          folly::split("[", name, parts);
          const std::string splitGroupIdStr =
              parts[1].substr(0, parts[1].size() - 1);
          EXPECT_TRUE(
              expectedSplitGroupIds.count(atoi(splitGroupIdStr.c_str())))
              << splitGroupIdStr;
        }
        return true;
      });
      ASSERT_EQ(numSplitGroupJoinNodes, expectedSplitGroupIds.size());
    }
  }
}

// Here we test various aspects of grouped/bucketed execution.
TEST_F(GroupedExecutionTest, groupedExecution) {
  // Create source file - we will read from it in 6 splits.
  const size_t numSplits{6};
  auto vectors = makeVectors(10, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);

  CursorParameters params;
  params.planNode = tableScanNode(ROW({}, {}));
  params.maxDrivers = 2;
  // We will have 10 split groups 'in total', but our task will only handle
  // three of them: 1, 5 and 8.
  // Split 0 is from split group 1.
  // Splits 1 and 2 are from split group 5.
  // Splits 3, 4 and 5 are from split group 8.
  params.executionStrategy = core::ExecutionStrategy::kGrouped;
  params.groupedExecutionLeafNodeIds.emplace(params.planNode->id());
  params.numSplitGroups = 3;
  params.numConcurrentSplitGroups = 2;

  // Create the cursor with the task underneath. It is not started yet.
  auto cursor = TaskCursor::create(params);
  auto task = cursor->task();

  // Add one splits before start to ensure we can handle such cases.
  task->addSplit("0", makeHiveSplitWithGroup(filePath->path, 8));

  // Start task now.
  cursor->start();

  // Only one split group should be in the processing mode, so 2 drivers.
  EXPECT_EQ(2, task->numRunningDrivers());
  EXPECT_EQ(std::unordered_set<int32_t>{}, getCompletedSplitGroups(task));

  // Add the rest of splits
  task->addSplit("0", makeHiveSplitWithGroup(filePath->path, 1));
  task->addSplit("0", makeHiveSplitWithGroup(filePath->path, 5));
  task->addSplit("0", makeHiveSplitWithGroup(filePath->path, 8));
  task->addSplit("0", makeHiveSplitWithGroup(filePath->path, 5));
  task->addSplit("0", makeHiveSplitWithGroup(filePath->path, 8));

  // Only two split groups should be in the processing mode, so 4 drivers.
  EXPECT_EQ(4, task->numRunningDrivers());
  EXPECT_EQ(std::unordered_set<int32_t>{}, getCompletedSplitGroups(task));

  // Finalize one split group (8) and wait until 2 drivers are finished.
  task->noMoreSplitsForGroup("0", 8);
  waitForFinishedDrivers(task, 2);

  // As one split group is finished, another one should kick in, so 4 drivers.
  EXPECT_EQ(4, task->numRunningDrivers());
  EXPECT_EQ(std::unordered_set<int32_t>({8}), getCompletedSplitGroups(task));

  // Finalize the second split group (5) and wait until 4 drivers are finished.
  task->noMoreSplitsForGroup("0", 5);
  waitForFinishedDrivers(task, 4);

  // As the second split group is finished, only one is left, so 2 drivers.
  EXPECT_EQ(2, task->numRunningDrivers());
  EXPECT_EQ(std::unordered_set<int32_t>({5, 8}), getCompletedSplitGroups(task));

  // Finalize the third split group (1) and wait until 6 drivers are finished.
  task->noMoreSplitsForGroup("0", 1);
  waitForFinishedDrivers(task, 6);

  // No split groups should be processed at the moment, so 0 drivers.
  EXPECT_EQ(0, task->numRunningDrivers());
  EXPECT_EQ(
      std::unordered_set<int32_t>({1, 5, 8}), getCompletedSplitGroups(task));

  // Make sure split groups with no splits are reported as complete.
  task->noMoreSplitsForGroup("0", 3);
  EXPECT_EQ(
      std::unordered_set<int32_t>({1, 3, 5, 8}), getCompletedSplitGroups(task));

  // Flag that we would have no more split groups.
  task->noMoreSplits("0");

  // Make sure we've got the right number of rows.
  int32_t numRead = 0;
  while (cursor->moveNext()) {
    auto vector = cursor->current();
    EXPECT_EQ(vector->childrenSize(), 0);
    numRead += vector->size();
  }

  // Task must be finished at this stage.
  EXPECT_EQ(exec::TaskState::kFinished, task->state());
  EXPECT_EQ(
      std::unordered_set<int32_t>({1, 3, 5, 8}), getCompletedSplitGroups(task));
  EXPECT_EQ(numRead, numSplits * 10'000);
}

TEST_F(GroupedExecutionTest, allGroupSplitsReceivedBeforeTaskStart) {
  // Create source file - we will read from it in 6 splits.
  const size_t numSplits{6};
  auto vectors = makeVectors(10, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);

  CursorParameters params;
  params.planNode = tableScanNode(ROW({}, {}));
  params.maxDrivers = 1;
  params.executionStrategy = core::ExecutionStrategy::kGrouped;
  params.groupedExecutionLeafNodeIds.emplace(params.planNode->id());
  params.numSplitGroups = 3;
  params.numConcurrentSplitGroups = 1;

  // Create the cursor with the task underneath. It is not started yet.
  auto cursor = TaskCursor::create(params);
  auto task = cursor->task();

  // Add all split groups before start to ensure we can handle such cases.
  task->addSplit("0", makeHiveSplitWithGroup(filePath->path, 0));
  task->addSplit("0", makeHiveSplitWithGroup(filePath->path, 1));
  task->addSplit("0", makeHiveSplitWithGroup(filePath->path, 2));
  task->addSplit("0", makeHiveSplitWithGroup(filePath->path, 0));
  task->addSplit("0", makeHiveSplitWithGroup(filePath->path, 1));
  task->addSplit("0", makeHiveSplitWithGroup(filePath->path, 2));
  task->noMoreSplits("0");

  // Start task now.
  cursor->start();
  waitForFinishedDrivers(task, 3);
  ASSERT_EQ(
      getCompletedSplitGroups(task), std::unordered_set<int32_t>({0, 1, 2}));

  // Make sure we've got the right number of rows.
  int32_t numReadRows{0};
  while (cursor->moveNext()) {
    auto vector = cursor->current();
    EXPECT_EQ(vector->childrenSize(), 0);
    numReadRows += vector->size();
  }

  // Task must be finished at this stage.
  ASSERT_EQ(task->state(), exec::TaskState::kFinished);
  ASSERT_EQ(numSplits * 10'000, numReadRows);
}
} // namespace facebook::velox::exec::test
