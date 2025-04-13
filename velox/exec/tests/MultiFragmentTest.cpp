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
#include "velox/common/testutil/TestValue.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/OutputBufferManager.h"
#include "velox/exec/PartitionedOutput.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/RoundRobinPartitionFunction.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/SerializedPageUtil.h"

using namespace facebook::velox::exec::test;

using facebook::velox::common::testutil::TestValue;
using facebook::velox::test::BatchMaker;

namespace facebook::velox::exec {
namespace {

struct TestParam {
  VectorSerde::Kind serdeKind;
  common::CompressionKind compressionKind;

  TestParam(
      VectorSerde::Kind _serdeKind,
      common::CompressionKind _compressionKind)
      : serdeKind(_serdeKind), compressionKind(_compressionKind) {}
};

class MultiFragmentTest : public HiveConnectorTestBase,
                          public testing::WithParamInterface<TestParam> {
 public:
  static std::vector<TestParam> getTestParams() {
    std::vector<TestParam> params;
    params.emplace_back(
        VectorSerde::Kind::kPresto, common::CompressionKind_NONE);
    params.emplace_back(
        VectorSerde::Kind::kCompactRow, common::CompressionKind_NONE);
    params.emplace_back(
        VectorSerde::Kind::kUnsafeRow, common::CompressionKind_NONE);
    params.emplace_back(
        VectorSerde::Kind::kPresto, common::CompressionKind_LZ4);
    params.emplace_back(
        VectorSerde::Kind::kCompactRow, common::CompressionKind_LZ4);
    params.emplace_back(
        VectorSerde::Kind::kUnsafeRow, common::CompressionKind_LZ4);
    return params;
  }

 protected:
  void SetUp() override {
    HiveConnectorTestBase::SetUp();
    exec::ExchangeSource::factories().clear();
    exec::ExchangeSource::registerFactory(createLocalExchangeSource);
    configSettings_[core::QueryConfig::kShuffleCompressionKind] =
        common::compressionKindToString(GetParam().compressionKind);
  }

  void TearDown() override {
    vectors_.clear();
    HiveConnectorTestBase::TearDown();
  }

  static std::string makeTaskId(const std::string& prefix, int num) {
    return fmt::format("local://{}-{}", prefix, num);
  }

  static std::string makeBadTaskId(const std::string& prefix, int num) {
    return fmt::format("bad://{}-{}", prefix, num);
  }

  static exec::Consumer noopConsumer() {
    return [](RowVectorPtr, ContinueFuture*) {
      return BlockingReason::kNotBlocked;
    };
  }

  std::shared_ptr<Task> makeTask(
      const std::string& taskId,
      const core::PlanNodePtr& planNode,
      int destination = 0,
      Consumer consumer = nullptr,
      int64_t maxMemory = memory::kMaxMemory,
      folly::Executor* executor = nullptr) {
    auto configCopy = configSettings_;
    auto queryCtx = core::QueryCtx::create(
        executor ? executor : executor_.get(),
        core::QueryConfig(std::move(configCopy)));
    queryCtx->testingOverrideMemoryPool(memory::memoryManager()->addRootPool(
        queryCtx->queryId(), maxMemory, MemoryReclaimer::create()));
    core::PlanFragment planFragment{planNode};
    return Task::create(
        taskId,
        std::move(planFragment),
        destination,
        std::move(queryCtx),
        Task::ExecutionMode::kParallel,
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
          std::make_shared<connector::hive::HiveConnectorSplit>(
              kHiveConnectorId,
              "file:" + filePath->getPath(),
              facebook::velox::dwio::common::FileFormat::DWRF),
          -1);
      task->addSplit("0", std::move(split));
      VLOG(1) << filePath->getPath() << "\n";
    }
    task->noMoreSplits("0");
  }

  exec::Split remoteSplit(const std::string& taskId) {
    return exec::Split(std::make_shared<RemoteConnectorSplit>(taskId));
  }

  void addRemoteSplits(
      std::shared_ptr<Task> task,
      const std::vector<std::string>& remoteTaskIds) {
    for (auto& taskId : remoteTaskIds) {
      task->addSplit("0", remoteSplit(taskId));
    }
    task->noMoreSplits("0");
  }

  void setupSources(int filePathCount, int rowsPerVector) {
    filePaths_ = makeFilePaths(filePathCount);
    vectors_ = makeVectors(filePaths_.size(), rowsPerVector);
    for (int i = 0; i < filePaths_.size(); i++) {
      writeToFile(filePaths_[i]->getPath(), vectors_[i]);
    }
    createDuckDbTable(vectors_);
  }

  int32_t enqueue(
      const std::string& taskId,
      int32_t destination,
      const RowVectorPtr& data) {
    auto page =
        toSerializedPage(data, GetParam().serdeKind, bufferManager_, pool());
    const auto pageSize = page->size();

    ContinueFuture unused;
    auto blocked =
        bufferManager_->enqueue(taskId, destination, std::move(page), &unused);
    VELOX_CHECK(!blocked);
    return pageSize;
  }

  void verifyExchangeStats(
      const std::shared_ptr<Task>& task,
      int32_t expectedNumPagesCount,
      int32_t expectedBackgroundCpuCount) const {
    auto taskStats = exec::toPlanStats(task->taskStats());

    const auto& exchangeOperatorStats = taskStats.at("0");
    ASSERT_GT(exchangeOperatorStats.rawInputBytes, 0);
    ASSERT_GT(exchangeOperatorStats.rawInputRows, 0);
    const auto& exchangeStats = taskStats.at("0").customStats;
    ASSERT_EQ(1, exchangeStats.count("localExchangeSource.numPages"));
    ASSERT_EQ(
        expectedNumPagesCount,
        exchangeStats.at("localExchangeSource.numPages").count);
    ASSERT_EQ(
        expectedBackgroundCpuCount,
        exchangeStats.at(ExchangeClient::kBackgroundCpuTimeMs).count);
    ASSERT_EQ(
        expectedBackgroundCpuCount, taskStats.at("0").backgroundTiming.count);
  }

  RowTypePtr rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5"},
          {BIGINT(), INTEGER(), SMALLINT(), REAL(), DOUBLE(), VARCHAR()})};
  std::unordered_map<std::string, std::string> configSettings_;
  std::vector<std::shared_ptr<TempFilePath>> filePaths_;
  std::vector<RowVectorPtr> vectors_;
  std::shared_ptr<OutputBufferManager> bufferManager_{
      OutputBufferManager::getInstanceRef()};
};

TEST_P(MultiFragmentTest, aggregationSingleKey) {
  setupSources(10, 1000);
  std::vector<std::shared_ptr<Task>> tasks;
  auto leafTaskId = makeTaskId("leaf", 0);
  core::PlanNodePtr partialAggPlan;
  core::PlanNodeId partitionNodeId;
  std::shared_ptr<Task> leafTask;
  {
    partialAggPlan =
        PlanBuilder()
            .tableScan(rowType_)
            .project({"c0 % 10 AS c0", "c1"})
            .partialAggregation({"c0"}, {"sum(c1)"})
            .partitionedOutput(
                {"c0"}, 3, /*outputLayout=*/{}, GetParam().serdeKind)
            .capturePlanNodeId(partitionNodeId)
            .planNode();

    leafTask = makeTask(leafTaskId, partialAggPlan, 0);
    tasks.push_back(leafTask);
    leafTask->start(4);
    addHiveSplits(leafTask, filePaths_);
  }

  core::PlanNodePtr finalAggPlan;
  std::vector<std::string> finalAggTaskIds;
  core::PlanNodeId exchangeNodeId;
  std::vector<std::shared_ptr<Task>> finalTasks;
  for (int i = 0; i < 3; i++) {
    finalAggPlan =
        PlanBuilder()
            .exchange(partialAggPlan->outputType(), GetParam().serdeKind)
            .capturePlanNodeId(exchangeNodeId)
            .finalAggregation({"c0"}, {"sum(a0)"}, {{BIGINT()}})
            .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam().serdeKind)
            .planNode();

    finalAggTaskIds.push_back(makeTaskId("final-agg", i));
    auto task = makeTask(finalAggTaskIds.back(), finalAggPlan, i);
    tasks.push_back(task);
    finalTasks.push_back(task);
    task->start(1);
    addRemoteSplits(task, {leafTaskId});
  }

  auto op = PlanBuilder()
                .exchange(finalAggPlan->outputType(), GetParam().serdeKind)
                .planNode();

  std::vector<Split> finalAggTaskSplits;
  for (auto finalAggTaskId : finalAggTaskIds) {
    finalAggTaskSplits.emplace_back(remoteSplit(finalAggTaskId));
  }
  test::AssertQueryBuilder(op, duckDbQueryRunner_)
      .splits(std::move(finalAggTaskSplits))
      .config(
          core::QueryConfig::kShuffleCompressionKind,
          compressionKindToString(GetParam().compressionKind))
      .assertResults("SELECT c0 % 10, sum(c1) FROM tmp GROUP BY 1");

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
      // For leaf task, it has total 21 memory pools: task pool + 4 plan
      // node pools (TableScan, FilterProject, PartialAggregation,
      // PartitionedOutput)
      // + 16 operator pools (4 drivers * number of plan nodes) + 4
      // connector pools for TableScan.
      ASSERT_EQ(numPools, 25);
    } else {
      // For root task, it has total 8 memory pools: task pool + 3 plan node
      // pools (Exchange, Aggregation, PartitionedOutput) and 4 leaf pools:
      // 3 operator pools (1 driver * number of plan nodes) + 1 exchange
      // client pool.
      ASSERT_EQ(numPools, 8);
    }
  }
  auto leafPlanStats = toPlanStats(leafTask->taskStats());
  const auto serdeKindRuntimsStats =
      leafPlanStats.at(partitionNodeId)
          .customStats.at(Operator::kShuffleSerdeKind);
  ASSERT_EQ(serdeKindRuntimsStats.count, 4);
  ASSERT_EQ(
      serdeKindRuntimsStats.min, static_cast<int64_t>(GetParam().serdeKind));
  ASSERT_EQ(
      serdeKindRuntimsStats.max, static_cast<int64_t>(GetParam().serdeKind));

  for (const auto& finalTask : finalTasks) {
    auto finalPlanStats = toPlanStats(finalTask->taskStats());
    const auto serdeKindRuntimsStats =
        finalPlanStats.at(exchangeNodeId)
            .customStats.at(Operator::kShuffleSerdeKind);
    ASSERT_EQ(serdeKindRuntimsStats.count, 1);
    ASSERT_EQ(
        serdeKindRuntimsStats.min, static_cast<int64_t>(GetParam().serdeKind));
    ASSERT_EQ(
        serdeKindRuntimsStats.max, static_cast<int64_t>(GetParam().serdeKind));
  }
}

TEST_P(MultiFragmentTest, aggregationMultiKey) {
  setupSources(10, 1'000);
  std::vector<std::shared_ptr<Task>> tasks;
  auto leafTaskId = makeTaskId("leaf", 0);
  core::PlanNodePtr partialAggPlan;
  {
    partialAggPlan =
        PlanBuilder()
            .tableScan(rowType_)
            .project({"c0 % 10 AS c0", "c1 % 2 AS c1", "c2"})
            .partialAggregation({"c0", "c1"}, {"sum(c2)"})
            .partitionedOutput(
                {"c0", "c1"}, 3, /*outputLayout=*/{}, GetParam().serdeKind)
            .planNode();

    auto leafTask = makeTask(leafTaskId, partialAggPlan, 0);
    tasks.push_back(leafTask);
    leafTask->start(4);
    addHiveSplits(leafTask, filePaths_);
  }

  core::PlanNodePtr finalAggPlan;
  std::vector<std::string> finalAggTaskIds;
  for (int i = 0; i < 3; i++) {
    finalAggPlan =
        PlanBuilder()
            .exchange(partialAggPlan->outputType(), GetParam().serdeKind)
            .finalAggregation({"c0", "c1"}, {"sum(a0)"}, {{BIGINT()}})
            .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam().serdeKind)
            .planNode();

    finalAggTaskIds.push_back(makeTaskId("final-agg", i));
    auto task = makeTask(finalAggTaskIds.back(), finalAggPlan, i);
    tasks.push_back(task);
    task->start(1);
    addRemoteSplits(task, {leafTaskId});
  }

  auto op = PlanBuilder()
                .exchange(finalAggPlan->outputType(), GetParam().serdeKind)
                .planNode();

  std::vector<Split> finalAggTaskSplits;
  for (auto finalAggTaskId : finalAggTaskIds) {
    finalAggTaskSplits.emplace_back(remoteSplit(finalAggTaskId));
  }
  test::AssertQueryBuilder(op, duckDbQueryRunner_)
      .splits(std::move(finalAggTaskSplits))
      .config(
          core::QueryConfig::kShuffleCompressionKind,
          common::compressionKindToString(GetParam().compressionKind))
      .assertResults("SELECT c0 % 10, c1 % 2, sum(c2) FROM tmp GROUP BY 1, 2");

  for (auto& task : tasks) {
    ASSERT_TRUE(waitForTaskCompletion(task.get())) << task->taskId();
  }
}

TEST_P(MultiFragmentTest, distributedTableScan) {
  setupSources(10, 1000);
  // Run the table scan several times to test the caching.
  for (int i = 0; i < 3; ++i) {
    auto leafTaskId = makeTaskId("leaf", 0);

    auto leafPlan =
        PlanBuilder()
            .tableScan(rowType_)
            .project({"c0 % 10", "c1 % 2", "c2"})
            .partitionedOutput({}, 1, {"c2", "p1", "p0"}, GetParam().serdeKind)
            .planNode();

    auto leafTask = makeTask(leafTaskId, leafPlan, 0);
    leafTask->start(4);
    addHiveSplits(leafTask, filePaths_);

    auto op = PlanBuilder()
                  .exchange(leafPlan->outputType(), GetParam().serdeKind)
                  .planNode();
    auto task =
        test::AssertQueryBuilder(op, duckDbQueryRunner_)
            .split(remoteSplit(leafTaskId))
            .config(
                core::QueryConfig::kShuffleCompressionKind,
                common::compressionKindToString(GetParam().compressionKind))
            .assertResults("SELECT c2, c1 % 2, c0 % 10 FROM tmp");

    verifyExchangeStats(task, 1, 1);

    ASSERT_TRUE(waitForTaskCompletion(leafTask.get())) << leafTask->taskId();
  }
}

// This test simulate the situation where an MergeExchange is aborted and
// causing a Driver thread to hold on to additional references to Task, and a
// deadlock at shutdown because of a tight loop inside the Driver thread.
//
// When the tasks correspond to a MergeExchange are aborted, we expect
// gracefully exiting of the task itself, and all relevant resources are cleaned
// up. What happens is that the tasks are aborted; however, the MergeExchange
// operator's ExchangeClient's are never closed, so the Driver threads are stuck
// in a tight request loop. This test ensures that after the Tasks have
// successfully aborted, we're only left with the correct amount of references
// to the Merge task.
TEST_P(MultiFragmentTest, abortMergeExchange) {
  setupSources(20, 1000);

  std::vector<std::shared_ptr<Task>> tasks;

  std::vector<std::shared_ptr<TempFilePath>> filePaths0(
      filePaths_.begin(), filePaths_.begin() + 10);
  std::vector<std::shared_ptr<TempFilePath>> filePaths1(
      filePaths_.begin() + 10, filePaths_.end());

  std::vector<std::vector<std::shared_ptr<TempFilePath>>> filePathsList = {
      filePaths0, filePaths1};

  std::vector<std::string> partialSortTaskIds;
  RowTypePtr outputType;

  core::PlanNodeId partitionNodeId;
  auto executor = folly::CPUThreadPoolExecutor(4, 4);
  for (int i = 0; i < 2; ++i) {
    auto sortTaskId = makeTaskId("orderby", static_cast<int>(tasks.size()));
    partialSortTaskIds.push_back(sortTaskId);
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto partialSortPlan =
        PlanBuilder(planNodeIdGenerator)
            .localMerge(
                {"c0"},
                {PlanBuilder(planNodeIdGenerator)
                     .tableScan(rowType_)
                     .orderBy({"c0"}, true)
                     .planNode()})
            .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam().serdeKind)
            .capturePlanNodeId(partitionNodeId)
            .planNode();

    auto sortTask = makeTask(
        sortTaskId,
        partialSortPlan,
        static_cast<int>(tasks.size()),
        nullptr,
        memory::kMaxMemory,
        &executor);
    tasks.push_back(sortTask);
    sortTask->start(4);
    addHiveSplits(sortTask, filePathsList[i]);
    outputType = partialSortPlan->outputType();
  }

  auto finalSortTaskId = makeTaskId("orderby", static_cast<int>(tasks.size()));
  core::PlanNodeId mergeExchangeId;
  auto finalSortPlan =
      PlanBuilder()
          .mergeExchange(outputType, {"c0"}, GetParam().serdeKind)
          .capturePlanNodeId(mergeExchangeId)
          .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam().serdeKind)
          .planNode();
  auto mergeTask = makeTask(finalSortTaskId, finalSortPlan, 0);
  tasks.push_back(mergeTask);
  mergeTask->start(1);
  addRemoteSplits(mergeTask, partialSortTaskIds);

  for (auto& task : tasks) {
    task->requestAbort();
    ASSERT_TRUE(waitForTaskAborted(task.get())) << task->taskId();
  }

  // Ensure that the threads in the executor can gracefully join
  executor.join();

  // Wait till all the terminations, closures and destructions so that
  // the reference count drops to 2.
  while (mergeTask.use_count() > 2) {
    std::this_thread::yield();
  }

  // The references to mergeTask should be two, one for the local variable
  // itself and one reference inside tasks variable.
  EXPECT_EQ(mergeTask.use_count(), 2);
}

TEST_P(MultiFragmentTest, mergeExchange) {
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

  core::PlanNodeId partitionNodeId;
  for (int i = 0; i < 2; ++i) {
    auto sortTaskId = makeTaskId("orderby", tasks.size());
    partialSortTaskIds.push_back(sortTaskId);
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto partialSortPlan =
        PlanBuilder(planNodeIdGenerator)
            .localMerge(
                {"c0"},
                {PlanBuilder(planNodeIdGenerator)
                     .tableScan(rowType_)
                     .orderBy({"c0"}, true)
                     .planNode()})
            .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam().serdeKind)
            .capturePlanNodeId(partitionNodeId)
            .planNode();

    auto sortTask = makeTask(sortTaskId, partialSortPlan, tasks.size());
    tasks.push_back(sortTask);
    sortTask->start(4);
    addHiveSplits(sortTask, filePathsList[i]);
    outputType = partialSortPlan->outputType();
  }

  auto finalSortTaskId = makeTaskId("orderby", tasks.size());
  core::PlanNodeId mergeExchangeId;
  auto finalSortPlan =
      PlanBuilder()
          .mergeExchange(outputType, {"c0"}, GetParam().serdeKind)
          .capturePlanNodeId(mergeExchangeId)
          .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam().serdeKind)
          .planNode();

  auto mergeTask = makeTask(finalSortTaskId, finalSortPlan, 0);
  tasks.push_back(mergeTask);
  mergeTask->start(1);
  addRemoteSplits(mergeTask, partialSortTaskIds);

  auto op = PlanBuilder().exchange(outputType, GetParam().serdeKind).planNode();

  test::AssertQueryBuilder(op, duckDbQueryRunner_)
      .split(remoteSplit(finalSortTaskId))
      .config(
          core::QueryConfig::kShuffleCompressionKind,
          common::compressionKindToString(GetParam().compressionKind))
      .assertResults(
          "SELECT * FROM tmp ORDER BY 1 NULLS LAST", std::vector<uint32_t>{0});

  for (auto& task : tasks) {
    ASSERT_TRUE(waitForTaskCompletion(task.get())) << task->taskId();
  }

  const auto finalSortStats = toPlanStats(mergeTask->taskStats());
  const auto& mergeExchangeStats = finalSortStats.at(mergeExchangeId);

  EXPECT_EQ(20'000, mergeExchangeStats.inputRows);
  EXPECT_EQ(20'000, mergeExchangeStats.rawInputRows);

  EXPECT_LT(0, mergeExchangeStats.inputBytes);
  EXPECT_LT(0, mergeExchangeStats.rawInputBytes);

  const auto serdeKindRuntimsStats =
      mergeExchangeStats.customStats.at(Operator::kShuffleSerdeKind);
  ASSERT_EQ(serdeKindRuntimsStats.count, 1);
  ASSERT_EQ(
      serdeKindRuntimsStats.min, static_cast<int64_t>(GetParam().serdeKind));
  ASSERT_EQ(
      serdeKindRuntimsStats.max, static_cast<int64_t>(GetParam().serdeKind));
}

// Test reordering and dropping columns in PartitionedOutput operator.
TEST_P(MultiFragmentTest, partitionedOutput) {
  setupSources(10, 1000);

  // Test dropping columns only
  {
    auto leafTaskId = makeTaskId("leaf", 0);
    auto leafPlan =
        PlanBuilder()
            .values(vectors_)
            .partitionedOutput({}, 1, {"c0", "c1"}, GetParam().serdeKind)
            .planNode();
    auto leafTask = makeTask(leafTaskId, leafPlan, 0);
    leafTask->start(4);
    auto op = PlanBuilder()
                  .exchange(leafPlan->outputType(), GetParam().serdeKind)
                  .planNode();

    test::AssertQueryBuilder(op, duckDbQueryRunner_)
        .split(remoteSplit(leafTaskId))
        .config(
            core::QueryConfig::kShuffleCompressionKind,
            common::compressionKindToString(GetParam().compressionKind))
        .assertResults("SELECT c0, c1 FROM tmp");

    ASSERT_TRUE(waitForTaskCompletion(leafTask.get())) << leafTask->taskId();
  }

  // Test reordering and dropping at the same time
  {
    auto leafTaskId = makeTaskId("leaf", 0);
    auto leafPlan =
        PlanBuilder()
            .values(vectors_)
            .partitionedOutput({}, 1, {"c3", "c0", "c2"}, GetParam().serdeKind)
            .planNode();
    auto leafTask = makeTask(leafTaskId, leafPlan, 0);
    leafTask->start(4);
    auto op = PlanBuilder()
                  .exchange(leafPlan->outputType(), GetParam().serdeKind)
                  .planNode();

    test::AssertQueryBuilder(op, duckDbQueryRunner_)
        .split(remoteSplit(leafTaskId))
        .config(
            core::QueryConfig::kShuffleCompressionKind,
            common::compressionKindToString(GetParam().compressionKind))
        .assertResults("SELECT c3, c0, c2 FROM tmp");

    ASSERT_TRUE(waitForTaskCompletion(leafTask.get())) << leafTask->taskId();
  }

  // Test producing duplicate columns
  {
    auto leafTaskId = makeTaskId("leaf", 0);
    auto leafPlan =
        PlanBuilder()
            .values(vectors_)
            .partitionedOutput(
                {},
                1,
                {"c0", "c1", "c2", "c3", "c4", "c3", "c2", "c1", "c0"},
                GetParam().serdeKind)
            .planNode();
    auto leafTask = makeTask(leafTaskId, leafPlan, 0);
    leafTask->start(4);
    auto op = PlanBuilder()
                  .exchange(leafPlan->outputType(), GetParam().serdeKind)
                  .planNode();

    test::AssertQueryBuilder(op, duckDbQueryRunner_)
        .split(remoteSplit(leafTaskId))
        .config(
            core::QueryConfig::kShuffleCompressionKind,
            common::compressionKindToString(GetParam().compressionKind))
        .assertResults("SELECT c0, c1, c2, c3, c4, c3, c2, c1, c0 FROM tmp");

    ASSERT_TRUE(waitForTaskCompletion(leafTask.get())) << leafTask->taskId();
  }

  // Test dropping the partitioning key
  {
    constexpr int32_t kFanout = 4;
    auto leafTaskId = makeTaskId("leaf", 0);
    auto leafPlan =
        PlanBuilder()
            .values(vectors_)
            .partitionedOutput(
                {"c5"}, kFanout, {"c2", "c0", "c3"}, GetParam().serdeKind)
            .planNode();
    auto leafTask = makeTask(leafTaskId, leafPlan, 0);
    leafTask->start(4);

    auto intermediatePlan =
        PlanBuilder()
            .exchange(leafPlan->outputType(), GetParam().serdeKind)
            .partitionedOutput({}, 1, {"c3", "c0", "c2"}, GetParam().serdeKind)
            .planNode();
    std::vector<std::string> intermediateTaskIds;
    for (auto i = 0; i < kFanout; ++i) {
      intermediateTaskIds.push_back(makeTaskId("intermediate", i));
      auto intermediateTask =
          makeTask(intermediateTaskIds.back(), intermediatePlan, i);
      intermediateTask->start(1);
      addRemoteSplits(intermediateTask, {leafTaskId});
    }

    auto op =
        PlanBuilder()
            .exchange(intermediatePlan->outputType(), GetParam().serdeKind)
            .planNode();

    std::vector<Split> intermediateSplits;
    for (auto intermediateTaskId : intermediateTaskIds) {
      intermediateSplits.emplace_back(remoteSplit(intermediateTaskId));
    }
    auto task =
        test::AssertQueryBuilder(op, duckDbQueryRunner_)
            .splits(std::move(intermediateSplits))
            .config(
                core::QueryConfig::kShuffleCompressionKind,
                common::compressionKindToString(GetParam().compressionKind))
            .assertResults("SELECT c3, c0, c2 FROM tmp");

    verifyExchangeStats(task, kFanout, kFanout);

    ASSERT_TRUE(waitForTaskCompletion(leafTask.get())) << leafTask->taskId();
  }

  // Test dropping all columns.
  {
    auto leafTaskId = makeTaskId("leaf", 0);
    auto leafPlan =
        PlanBuilder()
            .values(vectors_)
            .addNode(
                [](std::string nodeId,
                   core::PlanNodePtr source) -> core::PlanNodePtr {
                  return core::PartitionedOutputNode::broadcast(
                      nodeId, 1, ROW({}), GetParam().serdeKind, source);
                })
            .planNode();
    auto leafTask = makeTask(leafTaskId, leafPlan, 0);
    leafTask->start(4);
    leafTask->updateOutputBuffers(1, true);

    auto op = PlanBuilder()
                  .exchange(leafPlan->outputType(), GetParam().serdeKind)
                  .planNode();

    vector_size_t numRows = 0;
    for (const auto& vector : vectors_) {
      numRows += vector->size();
    }

    auto result =
        AssertQueryBuilder(op)
            .split(remoteSplit(leafTaskId))
            .config(
                core::QueryConfig::kShuffleCompressionKind,
                common::compressionKindToString(GetParam().compressionKind))
            .copyResults(pool());
    ASSERT_EQ(*result->type(), *ROW({}));
    ASSERT_EQ(result->size(), numRows);

    ASSERT_TRUE(waitForTaskCompletion(leafTask.get())) << leafTask->taskId();
  }

  // Test asynchronously deleting task buffer (due to abort from downstream).
  {
    auto leafTaskId = makeTaskId("leaf", 0);
    auto leafPlan =
        PlanBuilder()
            .values(vectors_)
            .partitionedOutput({}, 1, {"c0", "c1"}, GetParam().serdeKind)
            .planNode();
    auto leafTask = makeTask(leafTaskId, leafPlan, 0);
    leafTask->start(4);
    // Delete the results asynchronously to simulate abort from downstream.
    bufferManager_->deleteResults(leafTaskId, 0);

    ASSERT_TRUE(waitForTaskCompletion(leafTask.get())) << leafTask->taskId();
  }
}

TEST_P(MultiFragmentTest, noHashPartitionSkew) {
  setupSources(10, 1000);

  // Update the key column.
  int count{0};
  for (auto& vector : vectors_) {
    vector->childAt(0) = makeFlatVector<int64_t>(
        vector->childAt(0)->size(), [&](auto /*unused*/) { return count++; });
  };

  // Test dropping columns only.
  const int numPartitions{8};
  auto producerTaskId = makeTaskId("producer", 0);
  auto producerPlan =
      PlanBuilder()
          .values(vectors_)
          .partitionedOutput(
              {"c0"}, numPartitions, {"c0", "c1"}, GetParam().serdeKind)
          .planNode();
  auto producerTask = makeTask(producerTaskId, producerPlan, 0);
  producerTask->start(1);

  core::PlanNodeId partialAggregationNodeId;
  auto consumerPlan =
      PlanBuilder()
          .exchange(producerPlan->outputType(), GetParam().serdeKind)
          .localPartition({"c0"})
          .partialAggregation({"c0"}, {"count(1)"})
          .capturePlanNodeId(partialAggregationNodeId)
          .localPartition({})
          .finalAggregation()
          .singleAggregation({}, {"sum(1)"})
          .planNode();

  // This is computed based offline and shouldn't change across runs.
  const std::vector<int> expectedValues{
      1'189, 1'266, 1'274, 1'228, 1'250, 1'225, 1'308, 1'260};

  const int numConsumerDriverThreads{4};
  const auto runConsumer = [&](int partition) {
    const auto expectedResult = makeRowVector({makeFlatVector<int64_t>(
        std::vector<int64_t>{expectedValues[partition]})});
    SCOPED_TRACE(fmt::format("partition {}", partition));
    auto consumerTask =
        test::AssertQueryBuilder(consumerPlan)
            .split(remoteSplit(producerTaskId))
            .destination(partition)
            .config(
                core::QueryConfig::kShuffleCompressionKind,
                common::compressionKindToString(GetParam().compressionKind))
            .maxDrivers(numConsumerDriverThreads)
            .assertResults(expectedResult);

    // Verifies that each partial aggregation operator process a number of
    // inputs.
    auto consumerTaskStats = exec::toPlanStats(consumerTask->taskStats());
    const auto& partialAggregationNodeStats =
        consumerTaskStats.at(partialAggregationNodeId);
    ASSERT_EQ(
        partialAggregationNodeStats.customStats.at("hashtable.numDistinct")
            .count,
        numConsumerDriverThreads);
    ASSERT_GT(
        partialAggregationNodeStats.customStats.at("hashtable.numDistinct").min,
        0);
    ASSERT_GT(
        partialAggregationNodeStats.customStats.at("hashtable.numDistinct").max,
        0);
  };

  std::vector<std::thread> consumerThreads;
  for (int partition = 0; partition < numPartitions; ++partition) {
    consumerThreads.emplace_back([&, partition]() { runConsumer(partition); });
  }

  for (auto& consumerThread : consumerThreads) {
    consumerThread.join();
  }
}

TEST_P(MultiFragmentTest, noHivePartitionSkew) {
  setupSources(10, 1000);

  // Update the key column.
  int count{0};
  for (auto& vector : vectors_) {
    vector->childAt(0) = makeFlatVector<int64_t>(
        vector->childAt(0)->size(), [&](auto /*unused*/) { return count++; });
  };

  // Test dropping columns only.
  const int numBuckets = 256;
  const int numPartitions{8};
  auto producerTaskId = makeTaskId("producer", 0);
  auto producerPlan =
      PlanBuilder()
          .values(vectors_)
          .partitionedOutput(
              {"c0"},
              numPartitions,
              false,
              std::make_shared<connector::hive::HivePartitionFunctionSpec>(
                  numBuckets,
                  std::vector<column_index_t>{0},
                  std::vector<VectorPtr>{}),
              {"c0", "c1"},
              GetParam().serdeKind)
          .planNode();
  auto producerTask = makeTask(producerTaskId, producerPlan, 0);
  producerTask->start(1);

  core::PlanNodeId partialAggregationNodeId;
  auto consumerPlan =
      PlanBuilder()
          .exchange(producerPlan->outputType(), GetParam().serdeKind)
          .localPartition(numBuckets, {0}, {})
          .partialAggregation({"c0"}, {"count(1)"})
          .capturePlanNodeId(partialAggregationNodeId)
          .localPartition({})
          .finalAggregation()
          .singleAggregation({}, {"sum(1)"})
          .planNode();

  const int numConsumerDriverThreads{4};
  const auto runConsumer = [&](int partition) {
    // Hive partition evenly distribute rows across nodes.
    const auto expectedResult =
        makeRowVector({makeFlatVector<int64_t>(std::vector<int64_t>{1'250})});
    SCOPED_TRACE(fmt::format("partition {}", partition));
    auto consumerTask =
        test::AssertQueryBuilder(consumerPlan)
            .split(remoteSplit(producerTaskId))
            .destination(partition)
            .config(
                core::QueryConfig::kShuffleCompressionKind,
                common::compressionKindToString(GetParam().compressionKind))
            .maxDrivers(numConsumerDriverThreads)
            .assertResults(expectedResult);

    // Verifies that each partial aggregation operator process a number of
    // inputs.
    auto consumerTaskStats = exec::toPlanStats(consumerTask->taskStats());
    const auto& partialAggregationNodeStats =
        consumerTaskStats.at(partialAggregationNodeId);
    ASSERT_EQ(
        partialAggregationNodeStats.customStats.at("hashtable.numDistinct")
            .count,
        numConsumerDriverThreads);
    ASSERT_GT(
        partialAggregationNodeStats.customStats.at("hashtable.numDistinct").min,
        0);
    ASSERT_GT(
        partialAggregationNodeStats.customStats.at("hashtable.numDistinct").max,
        0);
  };

  std::vector<std::thread> consumerThreads;
  for (int partition = 0; partition < numPartitions; ++partition) {
    consumerThreads.emplace_back([&, partition]() { runConsumer(partition); });
  }

  for (auto& consumerThread : consumerThreads) {
    consumerThread.join();
  }
}

TEST_P(MultiFragmentTest, partitionedOutputWithLargeInput) {
  // Verify that partitionedOutput operator is able to split a single input
  // vector if it hits memory or row limits.
  // We create a large vector that hits the row limit (70% - 120% of 10,000).
  // This test exercises splitting up the input both from the edges and the
  // middle as it ends up splitting it into at least 3.
  setupSources(1, 30'000);
  // Single Partition
  {
    auto leafTaskId = makeTaskId("leaf", 0);
    auto leafPlan =
        PlanBuilder()
            .values(vectors_)
            .partitionedOutput(
                {}, 1, {"c0", "c1", "c2", "c3", "c4"}, GetParam().serdeKind)
            .planNode();
    auto leafTask = makeTask(leafTaskId, leafPlan, 0, nullptr, 4 << 20);
    leafTask->start(1);
    auto op = PlanBuilder()
                  .exchange(leafPlan->outputType(), GetParam().serdeKind)
                  .planNode();

    auto task =
        test::AssertQueryBuilder(op, duckDbQueryRunner_)
            .split(remoteSplit(leafTaskId))
            .config(
                core::QueryConfig::kShuffleCompressionKind,
                common::compressionKindToString(GetParam().compressionKind))
            .assertResults("SELECT c0, c1, c2, c3, c4 FROM tmp");
    ASSERT_TRUE(waitForTaskCompletion(leafTask.get()))
        << leafTask->taskId() << "state: " << leafTask->state();
    auto taskStats = toPlanStats(leafTask->taskStats());
    ASSERT_GT(taskStats.at("1").outputVectors, 2);
  }

  // Multiple partitions but round-robin.
  {
    constexpr int32_t kFanout = 2;
    auto leafTaskId = makeTaskId("leaf", 0);
    auto leafPlan =
        PlanBuilder()
            .values(vectors_)
            .partitionedOutput(
                {},
                kFanout,
                false,
                std::make_shared<exec::RoundRobinPartitionFunctionSpec>(),
                {"c0", "c1", "c2", "c3", "c4"},
                GetParam().serdeKind)
            .planNode();
    auto leafTask = makeTask(leafTaskId, leafPlan, 0);
    leafTask->start(1);

    auto intermediatePlan =
        PlanBuilder()
            .exchange(leafPlan->outputType(), GetParam().serdeKind)
            .partitionedOutput(
                {}, 1, {"c0", "c1", "c2", "c3", "c4"}, GetParam().serdeKind)
            .planNode();
    std::vector<std::string> intermediateTaskIds;
    for (auto i = 0; i < kFanout; ++i) {
      intermediateTaskIds.push_back(makeTaskId("intermediate", i));
      auto intermediateTask =
          makeTask(intermediateTaskIds.back(), intermediatePlan, i);
      intermediateTask->start(1);
      addRemoteSplits(intermediateTask, {leafTaskId});
    }

    auto op =
        PlanBuilder()
            .exchange(intermediatePlan->outputType(), GetParam().serdeKind)
            .planNode();

    std::vector<Split> intermediateSplits;
    for (auto intermediateTaskId : intermediateTaskIds) {
      intermediateSplits.emplace_back(remoteSplit(intermediateTaskId));
    }
    auto task =
        test::AssertQueryBuilder(op, duckDbQueryRunner_)
            .splits(std::move(intermediateSplits))
            .config(
                core::QueryConfig::kShuffleCompressionKind,
                common::compressionKindToString(GetParam().compressionKind))
            .assertResults("SELECT c0, c1, c2, c3, c4 FROM tmp");
    ASSERT_TRUE(waitForTaskCompletion(leafTask.get()))
        << "state: " << leafTask->state();
    auto taskStats = toPlanStats(leafTask->taskStats());
    ASSERT_GT(taskStats.at("1").outputVectors, 2);
  }
}

TEST_P(MultiFragmentTest, broadcast) {
  auto data = makeRowVector(
      {makeFlatVector<int32_t>(1'000, [](auto row) { return row; })});

  // Make leaf task: Values -> Repartitioning (broadcast)
  std::vector<std::shared_ptr<Task>> tasks;
  auto leafTaskId = makeTaskId("leaf", 0);
  auto leafPlan = PlanBuilder()
                      .values({data})
                      .partitionedOutputBroadcast(
                          /*outputLayout=*/{}, GetParam().serdeKind)
                      .planNode();
  auto leafTask = makeTask(leafTaskId, leafPlan, 0);
  tasks.emplace_back(leafTask);
  leafTask->start(1);

  // Make next stage tasks.
  core::PlanNodePtr finalAggPlan;
  std::vector<std::string> finalAggTaskIds;
  for (int i = 0; i < 3; i++) {
    finalAggPlan =
        PlanBuilder()
            .exchange(leafPlan->outputType(), GetParam().serdeKind)
            .singleAggregation({}, {"count(1)"})
            .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam().serdeKind)
            .planNode();

    finalAggTaskIds.push_back(makeTaskId("final-agg", i));
    auto task = makeTask(finalAggTaskIds.back(), finalAggPlan, i);
    tasks.emplace_back(task);
    task->start(1);
    leafTask->updateOutputBuffers(i + 1, false);
    addRemoteSplits(task, {leafTaskId});
  }
  leafTask->updateOutputBuffers(finalAggTaskIds.size(), true);

  // Collect results from multiple tasks.
  auto op = PlanBuilder()
                .exchange(finalAggPlan->outputType(), GetParam().serdeKind)
                .planNode();

  std::vector<Split> finalAggTaskSplits;
  for (auto finalAggTaskId : finalAggTaskIds) {
    finalAggTaskSplits.emplace_back(remoteSplit(finalAggTaskId));
  }
  test::AssertQueryBuilder(op, duckDbQueryRunner_)
      .splits(std::move(finalAggTaskSplits))
      .config(
          core::QueryConfig::kShuffleCompressionKind,
          common::compressionKindToString(GetParam().compressionKind))
      .assertResults("SELECT UNNEST(array[1000, 1000, 1000])");

  for (auto& task : tasks) {
    ASSERT_TRUE(waitForTaskCompletion(task.get())) << task->taskId();
  }

  // Make sure duplicate 'updateOutputBuffers' message after task
  // completion doesn't cause an error.
  leafTask->updateOutputBuffers(finalAggTaskIds.size(), true);
}

TEST_P(MultiFragmentTest, roundRobinPartition) {
  auto data = {
      makeRowVector({
          makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
      }),
      makeRowVector({
          makeFlatVector<int64_t>({6, 7, 8}),
      }),
      makeRowVector({
          makeFlatVector<int64_t>({9, 10}),
      }),
  };

  createDuckDbTable(data);

  // Make leaf task: Values -> Round-robin repartitioning (2-way).
  auto leafTaskId = makeTaskId("leaf", 0);
  auto leafPlan =
      PlanBuilder()
          .values(data)
          .partitionedOutput(
              {},
              2,
              false,
              std::make_shared<exec::RoundRobinPartitionFunctionSpec>(),
              /*outputLayout=*/{},
              GetParam().serdeKind)
          .planNode();
  auto leafTask = makeTask(leafTaskId, leafPlan, 0);

  std::vector<std::shared_ptr<Task>> tasks;
  auto addTask = [&](std::shared_ptr<Task> task,
                     const std::vector<std::string>& remoteTaskIds) {
    tasks.emplace_back(task);
    task->start(1);
    if (!remoteTaskIds.empty()) {
      addRemoteSplits(task, remoteTaskIds);
    }
  };

  addTask(leafTask, {});

  // Collect result from 2 output buffers.
  core::PlanNodePtr collectPlan;
  std::vector<std::string> collectTaskIds;
  for (int i = 0; i < 2; i++) {
    collectPlan =
        PlanBuilder()
            .exchange(leafPlan->outputType(), GetParam().serdeKind)
            .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam().serdeKind)
            .planNode();

    collectTaskIds.push_back(makeTaskId("collect", i));
    auto task = makeTask(collectTaskIds.back(), collectPlan, i);
    addTask(task, {leafTaskId});
  }

  // Collect everything.
  auto finalPlan = PlanBuilder()
                       .exchange(leafPlan->outputType(), GetParam().serdeKind)
                       .planNode();

  std::vector<Split> collectTaskSplits;
  for (auto collectTaskId : collectTaskIds) {
    collectTaskSplits.emplace_back(remoteSplit(collectTaskId));
  }
  test::AssertQueryBuilder(finalPlan, duckDbQueryRunner_)
      .splits(std::move(collectTaskSplits))
      .config(
          core::QueryConfig::kShuffleCompressionKind,
          common::compressionKindToString(GetParam().compressionKind))
      .assertResults("SELECT * FROM tmp");

  for (auto& task : tasks) {
    ASSERT_TRUE(waitForTaskCompletion(task.get())) << task->taskId();
  }
}

// Test PartitionedOutput operator with constant partitioning keys.
TEST_P(MultiFragmentTest, constantKeys) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>(
          1'000, [](auto row) { return row; }, nullEvery(7)),
  });

  std::vector<std::shared_ptr<Task>> tasks;
  auto addTask = [&](std::shared_ptr<Task> task,
                     const std::vector<std::string>& remoteTaskIds) {
    tasks.emplace_back(task);
    task->start(1);
    if (!remoteTaskIds.empty()) {
      addRemoteSplits(task, remoteTaskIds);
    }
  };

  // Make leaf task: Values -> Repartitioning (3-way)
  auto leafTaskId = makeTaskId("leaf", 0);
  auto leafPlan = PlanBuilder()
                      .values({data})
                      .partitionedOutput(
                          {"c0", "123"}, 3, true, {"c0"}, GetParam().serdeKind)
                      .planNode();
  auto leafTask = makeTask(leafTaskId, leafPlan, 0);
  addTask(leafTask, {});

  // Make next stage tasks to count nulls.
  core::PlanNodePtr finalAggPlan;
  std::vector<std::string> finalAggTaskIds;
  for (int i = 0; i < 3; i++) {
    finalAggPlan =
        PlanBuilder()
            .exchange(leafPlan->outputType(), GetParam().serdeKind)
            .project({"c0 is null AS co_is_null"})
            .partialAggregation({}, {"count_if(co_is_null)", "count(1)"})
            .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam().serdeKind)
            .planNode();

    finalAggTaskIds.push_back(makeTaskId("final-agg", i));
    auto task = makeTask(finalAggTaskIds.back(), finalAggPlan, i);
    addTask(task, {leafTaskId});
  }

  // Collect results and verify number of nulls is 3 times larger than in the
  // original data.
  auto op = PlanBuilder()
                .exchange(finalAggPlan->outputType(), GetParam().serdeKind)
                .finalAggregation(
                    {}, {"sum(a0)", "sum(a1)"}, {{BIGINT()}, {BIGINT()}})
                .planNode();

  std::vector<Split> finalAggTaskSplits;
  for (auto finalAggTaskId : finalAggTaskIds) {
    finalAggTaskSplits.emplace_back(remoteSplit(finalAggTaskId));
  }
  test::AssertQueryBuilder(op, duckDbQueryRunner_)
      .splits(std::move(finalAggTaskSplits))
      .config(
          core::QueryConfig::kShuffleCompressionKind,
          common::compressionKindToString(GetParam().compressionKind))
      .assertResults(
          "SELECT 3 * ceil(1000.0 / 7) /* number of null rows */, 1000 + 2 * ceil(1000.0 / 7) /* total number of rows */");

  for (auto& task : tasks) {
    ASSERT_TRUE(waitForTaskCompletion(task.get())) << task->taskId();
  }
}

TEST_P(MultiFragmentTest, replicateNullsAndAny) {
  auto data = makeRowVector({makeFlatVector<int32_t>(
      1'000, [](auto row) { return row; }, nullEvery(7))});

  std::vector<std::shared_ptr<Task>> tasks;
  auto addTask = [&](std::shared_ptr<Task> task,
                     const std::vector<std::string>& remoteTaskIds) {
    tasks.emplace_back(task);
    task->start(1);
    if (!remoteTaskIds.empty()) {
      addRemoteSplits(task, remoteTaskIds);
    }
  };

  // Make leaf task: Values -> Repartitioning (3-way)
  auto leafTaskId = makeTaskId("leaf", 0);
  auto leafPlan =
      PlanBuilder()
          .values({data})
          .partitionedOutput(
              {"c0"}, 3, true, /*outputLayout=*/{}, GetParam().serdeKind)
          .planNode();
  auto leafTask = makeTask(leafTaskId, leafPlan, 0);
  addTask(leafTask, {});

  // Make next stage tasks to count nulls.
  core::PlanNodePtr finalAggPlan;
  std::vector<std::string> finalAggTaskIds;
  for (int i = 0; i < 3; i++) {
    finalAggPlan =
        PlanBuilder()
            .exchange(leafPlan->outputType(), GetParam().serdeKind)
            .project({"c0 is null AS co_is_null"})
            .partialAggregation({}, {"count_if(co_is_null)", "count(1)"})
            .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam().serdeKind)
            .planNode();

    finalAggTaskIds.push_back(makeTaskId("final-agg", i));
    auto task = makeTask(finalAggTaskIds.back(), finalAggPlan, i);
    addTask(task, {leafTaskId});
  }

  // Collect results and verify number of nulls is 3 times larger than in the
  // original data.
  auto op = PlanBuilder()
                .exchange(finalAggPlan->outputType(), GetParam().serdeKind)
                .finalAggregation(
                    {}, {"sum(a0)", "sum(a1)"}, {{BIGINT()}, {BIGINT()}})
                .planNode();

  std::vector<Split> finalAggTaskSplits;
  for (auto finalAggTaskId : finalAggTaskIds) {
    finalAggTaskSplits.emplace_back(remoteSplit(finalAggTaskId));
  }
  test::AssertQueryBuilder(op, duckDbQueryRunner_)
      .splits(std::move(finalAggTaskSplits))
      .config(
          core::QueryConfig::kShuffleCompressionKind,
          common::compressionKindToString(GetParam().compressionKind))
      .assertResults(
          "SELECT 3 * ceil(1000.0 / 7) /* number of null rows */, 1000 + 2 * ceil(1000.0 / 7) /* total number of rows */");

  for (auto& task : tasks) {
    ASSERT_TRUE(waitForTaskCompletion(task.get())) << task->taskId();
  }
}

// Test query finishing before all splits have been scheduled.
TEST_P(MultiFragmentTest, limit) {
  auto data = makeRowVector({makeFlatVector<int32_t>(
      1'000, [](auto row) { return row; }, nullEvery(7))});

  auto file = TempFilePath::create();
  writeToFile(file->getPath(), {data});

  // Make leaf task: Values -> PartialLimit(10) -> Repartitioning(0).
  auto leafTaskId = makeTaskId("leaf", 0);
  auto leafPlan =
      PlanBuilder()
          .tableScan(std::dynamic_pointer_cast<const RowType>(data->type()))
          .limit(0, 10, true)
          .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam().serdeKind)
          .planNode();
  auto leafTask = makeTask(leafTaskId, leafPlan, 0);
  leafTask->start(1);

  leafTask.get()->addSplit(
      "0", exec::Split(makeHiveConnectorSplit(file->getPath())));

  // Make final task: Exchange -> FinalLimit(10).
  auto plan = PlanBuilder()
                  .exchange(leafPlan->outputType(), GetParam().serdeKind)
                  .localPartition(std::vector<std::string>{})
                  .limit(0, 10, false)
                  .planNode();

  // Expect the task to produce results before receiving no-more-splits message.
  auto task =
      test::AssertQueryBuilder(plan, duckDbQueryRunner_)
          .split(remoteSplit(leafTaskId))
          .config(
              core::QueryConfig::kShuffleCompressionKind,
              common::compressionKindToString(GetParam().compressionKind))
          .assertResults(
              "VALUES (null), (1), (2), (3), (4), (5), (6), (null), (8), (9)");
  ASSERT_TRUE(waitForTaskCompletion(task.get())) << task->taskId();
  ASSERT_TRUE(waitForTaskCompletion(leafTask.get())) << leafTask->taskId();
}

TEST_P(MultiFragmentTest, mergeExchangeOverEmptySources) {
  std::vector<std::shared_ptr<Task>> tasks;
  std::vector<std::string> leafTaskIds;

  auto data = makeRowVector(rowType_, 0);

  for (int i = 0; i < 2; ++i) {
    auto taskId = makeTaskId("leaf-", i);
    leafTaskIds.push_back(taskId);
    auto plan =
        PlanBuilder()
            .values({data})
            .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam().serdeKind)
            .planNode();

    auto task = makeTask(taskId, plan, tasks.size());
    tasks.push_back(task);
    task->start(4);
  }

  auto exchangeTaskId = makeTaskId("exchange-", 0);
  auto plan = PlanBuilder()
                  .mergeExchange(rowType_, {"c0"}, GetParam().serdeKind)
                  .singleAggregation({"c0"}, {"count(1)"})
                  .planNode();

  std::vector<Split> leafTaskSplits;
  for (auto leafTaskId : leafTaskIds) {
    leafTaskSplits.emplace_back(remoteSplit(leafTaskId));
  }
  test::AssertQueryBuilder(plan, duckDbQueryRunner_)
      .splits(std::move(leafTaskSplits))
      .config(
          core::QueryConfig::kShuffleCompressionKind,
          common::compressionKindToString(GetParam().compressionKind))
      .assertResults("");

  for (auto& task : tasks) {
    ASSERT_TRUE(waitForTaskCompletion(task.get())) << task->taskId();
  }
}

namespace {
core::PlanNodePtr makeJoinOverExchangePlan(
    const RowTypePtr& exchangeType,
    const RowVectorPtr& buildData,
    VectorSerde::Kind serdeKind) {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  return PlanBuilder(planNodeIdGenerator)
      .exchange(exchangeType, serdeKind)
      .hashJoin(
          {"c0"},
          {"u_c0"},
          PlanBuilder(planNodeIdGenerator).values({buildData}).planNode(),
          "",
          {"c0"})
      .partitionedOutput({}, 1, /*outputLayout=*/{}, serdeKind)
      .planNode();
}
} // namespace

TEST_P(MultiFragmentTest, earlyCompletion) {
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
                  .partitionedOutput(
                      {"c0"}, 2, /*outputLayout=*/{}, GetParam().serdeKind)
                  .planNode();

  auto task = makeTask(leafTaskId, plan, tasks.size());
  tasks.push_back(task);
  task->start(1);

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

    auto joinPlan = makeJoinOverExchangePlan(
        asRowType(data->type()), buildData, GetParam().serdeKind);

    joinOutputType = joinPlan->outputType();

    auto taskId = makeTaskId("join", i);
    joinTaskIds.push_back(taskId);

    auto task = makeTask(taskId, joinPlan, i);
    tasks.push_back(task);
    task->start(4);

    addRemoteSplits(task, {leafTaskId});
  }

  // Create output task.
  auto outputPlan =
      PlanBuilder().exchange(joinOutputType, GetParam().serdeKind).planNode();

  std::vector<Split> joinTaskSplits;
  for (auto joinTaskId : joinTaskIds) {
    joinTaskSplits.emplace_back(remoteSplit(joinTaskId));
  }
  test::AssertQueryBuilder(outputPlan, duckDbQueryRunner_)
      .splits(std::move(joinTaskSplits))
      .config(
          core::QueryConfig::kShuffleCompressionKind,
          common::compressionKindToString(GetParam().compressionKind))
      .assertResults("SELECT UNNEST([3, 3, 3, 3, 4, 4, 4, 4])");

  for (auto& task : tasks) {
    ASSERT_TRUE(waitForTaskCompletion(task.get())) << task->taskId();
  }
}

TEST_P(MultiFragmentTest, earlyCompletionBroadcast) {
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
                  .partitionedOutputBroadcast(
                      /*outputLayout=*/{}, GetParam().serdeKind)
                  .planNode();

  auto leafTask = makeTask(leafTaskId, plan, tasks.size());
  tasks.push_back(leafTask);
  leafTask->start(1);

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

    auto joinPlan = makeJoinOverExchangePlan(
        asRowType(data->type()), buildData, GetParam().serdeKind);

    joinOutputType = joinPlan->outputType();

    auto taskId = makeTaskId("join", i);
    joinTaskIds.push_back(taskId);

    auto task = makeTask(taskId, joinPlan, i);
    tasks.push_back(task);
    task->start(4);

    leafTask->updateOutputBuffers(i + 1, false);

    addRemoteSplits(task, {leafTaskId});
  }
  leafTask->updateOutputBuffers(joinTaskIds.size(), true);

  // Create output task.
  auto outputPlan =
      PlanBuilder().exchange(joinOutputType, GetParam().serdeKind).planNode();

  std::vector<Split> joinTaskSplits;
  for (auto joinTaskId : joinTaskIds) {
    joinTaskSplits.emplace_back(remoteSplit(joinTaskId));
  }
  test::AssertQueryBuilder(outputPlan, duckDbQueryRunner_)
      .splits(std::move(joinTaskSplits))
      .config(
          core::QueryConfig::kShuffleCompressionKind,
          common::compressionKindToString(GetParam().compressionKind))
      .assertResults("SELECT UNNEST([10, 10, 10, 10])");

  for (auto& task : tasks) {
    ASSERT_TRUE(waitForTaskCompletion(task.get())) << task->taskId();
  }
}

TEST_P(MultiFragmentTest, earlyCompletionMerge) {
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
                  .partitionedOutput(
                      {"c0"}, 2, /*outputLayout=*/{}, GetParam().serdeKind)
                  .planNode();

  auto task = makeTask(leafTaskId, plan, tasks.size());
  tasks.push_back(task);
  task->start(1);

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
            .mergeExchange(
                asRowType(data->type()), {"c0"}, GetParam().serdeKind)
            .hashJoin(
                {"c0"},
                {"u_c0"},
                PlanBuilder(planNodeIdGenerator).values({buildData}).planNode(),
                "",
                {"c0"})
            .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam().serdeKind)
            .planNode();

    joinOutputType = joinPlan->outputType();

    auto taskId = makeTaskId("join", i);
    joinTaskIds.push_back(taskId);

    auto task = makeTask(taskId, joinPlan, i);
    tasks.push_back(task);
    task->start(4);

    addRemoteSplits(task, {leafTaskId});
  }

  // Create output task.
  auto outputPlan =
      PlanBuilder().exchange(joinOutputType, GetParam().serdeKind).planNode();

  std::vector<Split> joinTaskSplits;
  for (auto joinTaskId : joinTaskIds) {
    joinTaskSplits.emplace_back(remoteSplit(joinTaskId));
  }
  test::AssertQueryBuilder(outputPlan, duckDbQueryRunner_)
      .splits(std::move(joinTaskSplits))
      .config(
          core::QueryConfig::kShuffleCompressionKind,
          common::compressionKindToString(GetParam().compressionKind))
      .assertResults("SELECT UNNEST([3, 3, 3, 3, 4, 4, 4, 4])");

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

TEST_P(MultiFragmentTest, exchangeDestruction) {
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

  leafPlan =
      PlanBuilder()
          .tableScan(rowType_)
          .project({"c0 % 10 AS c0", "c1"})
          .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam().serdeKind)
          .planNode();

  auto leafTask = makeTask(leafTaskId, leafPlan, 0);
  leafTask->start(1);
  addHiveSplits(leafTask, filePaths_);

  auto rootPlan =
      PlanBuilder()
          .exchange(leafPlan->outputType(), GetParam().serdeKind)
          .addNode([&leafPlan](std::string id, core::PlanNodePtr node) {
            return std::make_shared<SlowNode>(id, std::move(node));
          })
          .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam().serdeKind)
          .planNode();

  auto rootTask = makeTask("root-task", rootPlan, 0);
  rootTask->start(1);
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

TEST_P(MultiFragmentTest, cancelledExchange) {
  // Create a source fragment borrow the output type from it.
  auto planFragment =
      exec::test::PlanBuilder()
          .tableScan(rowType_)
          .filter("c0 % 5 = 1")
          .partitionedOutput({}, 1, {"c0", "c1"}, GetParam().serdeKind)
          .planFragment();

  // Create task with exchange.
  auto planFragmentWithExchange =
      exec::test::PlanBuilder()
          .exchange(planFragment.planNode->outputType(), GetParam().serdeKind)
          .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam().serdeKind)
          .planFragment();
  auto exchangeTask =
      makeTask("output.0.0.1", planFragmentWithExchange.planNode, 0);
  // Start the task and abort it straight away.
  exchangeTask->start(2);
  exchangeTask->requestAbort();

  /* sleep override */
  // Wait till all the terminations, closures and destructions are done.
  std::this_thread::sleep_for(std::chrono::seconds(1));
  // We expect no references left except for ours.
  EXPECT_EQ(1, exchangeTask.use_count());
}

class TestCustomExchangeNode : public core::PlanNode {
 public:
  TestCustomExchangeNode(
      const core::PlanNodeId& id,
      const RowTypePtr type,
      VectorSerde::Kind serdeKind)
      : PlanNode(id), outputType_(type), serdeKind_(serdeKind) {}

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  VectorSerde::Kind serdeKind() const {
    return serdeKind_;
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

  const RowTypePtr outputType_;
  const VectorSerde::Kind serdeKind_;
};

class TestCustomExchange : public exec::Exchange {
 public:
  TestCustomExchange(
      int32_t operatorId,
      DriverCtx* ctx,
      const std::shared_ptr<const TestCustomExchangeNode>& customExchangeNode,
      std::shared_ptr<ExchangeClient> exchangeClient)
      : exec::Exchange(
            operatorId,
            ctx,
            std::make_shared<core::ExchangeNode>(
                customExchangeNode->id(),
                customExchangeNode->outputType(),
                customExchangeNode->serdeKind()),
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

TEST_P(MultiFragmentTest, customPlanNodeWithExchangeClient) {
  setupSources(5, 100);
  Operator::registerOperator(std::make_unique<TestCustomExchangeTranslator>());
  auto leafTaskId = makeTaskId("leaf", 0);
  core::PlanNodeId partitionNodeId;
  auto leafPlan =
      PlanBuilder()
          .values(vectors_)
          .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam().serdeKind)
          .capturePlanNodeId(partitionNodeId)
          .planNode();
  auto leafTask = makeTask(leafTaskId, leafPlan, 0);
  leafTask->start(1);

  CursorParameters params;
  params.queryConfigs.emplace(
      core::QueryConfig::kShuffleCompressionKind,
      common::compressionKindToString(GetParam().compressionKind));
  core::PlanNodeId testNodeId;
  params.maxDrivers = 1;
  params.planNode =
      PlanBuilder()
          .addNode([&leafPlan](std::string id, core::PlanNodePtr /* input */) {
            return std::make_shared<TestCustomExchangeNode>(
                id, leafPlan->outputType(), GetParam().serdeKind);
          })
          .capturePlanNodeId(testNodeId)
          .planNode();

  auto cursor = TaskCursor::create(params);
  auto task = cursor->task();
  addRemoteSplits(task, {leafTaskId});
  while (cursor->moveNext()) {
  }
  ASSERT_TRUE(waitForTaskCompletion(leafTask.get(), 3'000'000))
      << leafTask->taskId();
  ASSERT_TRUE(waitForTaskCompletion(task.get(), 3'000'000)) << task->taskId();

  EXPECT_NE(
      toPlanStats(task->taskStats())
          .at(testNodeId)
          .customStats.count("testCustomExchangeStat"),
      0);

  auto planStats = toPlanStats(leafTask->taskStats());
  const auto serdeKindRuntimsStats =
      planStats.at(partitionNodeId).customStats.at(Operator::kShuffleSerdeKind);
  ASSERT_EQ(serdeKindRuntimsStats.count, 1);
  ASSERT_EQ(
      serdeKindRuntimsStats.min, static_cast<int64_t>(GetParam().serdeKind));
  ASSERT_EQ(
      serdeKindRuntimsStats.max, static_cast<int64_t>(GetParam().serdeKind));
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
DEBUG_ONLY_TEST_P(
    MultiFragmentTest,
    raceBetweenTaskTerminateAndTaskNoMoreSplits) {
  setupSources(10, 1000);
  auto leafTaskId = makeTaskId("leaf", 0);
  core::PlanNodePtr leafPlan =
      PlanBuilder()
          .tableScan(rowType_)
          .project({"c0 % 10 AS c0", "c1"})
          .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam().serdeKind)
          .planNode();
  auto leafTask = makeTask(leafTaskId, leafPlan, 0);
  leafTask->start(1);
  addHiveSplits(leafTask, filePaths_);

  const std::string kRootTaskId("root-task");
  std::atomic_bool readyToTerminate{false};
  folly::EventCount blockTerminate;
  std::atomic_bool noMoreSplits{false};
  folly::EventCount blockNoMoreSplits;
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Task::setError",
      std::function<void(Task*)>(([&](Task* task) {
        if (task->taskId() != kRootTaskId) {
          return;
        }
        // Trigger to add more split after the task has run into error.
        task->addSplit("0", remoteSplit(leafTaskId));
      })));

  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Task::terminate",
      std::function<void(Task*)>(([&](Task* task) {
        if (task->taskId() != kRootTaskId) {
          return;
        }
        // Unblock no more split call in the middle of task terminate execution.
        noMoreSplits.store(true);
        blockNoMoreSplits.notifyAll();
        blockTerminate.await([&]() { return readyToTerminate.load(); });
      })));
  auto rootPlan = PlanBuilder()
                      .exchange(leafPlan->outputType(), GetParam().serdeKind)
                      .finalAggregation({"c0"}, {"count(c1)"}, {{BIGINT()}})
                      .planNode();

  const int64_t kRootMemoryLimit = 1 << 20;
  auto rootTask = makeTask(
      kRootTaskId,
      rootPlan,
      0,
      [](RowVectorPtr /*unused*/, ContinueFuture* /*unused*/)
          -> BlockingReason { return BlockingReason::kNotBlocked; },
      kRootMemoryLimit);
  rootTask->start(1);
  rootTask->addSplit("0", remoteSplit(leafTaskId));
  blockNoMoreSplits.await([&]() { return noMoreSplits.load(); });
  rootTask->noMoreSplits("0");
  // Unblock task terminate execution after no more split call finishes.
  readyToTerminate.store(true);
  blockTerminate.notifyAll();
  ASSERT_TRUE(waitForTaskFailure(rootTask.get(), 1'000'000'000));
}

TEST_P(MultiFragmentTest, taskTerminateWithPendingOutputBuffers) {
  setupSources(8, 1000);
  auto taskId = makeTaskId("task", 0);
  core::PlanNodePtr leafPlan;
  leafPlan =
      PlanBuilder()
          .tableScan(rowType_)
          .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam().serdeKind)
          .planNode();

  auto task = makeTask(taskId, leafPlan, 0);
  task->start(1);
  addHiveSplits(task, filePaths_);

  const uint64_t maxBytes = std::numeric_limits<uint64_t>::max();
  const int destination = 0;
  std::vector<std::unique_ptr<folly::IOBuf>> receivedIobufs;
  int64_t sequence = 0;
  for (;;) {
    auto dataPromise = ContinuePromise("WaitForOutput");
    bool complete{false};
    ASSERT_TRUE(bufferManager_->getData(
        taskId,
        destination,
        maxBytes,
        sequence,
        [&](std::vector<std::unique_ptr<folly::IOBuf>> iobufs,
            int64_t inSequence,
            std::vector<int64_t> /*remainingBytes*/) {
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

DEBUG_ONLY_TEST_P(
    MultiFragmentTest,
    taskTerminateWithProblematicRemainingRemoteSplits) {
  // Start the task with 2 drivers.
  auto probeData =
      makeRowVector({"p_c0"}, {makeFlatVector<int64_t>({1, 2, 3})});
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId exchangeNodeId;
  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .values({probeData}, true)
          .hashJoin(
              {"p_c0"},
              {"c0"},
              PlanBuilder(planNodeIdGenerator)
                  .exchange(rowType_, GetParam().serdeKind)
                  .capturePlanNodeId(exchangeNodeId)
                  .planNode(),
              "",
              {"c0"})
          .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam().serdeKind)
          .planNode();
  auto taskId = makeTaskId("final", 0);
  auto task = makeTask(taskId, plan, 0);
  task->start(2);

  std::atomic<bool> driverRunWaitFlag{true};
  folly::EventCount driverRunWait;
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal",
      std::function<void(const Driver*)>([&](const Driver* /* unused */) {
        // Block on driver run so that added bad split is not immediately
        // consumed. It gets unblocked when task termination state is set.
        driverRunWait.await([&]() { return !(driverRunWaitFlag.load()); });
      }));

  std::atomic<bool> taskSetErrorWaitFlag{true};
  folly::EventCount taskSetErrorWait;
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Task::setError",
      std::function<void(const Task*)>([&](const Task* /* unused */) {
        taskSetErrorWait.await(
            [&]() { return !(taskSetErrorWaitFlag.load()); });
      }));

  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Task::terminate",
      std::function<void(const Task*)>([&](const Task* /* unused */) {
        driverRunWaitFlag = false;
        driverRunWait.notifyAll();
      }));

  std::thread failThread([&]() {
    try {
      VELOX_FAIL("Test terminate task");
    } catch (const VeloxException& e) {
      task->setError(std::current_exception());
    }
  });

  // Add one more bad split, making sure `remainingRemoteSplits` is not empty
  // and processing it would cause an exception.
  task->addSplit(exchangeNodeId, remoteSplit(makeBadTaskId("leaf", 0)));
  taskSetErrorWaitFlag = false;
  taskSetErrorWait.notifyAll();

  // Wait for the task to fail, and make sure the task has been deleted instead
  // of hanging as a zombie task.
  ASSERT_TRUE(waitForTaskFailure(task.get(), 3'000'000)) << task->taskId();
  failThread.join();
}

DEBUG_ONLY_TEST_P(MultiFragmentTest, mergeWithEarlyTermination) {
  setupSources(10, 1000);

  std::vector<std::shared_ptr<TempFilePath>> filePaths(
      filePaths_.begin(), filePaths_.begin());

  std::vector<std::string> partialSortTaskIds;
  auto sortTaskId = makeTaskId("orderby", 0);
  partialSortTaskIds.push_back(sortTaskId);
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto partialSortPlan =
      PlanBuilder(planNodeIdGenerator)
          .localMerge(
              {"c0"},
              {PlanBuilder(planNodeIdGenerator)
                   .tableScan(rowType_)
                   .orderBy({"c0"}, true)
                   .planNode()})
          .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam().serdeKind)
          .planNode();

  auto partialSortTask = makeTask(sortTaskId, partialSortPlan, 1);
  partialSortTask->start(1);
  addHiveSplits(partialSortTask, filePaths);

  std::atomic<bool> blockMergeOnce{true};
  std::atomic<bool> mergeIsBlockedReady{false};
  folly::EventCount mergeIsBlockedWait;
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Merge::isBlocked",
      std::function<void(const Operator*)>([&](const Operator* op) {
        if (op->operatorType() != "MergeExchange") {
          return;
        }
        if (!blockMergeOnce.exchange(false)) {
          return;
        }
        mergeIsBlockedWait.await([&]() { return mergeIsBlockedReady.load(); });
        // Trigger early termination.
        op->testingOperatorCtx()->task()->requestAbort();
      }));

  auto finalSortTaskId = makeTaskId("orderby", 1);
  auto finalSortPlan =
      PlanBuilder()
          .mergeExchange(
              partialSortPlan->outputType(), {"c0"}, GetParam().serdeKind)
          .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam().serdeKind)
          .planNode();
  auto finalSortTask = makeTask(finalSortTaskId, finalSortPlan, 0);
  finalSortTask->start(1);
  addRemoteSplits(finalSortTask, partialSortTaskIds);

  mergeIsBlockedReady.store(true);
  mergeIsBlockedWait.notifyAll();

  ASSERT_TRUE(waitForTaskCompletion(partialSortTask.get(), 1'000'000'000));
  ASSERT_TRUE(waitForTaskAborted(finalSortTask.get(), 1'000'000'000));
}

class DataFetcher {
 public:
  DataFetcher(const std::string& taskId, int32_t destination, int64_t maxBytes)
      : taskId_{taskId}, destination_{destination}, maxBytes_{maxBytes} {}

  /// Starts fetching data for the task and destination specified in the
  /// constructor. Returns a future that gets completes once all the data has
  /// been fetched.
  ContinueFuture fetch() {
    auto [p, f] = makeVeloxContinuePromiseContract("DataFetcher");
    promise_ = std::move(p);
    doFetch(kInitialSequence);
    return std::move(f);
  }

  struct Stats {
    /// Number of possibly multi-page packets received.
    int32_t numPackets;
    /// Number of pages received. Includes the last null page.
    int32_t numPages;
    /// Total number of bytes received across all pages.
    int64_t totalBytes;

    /// Average number of bytes per packet.
    int64_t averagePacketBytes() const {
      return numPackets > 0 ? (totalBytes / numPackets) : 0;
    }

    std::string toString() const {
      return fmt::format(
          "numPackets {} numPages {} totalBytes {}",
          numPackets,
          numPages,
          totalBytes);
    }
  };

  Stats stats() const {
    return {numPackets_, numPages_, totalBytes_};
  }

  std::string getPacketPageSizes() const {
    std::stringstream out;
    for (const auto& sizes : packetPageSizes_) {
      out << sizes.size() << " pages: [" << folly::join(", ", sizes) << "], ";
    }
    return to<std::string>(packetPageSizes_.size()) + " packets: [" +
        out.str() + "]";
  }

  std::atomic<bool>& bufferFull() {
    return bufferFull_;
  }

  std::atomic<bool>& bufferDone() {
    return bufferDone_;
  }

  folly::EventCount& bufferFullOrDoneWait() {
    return bufferFullOrDoneWait_;
  }

 private:
  static constexpr int64_t kInitialSequence = 0;

  void doFetch(int64_t sequence) {
    // We only want to consume data when the buffer is full or done because we
    // want to make sure maxBytes is respected, so there needs to be enough data
    // in the buffer for maxBytes to have an effect.
    bufferFullOrDoneWait_.await(
        [&]() { return bufferFull_.load() || bufferDone_.load(); });
    // Reset the bufferFull_ flag as we're about to consume some data and we
    // want to detect when it fills up again. Note that we don't reset the
    // bufferDone_ flag because once it's done it's done.
    bufferFull_ = false;

    bool ok = bufferManager_->getData(
        taskId_,
        destination_,
        maxBytes_,
        sequence,
        [&](auto pages, auto sequence, auto /*remainingBytes*/) mutable {
          const auto nextSequence = sequence + pages.size();
          const bool atEnd = processData(std::move(pages), sequence);
          bufferManager_->acknowledge(taskId_, destination_, nextSequence);

          if (atEnd) {
            bufferManager_->deleteResults(taskId_, destination_);
            promise_.setValue();
          } else {
            doFetch(nextSequence);
          }
        });
    VELOX_CHECK(ok);
  }

  bool processData(
      std::vector<std::unique_ptr<folly::IOBuf>> pages,
      int64_t sequence) {
    numPages_ += pages.size();
    ++numPackets_;

    /// Save the page sizes of each packet to help with diagnosis.
    auto pageSizes = std::vector<std::size_t>();

    int64_t numBytes = 0;
    bool atEnd = false;
    for (const auto& page : pages) {
      if (page == nullptr) {
        VELOX_CHECK(!atEnd);
        atEnd = true;
      } else {
        auto chainDataLength = page->computeChainDataLength();
        numBytes += chainDataLength;
        pageSizes.push_back(chainDataLength);
      }
    }

    totalBytes_ += numBytes;
    packetPageSizes_.push_back(std::move(pageSizes));
    return atEnd;
  }

  const std::string taskId_;
  const int32_t destination_;
  const int64_t maxBytes_;
  ContinuePromise promise_{ContinuePromise::makeEmpty()};
  int32_t numPackets_{0};
  int32_t numPages_{0};
  int64_t totalBytes_{0};
  /// All the pages sizes of each packet.
  std::vector<std::vector<std::size_t>> packetPageSizes_;

  /// Flag that gets set when the OutputBuffer is full.
  std::atomic<bool> bufferFull_{false};
  /// Flag that gets set when the OutputBuffer sees that all upstream Drivers
  /// have finished.
  std::atomic<bool> bufferDone_{false};
  /// Used to notify DataFetcher that one of the above bool flags has been set.
  folly::EventCount bufferFullOrDoneWait_;

  std::shared_ptr<OutputBufferManager> bufferManager_{
      OutputBufferManager::getInstanceRef()};
};

/// Verify that POBM::getData() honors maxBytes parameter roughly at 1MB
/// granularity. It can do so only if PartitionedOutput operator limits the size
/// of individual pages. PartitionedOutput operator is expected to limit page
/// sizes to no more than 1MB give and take 30%.
DEBUG_ONLY_TEST_P(MultiFragmentTest, maxBytes) {
  if (GetParam().compressionKind != common::CompressionKind_NONE) {
    // NOTE: different compression generates different serialized byte size so
    // only test with no-compression to ease testing.s
    return;
  }
  std::string s(25, 'x');
  // Keep the row count under 7000 to avoid hitting the row limit in the
  // operator instead.
  auto data = makeRowVector({
      makeFlatVector<int64_t>(5'000, [](auto row) { return row; }),
      makeFlatVector<int64_t>(5'000, [](auto row) { return row; }),
      makeConstant(StringView(s), 5'000),
  });

  core::PlanNodeId outputNodeId;
  auto plan =
      PlanBuilder()
          .values({data}, false, 100)
          .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam().serdeKind)
          .capturePlanNodeId(outputNodeId)
          .planNode();

  int32_t testIteration = 0;
  DataFetcher::Stats prevStats;

  auto test = [&](int64_t maxBytes) {
    const auto taskId = fmt::format("test.{}", testIteration++);

    SCOPED_TRACE(taskId);
    SCOPED_TRACE(fmt::format("maxBytes: {}", maxBytes));

    DataFetcher fetcher(taskId, 0, maxBytes);

    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::OutputBuffer::enqueue",
        std::function<void(const OutputBuffer*)>(
            [&](const OutputBuffer* /* unused */) {
              fetcher.bufferFull() = true;
              fetcher.bufferFullOrDoneWait().notifyAll();
            }));

    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::OutputBuffer::checkIfDone",
        std::function<void(const OutputBuffer*)>(
            [&](const OutputBuffer* /* unused */) {
              fetcher.bufferDone() = true;
              fetcher.bufferFullOrDoneWait().notifyAll();
            }));

    auto task = makeTask(taskId, plan, 0);
    task->start(1);
    task->updateOutputBuffers(1, true);

    fetcher.fetch().wait();

    ASSERT_TRUE(waitForTaskCompletion(task.get()));

    const auto stats = fetcher.stats();
    if (testIteration > 1) {
      ASSERT_EQ(prevStats.numPages, stats.numPages);
      ASSERT_EQ(prevStats.totalBytes, stats.totalBytes);
      ASSERT_GT(prevStats.numPackets, stats.numPackets)
          << stats.toString() << " " << fetcher.getPacketPageSizes();
    }

    ASSERT_LT(stats.averagePacketBytes(), maxBytes * 1.5);

    auto taskStats = toPlanStats(task->taskStats());
    const auto& outputStats = taskStats.at(outputNodeId);

    ASSERT_EQ(outputStats.outputBytes, stats.totalBytes);
    ASSERT_EQ(outputStats.inputRows, 100 * data->size());
    ASSERT_EQ(outputStats.outputRows, 100 * data->size());
    prevStats = stats;
  };

  static const int64_t kMB = 1 << 20;
  test(kMB);
  test(3 * kMB);
  test(5 * kMB);
  test(10 * kMB);
  test(20 * kMB);
  test(40 * kMB);
}

// Verifies that ExchangeClient stats are populated even if task fails.
DEBUG_ONLY_TEST_P(MultiFragmentTest, exchangeStatsOnFailure) {
  // Triggers a failure after fetching first 10 pages.
  std::atomic_uint64_t expectedReceivedPages{0};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::test::LocalExchangeSource",
      std::function<void(exec::ExchangeSource * data)>(
          [&](exec::ExchangeSource* source) {
            auto* queue = source->testingQueue();
            const auto receivedPages = queue->receivedPages();
            if (receivedPages > 10) {
              expectedReceivedPages = receivedPages;

              VELOX_FAIL("Forced failure after {} pages", receivedPages);
            }
          }));

  std::string s(25, 'x');
  auto data = makeRowVector({
      makeFlatVector<int64_t>(10'000, [](auto row) { return row; }),
      makeConstant(StringView(s), 10'000),
  });

  auto producerPlan =
      PlanBuilder()
          .values({data}, false, 30)
          .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam().serdeKind)
          .planNode();

  auto producerTaskId = makeTaskId("producer", 0);
  auto producerTask = makeTask(producerTaskId, producerPlan, 0);
  producerTask->start(1);
  producerTask->updateOutputBuffers(1, true);

  auto plan = PlanBuilder()
                  .exchange(producerPlan->outputType(), GetParam().serdeKind)
                  .planNode();

  auto task = makeTask("t", plan, 0, noopConsumer());
  task->start(4);
  task->addSplit("0", remoteSplit(producerTaskId));
  task->noMoreSplits("0");

  ASSERT_TRUE(test::waitForTaskFailure(task.get(), 3'000'000));

  ASSERT_TRUE(task->errorMessage().find("Forced failure") != std::string::npos)
      << "Got: [" << task->errorMessage() << "]";

  auto stats = toPlanStats(task->taskStats());

  EXPECT_EQ(
      expectedReceivedPages,
      stats.at("0").customStats.at("numReceivedPages").sum);

  ASSERT_TRUE(waitForTaskCompletion(producerTask.get(), 3'000'000));
}

TEST_P(MultiFragmentTest, earlyTaskFailure) {
  setupSources(1, 10);

  const auto partialSortTaskId = makeTaskId("partialSortBy", 0);
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto partialSortPlan =
      PlanBuilder(planNodeIdGenerator)
          .localMerge(
              {"c0"},
              {PlanBuilder(planNodeIdGenerator)
                   .tableScan(rowType_)
                   .orderBy({"c0"}, true)
                   .planNode()})
          .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam().serdeKind)
          .planNode();
  for (bool internalFailure : {false, true}) {
    SCOPED_TRACE(fmt::format("internalFailure: {}", internalFailure));

    auto partialSortTask = makeTask(partialSortTaskId, partialSortPlan, 1);
    partialSortTask->start(1);
    addHiveSplits(partialSortTask, filePaths_);
    auto outputType = partialSortPlan->outputType();

    auto finalSortTaskId = makeTaskId("finalSortBy", 0);
    auto finalSortPlan =
        PlanBuilder()
            .mergeExchange(outputType, {"c0"}, GetParam().serdeKind)
            .partitionedOutput({}, 1)
            .planNode();

    auto finalSortTask = makeTask(finalSortTaskId, finalSortPlan, 0);
    if (internalFailure) {
      try {
        VELOX_FAIL("memoryAbortTest");
      } catch (const VeloxRuntimeError& e) {
        finalSortTask->pool()->abort(std::current_exception());
      }
    } else {
      finalSortTask->requestAbort().wait();
    }

    finalSortTask->start(1);
    addRemoteSplits(finalSortTask, std::vector<std::string>{partialSortTaskId});
    partialSortTask->requestAbort().wait();

    if (internalFailure) {
      ASSERT_TRUE(waitForTaskFailure(finalSortTask.get()))
          << finalSortTask->taskId();
    } else {
      ASSERT_TRUE(waitForTaskAborted(finalSortTask.get()))
          << finalSortTask->taskId();
    }
    ASSERT_TRUE(waitForTaskAborted(partialSortTask.get()))
        << partialSortTask->taskId();
  }
}

TEST_P(MultiFragmentTest, mergeSmallBatchesInExchange) {
  auto data = makeRowVector({makeFlatVector<int32_t>({1, 2, 3})});

  const int32_t numPartitions = 100;
  auto producerPlan = test::PlanBuilder()
                          .values({data})
                          .partitionedOutput(
                              {"c0"},
                              numPartitions,
                              /*outputLayout=*/{},
                              GetParam().serdeKind)
                          .planNode();
  const auto producerTaskId = "local://t1";

  auto plan = test::PlanBuilder()
                  .exchange(asRowType(data->type()), GetParam().serdeKind)
                  .planNode();

  auto expected = makeRowVector({
      makeFlatVector<int32_t>(3'000, [](auto row) { return 1 + row % 3; }),
  });

  auto test = [&](uint64_t maxBytes, int32_t expectedBatches) {
    auto producerTask = makeTask(producerTaskId, producerPlan);

    bufferManager_->initializeTask(
        producerTask,
        core::PartitionedOutputNode::Kind::kPartitioned,
        numPartitions,
        1);

    auto cleanupGuard = folly::makeGuard([&]() {
      producerTask->requestCancel();
      bufferManager_->removeTask(producerTaskId);
    });

    // Enqueue many small pages.
    const int32_t numPages = 1'000;
    for (auto i = 0; i < numPages; ++i) {
      enqueue(producerTaskId, 17, data);
    }
    bufferManager_->noMoreData(producerTaskId);

    auto task = test::AssertQueryBuilder(plan)
                    .split(remoteSplit(producerTaskId))
                    .destination(17)
                    .config(
                        core::QueryConfig::kPreferredOutputBatchBytes,
                        std::to_string(maxBytes))
                    .assertResults(expected);

    auto taskStats = exec::toPlanStats(task->taskStats());
    const auto& stats = taskStats.at("0");

    ASSERT_EQ(expected->size(), stats.outputRows);
    ASSERT_EQ(expectedBatches, stats.outputVectors);
    ASSERT_EQ(numPages, stats.customStats.at("numReceivedPages").sum);
  };

  if (GetParam().serdeKind == VectorSerde::Kind::kPresto) {
    test(1, 1'000);
    test(1'000, 56);
    test(10'000, 6);
    test(100'000, 1);
  } else if (GetParam().serdeKind == VectorSerde::Kind::kCompactRow) {
    test(1, 1'000);
    test(1'000, 39);
    test(10'000, 5);
    test(100'000, 2);
  } else {
    test(1, 1'000);
    test(1'000, 72);
    test(10'000, 7);
    test(100'000, 1);
  }
}

TEST_P(MultiFragmentTest, splitLargeCompactRowsInExchange) {
  if (GetParam().serdeKind != VectorSerde::Kind::kCompactRow) {
    return;
  }
  const uint64_t kNumColumns = 100;
  const uint64_t kNumRows = 2'000;
  const int32_t kNumPartitions = 2;

  std::vector<int64_t> columnElements;
  for (auto i = 0; i < kNumRows; ++i) {
    columnElements.push_back(i);
  }
  std::vector<VectorPtr> columns;
  for (auto i = 0; i < kNumColumns; ++i) {
    columns.push_back(makeFlatVector<int64_t>(columnElements));
  }

  // 'data' is a wide vector of 20k rows and 100 columns(children vectors). Each
  // row estimated to be 100 * 8b = 800b.
  auto data = makeRowVector(columns);

  auto producerPlan = test::PlanBuilder()
                          .values({data})
                          .partitionedOutput(
                              {"c0"},
                              kNumPartitions,
                              /*outputLayout=*/{},
                              VectorSerde::Kind::kCompactRow)
                          .planNode();
  const auto producerTaskId = "local://t1";

  auto plan =
      test::PlanBuilder()
          .exchange(asRowType(data->type()), VectorSerde::Kind::kCompactRow)
          .planNode();

  auto expected = makeRowVector(columns);

  auto test = [&](uint64_t maxBytes, int32_t expectedBatches) {
    auto producerTask = makeTask(producerTaskId, producerPlan);

    bufferManager_->initializeTask(
        producerTask,
        core::PartitionedOutputNode::Kind::kPartitioned,
        kNumPartitions,
        1);

    auto cleanupGuard = folly::makeGuard([&]() {
      producerTask->requestCancel();
      bufferManager_->removeTask(producerTaskId);
    });

    // Enqueue a single large page.
    enqueue(producerTaskId, 0, data);

    bufferManager_->noMoreData(producerTaskId);

    auto task = test::AssertQueryBuilder(plan)
                    .split(remoteSplit(producerTaskId))
                    .destination(0)
                    .config(
                        core::QueryConfig::kPreferredOutputBatchBytes,
                        std::to_string(maxBytes))
                    .assertResults(expected);

    auto taskStats = exec::toPlanStats(task->taskStats());
    const auto& stats = taskStats.at("0");

    ASSERT_EQ(expected->size(), stats.outputRows);
    ASSERT_EQ(expectedBatches, stats.outputVectors);
    ASSERT_EQ(1, stats.customStats.at("numReceivedPages").sum);
  };

  test(1, kNumRows / 64 + 1);
  test(1'000, kNumRows / 64 + 1);
  test(160'000, 14);
  test(10'000'000, 2);
}

TEST_P(MultiFragmentTest, compression) {
  constexpr int32_t kNumRepeats = 1'000'000;
  const auto data = makeRowVector({makeFlatVector<int64_t>({1, 2, 3})});

  const auto producerPlan =
      test::PlanBuilder()
          .values({data}, false, kNumRepeats)
          .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam().serdeKind)
          .planNode();

  const auto plan = test::PlanBuilder()
                        .exchange(asRowType(data->type()), GetParam().serdeKind)
                        .singleAggregation({}, {"sum(c0)"})
                        .planNode();

  const auto expected =
      makeRowVector({makeFlatVector<int64_t>(std::vector<int64_t>{6000000})});

  const auto test = [&](const std::string& producerTaskId,
                        float minCompressionRatio,
                        bool expectSkipCompression) {
    PartitionedOutput::testingSetMinCompressionRatio(minCompressionRatio);
    auto producerTask = makeTask(producerTaskId, producerPlan);
    producerTask->start(1);

    auto consumerTask =
        test::AssertQueryBuilder(plan)
            .split(remoteSplit(producerTaskId))
            .config(
                core::QueryConfig::kShuffleCompressionKind,
                common::compressionKindToString(GetParam().compressionKind))
            .destination(0)
            .assertResults(expected);

    auto consumerTaskStats = exec::toPlanStats(consumerTask->taskStats());
    const auto& consumerPlanStats = consumerTaskStats.at("0");
    ASSERT_EQ(
        consumerPlanStats.customStats.at(Operator::kShuffleCompressionKind).min,
        static_cast<common::CompressionKind>(GetParam().compressionKind));
    ASSERT_EQ(
        consumerPlanStats.customStats.at(Operator::kShuffleCompressionKind).max,
        static_cast<common::CompressionKind>(GetParam().compressionKind));
    ASSERT_EQ(data->size() * kNumRepeats, consumerPlanStats.outputRows);

    auto producerTaskStats = exec::toPlanStats(producerTask->taskStats());
    const auto& producerStats = producerTaskStats.at("1");
    ASSERT_EQ(
        producerStats.customStats.at(Operator::kShuffleCompressionKind).min,
        static_cast<common::CompressionKind>(GetParam().compressionKind));
    ASSERT_EQ(
        producerStats.customStats.at(Operator::kShuffleCompressionKind).max,
        static_cast<common::CompressionKind>(GetParam().compressionKind));
    if (GetParam().compressionKind == common::CompressionKind_NONE) {
      ASSERT_EQ(
          producerStats.customStats.count(
              IterativeVectorSerializer::kCompressedBytes),
          0);
      ASSERT_EQ(
          producerStats.customStats.count(
              IterativeVectorSerializer::kCompressionInputBytes),
          0);
      ASSERT_EQ(
          producerStats.customStats.count(
              IterativeVectorSerializer::kCompressionSkippedBytes),
          0);
      return;
    }
    // The data is extremely compressible, 1, 2, 3 repeated 1000000 times.
    if (!expectSkipCompression) {
      ASSERT_LT(
          producerStats.customStats
              .at(IterativeVectorSerializer::kCompressedBytes)
              .sum,
          producerStats.customStats
              .at(IterativeVectorSerializer::kCompressionInputBytes)
              .sum);
      ASSERT_EQ(producerStats.customStats.count("compressionSkippedBytes"), 0);
    } else {
      ASSERT_LT(
          0,
          producerStats.customStats
              .at(IterativeVectorSerializer::kCompressionSkippedBytes)
              .sum);
    }
  };

  {
    SCOPED_TRACE(
        fmt::format("compression kind {}", GetParam().compressionKind));
    {
      SCOPED_TRACE(fmt::format("minCompressionRatio 0.7"));
      test("local://t1", 0.7, false);
    }
    SCOPED_TRACE(fmt::format("minCompressionRatio 0.0000001"));
    { test("local://t2", 0.0000001, true); }
  }
}

TEST_P(MultiFragmentTest, scaledTableScan) {
  const int numSplits = 20;
  std::vector<std::shared_ptr<TempFilePath>> splitFiles;
  std::vector<RowVectorPtr> splitVectors;
  for (auto i = 0; i < numSplits; ++i) {
    auto vectors = makeVectors(10, 1'000);
    auto filePath = TempFilePath::create();
    writeToFile(filePath->getPath(), vectors);
    splitFiles.push_back(std::move(filePath));
    splitVectors.insert(splitVectors.end(), vectors.begin(), vectors.end());
  }

  createDuckDbTable(splitVectors);

  struct {
    bool scaleEnabled;
    double scaleUpMemoryUsageRatio;
    bool expectScaleUp;

    std::string debugString() const {
      return fmt::format(
          "scaleEnabled {}, scaleUpMemoryUsageRatio {}, expectScaleUp {}",
          scaleEnabled,
          scaleUpMemoryUsageRatio,
          expectScaleUp);
    }
  } testSettings[] = {
      {false, 0.9, false},
      {true, 0.9, true},
      {false, 1.0, false},
      {true, 1.0, true},
      {false, 0.00001, false},
      {true, 0.00001, false},
      {false, 0.0, false},
      {true, 0.0, false}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    core::PlanNodeId scanNodeId;
    configSettings_[core::QueryConfig::kTableScanScaledProcessingEnabled] =
        testData.scaleEnabled ? "true" : "false";
    configSettings_[core::QueryConfig::kTableScanScaleUpMemoryUsageRatio] =
        std::to_string(testData.scaleUpMemoryUsageRatio);

    const auto leafPlan =
        PlanBuilder()
            .tableScan(rowType_)
            .capturePlanNodeId(scanNodeId)
            .partialAggregation(
                {"c5"}, {"max(c0)", "sum(c1)", "sum(c2)", "sum(c3)", "sum(c4)"})
            .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam().serdeKind)
            .planNode();

    const auto leafTaskId = "local://leaf-0";
    auto leafTask = makeTask(leafTaskId, leafPlan, 0, nullptr, 128ULL << 20);
    const auto numLeafDrivers{4};
    leafTask->start(numLeafDrivers);
    addHiveSplits(leafTask, splitFiles);

    const auto finalAggPlan =
        PlanBuilder()
            .exchange(leafPlan->outputType(), GetParam().serdeKind)
            .finalAggregation(
                {"c5"},
                {"max(a0)", "sum(a1)", "sum(a2)", "sum(a3)", "sum(a4)"},
                {{BIGINT()}, {INTEGER()}, {SMALLINT()}, {REAL()}, {DOUBLE()}})
            .partitionedOutput({}, 1, /*outputLayout=*/{}, GetParam().serdeKind)
            .planNode();

    const auto finalAggTaskId = "local://final-agg-0";
    auto finalAggTask = makeTask(finalAggTaskId, finalAggPlan, 0);
    const auto numFinalAggrDrivers{1};
    finalAggTask->start(numFinalAggrDrivers);
    addRemoteSplits(finalAggTask, {leafTaskId});

    const auto resultPlan =
        PlanBuilder()
            .exchange(finalAggPlan->outputType(), GetParam().serdeKind)
            .planNode();

    test::AssertQueryBuilder(resultPlan, duckDbQueryRunner_)
        .split(remoteSplit(finalAggTaskId))
        .config(
            core::QueryConfig::kShuffleCompressionKind,
            common::compressionKindToString(GetParam().compressionKind))
        .assertResults(
            "SELECT c5, max(c0), sum(c1), sum(c2), sum(c3), sum(c4) FROM tmp group by c5");

    ASSERT_TRUE(waitForTaskCompletion(leafTask.get())) << leafTask->taskId();
    ASSERT_TRUE(waitForTaskCompletion(finalAggTask.get()))
        << finalAggTask->taskId();

    auto planStats = toPlanStats(leafTask->taskStats());
    if (testData.scaleEnabled) {
      ASSERT_EQ(
          planStats.at(scanNodeId)
              .customStats.count(TableScan::kNumRunningScaleThreads),
          1);
      if (testData.expectScaleUp) {
        ASSERT_GE(
            planStats.at(scanNodeId)
                .customStats[TableScan::kNumRunningScaleThreads]
                .sum,
            1);
        ASSERT_LE(
            planStats.at(scanNodeId)
                .customStats[TableScan::kNumRunningScaleThreads]
                .sum,
            numLeafDrivers);
      } else {
        ASSERT_EQ(
            planStats.at(scanNodeId)
                .customStats.count(TableScan::kNumRunningScaleThreads),
            1);
      }
    } else {
      ASSERT_EQ(
          planStats.at(scanNodeId)
              .customStats.count(TableScan::kNumRunningScaleThreads),
          0);
    }
  }
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    MultiFragmentTest,
    MultiFragmentTest,
    testing::ValuesIn(MultiFragmentTest::getTestParams()),
    [](const testing::TestParamInfo<TestParam>& info) {
      return fmt::format(
          "{}_{}",
          VectorSerde::kindName(info.param.serdeKind),
          compressionKindToString(info.param.compressionKind));
    });
} // namespace
} // namespace facebook::velox::exec
