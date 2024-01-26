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

#include "velox/exec/Task.h"
#include "folly/experimental/EventCount.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/future/VeloxPromise.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/exec/OutputBufferManager.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/Values.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/Cursor.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

using namespace facebook::velox;
using namespace facebook::velox::common::testutil;

namespace facebook::velox::exec::test {
namespace {
// A test join node whose build is skewed in terms of process time. The driver
// id 0 processes slower than other drivers if paralelism greater than 1
class TestSkewedJoinNode : public core::PlanNode {
 public:
  TestSkewedJoinNode(
      const core::PlanNodeId& id,
      core::PlanNodePtr left,
      core::PlanNodePtr right,
      const uint64_t slowJoinBuildDelaySeconds)
      : PlanNode(id),
        sources_{std::move(left), std::move(right)},
        slowJoinBuildDelaySeconds_(slowJoinBuildDelaySeconds) {}

  const RowTypePtr& outputType() const override {
    return sources_[0]->outputType();
  }

  const std::vector<core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  std::string_view name() const override {
    return "test skewed join";
  }

  int64_t slowJoinBuildDelaySeconds() const {
    return slowJoinBuildDelaySeconds_;
  }

 private:
  void addDetails(std::stringstream& /* stream */) const override {}

  std::vector<core::PlanNodePtr> sources_;
  uint64_t slowJoinBuildDelaySeconds_;
};

// A dummy test join bridge operator
class TestSkewedJoinBridge : public exec::JoinBridge {
 public:
  void setBuildFinished() {
    std::vector<ContinuePromise> promises;
    {
      std::lock_guard<std::mutex> l(mutex_);
      VELOX_CHECK(
          !buildFinished_.has_value(),
          "setBuildFinished may be called only once");
      buildFinished_ = true;
      promises = std::move(promises_);
    }
    notify(std::move(promises));
  }

  std::optional<bool> buildFinishedOrFuture(ContinueFuture* future) {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK(!cancelled_, "Getting data after the build side is aborted");
    if (buildFinished_.has_value()) {
      return buildFinished_;
    }
    promises_.emplace_back("TestSkewedJoinBridge::buildFinishedOrFuture");
    *future = promises_.back().getSemiFuture();
    return std::nullopt;
  }

 private:
  std::optional<bool> buildFinished_;
};

// A dummy test join build operator that simulates driver with id 0 to process
// slower than other drivers.
class TestSkewedJoinBuild : public exec::Operator {
 public:
  TestSkewedJoinBuild(
      int32_t operatorId,
      exec::DriverCtx* driverCtx,
      std::shared_ptr<const TestSkewedJoinNode> joinNode)
      : Operator(
            driverCtx,
            nullptr,
            operatorId,
            joinNode->id(),
            "TestSkewedJoinBuild"),
        slowJoinBuildDelaySeconds_(joinNode->slowJoinBuildDelaySeconds()) {}

  void addInput(RowVectorPtr /* input */) override {
    // Make driver with id 0 slower than other drivers
    auto driverId = operatorCtx_->driverCtx()->driverId;
    if (driverId == 0) {
      std::this_thread::sleep_for(
          std::chrono::seconds(slowJoinBuildDelaySeconds_));
    }
  }

  bool needsInput() const override {
    return !noMoreInput_;
  }

  RowVectorPtr getOutput() override {
    return nullptr;
  }

  void noMoreInput() override {
    Operator::noMoreInput();
    std::vector<ContinuePromise> promises;
    std::vector<std::shared_ptr<exec::Driver>> peers;
    // The last Driver to hit CustomJoinBuild::finish gathers the data from
    // all build Drivers and hands it over to the probe side. At this
    // point all build Drivers are continued and will free their
    // state. allPeersFinished is true only for the last Driver of the
    // build pipeline.
    if (!operatorCtx_->task()->allPeersFinished(
            planNodeId(), operatorCtx_->driver(), &future_, promises, peers)) {
      return;
    }
    VELOX_FAIL("Last driver should not finish successfully.");
  }

  exec::BlockingReason isBlocked(ContinueFuture* future) override {
    if (!future_.valid()) {
      return exec::BlockingReason::kNotBlocked;
    }
    *future = std::move(future_);
    return exec::BlockingReason::kWaitForJoinBuild;
  }

  bool isFinished() override {
    return !future_.valid() && noMoreInput_;
  }

 private:
  ContinueFuture future_{ContinueFuture::makeEmpty()};
  int64_t slowJoinBuildDelaySeconds_;
};

// A dummy test join probe operator
class TestSkewedJoinProbe : public exec::Operator {
 public:
  TestSkewedJoinProbe(
      int32_t operatorId,
      exec::DriverCtx* driverCtx,
      std::shared_ptr<const TestSkewedJoinNode> joinNode)
      : Operator(
            driverCtx,
            nullptr,
            operatorId,
            joinNode->id(),
            "CustomJoinProbe") {}

  bool needsInput() const override {
    return !finished_;
  }

  void addInput(RowVectorPtr /* input */) override {}

  RowVectorPtr getOutput() override {
    finished_ = true;
    return nullptr;
  }

  exec::BlockingReason isBlocked(ContinueFuture* future) override {
    auto joinBridge = operatorCtx_->task()->getCustomJoinBridge(
        operatorCtx_->driverCtx()->splitGroupId, planNodeId());
    auto buildFinished =
        std::dynamic_pointer_cast<TestSkewedJoinBridge>(joinBridge)
            ->buildFinishedOrFuture(future);
    if (!buildFinished.has_value()) {
      return exec::BlockingReason::kWaitForJoinBuild;
    }
    return exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return finished_;
  }

 private:
  bool finished_{false};
};

class TestSkewedJoinBridgeTranslator
    : public exec::Operator::PlanNodeTranslator {
  std::unique_ptr<exec::Operator> toOperator(
      exec::DriverCtx* ctx,
      int32_t id,
      const core::PlanNodePtr& node) override {
    if (auto joinNode =
            std::dynamic_pointer_cast<const TestSkewedJoinNode>(node)) {
      return std::make_unique<TestSkewedJoinProbe>(id, ctx, joinNode);
    }
    return nullptr;
  }

  std::unique_ptr<exec::JoinBridge> toJoinBridge(
      const core::PlanNodePtr& node) override {
    if (std::dynamic_pointer_cast<const TestSkewedJoinNode>(node)) {
      return std::make_unique<TestSkewedJoinBridge>();
    }
    return nullptr;
  }

  exec::OperatorSupplier toOperatorSupplier(
      const core::PlanNodePtr& node) override {
    if (auto joinNode =
            std::dynamic_pointer_cast<const TestSkewedJoinNode>(node)) {
      return [joinNode](int32_t operatorId, exec::DriverCtx* ctx) {
        return std::make_unique<TestSkewedJoinBuild>(operatorId, ctx, joinNode);
      };
    }
    return nullptr;
  }
};

class ExternalBlocker {
 public:
  folly::SemiFuture<folly::Unit> continueFuture() {
    if (isBlocked_) {
      auto [promise, future] = makeVeloxContinuePromiseContract();
      continuePromise_ = std::move(promise);
      return std::move(future);
    }
    return folly::SemiFuture<folly::Unit>();
  }

  void unblock() {
    if (isBlocked_) {
      continuePromise_.setValue();
      isBlocked_ = false;
    }
  }

  void block() {
    isBlocked_ = true;
  }

  bool isBlocked() const {
    return isBlocked_;
  }

 private:
  bool isBlocked_ = false;
  folly::Promise<folly::Unit> continuePromise_;
};

// A test node that normally just re-project/passthrough the output from input
// When the node is blocked by external even (via externalBlocker), the operator
// will signal kBlocked. The pipeline can ONLY proceed again when it is
// unblocked externally.
class TestExternalBlockableNode : public core::PlanNode {
 public:
  TestExternalBlockableNode(
      const core::PlanNodeId& id,
      core::PlanNodePtr source,
      std::shared_ptr<ExternalBlocker> externalBlocker)
      : PlanNode(id),
        sources_{std::move(source)},
        externalBlocker_(std::move(externalBlocker)) {}

  const RowTypePtr& outputType() const override {
    return sources_[0]->outputType();
  }

  const std::vector<core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  std::string_view name() const override {
    return "external blocking node";
  }

  ExternalBlocker* externalBlocker() const {
    return externalBlocker_.get();
  }

 private:
  void addDetails(std::stringstream& /* stream */) const override {}

  std::vector<core::PlanNodePtr> sources_;
  std::shared_ptr<ExternalBlocker> externalBlocker_;
};

class TestExternalBlockableOperator : public exec::Operator {
 public:
  TestExternalBlockableOperator(
      int32_t operatorId,
      exec::DriverCtx* driverCtx,
      std::shared_ptr<const TestExternalBlockableNode> node)
      : Operator(
            driverCtx,
            node->outputType(),
            operatorId,
            node->id(),
            "ExternalBlockable"),
        externalBlocker_(node->externalBlocker()) {}

  bool needsInput() const override {
    return !noMoreInput_;
  }

  void addInput(RowVectorPtr input) override {
    input_ = std::move(input);
  }

  RowVectorPtr getOutput() override {
    // If this operator is signaled to be blocked externally
    if (externalBlocker_->isBlocked()) {
      continueFuture_ = externalBlocker_->continueFuture();
      return nullptr;
    }
    auto output = std::move(input_);
    input_ = nullptr;
    return output;
  }

  exec::BlockingReason isBlocked(ContinueFuture* future) override {
    if (continueFuture_.valid()) {
      *future = std::move(continueFuture_);
      return exec::BlockingReason::kWaitForConsumer;
    }
    return exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return noMoreInput_;
  }

 private:
  RowVectorPtr input_;
  ExternalBlocker* externalBlocker_;
  folly::SemiFuture<folly::Unit> continueFuture_;
};

class TestExternalBlockableTranslator
    : public exec::Operator::PlanNodeTranslator {
  std::unique_ptr<exec::Operator> toOperator(
      exec::DriverCtx* ctx,
      int32_t id,
      const core::PlanNodePtr& node) override {
    if (auto castedNode =
            std::dynamic_pointer_cast<const TestExternalBlockableNode>(node)) {
      return std::make_unique<TestExternalBlockableOperator>(
          id, ctx, castedNode);
    }
    return nullptr;
  }
};

// A test node creates operator that allocate memory from velox memory pool on
// construction.
class TestBadMemoryNode : public core::PlanNode {
 public:
  TestBadMemoryNode(const core::PlanNodeId& id, core::PlanNodePtr source)
      : PlanNode(id), sources_{std::move(source)} {}

  const RowTypePtr& outputType() const override {
    return sources_[0]->outputType();
  }

  const std::vector<core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  std::string_view name() const override {
    return "bad memory node";
  }

 private:
  void addDetails(std::stringstream& /* stream */) const override {}

  std::vector<core::PlanNodePtr> sources_;
};

class TestBadMemoryOperator : public exec::Operator {
 public:
  TestBadMemoryOperator(
      int32_t operatorId,
      exec::DriverCtx* driverCtx,
      std::shared_ptr<const TestBadMemoryNode> node)
      : Operator(
            driverCtx,
            node->outputType(),
            operatorId,
            node->id(),
            "BadMemory") {
    pool()->allocateNonContiguous(1, allocation_);
  }

  bool needsInput() const override {
    return !noMoreInput_;
  }

  void addInput(RowVectorPtr /*unused*/) override {}

  RowVectorPtr getOutput() override {
    return nullptr;
  }

  exec::BlockingReason isBlocked(ContinueFuture* /*unused*/) override {
    return exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return noMoreInput_;
  }

 private:
  memory::Allocation allocation_;
};

class TestBadMemoryTranslator : public exec::Operator::PlanNodeTranslator {
  std::unique_ptr<exec::Operator> toOperator(
      exec::DriverCtx* ctx,
      int32_t id,
      const core::PlanNodePtr& node) override {
    if (auto castedNode =
            std::dynamic_pointer_cast<const TestBadMemoryNode>(node)) {
      return std::make_unique<TestBadMemoryOperator>(id, ctx, castedNode);
    }
    return nullptr;
  }
};
} // namespace
class TaskTest : public HiveConnectorTestBase {
 protected:
  static std::pair<std::shared_ptr<exec::Task>, std::vector<RowVectorPtr>>
  executeSingleThreaded(
      core::PlanFragment plan,
      const std::unordered_map<std::string, std::vector<std::string>>&
          filePaths = {}) {
    auto task = Task::create(
        "single.execution.task.0", plan, 0, std::make_shared<core::QueryCtx>());

    for (const auto& [nodeId, paths] : filePaths) {
      for (const auto& path : paths) {
        task->addSplit(nodeId, exec::Split(makeHiveConnectorSplit(path)));
      }
      task->noMoreSplits(nodeId);
    }

    VELOX_CHECK(task->supportsSingleThreadedExecution());

    vector_size_t numRows = 0;
    std::vector<RowVectorPtr> results;
    for (;;) {
      auto result = task->next();
      if (!result) {
        break;
      }

      for (auto& child : result->children()) {
        child->loadedVector();
      }
      results.push_back(result);
      numRows += result->size();
    }

    VELOX_CHECK(waitForTaskCompletion(task.get()));

    auto planNodeStats = toPlanStats(task->taskStats());
    VELOX_CHECK(planNodeStats.count(plan.planNode->id()));
    VELOX_CHECK_EQ(numRows, planNodeStats.at(plan.planNode->id()).outputRows);
    VELOX_CHECK_EQ(
        results.size(), planNodeStats.at(plan.planNode->id()).outputVectors);

    return {task, results};
  }
};

TEST_F(TaskTest, wrongPlanNodeForSplit) {
  auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
      "test",
      "file:/tmp/abc",
      facebook::velox::dwio::common::FileFormat::DWRF,
      0,
      100);

  auto plan = PlanBuilder()
                  .tableScan(ROW({"a", "b"}, {INTEGER(), DOUBLE()}))
                  .project({"a * a", "b + b"})
                  .planFragment();

  auto task = Task::create(
      "task-1",
      std::move(plan),
      0,
      std::make_shared<core::QueryCtx>(driverExecutor_.get()));

  ASSERT_EQ(
      task->toString(), "{Task task-1 (task-1)Plan: -- Project\n\n drivers:\n");
  ASSERT_EQ(
      folly::toPrettyJson(task->toJson()),
      "{\n  \"concurrentSplitGroups\": 1,\n  \"drivers\": [],\n  \"exchangeClientByPlanNode\": {},\n  \"groupedPartitionedOutput\": false,\n  \"id\": \"task-1\",\n  \"noMoreOutputBuffers\": false,\n  \"numDriversPerSplitGroup\": 0,\n  \"numDriversUngrouped\": 0,\n  \"numFinishedDrivers\": 0,\n  \"numRunningDrivers\": 0,\n  \"numRunningSplitGroups\": 0,\n  \"numThreads\": 0,\n  \"numTotalDrivers\": 0,\n  \"onThreadSince\": \"0\",\n  \"partitionedOutputConsumed\": false,\n  \"pauseRequested\": false,\n  \"plan\": \"-- Project[expressions: (p0:INTEGER, multiply(ROW[\\\"a\\\"],ROW[\\\"a\\\"])), (p1:DOUBLE, plus(ROW[\\\"b\\\"],ROW[\\\"b\\\"]))] -> p0:INTEGER, p1:DOUBLE\\n  -- TableScan[table: hive_table] -> a:INTEGER, b:DOUBLE\\n\",\n  \"shortId\": \"task-1\",\n  \"state\": \"Running\",\n  \"terminateRequested\": false\n}");
  ASSERT_EQ(
      folly::toPrettyJson(task->toShortJson()),
      "{\n  \"id\": \"task-1\",\n  \"numFinishedDrivers\": 0,\n  \"numRunningDrivers\": 0,\n  \"numThreads\": 0,\n  \"numTotalDrivers\": 0,\n  \"pauseRequested\": false,\n  \"shortId\": \"task-1\",\n  \"state\": \"Running\",\n  \"terminateRequested\": false\n}");

  // Add split for the source node.
  task->addSplit("0", exec::Split(folly::copy(connectorSplit)));

  // Add an empty split.
  task->addSplit("0", exec::Split());

  // Try to add split for a non-source node.
  auto errorMessage =
      "Splits can be associated only with leaf plan nodes which require splits. Plan node ID 1 doesn't refer to such plan node.";
  VELOX_ASSERT_THROW(
      task->addSplit("1", exec::Split(folly::copy(connectorSplit))),
      errorMessage)

  VELOX_ASSERT_THROW(
      task->addSplitWithSequence(
          "1", exec::Split(folly::copy(connectorSplit)), 3),
      errorMessage)

  VELOX_ASSERT_THROW(task->setMaxSplitSequenceId("1", 9), errorMessage)

  VELOX_ASSERT_THROW(task->noMoreSplits("1"), errorMessage)

  VELOX_ASSERT_THROW(task->noMoreSplitsForGroup("1", 5), errorMessage)

  // Try to add split for non-existent node.
  errorMessage =
      "Splits can be associated only with leaf plan nodes which require splits. Plan node ID 12 doesn't refer to such plan node.";
  VELOX_ASSERT_THROW(
      task->addSplit("12", exec::Split(folly::copy(connectorSplit))),
      errorMessage)

  VELOX_ASSERT_THROW(
      task->addSplitWithSequence(
          "12", exec::Split(folly::copy(connectorSplit)), 3),
      errorMessage)

  VELOX_ASSERT_THROW(task->setMaxSplitSequenceId("12", 9), errorMessage)

  VELOX_ASSERT_THROW(task->noMoreSplits("12"), errorMessage)

  VELOX_ASSERT_THROW(task->noMoreSplitsForGroup("12", 5), errorMessage)

  // Try to add split for a Values source node.
  plan =
      PlanBuilder()
          .values({makeRowVector(ROW({"a", "b"}, {INTEGER(), DOUBLE()}), 10)})
          .project({"a * a", "b + b"})
          .planFragment();

  auto valuesTask = Task::create(
      "task-2",
      std::move(plan),
      0,
      std::make_shared<core::QueryCtx>(driverExecutor_.get()));
  errorMessage =
      "Splits can be associated only with leaf plan nodes which require splits. Plan node ID 0 doesn't refer to such plan node.";
  VELOX_ASSERT_THROW(
      valuesTask->addSplit("0", exec::Split(folly::copy(connectorSplit))),
      errorMessage)
}

TEST_F(TaskTest, duplicatePlanNodeIds) {
  auto plan = PlanBuilder()
                  .tableScan(ROW({"a", "b"}, {INTEGER(), DOUBLE()}))
                  .hashJoin(
                      {"a"},
                      {"a1"},
                      PlanBuilder()
                          .tableScan(ROW({"a1", "b1"}, {INTEGER(), DOUBLE()}))
                          .planNode(),
                      "",
                      {"b", "b1"})
                  .planFragment();

  VELOX_ASSERT_THROW(
      Task::create(
          "task-1",
          std::move(plan),
          0,
          std::make_shared<core::QueryCtx>(driverExecutor_.get())),
      "Plan node IDs must be unique. Found duplicate ID: 0.")
}

// This test simulates the following execution sequence that potentially can
// cause a deadlock:
// 1. A join task comes in to execution.
// 2. All join bridges finished except for the last one.
// 3. An abort request comes in to terminate the task.
// 4. Task::terminate() acquires the task lock, trying to clear/clean various
// states that contain a bunch of futures, including the ones in BarrierState
// that serve the purpose of making sure all join builds finish before
// proceeding.
// 5. If not handled correctly, the futures that are tried to be cleaned will
// execute the error block chained in the future, that tries to set error in the
// task. Setting error requires to acquire the same task lock again.
// 6. Since we use immediate executor to execute these futures, a deadlock
// happens.
TEST_F(TaskTest, testTerminateDeadlock) {
  const int64_t kSlowJoinBuildDelaySeconds = 2;
  const int64_t kTaskAbortDelaySeconds = 1;
  const int64_t kMaxErrorTimeSeconds = 3;
  exec::Operator::registerOperator(
      std::make_unique<TestSkewedJoinBridgeTranslator>());

  auto leftBatch = makeRowVector(
      {makeFlatVector<int32_t>(100, [](auto row) { return row; })});
  auto rightBatch = makeRowVector(
      {makeFlatVector<int32_t>(10, [](auto row) { return row; })});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto leftNode =
      PlanBuilder(planNodeIdGenerator).values({leftBatch}, true).planNode();
  auto rightNode =
      PlanBuilder(planNodeIdGenerator).values({rightBatch}, true).planNode();

  CursorParameters params;
  params.maxDrivers = 2;
  params.planNode =
      PlanBuilder(planNodeIdGenerator)
          .addNode([&leftNode, &rightNode, &kSlowJoinBuildDelaySeconds](
                       std::string id, core::PlanNodePtr /* input */) {
            return std::make_shared<TestSkewedJoinNode>(
                id,
                std::move(leftNode),
                std::move(rightNode),
                kSlowJoinBuildDelaySeconds);
          })
          .project({"c0"})
          .planNode();

  auto cursor = TaskCursor::create(params);

  folly::via(cursor->task()->queryCtx()->executor(), [&]() {
    // We abort after all but last join bridges finish execution. We do this
    // in another thread because cursor->moveNext() will block.
    std::this_thread::sleep_for(std::chrono::seconds(kTaskAbortDelaySeconds));
    cursor->task()->requestAbort();
  }).onTimeout(std::chrono::seconds(kMaxErrorTimeSeconds), [&]() {
    // Task should be aborted by now. If it ever comes here, it is likely due
    // to a deadlock.
    ASSERT_TRUE(false);
  });

  VELOX_ASSERT_THROW(cursor->moveNext(), "Aborted for external error");
}

TEST_F(TaskTest, singleThreadedExecution) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>(1'000, [](auto row) { return row; }),
  });

  // Filter + Project.
  auto plan = PlanBuilder()
                  .values({data, data})
                  .filter("c0 < 100")
                  .project({"c0 + 5"})
                  .planFragment();

  auto expectedResult = makeRowVector({
      makeFlatVector<int64_t>(100, [](auto row) { return row + 5; }),
  });

  uint64_t numCreatedTasks = Task::numCreatedTasks();
  uint64_t numDeletedTasks = Task::numDeletedTasks();
  {
    auto [task, results] = executeSingleThreaded(plan);
    assertEqualResults(
        std::vector<RowVectorPtr>{expectedResult, expectedResult}, results);
  }
  ASSERT_EQ(numCreatedTasks + 1, Task::numCreatedTasks());
  ASSERT_EQ(numDeletedTasks + 1, Task::numDeletedTasks());

  // Project + Aggregation.
  plan = PlanBuilder()
             .values({data, data})
             .project({"c0 % 5 as k", "c0"})
             .singleAggregation({"k"}, {"sum(c0)", "avg(c0)"})
             .planFragment();

  expectedResult = makeRowVector({
      makeFlatVector<int64_t>({0, 1, 2, 3, 4}),
      makeFlatVector<int64_t>(
          {995 * 200,
           995 * 200 + 400,
           995 * 200 + 800,
           995 * 200 + 1200,
           995 * 200 + 1600}),
      makeFlatVector<double>(
          {995 / 2.0,
           995 / 2.0 + 1,
           995 / 2.0 + 2,
           995 / 2.0 + 3,
           995 / 2.0 + 4}),
  });

  ++numCreatedTasks;
  ++numDeletedTasks;
  {
    auto [task, results] = executeSingleThreaded(plan);
    assertEqualResults({expectedResult}, results);
  }
  ASSERT_EQ(numCreatedTasks + 1, Task::numCreatedTasks());
  ASSERT_EQ(numDeletedTasks + 1, Task::numDeletedTasks());

  // Project + Aggregation over TableScan.
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, {data, data});

  core::PlanNodeId scanId;
  plan = PlanBuilder()
             .tableScan(asRowType(data->type()))
             .capturePlanNodeId(scanId)
             .project({"c0 % 5 as k", "c0"})
             .singleAggregation({"k"}, {"sum(c0)", "avg(c0)"})
             .planFragment();

  {
    auto [task, results] =
        executeSingleThreaded(plan, {{scanId, {filePath->path}}});
    assertEqualResults({expectedResult}, results);
  }

  // Query failure.
  plan = PlanBuilder().values({data, data}).project({"c0 / 0"}).planFragment();
  VELOX_ASSERT_THROW(executeSingleThreaded(plan), "division by zero");
}

TEST_F(TaskTest, singleThreadedHashJoin) {
  auto left = makeRowVector(
      {"t_c0", "t_c1"},
      {
          makeFlatVector<int64_t>({1, 2, 3, 4}),
          makeFlatVector<int64_t>({10, 20, 30, 40}),
      });
  auto leftPath = TempFilePath::create();
  writeToFile(leftPath->path, {left});

  auto right = makeRowVector(
      {"u_c0"},
      {
          makeFlatVector<int64_t>({0, 1, 3, 5}),
      });
  auto rightPath = TempFilePath::create();
  writeToFile(rightPath->path, {right});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId leftScanId;
  core::PlanNodeId rightScanId;
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .tableScan(asRowType(left->type()))
                  .capturePlanNodeId(leftScanId)
                  .hashJoin(
                      {"t_c0"},
                      {"u_c0"},
                      PlanBuilder(planNodeIdGenerator)
                          .tableScan(asRowType(right->type()))
                          .capturePlanNodeId(rightScanId)
                          .planNode(),
                      "",
                      {"t_c0", "t_c1", "u_c0"})
                  .planFragment();

  auto expectedResult = makeRowVector({
      makeFlatVector<int64_t>({1, 3}),
      makeFlatVector<int64_t>({10, 30}),
      makeFlatVector<int64_t>({1, 3}),
  });

  {
    auto [task, results] = executeSingleThreaded(
        plan,
        {{leftScanId, {leftPath->path}}, {rightScanId, {rightPath->path}}});
    assertEqualResults({expectedResult}, results);
  }
}

TEST_F(TaskTest, singleThreadedCrossJoin) {
  auto left = makeRowVector({"t_c0"}, {makeFlatVector<int64_t>({1, 2, 3})});
  auto leftPath = TempFilePath::create();
  writeToFile(leftPath->path, {left});

  auto right = makeRowVector({"u_c0"}, {makeFlatVector<int64_t>({10, 20})});
  auto rightPath = TempFilePath::create();
  writeToFile(rightPath->path, {right});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId leftScanId;
  core::PlanNodeId rightScanId;
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .tableScan(asRowType(left->type()))
                  .capturePlanNodeId(leftScanId)
                  .nestedLoopJoin(
                      PlanBuilder(planNodeIdGenerator)
                          .tableScan(asRowType(right->type()))
                          .capturePlanNodeId(rightScanId)
                          .planNode(),
                      {"t_c0", "u_c0"})
                  .planFragment();

  auto expectedResult = makeRowVector({
      makeFlatVector<int64_t>({1, 1, 2, 2, 3, 3}),
      makeFlatVector<int64_t>({10, 20, 10, 20, 10, 20}),

  });

  {
    auto [task, results] = executeSingleThreaded(
        plan,
        {{leftScanId, {leftPath->path}}, {rightScanId, {rightPath->path}}});
    assertEqualResults({expectedResult}, results);
  }
}

TEST_F(TaskTest, singleThreadedExecutionExternalBlockable) {
  exec::Operator::registerOperator(
      std::make_unique<TestExternalBlockableTranslator>());
  auto data = makeRowVector({
      makeFlatVector<int64_t>(1'000, [](auto row) { return row; }),
  });
  auto blocker = std::make_shared<ExternalBlocker>();
  // Filter + Project.
  auto plan =
      PlanBuilder()
          .values({data, data, data})
          .addNode([blocker](std::string id, core::PlanNodePtr input) mutable {
            return std::make_shared<TestExternalBlockableNode>(
                id, input, std::move(blocker));
          })
          .project({"c0"})
          .planFragment();

  ContinueFuture continueFuture = ContinueFuture::makeEmpty();
  // First pass, we don't activate the external blocker, expect the task to run
  // without being blocked.
  auto nonBlockingTask = Task::create(
      "single.execution.task.0", plan, 0, std::make_shared<core::QueryCtx>());
  std::vector<RowVectorPtr> results;
  for (;;) {
    auto result = nonBlockingTask->next(&continueFuture);
    if (!result) {
      break;
    }
    EXPECT_FALSE(continueFuture.valid());
    results.push_back(std::move(result));
  }
  EXPECT_EQ(3, results.size());

  results.clear();
  continueFuture = ContinueFuture::makeEmpty();
  // Second pass, we will now use external blockers to block the task.
  auto blockingTask = Task::create(
      "single.execution.task.1", plan, 0, std::make_shared<core::QueryCtx>());
  // Before we block, we expect `next` to get data normally.
  results.push_back(blockingTask->next(&continueFuture));
  EXPECT_TRUE(results.back() != nullptr);
  // Now, we want to block the pipeline by external event. We expect `next` to
  // return null.  The `future` should be updated for the caller to wait before
  // calling next() again
  blocker->block();
  EXPECT_EQ(nullptr, blockingTask->next(&continueFuture));
  EXPECT_TRUE(continueFuture.valid() && !continueFuture.isReady());
  // After the pipeline is unblocked by external event, `continueFuture` should
  // get realized right away
  blocker->unblock();
  std::move(continueFuture).wait();
  // Now, we should be able to normally get data from Task.
  for (;;) {
    auto result = blockingTask->next(&continueFuture);
    if (!result) {
      break;
    }
    results.push_back(std::move(result));
  }
  EXPECT_EQ(3, results.size());
}

TEST_F(TaskTest, supportsSingleThreadedExecution) {
  auto plan = PlanBuilder()
                  .tableScan(ROW({"c0"}, {BIGINT()}))
                  .project({"c0 % 10"})
                  .partitionedOutput({}, 1, std::vector<std::string>{"p0"})
                  .planFragment();
  auto task = Task::create(
      "single.execution.task.0", plan, 0, std::make_shared<core::QueryCtx>());

  // PartitionedOutput does not support single threaded execution, therefore the
  // task doesn't support it either.
  ASSERT_FALSE(task->supportsSingleThreadedExecution());
}

TEST_F(TaskTest, updateBroadCastOutputBuffers) {
  auto plan = PlanBuilder()
                  .tableScan(ROW({"c0"}, {BIGINT()}))
                  .project({"c0 % 10"})
                  .partitionedOutputBroadcast({})
                  .planFragment();
  auto bufferManager = OutputBufferManager::getInstance().lock();
  {
    auto task = Task::create(
        "t0", plan, 0, std::make_shared<core::QueryCtx>(driverExecutor_.get()));

    task->start(1, 1);

    ASSERT_TRUE(task->updateOutputBuffers(10, true /*noMoreBuffers*/));

    // Calls after no-more-buffers are ignored.
    ASSERT_FALSE(task->updateOutputBuffers(11, false));

    task->requestCancel();
  }

  {
    auto task = Task::create(
        "t1", plan, 0, std::make_shared<core::QueryCtx>(driverExecutor_.get()));

    task->start(1, 1);

    ASSERT_TRUE(task->updateOutputBuffers(5, false));
    ASSERT_TRUE(task->updateOutputBuffers(10, false));

    task->requestAbort();

    // Calls after task has been removed from the buffer manager (via abort) are
    // ignored.
    ASSERT_FALSE(task->updateOutputBuffers(15, true));
  }
}

DEBUG_ONLY_TEST_F(TaskTest, outputDriverFinishEarly) {
  const int32_t numBatches = 10;
  std::vector<RowVectorPtr> dataBatches;
  dataBatches.reserve(numBatches);
  for (int32_t i = 0; i < numBatches; ++i) {
    dataBatches.push_back(makeRowVector({makeFlatVector<int64_t>({0, 1, 10})}));
  }

  // Create a query plan fragment with one input pipeline and one output
  // pipeline and both have only one driver.
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .localMerge(
              {"c0"},
              {PlanBuilder(planNodeIdGenerator).values(dataBatches).planNode()})
          .limit(0, 1, false)
          .planNode();

  // Set up the test value to generate the race condition that the output
  // pipeline finishes early and terminate the task while the input pipeline
  // driver is running on thread.
  ContinuePromise valuePromise("mergePromise");
  ContinueFuture valueFuture = valuePromise.getSemiFuture();
  ContinuePromise driverPromise("driverPromise");
  ContinueFuture driverFuture = driverPromise.getSemiFuture();
  ContinuePromise noMoreInputPromise("noMoreInputPromise");
  ContinueFuture noMoreInputFuture = noMoreInputPromise.getSemiFuture();

  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Values::getOutput",
      std::function<void(const velox::exec::Values*)>(
          ([&](const velox::exec::Values* values) {
            // Only blocks the value node on the second output.
            if (values->testingCurrent() != 1) {
              return;
            }
            noMoreInputPromise.setValue();
            std::move(valueFuture).wait();
            driverPromise.setValue();
          })));

  // There's two Drivers on two separate threads that are run with a LocalMerge
  // in between.  It's possible that the Driver executing the Values operator
  // gets through one iteration, then the Driver executing the Limit operator
  // gets through one iteration, at which point the Task is terminated.  To
  // ensure the Driver executing the Values operator gets to the second
  // iteration, we wait on the future here at the CallbackSink following the
  // Limit operator.
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal::noMoreInput",
      std::function<void(Operator*)>(([&](Operator* op) {
        if (op->operatorType() != "CallbackSink") {
          return;
        }
        std::move(noMoreInputFuture).wait();
      })));

  CursorParameters params;
  params.planNode = plan;
  params.queryCtx = std::make_shared<core::QueryCtx>(driverExecutor_.get());
  params.queryCtx->testingOverrideConfigUnsafe(
      {{core::QueryConfig::kPreferredOutputBatchRows, "1"}});

  {
    auto cursor = TaskCursor::create(params);
    std::vector<RowVectorPtr> result;
    auto* task = cursor->task().get();
    while (cursor->moveNext()) {
      result.push_back(cursor->current());
    }
    assertResults(
        result,
        params.planNode->outputType(),
        "VALUES (0)",
        duckDbQueryRunner_);
    ASSERT_TRUE(waitForTaskStateChange(task, TaskState::kFinished, 3'000'000));
  }
  valuePromise.setValue();
  // Wait for Values driver to complete.
  driverFuture.wait();
}

/// Test that we export operator stats for unfinished (running) operators.
DEBUG_ONLY_TEST_F(TaskTest, liveStats) {
  constexpr int32_t numBatches = 10;
  std::vector<RowVectorPtr> dataBatches;
  dataBatches.reserve(numBatches);
  for (int32_t i = 0; i < numBatches; ++i) {
    dataBatches.push_back(makeRowVector({makeFlatVector<int64_t>({0, 1, 10})}));
  }

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator).values(dataBatches).planNode();

  Task* task = nullptr;
  std::array<TaskStats, numBatches + 1> liveStats; // [0, 10].
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Values::getOutput",
      std::function<void(const velox::exec::Values*)>(
          ([&](const velox::exec::Values* values) {
            liveStats[values->testingCurrent()] = task->taskStats();
          })));

  CursorParameters params;
  params.planNode = plan;
  params.queryCtx = std::make_shared<core::QueryCtx>(executor_.get());
  params.queryCtx->testingOverrideConfigUnsafe(
      {{core::QueryConfig::kPreferredOutputBatchRows, "1"}});

  auto cursor = TaskCursor::create(params);
  std::vector<RowVectorPtr> result;
  task = cursor->task().get();
  while (cursor->moveNext()) {
    result.push_back(cursor->current());
  }
  EXPECT_TRUE(waitForTaskCompletion(task)) << task->taskId();

  TaskStats finishStats = task->taskStats();

  for (auto i = 0; i < numBatches; ++i) {
    const auto& operatorStats = liveStats[i].pipelineStats[0].operatorStats[0];
    EXPECT_EQ(i, operatorStats.getOutputTiming.count);
    EXPECT_EQ(32 * i, operatorStats.outputBytes);
    EXPECT_EQ(3 * i, operatorStats.outputPositions);
    EXPECT_EQ(i, operatorStats.outputVectors);
    EXPECT_EQ(0, operatorStats.finishTiming.count);
    EXPECT_EQ(0, operatorStats.backgroundTiming.count);

    EXPECT_EQ(1, liveStats[i].numTotalDrivers);
    EXPECT_EQ(0, liveStats[i].numCompletedDrivers);
    EXPECT_EQ(0, liveStats[i].numTerminatedDrivers);
    EXPECT_EQ(1, liveStats[i].numRunningDrivers);
    EXPECT_EQ(0, liveStats[i].numBlockedDrivers.size());

    EXPECT_EQ(0, liveStats[i].executionEndTimeMs);
    EXPECT_EQ(0, liveStats[i].terminationTimeMs);
  }

  EXPECT_EQ(1, finishStats.numTotalDrivers);
  EXPECT_EQ(1, finishStats.numCompletedDrivers);
  EXPECT_EQ(0, finishStats.numTerminatedDrivers);
  EXPECT_EQ(0, finishStats.numRunningDrivers);
  EXPECT_EQ(0, finishStats.numBlockedDrivers.size());

  const auto& operatorStats = finishStats.pipelineStats[0].operatorStats[0];
  EXPECT_EQ(numBatches + 1, operatorStats.getOutputTiming.count);
  EXPECT_EQ(32 * numBatches, operatorStats.outputBytes);
  EXPECT_EQ(3 * numBatches, operatorStats.outputPositions);
  EXPECT_EQ(numBatches, operatorStats.outputVectors);
  EXPECT_EQ(1, operatorStats.finishTiming.count);
  // No operators with background CPU time yet.
  EXPECT_EQ(0, operatorStats.backgroundTiming.count);

  EXPECT_NE(0, finishStats.executionEndTimeMs);
  EXPECT_NE(0, finishStats.terminationTimeMs);
  const auto terminationTimeMs = finishStats.terminationTimeMs;

  // Sleep to allow time to pass, so the values change.
  std::this_thread::sleep_for(std::chrono::milliseconds{5});
  EXPECT_NE(0, task->timeSinceEndMs());
  EXPECT_NE(0, task->timeSinceTerminationMs());

  // This should be a no-op.
  task->requestCancel();
  EXPECT_EQ(terminationTimeMs, task->taskStats().terminationTimeMs);
}

TEST_F(TaskTest, outputBufferSize) {
  constexpr int32_t numBatches = 10;
  std::vector<RowVectorPtr> dataBatches;
  dataBatches.reserve(numBatches);
  const int numRows = numBatches * 3;
  for (int32_t i = 0; i < numBatches; ++i) {
    dataBatches.push_back(makeRowVector({makeFlatVector<int64_t>({0, 1, 10})}));
  }

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(dataBatches)
                  .partitionedOutput({}, 1)
                  .planNode();

  CursorParameters params;
  params.planNode = plan;
  params.queryCtx = std::make_shared<core::QueryCtx>(executor_.get());

  // Produce the results to output buffer manager but not consuming them to
  // check the buffer utilization in test.
  auto cursor = TaskCursor::create(params);
  std::vector<RowVectorPtr> result;
  Task* task = cursor->task().get();
  while (cursor->moveNext()) {
    result.push_back(cursor->current());
  }

  const TaskStats finishStats = task->taskStats();
  // We only have one task and the task has outputBuffer which won't be
  // consumed, verify 0 < outputBufferUtilization < 1.
  // Need to call requestCancel to explicitly terminate the task.
  ASSERT_GT(finishStats.outputBufferUtilization, 0);
  ASSERT_LT(finishStats.outputBufferUtilization, 1);
  ASSERT_TRUE(finishStats.outputBufferOverutilized);
  ASSERT_TRUE(finishStats.outputBufferStats.has_value());
  const auto outputStats = finishStats.outputBufferStats.value();
  ASSERT_EQ(outputStats.kind, core::PartitionedOutputNode::Kind::kPartitioned);
  ASSERT_EQ(outputStats.totalRowsSent, numRows);
  ASSERT_GT(outputStats.totalPagesSent, 0);
  ASSERT_GT(outputStats.bufferedBytes, 0);
  ASSERT_EQ(outputStats.bufferedPages, outputStats.totalPagesSent);
  task->requestCancel();
}

DEBUG_ONLY_TEST_F(TaskTest, findPeerOperators) {
  const std::vector<RowVectorPtr> probeVectors = {makeRowVector(
      {"t_c0", "t_c1"},
      {
          makeFlatVector<int64_t>({1, 2, 3, 4}),
          makeFlatVector<int64_t>({10, 20, 30, 40}),
      })};

  const std::vector<RowVectorPtr> buildVectors = {makeRowVector(
      {"u_c0"},
      {
          makeFlatVector<int64_t>({0, 1, 3, 5}),
      })};

  const std::vector<int> numDrivers = {1, 4};
  for (int numDriver : numDrivers) {
    SCOPED_TRACE(fmt::format("numDriver {}", numDriver));
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    CursorParameters params;
    params.planNode = PlanBuilder(planNodeIdGenerator)
                          .values(probeVectors, true)
                          .hashJoin(
                              {"t_c0"},
                              {"u_c0"},
                              PlanBuilder(planNodeIdGenerator)
                                  .values(buildVectors, true)
                                  .planNode(),
                              "",
                              {"t_c0", "t_c1", "u_c0"})
                          .planNode();
    params.queryCtx = std::make_shared<core::QueryCtx>(driverExecutor_.get());
    params.maxDrivers = numDriver;

    auto cursor = TaskCursor::create(params);
    auto* task = cursor->task().get();

    // Set up a testvalue to trigger task abort when hash build tries to reserve
    // memory.
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::addInput",
        std::function<void(Operator*)>([&](Operator* testOp) {
          if (testOp->operatorType() != "HashBuild") {
            return;
          }
          const int pipelineId =
              testOp->testingOperatorCtx()->driverCtx()->pipelineId;
          auto ops = task->findPeerOperators(pipelineId, testOp);
          ASSERT_EQ(ops.size(), numDriver);
          bool foundSelf{false};
          for (auto* op : ops) {
            auto* opCtx = op->testingOperatorCtx();
            ASSERT_EQ(op->operatorType(), "HashBuild");
            if (op == testOp) {
              foundSelf = true;
            }
            auto* driver = opCtx->driver();
            ASSERT_EQ(op, driver->findOperator(opCtx->operatorId()));
            VELOX_ASSERT_THROW(driver->findOperator(-1), "");
            VELOX_ASSERT_THROW(driver->findOperator(numDriver + 10), "");
          }
          ASSERT_TRUE(foundSelf);
        }));

    while (cursor->moveNext()) {
    }
    ASSERT_TRUE(waitForTaskCompletion(task, 5'000'000));
  }
}

DEBUG_ONLY_TEST_F(TaskTest, raceBetweenTaskPauseAndTerminate) {
  const std::vector<RowVectorPtr> values = {makeRowVector(
      {"t_c0", "t_c1"},
      {
          makeFlatVector<int64_t>({1, 2, 3, 4}),
          makeFlatVector<int64_t>({10, 20, 30, 40}),
      })};

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  CursorParameters params;
  params.planNode =
      PlanBuilder(planNodeIdGenerator).values(values, true).planNode();
  params.queryCtx = std::make_shared<core::QueryCtx>(driverExecutor_.get());
  params.maxDrivers = 1;

  auto cursor = TaskCursor::create(params);
  auto* task = cursor->task().get();
  folly::EventCount taskPauseWait;
  std::atomic<bool> taskPaused{false};

  folly::EventCount taskPauseStartWait;
  std::atomic<bool> taskPauseStarted{false};

  // Set up a testvalue to trigger task abort when hash build tries to reserve
  // memory.
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal::addInput",
      std::function<void(Operator*)>([&](Operator* testOp) {
        if (taskPauseStarted.exchange(true)) {
          return;
        }
        taskPauseStartWait.notifyAll();
        taskPauseWait.await([&]() { return taskPaused.load(); });
      }));

  std::thread taskThread([&]() {
    try {
      while (cursor->moveNext()) {
      };
    } catch (VeloxRuntimeError& ex) {
    }
  });

  taskPauseStartWait.await([&]() { return taskPauseStarted.load(); });

  ASSERT_EQ(task->numTotalDrivers(), 1);
  ASSERT_EQ(task->numFinishedDrivers(), 0);
  ASSERT_EQ(task->numRunningDrivers(), 1);

  auto pauseFuture = task->requestPause();
  taskPaused = true;
  taskPauseWait.notifyAll();
  pauseFuture.wait();

  ASSERT_EQ(task->numTotalDrivers(), 1);
  ASSERT_EQ(task->numFinishedDrivers(), 0);
  ASSERT_EQ(task->numRunningDrivers(), 1);

  task->requestAbort().wait();

  ASSERT_EQ(task->numTotalDrivers(), 1);
  ASSERT_EQ(task->numFinishedDrivers(), 0);
  ASSERT_EQ(task->numRunningDrivers(), 0);

  Task::resume(task->shared_from_this());

  ASSERT_EQ(task->numTotalDrivers(), 1);
  ASSERT_EQ(task->numFinishedDrivers(), 1);
  ASSERT_EQ(task->numRunningDrivers(), 0);

  taskThread.join();
}

TEST_F(TaskTest, driverCreationMemoryAllocationCheck) {
  exec::Operator::registerOperator(std::make_unique<TestBadMemoryTranslator>());
  auto data = makeRowVector({
      makeFlatVector<int64_t>(1'000, [](auto row) { return row; }),
  });
  auto plan =
      PlanBuilder()
          .values({data})
          .addNode([&](std::string id, core::PlanNodePtr input) mutable {
            return std::make_shared<TestBadMemoryNode>(id, input);
          })
          .planFragment();
  for (bool singleThreadExecution : {false, true}) {
    SCOPED_TRACE(fmt::format("singleThreadExecution: ", singleThreadExecution));
    auto badTask = Task::create(
        "driverCreationMemoryAllocationCheck",
        plan,
        0,
        std::make_shared<core::QueryCtx>());
    if (singleThreadExecution) {
      VELOX_ASSERT_THROW(
          badTask->start(1), "Unexpected memory pool allocations");
    } else {
      VELOX_ASSERT_THROW(badTask->next(), "Unexpected memory pool allocations");
    }
  }
}

TEST_F(TaskTest, spillDirectoryLifecycleManagement) {
  // Marks the spill directory as not already created and ensures that the Task
  // handles creating it on first use and eventually deleting it on destruction.
  auto data = makeRowVector({
      makeFlatVector<int64_t>(1'000, [](auto row) { return row % 300; }),
      makeFlatVector<int64_t>(1'000, [](auto row) { return row; }),
  });

  core::PlanNodeId aggrNodeId;
  const auto plan = PlanBuilder()
                        .values({data})
                        .singleAggregation({"c0"}, {"sum(c1)"}, {})
                        .capturePlanNodeId(aggrNodeId)
                        .planNode();
  CursorParameters params;
  params.planNode = plan;
  params.queryCtx = std::make_shared<core::QueryCtx>(driverExecutor_.get());
  params.queryCtx->testingOverrideConfigUnsafe(
      {{core::QueryConfig::kSpillEnabled, "true"},
       {core::QueryConfig::kAggregationSpillEnabled, "true"},
       {core::QueryConfig::kTestingSpillPct, "100"}});
  params.maxDrivers = 1;

  auto cursor = TaskCursor::create(params);
  std::shared_ptr<Task> task = cursor->task();
  auto rootTempDir = exec::test::TempDirectoryPath::create();
  auto tmpDirectoryPath =
      rootTempDir->path + "/spillDirectoryLifecycleManagement";
  task->setSpillDirectory(tmpDirectoryPath, false);

  while (cursor->moveNext()) {
  }
  ASSERT_TRUE(waitForTaskCompletion(task.get(), 5'000'000));
  EXPECT_EQ(exec::TaskState::kFinished, task->state());
  auto taskStats = exec::toPlanStats(task->taskStats());
  auto& stats = taskStats.at(aggrNodeId);
  ASSERT_GT(stats.spilledRows, 0);
  cursor.reset(); // ensure 'task' has no other shared pointer.
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
}

TEST_F(TaskTest, spillDirNotCreated) {
  // Verify that no spill directory is created if spilling is not engaged.
  const std::vector<RowVectorPtr> probeVectors = {makeRowVector(
      {"t_c0", "t_c1"},
      {
          makeFlatVector<int64_t>({1, 2, 3, 4}),
          makeFlatVector<int64_t>({10, 20, 30, 40}),
      })};

  const std::vector<RowVectorPtr> buildVectors = {makeRowVector(
      {"u_c0"},
      {
          makeFlatVector<int64_t>({0, 1, 3, 5}),
      })};

  core::PlanNodeId hashJoinNodeId;
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  CursorParameters params;
  // We use a hash join here as a Spiller object is created upfront which helps
  // us ensure that the directory creation is delayed till spilling is executed.
  params.planNode = PlanBuilder(planNodeIdGenerator)
                        .values(probeVectors, true)
                        .hashJoin(
                            {"t_c0"},
                            {"u_c0"},
                            PlanBuilder(planNodeIdGenerator)
                                .values(buildVectors, true)
                                .planNode(),
                            "",
                            {"t_c0", "t_c1", "u_c0"})
                        .capturePlanNodeId(hashJoinNodeId)
                        .planNode();
  params.queryCtx = std::make_shared<core::QueryCtx>(driverExecutor_.get());
  params.queryCtx->testingOverrideConfigUnsafe(
      {{core::QueryConfig::kSpillEnabled, "true"},
       {core::QueryConfig::kJoinSpillEnabled, "true"},
       {core::QueryConfig::kTestingSpillPct, "0"}});
  params.maxDrivers = 1;

  auto cursor = TaskCursor::create(params);
  auto* task = cursor->task().get();
  auto rootTempDir = exec::test::TempDirectoryPath::create();
  auto tmpDirectoryPath = rootTempDir->path + "/spillDirNotCreated";
  task->setSpillDirectory(tmpDirectoryPath, false);

  while (cursor->moveNext()) {
  }
  ASSERT_TRUE(waitForTaskCompletion(task, 5'000'000));
  EXPECT_EQ(exec::TaskState::kFinished, task->state());
  auto taskStats = exec::toPlanStats(task->taskStats());
  auto& stats = taskStats.at(hashJoinNodeId);
  ASSERT_EQ(stats.spilledRows, 0);
  // Check for spill folder without destroying the Task object to ensure its
  // destructor has not removed the directory if it was created earlier.
  auto fs = filesystems::getFileSystem(tmpDirectoryPath, nullptr);
  EXPECT_FALSE(fs->exists(tmpDirectoryPath));
}

DEBUG_ONLY_TEST_F(TaskTest, resumeAfterTaskFinish) {
  auto probeVector = makeRowVector(
      {"t_c0"}, {makeFlatVector<int32_t>(10, [](auto row) { return row; })});
  auto buildVector = makeRowVector(
      {"u_c0"}, {makeFlatVector<int32_t>(10, [](auto row) { return row; })});
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .values({probeVector})
          .hashJoin(
              {"t_c0"},
              {"u_c0"},
              PlanBuilder(planNodeIdGenerator).values({buildVector}).planNode(),
              "",
              {"t_c0", "u_c0"})
          .planFragment();

  std::atomic<bool> valuesWaitFlag{true};
  folly::EventCount valuesWait;
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Values::getOutput",
      std::function<void(const velox::exec::Values*)>(
          ([&](const velox::exec::Values* values) {
            valuesWait.await([&]() { return !valuesWaitFlag.load(); });
          })));

  auto task = Task::create(
      "task",
      std::move(plan),
      0,
      std::make_shared<core::QueryCtx>(driverExecutor_.get()));
  task->start(4, 1);

  // Request pause and then unblock operators to proceed.
  auto pauseWait = task->requestPause();
  valuesWaitFlag = false;
  valuesWait.notifyAll();
  // Wait for task pause to complete.
  pauseWait.wait();
  // Finish the task and for a hash join, the probe operator should still be in
  // waiting for build stage.
  task->testingFinish();
  // Resume the task and expect all drivers to close.
  Task::resume(task);
  ASSERT_TRUE(waitForTaskCompletion(task.get()));
  task.reset();
  waitForAllTasksToBeDeleted();
}

DEBUG_ONLY_TEST_F(TaskTest, taskReclaimStats) {
  const auto data = makeRowVector({
      makeFlatVector<int64_t>(50, folly::identity),
      makeFlatVector<int64_t>(50, folly::identity),
  });
  const auto plan =
      PlanBuilder()
          .values({data})
          .partitionedOutput({}, 1, std::vector<std::string>{"c0"})
          .planFragment();

  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Task::requestPauseLocked",
      std::function<void(Task*)>(([&](Task* /*unused*/) {
        // Inject some delay for task memory reclaim stats verification.
        std::this_thread::sleep_for(std::chrono::milliseconds(1)); // NOLINT
      })));

  auto queryPool = memory::memoryManager()->addRootPool(
      "taskReclaimStats", 1UL << 30, exec::MemoryReclaimer::create());
  auto queryCtx = std::make_shared<core::QueryCtx>(
      driverExecutor_.get(),
      core::QueryConfig{{}},
      std::unordered_map<std::string, std::shared_ptr<Config>>{},
      nullptr,
      std::move(queryPool),
      nullptr);
  auto task = Task::create("task", std::move(plan), 0, std::move(queryCtx));
  task->start(4, 1);

  const int numReclaims{10};
  for (int i = 0; i < numReclaims; ++i) {
    MemoryReclaimer::Stats stats;
    task->pool()->reclaim(1000, 1UL << 30, stats);
  }
  const auto taskStats = task->taskStats();
  ASSERT_EQ(taskStats.memoryReclaimCount, numReclaims);
  ASSERT_GT(taskStats.memoryReclaimMs, 0);

  // Fail the task to finish test.
  task->requestAbort();
  ASSERT_TRUE(waitForTaskAborted(task.get()));
  task.reset();
  waitForAllTasksToBeDeleted();
}

DEBUG_ONLY_TEST_F(TaskTest, driverEnqueAfterFailedAndPausedTask) {
  const auto data = makeRowVector({
      makeFlatVector<int64_t>(50, [](auto row) { return row; }),
      makeFlatVector<int64_t>(50, [](auto row) { return row; }),
  });
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  const auto plan = PlanBuilder(planNodeIdGenerator)
                        .values({data})
                        .singleAggregation({"c0"}, {"sum(c1)"}, {})
                        .planFragment();

  std::atomic<bool> driverWaitFlag{true};
  folly::EventCount driverWait;
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Task::enter",
      std::function<void(const velox::exec::ThreadState*)>(
          ([&](const velox::exec::ThreadState* /*unused*/) {
            driverWait.await([&]() { return !driverWaitFlag.load(); });
          })));

  auto task = Task::create(
      "task",
      std::move(plan),
      0,
      std::make_shared<core::QueryCtx>(driverExecutor_.get()));
  task->start(4, 1);

  // Request pause.
  auto pauseWait = task->requestPause();
  // Fail the task.
  task->requestAbort();

  // Unblock drivers.
  driverWaitFlag = false;
  driverWait.notifyAll();
  // Let driver threads run before resume.
  std::this_thread::sleep_for(std::chrono::seconds(1));
  // Wait for task pause to complete.
  pauseWait.wait();

  // Resume the task and expect all drivers to close.
  Task::resume(task);
  ASSERT_TRUE(waitForTaskAborted(task.get()));
  task.reset();
  waitForAllTasksToBeDeleted();
}
} // namespace facebook::velox::exec::test
