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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/future/VeloxPromise.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/exec/PartitionedOutputBufferManager.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/Values.h"
#include "velox/exec/tests/utils/Cursor.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"

using namespace facebook::velox;
using namespace facebook::velox::common::testutil;

namespace facebook::velox::exec::test {

class TaskTest : public HiveConnectorTestBase {
 protected:
  static std::pair<std::shared_ptr<exec::Task>, std::vector<RowVectorPtr>>
  executeSingleThreaded(
      core::PlanFragment plan,
      const std::unordered_map<std::string, std::vector<std::string>>&
          filePaths = {}) {
    auto task = std::make_shared<exec::Task>(
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

  exec::Task task(
      "task-1",
      std::move(plan),
      0,
      std::make_shared<core::QueryCtx>(driverExecutor_.get()));

  // Add split for the source node.
  task.addSplit("0", exec::Split(folly::copy(connectorSplit)));

  // Add an empty split.
  task.addSplit("0", exec::Split());

  // Try to add split for a non-source node.
  auto errorMessage =
      "Splits can be associated only with leaf plan nodes which require splits. Plan node ID 1 doesn't refer to such plan node.";
  VELOX_ASSERT_THROW(
      task.addSplit("1", exec::Split(folly::copy(connectorSplit))),
      errorMessage)

  VELOX_ASSERT_THROW(
      task.addSplitWithSequence(
          "1", exec::Split(folly::copy(connectorSplit)), 3),
      errorMessage)

  VELOX_ASSERT_THROW(task.setMaxSplitSequenceId("1", 9), errorMessage)

  VELOX_ASSERT_THROW(task.noMoreSplits("1"), errorMessage)

  VELOX_ASSERT_THROW(task.noMoreSplitsForGroup("1", 5), errorMessage)

  // Try to add split for non-existent node.
  errorMessage =
      "Splits can be associated only with leaf plan nodes which require splits. Plan node ID 12 doesn't refer to such plan node.";
  VELOX_ASSERT_THROW(
      task.addSplit("12", exec::Split(folly::copy(connectorSplit))),
      errorMessage)

  VELOX_ASSERT_THROW(
      task.addSplitWithSequence(
          "12", exec::Split(folly::copy(connectorSplit)), 3),
      errorMessage)

  VELOX_ASSERT_THROW(task.setMaxSplitSequenceId("12", 9), errorMessage)

  VELOX_ASSERT_THROW(task.noMoreSplits("12"), errorMessage)

  VELOX_ASSERT_THROW(task.noMoreSplitsForGroup("12", 5), errorMessage)

  // Try to add split for a Values source node.
  plan =
      PlanBuilder()
          .values({makeRowVector(ROW({"a", "b"}, {INTEGER(), DOUBLE()}), 10)})
          .project({"a * a", "b + b"})
          .planFragment();

  exec::Task valuesTask(
      "task-2",
      std::move(plan),
      0,
      std::make_shared<core::QueryCtx>(driverExecutor_.get()));
  errorMessage =
      "Splits can be associated only with leaf plan nodes which require splits. Plan node ID 0 doesn't refer to such plan node.";
  VELOX_ASSERT_THROW(
      valuesTask.addSplit("0", exec::Split(folly::copy(connectorSplit))),
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
      exec::Task(
          "task-1",
          std::move(plan),
          0,
          std::make_shared<core::QueryCtx>(driverExecutor_.get())),
      "Plan node IDs must be unique. Found duplicate ID: 0.")
}

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

  auto cursor = std::make_unique<TaskCursor>(params);

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
  auto nonBlockingTask = std::make_shared<exec::Task>(
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
  auto blockingTask = std::make_shared<exec::Task>(
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
  auto task = std::make_shared<exec::Task>(
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
  auto bufferManager = PartitionedOutputBufferManager::getInstance().lock();
  {
    auto task = std::make_shared<exec::Task>(
        "t0", plan, 0, std::make_shared<core::QueryCtx>(driverExecutor_.get()));

    task->start(task, 1, 1);

    ASSERT_TRUE(task->updateBroadcastOutputBuffers(10, true /*noMoreBuffers*/));

    // Calls after no-more-buffers are ignored.
    ASSERT_FALSE(task->updateBroadcastOutputBuffers(11, false));

    task->requestCancel();
  }

  {
    auto task = std::make_shared<exec::Task>(
        "t1", plan, 0, std::make_shared<core::QueryCtx>(driverExecutor_.get()));

    task->start(task, 1, 1);

    ASSERT_TRUE(task->updateBroadcastOutputBuffers(5, false));
    ASSERT_TRUE(task->updateBroadcastOutputBuffers(10, false));

    task->requestAbort();

    // Calls after task has been removed from the buffer manager (via abort) are
    // ignored.
    ASSERT_FALSE(task->updateBroadcastOutputBuffers(15, true));
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

  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Values::getOutput",
      std::function<void(const velox::exec::Values*)>(
          ([&](const velox::exec::Values* values) {
            // Only blocks the value node on the second output.
            if (values->testingCurrent() != 1) {
              return;
            }
            std::move(valueFuture).wait();
            driverPromise.setValue();
          })));

  CursorParameters params;
  params.planNode = plan;
  params.queryCtx = std::make_shared<core::QueryCtx>(driverExecutor_.get());
  params.queryCtx->setConfigOverridesUnsafe(
      {{core::QueryConfig::kPreferredOutputBatchRows, "1"}});

  {
    auto cursor = std::make_unique<TaskCursor>(params);
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
  params.queryCtx->setConfigOverridesUnsafe(
      {{core::QueryConfig::kPreferredOutputBatchRows, "1"}});

  auto cursor = std::make_unique<TaskCursor>(params);
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

    EXPECT_EQ(1, liveStats[i].numTotalDrivers);
    EXPECT_EQ(0, liveStats[i].numCompletedDrivers);
    EXPECT_EQ(0, liveStats[i].numTerminatedDrivers);
    EXPECT_EQ(1, liveStats[i].numRunningDrivers);
    EXPECT_EQ(0, liveStats[i].numBlockedDrivers.size());
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
}

} // namespace facebook::velox::exec::test
