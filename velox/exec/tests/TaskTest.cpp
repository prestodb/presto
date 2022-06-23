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
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/exec/tests/utils/Cursor.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"

using namespace facebook::velox;

namespace facebook::velox::exec::test {
class TaskTest : public HiveConnectorTestBase {
 protected:
  static std::pair<std::shared_ptr<exec::Task>, std::vector<RowVectorPtr>>
  executeSingleThreaded(
      core::PlanFragment plan,
      const std::vector<std::string>& filePaths /**/ = {}) {
    auto task = std::make_shared<exec::Task>(
        "single.execution.task.0", plan, 0, std::make_shared<core::QueryCtx>());

    if (!filePaths.empty()) {
      for (const auto& filePath : filePaths) {
        task->addSplit("0", exec::Split(makeHiveConnectorSplit(filePath)));
      }
      task->noMoreSplits("0");
    }

    std::vector<RowVectorPtr> results;
    for (;;) {
      auto result = task->next();
      if (!result) {
        break;
      }

      result->loadedVector();
      results.push_back(result);
    }

    VELOX_CHECK(waitForTaskCompletion(task.get()));

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
      "task-1", std::move(plan), 0, core::QueryCtx::createForTest());

  // Add split for the source node.
  task.addSplit("0", exec::Split(folly::copy(connectorSplit)));

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
      "task-2", std::move(plan), 0, core::QueryCtx::createForTest());
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
      exec::Task("task-1", std::move(plan), 0, core::QueryCtx::createForTest()),
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

  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
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

  {
    auto [task, results] = executeSingleThreaded(plan);
    assertEqualResults({expectedResult, expectedResult}, results);
  }

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

  {
    auto [task, results] = executeSingleThreaded(plan);
    assertEqualResults({expectedResult}, results);
  }

  // Project + Aggregation over TableScan.
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, {data, data});

  plan = PlanBuilder()
             .tableScan(asRowType(data->type()))
             .project({"c0 % 5 as k", "c0"})
             .singleAggregation({"k"}, {"sum(c0)", "avg(c0)"})
             .planFragment();

  {
    auto [task, results] = executeSingleThreaded(plan, {filePath->path});
    assertEqualResults({expectedResult}, results);
  }

  // Query failure.
  plan = PlanBuilder().values({data, data}).project({"c0 / 0"}).planFragment();
  VELOX_ASSERT_THROW(executeSingleThreaded(plan), "division by zero");
}
} // namespace facebook::velox::exec::test
