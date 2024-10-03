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
#include "velox/common/memory/MemoryArbitrator.h"
#include "velox/common/memory/SharedArbitrator.h"
#include "velox/common/testutil/TestValue.h"
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
#include "velox/vector/fuzzer/VectorFuzzer.h"

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
// When the node is blocked by external event (via externalBlocker), the
// operator will signal kBlocked. The pipeline can ONLY proceed again when it is
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
  executeSerial(
      core::PlanFragment plan,
      const std::unordered_map<std::string, std::vector<std::string>>&
          filePaths = {}) {
    static std::atomic_uint64_t taskId{0};
    auto task = Task::create(
        fmt::format("single.execution.task.{}", taskId++),
        plan,
        0,
        core::QueryCtx::create(),
        Task::ExecutionMode::kSerial);

    for (const auto& [nodeId, paths] : filePaths) {
      for (const auto& path : paths) {
        task->addSplit(nodeId, exec::Split(makeHiveConnectorSplit(path)));
      }
      task->noMoreSplits(nodeId);
    }

    VELOX_CHECK(task->supportSerialExecutionMode());

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

TEST_F(TaskTest, toJson) {
  auto plan = PlanBuilder()
                  .tableScan(ROW({"a", "b"}, {INTEGER(), DOUBLE()}))
                  .project({"a * a", "b + b"})
                  .planFragment();

  auto task = Task::create(
      "task-1",
      std::move(plan),
      0,
      core::QueryCtx::create(driverExecutor_.get()),
      Task::ExecutionMode::kParallel);

  ASSERT_EQ(
      task->toString(),
      "{Task task-1 (task-1)\n"
      "Plan:\n"
      "-- Project[1][expressions: (p0:INTEGER, multiply(ROW[\"a\"],ROW[\"a\"])), (p1:DOUBLE, plus(ROW[\"b\"],ROW[\"b\"]))] -> p0:INTEGER, p1:DOUBLE\n"
      "  -- TableScan[0][table: hive_table] -> a:INTEGER, b:DOUBLE\n"
      "\n}");
  ASSERT_EQ(
      folly::toPrettyJson(task->toJson()),
      "{\n  \"concurrentSplitGroups\": 1,\n  \"drivers\": {},\n  \"exchangeClientByPlanNode\": {},\n  \"groupedPartitionedOutput\": false,\n  \"id\": \"task-1\",\n  \"noMoreOutputBuffers\": false,\n  \"numDriversPerSplitGroup\": 0,\n  \"numDriversUngrouped\": 0,\n  \"numFinishedDrivers\": 0,\n  \"numRunningDrivers\": 0,\n  \"numRunningSplitGroups\": 0,\n  \"numThreads\": 0,\n  \"numTotalDrivers\": 0,\n  \"onThreadSince\": \"0\",\n  \"partitionedOutputConsumed\": false,\n  \"pauseRequested\": false,\n  \"plan\": \"-- Project[1][expressions: (p0:INTEGER, multiply(ROW[\\\"a\\\"],ROW[\\\"a\\\"])), (p1:DOUBLE, plus(ROW[\\\"b\\\"],ROW[\\\"b\\\"]))] -> p0:INTEGER, p1:DOUBLE\\n  -- TableScan[0][table: hive_table] -> a:INTEGER, b:DOUBLE\\n\",\n  \"shortId\": \"task-1\",\n  \"state\": \"Running\",\n  \"terminateRequested\": false\n}");
  ASSERT_EQ(
      folly::toPrettyJson(task->toShortJson()),
      "{\n  \"id\": \"task-1\",\n  \"numFinishedDrivers\": 0,\n  \"numRunningDrivers\": 0,\n  \"numThreads\": 0,\n  \"numTotalDrivers\": 0,\n  \"pauseRequested\": false,\n  \"shortId\": \"task-1\",\n  \"state\": \"Running\",\n  \"terminateRequested\": false\n}");

  task->start(2);

  ASSERT_NO_THROW(task->toJson());
  ASSERT_NO_THROW(task->toShortJson());

  task->noMoreSplits("0");
  waitForTaskCompletion(task.get());

  ASSERT_NO_THROW(task->toJson());
  ASSERT_NO_THROW(task->toShortJson());
}

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
      core::QueryCtx::create(driverExecutor_.get()),
      Task::ExecutionMode::kParallel);

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
      core::QueryCtx::create(driverExecutor_.get()),
      Task::ExecutionMode::kParallel);
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
          core::QueryCtx::create(driverExecutor_.get()),
          Task::ExecutionMode::kParallel),
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

  // There should be some Drivers closed by the Task due to termination.
  // They also should all be empty, otherwise we have zombie Drivers.
  const auto& driversClosedByTask =
      cursor->task()->testingDriversClosedByTask();
  EXPECT_GT(driversClosedByTask.size(), 0);
  for (const auto& driverWeak : driversClosedByTask) {
    EXPECT_EQ(driverWeak.lock(), nullptr);
  }
  EXPECT_EQ(
      cursor->task()->toString().find("zombie drivers:"), std::string::npos);
}

TEST_F(TaskTest, serialExecution) {
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
    auto [task, results] = executeSerial(plan);
    assertEqualResults(
        std::vector<RowVectorPtr>{expectedResult, expectedResult}, results);
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
    auto [task, results] = executeSerial(plan);
    assertEqualResults({expectedResult}, results);
  }

  // Project + Aggregation over TableScan.
  auto filePath = TempFilePath::create();
  writeToFile(filePath->getPath(), {data, data});

  core::PlanNodeId scanId;
  plan = PlanBuilder()
             .tableScan(asRowType(data->type()))
             .capturePlanNodeId(scanId)
             .project({"c0 % 5 as k", "c0"})
             .singleAggregation({"k"}, {"sum(c0)", "avg(c0)"})
             .planFragment();

  {
    auto [task, results] =
        executeSerial(plan, {{scanId, {filePath->getPath()}}});
    assertEqualResults({expectedResult}, results);
  }

  // Query failure.
  plan = PlanBuilder().values({data, data}).project({"c0 / 0"}).planFragment();
  VELOX_ASSERT_THROW(executeSerial(plan), "division by zero");
}

// The purpose of the test is to check the running task list APIs.
TEST_F(TaskTest, runningTaskList) {
  const auto data = makeRowVector({
      makeFlatVector<int64_t>(1'000, [](auto row) { return row; }),
  });

  ASSERT_EQ(Task::numRunningTasks(), 0);
  ASSERT_TRUE(Task::getRunningTasks().empty());

  const auto plan = PlanBuilder()
                        .values({data, data})
                        .filter("c0 < 100")
                        .project({"c0 + 5"})
                        .planFragment();

  // This is to verify that runningTaskList API returns a completed task which
  // still has pending references.
  std::vector<std::shared_ptr<Task>> expectedRunningTasks;
  expectedRunningTasks.push_back(executeSerial(plan).first);
  ASSERT_EQ(Task::numRunningTasks(), 1);
  ASSERT_EQ(Task::getRunningTasks().size(), 1);

  expectedRunningTasks.push_back(executeSerial(plan).first);
  ASSERT_EQ(Task::numRunningTasks(), 2);
  ASSERT_EQ(Task::getRunningTasks().size(), 2);

  expectedRunningTasks.push_back(executeSerial(plan).first);
  ASSERT_EQ(Task::numRunningTasks(), 3);
  ASSERT_EQ(Task::getRunningTasks().size(), 3);

  std::set<std::string> expectedTaskIdSet;
  for (const auto& task : expectedRunningTasks) {
    expectedTaskIdSet.insert(task->taskId());
  }
  ASSERT_EQ(expectedTaskIdSet.size(), 3);
  std::vector<std::shared_ptr<Task>> runningTasks = Task::getRunningTasks();
  ASSERT_EQ(runningTasks.size(), 3);
  for (const auto& task : runningTasks) {
    ASSERT_EQ(expectedTaskIdSet.count(task->taskId()), 1);
  }

  expectedTaskIdSet.erase(expectedRunningTasks.back()->taskId());
  expectedRunningTasks.pop_back();
  ASSERT_EQ(expectedTaskIdSet.size(), 2);

  runningTasks.clear();
  runningTasks = Task::getRunningTasks();
  ASSERT_EQ(runningTasks.size(), 2);
  for (const auto& task : runningTasks) {
    ASSERT_EQ(expectedTaskIdSet.count(task->taskId()), 1);
  }

  runningTasks.clear();
  expectedRunningTasks.clear();

  ASSERT_EQ(Task::numRunningTasks(), 0);
  ASSERT_TRUE(Task::getRunningTasks().empty());
}

TEST_F(TaskTest, serialHashJoin) {
  auto left = makeRowVector(
      {"t_c0", "t_c1"},
      {
          makeFlatVector<int64_t>({1, 2, 3, 4}),
          makeFlatVector<int64_t>({10, 20, 30, 40}),
      });
  auto leftPath = TempFilePath::create();
  writeToFile(leftPath->getPath(), {left});

  auto right = makeRowVector(
      {"u_c0"},
      {
          makeFlatVector<int64_t>({0, 1, 3, 5}),
      });
  auto rightPath = TempFilePath::create();
  writeToFile(rightPath->getPath(), {right});

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
    auto [task, results] = executeSerial(
        plan,
        {{leftScanId, {leftPath->getPath()}},
         {rightScanId, {rightPath->getPath()}}});
    assertEqualResults({expectedResult}, results);
  }
}

TEST_F(TaskTest, serialCrossJoin) {
  auto left = makeRowVector({"t_c0"}, {makeFlatVector<int64_t>({1, 2, 3})});
  auto leftPath = TempFilePath::create();
  writeToFile(leftPath->getPath(), {left});

  auto right = makeRowVector({"u_c0"}, {makeFlatVector<int64_t>({10, 20})});
  auto rightPath = TempFilePath::create();
  writeToFile(rightPath->getPath(), {right});

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
    auto [task, results] = executeSerial(
        plan,
        {{leftScanId, {leftPath->getPath()}},
         {rightScanId, {rightPath->getPath()}}});
    assertEqualResults({expectedResult}, results);
  }
}

TEST_F(TaskTest, serialExecutionExternalBlockable) {
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
      "single.execution.task.0",
      plan,
      0,
      core::QueryCtx::create(),
      Task::ExecutionMode::kSerial);
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
      "single.execution.task.1",
      plan,
      0,
      core::QueryCtx::create(),
      Task::ExecutionMode::kSerial);
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

TEST_F(TaskTest, supportSerialExecutionMode) {
  auto plan = PlanBuilder()
                  .tableScan(ROW({"c0"}, {BIGINT()}))
                  .project({"c0 % 10"})
                  .partitionedOutput({}, 1, std::vector<std::string>{"p0"})
                  .planFragment();
  auto task = Task::create(
      "single.execution.task.0",
      plan,
      0,
      core::QueryCtx::create(),
      Task::ExecutionMode::kSerial);

  // PartitionedOutput does not support serial execution mode, therefore the
  // task doesn't support it either.
  ASSERT_FALSE(task->supportSerialExecutionMode());
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
        "t0",
        plan,
        0,
        core::QueryCtx::create(driverExecutor_.get()),
        Task::ExecutionMode::kParallel);

    task->start(1, 1);

    ASSERT_TRUE(task->updateOutputBuffers(10, true /*noMoreBuffers*/));

    // Calls after no-more-buffers are ignored.
    ASSERT_FALSE(task->updateOutputBuffers(11, false));

    task->requestCancel();
  }

  {
    auto task = Task::create(
        "t1",
        plan,
        0,
        core::QueryCtx::create(driverExecutor_.get()),
        Task::ExecutionMode::kParallel);

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
  params.queryCtx = core::QueryCtx::create(driverExecutor_.get());
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
    ASSERT_FALSE(task->getCancellationToken().isCancellationRequested());
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
  params.queryCtx = core::QueryCtx::create(executor_.get());
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
    EXPECT_EQ(2 * (i + 1), operatorStats.isBlockedTiming.count);
    EXPECT_EQ(0, operatorStats.finishTiming.count);
    EXPECT_EQ(0, operatorStats.backgroundTiming.count);

    EXPECT_EQ(1, liveStats[i].numTotalDrivers);
    EXPECT_EQ(0, liveStats[i].numCompletedDrivers);
    EXPECT_EQ(0, liveStats[i].numTerminatedDrivers);
    EXPECT_EQ(1, liveStats[i].numRunningDrivers);
    EXPECT_EQ(0, liveStats[i].numQueuedDrivers);
    EXPECT_EQ(0, liveStats[i].numBlockedDrivers.size());

    EXPECT_EQ(0, liveStats[i].executionEndTimeMs);
    EXPECT_EQ(0, liveStats[i].terminationTimeMs);
  }

  EXPECT_EQ(1, finishStats.numTotalDrivers);
  EXPECT_EQ(1, finishStats.numCompletedDrivers);
  EXPECT_EQ(0, finishStats.numQueuedDrivers);
  EXPECT_EQ(0, finishStats.numTerminatedDrivers);
  EXPECT_EQ(0, finishStats.numRunningDrivers);
  EXPECT_EQ(0, finishStats.numBlockedDrivers.size());

  const auto& operatorStats = finishStats.pipelineStats[0].operatorStats[0];
  EXPECT_EQ(numBatches + 1, operatorStats.getOutputTiming.count);
  EXPECT_EQ(32 * numBatches, operatorStats.outputBytes);
  EXPECT_EQ(3 * numBatches, operatorStats.outputPositions);
  EXPECT_EQ(numBatches, operatorStats.outputVectors);
  // isBlocked() should be called at least twice for each batch
  EXPECT_LE(2 * numBatches, operatorStats.isBlockedTiming.count);
  EXPECT_EQ(2, operatorStats.finishTiming.count);
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
  params.queryCtx = core::QueryCtx::create(executor_.get());

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

DEBUG_ONLY_TEST_F(TaskTest, inconsistentExecutionMode) {
  {
    // Scenario 1: Parallel execution starts first then kicks in Serial
    // execution.

    // Let parallel execution pause a bit so that we can call serial API on Task
    // to trigger inconsistent execution mode failure.
    folly::EventCount getOutputWait;
    std::atomic_bool getOutputWaitFlag{false};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Values::getOutput",
        std::function<void(Values*)>([&](Values* /*unused*/) {
          getOutputWait.await([&]() { return getOutputWaitFlag.load(); });
        }));
    auto data = makeRowVector({
        makeFlatVector<int64_t>(1'000, [](auto row) { return row; }),
    });

    CursorParameters params;
    params.planNode =
        PlanBuilder().values({data, data, data}).project({"c0"}).planNode();
    params.queryCtx = core::QueryCtx::create(driverExecutor_.get());
    params.maxDrivers = 4;

    auto cursor = TaskCursor::create(params);
    auto* task = cursor->task().get();

    cursor->start();
    VELOX_ASSERT_THROW(task->next(), "Inconsistent task execution mode.");
    getOutputWaitFlag = true;
    getOutputWait.notify();
    while (cursor->hasNext()) {
      cursor->moveNext();
    }
    waitForTaskCompletion(task);
  }

  {
    // Scenario 2: Serial execution starts first then kicks in Parallel
    // execution mode.

    auto data = makeRowVector({
        makeFlatVector<int64_t>(1'000, [](auto row) { return row; }),
    });
    auto plan =
        PlanBuilder().values({data, data, data}).project({"c0"}).planFragment();
    auto queryCtx = core::QueryCtx::create(driverExecutor_.get());
    auto task =
        Task::create("task.0", plan, 0, queryCtx, Task::ExecutionMode::kSerial);

    task->next();
    VELOX_ASSERT_THROW(task->start(4, 1), "Inconsistent task execution mode.");
    while (task->next() != nullptr) {
    }
  }
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
    params.queryCtx = core::QueryCtx::create(driverExecutor_.get());
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

class TaskPauseTest : public TaskTest {
 public:
  void testPause() {
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
    params.queryCtx = core::QueryCtx::create(driverExecutor_.get());
    params.maxDrivers = 1;

    cursor_ = TaskCursor::create(params);
    task_ = cursor_->task().get();
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

    taskThread_ = std::thread([&]() {
      try {
        while (cursor_->moveNext()) {
        };
      } catch (VeloxRuntimeError&) {
      }
    });

    taskPauseStartWait.await([&]() { return taskPauseStarted.load(); });

    ASSERT_EQ(task_->numTotalDrivers(), 1);
    ASSERT_EQ(task_->numFinishedDrivers(), 0);
    ASSERT_EQ(task_->numRunningDrivers(), 1);

    auto pauseFuture = task_->requestPause();
    taskPaused = true;
    taskPauseWait.notifyAll();
    pauseFuture.wait();

    ASSERT_EQ(task_->numTotalDrivers(), 1);
    ASSERT_EQ(task_->numFinishedDrivers(), 0);
    ASSERT_EQ(task_->numRunningDrivers(), 1);
    ASSERT_TRUE(task_->pauseRequested());
  }

  void TearDown() override {
    cursor_.reset();
    HiveConnectorTestBase::TearDown();
  }

 protected:
  Task* task_{nullptr};
  std::unique_ptr<TaskCursor> cursor_{};
  std::thread taskThread_{};
};

DEBUG_ONLY_TEST_F(TaskPauseTest, raceBetweenTaskPauseAndTerminate) {
  testPause();
  task_->requestAbort().wait();

  ASSERT_EQ(task_->numTotalDrivers(), 1);
  ASSERT_EQ(task_->numFinishedDrivers(), 0);
  ASSERT_EQ(task_->numRunningDrivers(), 0);

  Task::resume(task_->shared_from_this());

  ASSERT_EQ(task_->numTotalDrivers(), 1);
  ASSERT_EQ(task_->numFinishedDrivers(), 1);
  ASSERT_EQ(task_->numRunningDrivers(), 0);

  taskThread_.join();
}

DEBUG_ONLY_TEST_F(TaskPauseTest, resumeFuture) {
  // Test for trivial wait on Task::pauseRequested future.
  testPause();
  folly::EventCount taskResumeAllowedWait;
  std::atomic<bool> taskResumeAllowed{false};
  std::thread observeThread([&]() {
    ContinueFuture future = ContinueFuture::makeEmpty();
    const bool paused = task_->pauseRequested(&future);
    ASSERT_TRUE(paused);
    taskResumeAllowed = true;
    taskResumeAllowedWait.notifyAll();
    future.wait();
    ASSERT_FALSE(task_->pauseRequested());
  });

  taskResumeAllowedWait.await([&]() { return taskResumeAllowed.load(); });
  std::this_thread::sleep_for(std::chrono::milliseconds(100)); // NOLINT
  Task::resume(task_->shared_from_this());

  taskThread_.join();
  observeThread.join();

  ASSERT_EQ(task_->numTotalDrivers(), 1);
  ASSERT_EQ(task_->numFinishedDrivers(), 1);
  ASSERT_EQ(task_->numRunningDrivers(), 0);
}

DEBUG_ONLY_TEST_F(TaskPauseTest, resumeFutureAfterTaskTerminated) {
  testPause();
  ContinueFuture future = ContinueFuture::makeEmpty();
  const bool paused = task_->pauseRequested(&future);
  ASSERT_TRUE(paused);
  ASSERT_EQ(task_->numTotalDrivers(), 1);
  ASSERT_EQ(task_->numFinishedDrivers(), 0);
  ASSERT_EQ(task_->numRunningDrivers(), 1);
  task_->requestCancel().wait();
  ASSERT_EQ(task_->numTotalDrivers(), 1);
  ASSERT_EQ(task_->numFinishedDrivers(), 0);
  ASSERT_EQ(task_->numRunningDrivers(), 0);
  std::this_thread::sleep_for(std::chrono::milliseconds(100)); // NOLINT
  ASSERT_FALSE(future.isReady());
  Task::resume(task_->shared_from_this());
  ASSERT_TRUE(future.isReady());
  future.wait();
  ASSERT_FALSE(task_->pauseRequested());
  taskThread_.join();
}

DEBUG_ONLY_TEST_F(TaskTest, driverCounters) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>(1'000, [](auto row) { return row; }),
  });
  auto filePath = TempFilePath::create();
  writeToFile(filePath->getPath(), {data, data});

  core::PlanNodeId scanNodeId;
  auto plan = PlanBuilder()
                  .tableScan(asRowType(data->type()))
                  .capturePlanNodeId(scanNodeId)
                  .project({"c0 % 5 as k", "c0"})
                  .planFragment();

  CursorParameters params;
  params.planNode = plan.planNode;
  params.queryCtx = core::QueryCtx::create(driverExecutor_.get());
  // The queue size in the executor is 3, so we will have 1 driver queued.
  params.maxDrivers = 4;

  auto cursor = TaskCursor::create(params);
  auto task = cursor->task();

  // Mutex used to pause drivers' execution, so they appear 'on thread'.
  std::shared_mutex pauseDriverRunInternal;
  pauseDriverRunInternal.lock();
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal",
      std::function<void(Driver*)>([&](Driver*) {
        pauseDriverRunInternal.lock_shared();
        pauseDriverRunInternal.unlock_shared();
      }));

  // Run the task in this thread using cursor.
  std::thread taskThread([&]() {
    try {
      while (cursor->moveNext()) {
      };
    } catch (VeloxRuntimeError&) {
    }
  });

  // Convenience functtor allowing us to ensure the drivers are in the certain
  // state.
  Task::DriverCounts driverCounts;
  auto waitForConditionFunc = [&](auto conditionFunc) {
    // In case things go wrong, we want to bail out sooner and don't wait until
    // the test times out, hence we limit waiting time for the condition.
    size_t numTries{0};
    while (numTries++ < 20) {
      driverCounts = task->driverCounts();
      if (conditionFunc()) {
        break;
      }
      /* sleep override */
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  };

  // Wait till 3 drivers are on thread. They will be paused at the start with
  // the help of the testvalue.
  waitForConditionFunc(
      [&]() { return (driverCounts.numOnThreadDrivers == 3); });
  // Expect 3 drivers on the thread.
  ASSERT_EQ(driverCounts.numOnThreadDrivers, 3);
  ASSERT_EQ(driverCounts.numQueuedDrivers, 1);
  ASSERT_EQ(driverCounts.numSuspendedDrivers, 0);
  ASSERT_EQ(driverCounts.numBlockedDrivers.size(), 0);

  // Allow drivers to proceed, all will be blocked as there are no splits
  // supplied yet.
  pauseDriverRunInternal.unlock();
  waitForConditionFunc([&]() {
    if (driverCounts.numBlockedDrivers.size() > 0) {
      auto it = driverCounts.numBlockedDrivers.begin();
      return (it->first == BlockingReason::kWaitForSplit && it->second == 4);
    }
    return false;
  });
  // Expect all drivers are blocked on waiting for the splits.
  ASSERT_EQ(driverCounts.numOnThreadDrivers, 0);
  ASSERT_EQ(driverCounts.numQueuedDrivers, 0);
  ASSERT_EQ(driverCounts.numSuspendedDrivers, 0);
  ASSERT_EQ(driverCounts.numBlockedDrivers.size(), 1);
  auto it = driverCounts.numBlockedDrivers.begin();
  ASSERT_EQ(it->first, BlockingReason::kWaitForSplit);
  ASSERT_EQ(it->second, 4);

  // Now add a split, finalize splits and wait for the task to finish.
  auto split = exec::Split(makeHiveConnectorSplit(
      filePath->getPath(), 0, std::numeric_limits<uint64_t>::max(), 1));
  task->addSplit(scanNodeId, std::move(split));
  task->noMoreSplits(scanNodeId);
  taskThread.join();
  waitForConditionFunc(
      [&]() { return (driverCounts.numOnThreadDrivers == 0); });
  // Expect no drivers in any of the counters.
  ASSERT_EQ(driverCounts.numOnThreadDrivers, 0);
  ASSERT_EQ(driverCounts.numQueuedDrivers, 0);
  ASSERT_EQ(driverCounts.numSuspendedDrivers, 0);
  ASSERT_EQ(driverCounts.numBlockedDrivers.size(), 0);
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
        core::QueryCtx::create(
            singleThreadExecution ? nullptr : driverExecutor_.get()),
        singleThreadExecution ? Task::ExecutionMode::kSerial
                              : Task::ExecutionMode::kParallel);
    if (singleThreadExecution) {
      VELOX_ASSERT_THROW(badTask->next(), "Unexpected memory pool allocations");
    } else {
      VELOX_ASSERT_THROW(
          badTask->start(1), "Unexpected memory pool allocations");
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
  params.queryCtx = core::QueryCtx::create(driverExecutor_.get());
  params.queryCtx->testingOverrideConfigUnsafe(
      {{core::QueryConfig::kSpillEnabled, "true"},
       {core::QueryConfig::kAggregationSpillEnabled, "true"}});
  params.maxDrivers = 1;

  auto cursor = TaskCursor::create(params);
  std::shared_ptr<Task> task = cursor->task();
  auto rootTempDir = exec::test::TempDirectoryPath::create();
  auto tmpDirectoryPath =
      rootTempDir->getPath() + "/spillDirectoryLifecycleManagement";
  task->setSpillDirectory(tmpDirectoryPath, false);

  TestScopedSpillInjection scopedSpillInjection(100);
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
  params.queryCtx = core::QueryCtx::create(driverExecutor_.get());
  params.queryCtx->testingOverrideConfigUnsafe(
      {{core::QueryConfig::kSpillEnabled, "true"},
       {core::QueryConfig::kJoinSpillEnabled, "true"}});
  params.maxDrivers = 1;

  auto cursor = TaskCursor::create(params);
  auto* task = cursor->task().get();
  auto rootTempDir = exec::test::TempDirectoryPath::create();
  auto tmpDirectoryPath = rootTempDir->getPath() + "/spillDirNotCreated";
  task->setSpillDirectory(tmpDirectoryPath, false);

  while (cursor->moveNext()) {
  }
  ASSERT_TRUE(waitForTaskCompletion(task, 5'000'000));
  ASSERT_EQ(exec::TaskState::kFinished, task->state());
  auto taskStats = exec::toPlanStats(task->taskStats());
  auto& stats = taskStats.at(hashJoinNodeId);
  ASSERT_EQ(stats.spilledRows, 0);
  // Check for spill folder without destroying the Task object to ensure its
  // destructor has not removed the directory if it was created earlier.
  auto fs = filesystems::getFileSystem(tmpDirectoryPath, nullptr);
  ASSERT_FALSE(fs->exists(tmpDirectoryPath));
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
      core::QueryCtx::create(driverExecutor_.get()),
      Task::ExecutionMode::kParallel);
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

DEBUG_ONLY_TEST_F(TaskTest, serialLongRunningOperatorInTaskReclaimerAbort) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>(1'000, [](auto row) { return row; }),
  });

  // Filter + Project.
  auto plan =
      PlanBuilder().values({data, data, data}).project({"c0"}).planFragment();

  auto queryCtx = core::QueryCtx::create(driverExecutor_.get());

  auto blockingTask = Task::create(
      "blocking.task.0", plan, 0, queryCtx, Task::ExecutionMode::kSerial);

  // Before we block, we expect `next` to get data normally.
  EXPECT_NE(nullptr, blockingTask->next());

  // Now, we want to block the pipeline by blocking Values operator. We expect
  // `next` to return null.  The `future` should be updated for the caller to
  // wait before calling next() again
  folly::EventCount getOutputWait;
  std::atomic_bool getOutputWaitFlag{false};
  folly::EventCount abortWait;
  std::atomic_bool abortWaitFlag{false};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Values::getOutput",
      std::function<void(Values*)>([&](Values* /*unused*/) {
        abortWaitFlag = true;
        abortWait.notify();
        getOutputWait.await([&]() { return getOutputWaitFlag.load(); });
      }));

  const std::string abortErrorMessage("Synthetic Exception");
  auto thread = std::thread(
      [&]() { VELOX_ASSERT_THROW(blockingTask->next(), abortErrorMessage); });

  try {
    VELOX_FAIL(abortErrorMessage);
  } catch (VeloxException&) {
    abortWait.await([&]() { return abortWaitFlag.load(); });
    blockingTask->pool()->abort(std::current_exception());
  }

  waitForTaskCompletion(blockingTask.get(), 5'000'000);

  // We expect that abort does not trigger the operator abort by checking if the
  // memory pool memory has been released or not.
  blockingTask->pool()->visitChildren([](auto* child) {
    if (child->isLeaf()) {
      EXPECT_EQ(child->stats().numReleases, 0);
    }
    return true;
  });

  getOutputWaitFlag = true;
  getOutputWait.notify();

  thread.join();

  blockingTask->taskCompletionFuture().wait();
  blockingTask->pool()->visitChildren([](auto* child) {
    if (child->isLeaf()) {
      EXPECT_EQ(child->stats().numReleases, 1);
    }
    return true;
  });
}

DEBUG_ONLY_TEST_F(TaskTest, longRunningOperatorInTaskReclaimerAbort) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>(1'000, [](auto row) { return row; }),
  });
  folly::EventCount getOutputWait;
  std::atomic_bool getOutputWaitFlag{false};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Values::getOutput",
      std::function<void(Values*)>([&](Values* /*unused*/) {
        getOutputWait.await([&]() { return getOutputWaitFlag.load(); });
      }));

  // Project only dummy plan
  auto plan =
      PlanBuilder().values({data, data, data}).project({"c0"}).planFragment();

  auto queryCtx = core::QueryCtx::create(driverExecutor_.get());

  auto blockingTask = Task::create(
      "blocking.task.0", plan, 0, queryCtx, Task::ExecutionMode::kParallel);

  blockingTask->start(4, 1);
  const std::string abortErrorMessage("Synthetic Exception");
  try {
    VELOX_FAIL(abortErrorMessage);
  } catch (VeloxException&) {
    blockingTask->pool()->abort(std::current_exception());
  }
  waitForTaskCompletion(blockingTask.get());

  // We expect that arbitration does not trigger release of the operator pools.
  blockingTask->pool()->visitChildren([](auto* child) {
    if (child->isLeaf()) {
      EXPECT_EQ(child->stats().numReleases, 0);
    }
    return true;
  });

  getOutputWaitFlag = true;
  getOutputWait.notify();

  VELOX_ASSERT_THROW(
      std::rethrow_exception(blockingTask->error()), abortErrorMessage);

  blockingTask->taskCompletionFuture().wait();
  blockingTask->pool()->visitChildren([](auto* child) {
    if (child->isLeaf()) {
      EXPECT_EQ(child->stats().numReleases, 1);
    }
    return true;
  });
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
  auto queryCtx = core::QueryCtx::create(
      driverExecutor_.get(),
      core::QueryConfig{{}},
      std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>{},
      nullptr,
      std::move(queryPool),
      nullptr);
  auto task = Task::create(
      "task",
      std::move(plan),
      0,
      std::move(queryCtx),
      Task::ExecutionMode::kParallel);
  task->start(4, 1);

  const int numReclaims{10};
  const uint64_t queryCapacity = task->pool()->parent()->capacity();
  for (int i = 0; i < numReclaims; ++i) {
    MemoryReclaimer::Stats stats;
    task->pool()->reclaim(1000, 1UL << 30, stats);
  }
  const int64_t reclaimedQueryCapacity =
      queryCapacity - task->pool()->parent()->capacity();
  ASSERT_GE(reclaimedQueryCapacity, 0);
  auto* arbitrator = dynamic_cast<memory::SharedArbitrator*>(
      memory::memoryManager()->arbitrator());
  if (arbitrator != nullptr) {
    arbitrator->testingFreeCapacity(reclaimedQueryCapacity);
  }

  auto taskStats = task->taskStats();
  ASSERT_EQ(taskStats.memoryReclaimCount, numReclaims);
  ASSERT_GT(taskStats.memoryReclaimMs, 0);

  // Fail the task to finish test.
  task->requestAbort();
  ASSERT_TRUE(waitForTaskAborted(task.get()));
  task.reset();
  waitForAllTasksToBeDeleted();
}

DEBUG_ONLY_TEST_F(TaskTest, taskPauseTime) {
  auto rowType = ROW({"c0", "c1"}, {INTEGER(), DOUBLE()});
  VectorFuzzer::Options opts;
  opts.vectorSize = 32;
  VectorFuzzer fuzzer(opts, pool_.get());
  std::vector<RowVectorPtr> valueInputs;
  for (int32_t i = 0; i < 4; ++i) {
    valueInputs.push_back(fuzzer.fuzzRow(rowType));
  }

  const auto plan =
      PlanBuilder()
          .values(valueInputs)
          .partitionedOutput({}, 1, std::vector<std::string>{"c0"})
          .planFragment();

  std::atomic_bool taskPauseWaitFlag{true};
  folly::EventCount taskPauseWait;
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Values::getOutput",
      std::function<void(const exec::Values*)>([&](const exec::Values* values) {
        if (taskPauseWaitFlag.exchange(false)) {
          taskPauseWait.notifyAll();
        }
        // Inject some delay for task pause stats verification.
        std::this_thread::sleep_for(std::chrono::milliseconds(10)); // NOLINT
      }));

  auto queryPool = memory::memoryManager()->addRootPool(
      "taskPauseTime", 1UL << 30, exec::MemoryReclaimer::create());
  auto queryCtx = core::QueryCtx::create(
      driverExecutor_.get(),
      core::QueryConfig{{}},
      std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>{},
      nullptr,
      std::move(queryPool),
      nullptr);
  auto task = Task::create(
      "task",
      std::move(plan),
      0,
      std::move(queryCtx),
      Task::ExecutionMode::kParallel);
  task->start(4, 1);

  // Wait for the task driver starts to run.
  taskPauseWait.await([&]() { return !taskPauseWaitFlag.load(); });
  // Pause the task
  task->requestPause().wait();
  // Inject some delay for task pause stats verification.
  std::this_thread::sleep_for(std::chrono::milliseconds(100)); // NOLINT
  // Resume the task.
  Task::resume(task);
  // Inject some delay for task resume to run for a while.
  std::this_thread::sleep_for(std::chrono::milliseconds(100)); // NOLINT
  // Fail the task to finish test.
  task->requestAbort();
  ASSERT_TRUE(waitForTaskAborted(task.get()));

  auto taskStats = task->taskStats();
  ASSERT_EQ(taskStats.pipelineStats.size(), 1);
  ASSERT_EQ(taskStats.pipelineStats[0].driverStats.size(), 1);
  const auto& driverStats = taskStats.pipelineStats[0].driverStats[0];
  const auto& totalPauseTime =
      driverStats.runtimeStats.at(DriverStats::kTotalPauseTime);
  ASSERT_EQ(totalPauseTime.count, 1);
  ASSERT_GE(totalPauseTime.sum, 0);
  const auto& totalOffThreadTime =
      driverStats.runtimeStats.at(DriverStats::kTotalOffThreadTime);
  ASSERT_EQ(totalOffThreadTime.count, 1);
  ASSERT_GE(totalOffThreadTime.sum, 0);

  task.reset();
  waitForAllTasksToBeDeleted();
}

TEST_F(TaskTest, updateStatsWhileCloseOffThreadDriver) {
  const auto data = makeRowVector({
      makeFlatVector<int64_t>(50, folly::identity),
      makeFlatVector<int64_t>(50, folly::identity),
  });
  const auto plan =
      PlanBuilder()
          .tableScan(asRowType(data->type()))
          .partitionedOutput({}, 1, std::vector<std::string>{"c0"})
          .planFragment();
  auto task = Task::create(
      "task",
      std::move(plan),
      0,
      core::QueryCtx::create(driverExecutor_.get()),
      Task::ExecutionMode::kParallel);
  task->start(4, 1);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  task->testingVisitDrivers(
      [](Driver* driver) { VELOX_CHECK(!driver->isOnThread()); });
  // Sleep a bit to make sure off thread time is not zero.
  std::this_thread::sleep_for(std::chrono::milliseconds{2});
  task->requestAbort();
  ASSERT_TRUE(waitForTaskAborted(task.get()));
  auto taskStats = task->taskStats();
  ASSERT_EQ(taskStats.pipelineStats.size(), 1);
  ASSERT_EQ(taskStats.pipelineStats[0].driverStats.size(), 4);
  const auto& driverStats = taskStats.pipelineStats[0].driverStats[0];
  ASSERT_EQ(driverStats.runtimeStats.count(DriverStats::kTotalPauseTime), 0);
  const auto& totalOffThreadTime =
      driverStats.runtimeStats.at(DriverStats::kTotalOffThreadTime);
  ASSERT_EQ(totalOffThreadTime.count, 1);
  ASSERT_GE(totalOffThreadTime.sum, 0);
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
      core::QueryCtx::create(driverExecutor_.get()),
      Task::ExecutionMode::kParallel);
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

DEBUG_ONLY_TEST_F(TaskTest, taskReclaimFailure) {
  const auto rowType =
      ROW({"c0", "c1", "c2"}, {INTEGER(), DOUBLE(), INTEGER()});
  const auto inputVectors = makeVectors(rowType, 128, 256);
  createDuckDbTable(inputVectors);

  const std::string spillTableError{"spillTableError"};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Spiller",
      std::function<void(Spiller*)>(
          [&](Spiller* /*unused*/) { VELOX_FAIL(spillTableError); }));

  TestScopedSpillInjection injection(100);
  const auto spillDirectory = exec::test::TempDirectoryPath::create();
  VELOX_ASSERT_THROW(
      AssertQueryBuilder(duckDbQueryRunner_)
          .spillDirectory(spillDirectory->getPath())
          .config(core::QueryConfig::kSpillEnabled, true)
          .config(core::QueryConfig::kAggregationSpillEnabled, true)
          .maxDrivers(1)
          .plan(PlanBuilder()
                    .values(inputVectors)
                    .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                    .planNode())
          .assertResults(
              "SELECT c0, c1, array_agg(c2) FROM tmp GROUP BY c0, c1"),
      spillTableError);
}

DEBUG_ONLY_TEST_F(TaskTest, taskDeletionPromise) {
  const auto rowType =
      ROW({"c0", "c1", "c2"}, {INTEGER(), DOUBLE(), INTEGER()});
  const auto inputVectors = makeVectors(rowType, 128, 256);
  createDuckDbTable(inputVectors);

  std::atomic_bool terminateWaitFlag{true};
  folly::EventCount terminateWait;
  std::atomic<Task*> task{nullptr};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Task::terminate",
      std::function<void(Task*)>(([&](Task* _task) {
        task = _task;
        terminateWait.await([&]() { return !terminateWaitFlag.load(); });
      })));

  std::thread queryThread([&]() {
    AssertQueryBuilder(duckDbQueryRunner_)
        .maxDrivers(1)
        .plan(PlanBuilder()
                  .values(inputVectors)
                  .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                  .planNode())
        .assertResults("SELECT c0, c1, array_agg(c2) FROM tmp GROUP BY c0, c1");
  });

  while (task == nullptr) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  // The task deletion future is not ful-filled as the task is not deleted.
  ASSERT_FALSE(task.load()
                   ->taskDeletionFuture()
                   .within(std::chrono::microseconds(1'000'000))
                   .wait()
                   .hasValue());
  auto deleteFuture = task.load()->taskDeletionFuture();
  terminateWaitFlag = false;
  terminateWait.notifyAll();
  // The task deletion future is ful-filled after the wait.
  ASSERT_TRUE(deleteFuture.wait().hasValue());
  queryThread.join();
}

DEBUG_ONLY_TEST_F(TaskTest, taskCancellation) {
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
      core::QueryCtx::create(driverExecutor_.get()),
      Task::ExecutionMode::kParallel);
  task->start(4, 1);
  auto cancellationToken = task->getCancellationToken();
  ASSERT_FALSE(cancellationToken.isCancellationRequested());

  // Request pause.
  auto pauseWait = task->requestPause();
  // Fail the task.
  task->requestAbort();
  ASSERT_TRUE(cancellationToken.isCancellationRequested());

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
  ASSERT_TRUE(cancellationToken.isCancellationRequested());

  task.reset();
  waitForAllTasksToBeDeleted();
}
} // namespace facebook::velox::exec::test
