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
#include <folly/Unit.h>
#include <folly/init/Init.h>
#include <velox/exec/Driver.h>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/Cursor.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

using facebook::velox::test::BatchMaker;

// A PlanNode that passes its input to its output and makes variable
// memory reservations.
// A PlanNode that passes its input to its output and periodically
// pauses and resumes other Tasks.
class TestingPauserNode : public core::PlanNode {
 public:
  explicit TestingPauserNode(core::PlanNodePtr input)
      : PlanNode("Pauser"), sources_{input} {}

  TestingPauserNode(const core::PlanNodeId& id, core::PlanNodePtr input)
      : PlanNode(id), sources_{input} {}

  const RowTypePtr& outputType() const override {
    return sources_[0]->outputType();
  }

  const std::vector<std::shared_ptr<const PlanNode>>& sources() const override {
    return sources_;
  }

  std::string_view name() const override {
    return "Pauser";
  }

 private:
  void addDetails(std::stringstream& /* stream */) const override {}

  std::vector<core::PlanNodePtr> sources_;
};

class DriverTest : public OperatorTestBase {
 protected:
  enum class ResultOperation {
    kRead,
    kReadSlow,
    kDrop,
    kCancel,
    kTerminate,
    kPause,
    kYield
  };

  void SetUp() override {
    OperatorTestBase::SetUp();
    rowType_ =
        ROW({"key", "m1", "m2", "m3", "m4", "m5", "m6", "m7"},
            {BIGINT(),
             BIGINT(),
             BIGINT(),
             BIGINT(),
             BIGINT(),
             BIGINT(),
             BIGINT(),
             BIGINT()});
  }

  void TearDown() override {
    for (auto& task : tasks_) {
      if (task != nullptr) {
        waitForTaskCompletion(task.get(), 1'000'000);
      }
    }
    // NOTE: destroy the tasks first to release all the allocated memory held
    // by the plan nodes (Values) in tasks.
    tasks_.clear();

    if (wakeupInitialized_) {
      wakeupCancelled_ = true;
      wakeupThread_.join();
    }
    OperatorTestBase::TearDown();
  }

  core::PlanNodePtr makeValuesFilterProject(
      const RowTypePtr& rowType,
      const std::string& filter,
      const std::string& project,
      int32_t numBatches,
      int32_t rowsInBatch,
      // applies to second column
      std::function<bool(int64_t)> filterFunc = nullptr,
      int32_t* filterHits = nullptr,
      bool addTestingPauser = false) {
    std::vector<RowVectorPtr> batches;
    for (int32_t i = 0; i < numBatches; ++i) {
      batches.push_back(std::dynamic_pointer_cast<RowVector>(
          BatchMaker::createBatch(rowType, rowsInBatch, *pool_)));
    }
    if (filterFunc) {
      int32_t hits = 0;
      for (auto& batch : batches) {
        auto child = batch->childAt(1)->as<FlatVector<int64_t>>();
        for (vector_size_t i = 0; i < child->size(); ++i) {
          if (!child->isNullAt(i) && filterFunc(child->valueAt(i))) {
            hits++;
          }
        }
      }
      *filterHits = hits;
    }

    PlanBuilder planBuilder;
    planBuilder.values(batches, true).planNode();

    if (!filter.empty()) {
      planBuilder.filter(filter);
    }

    if (!project.empty()) {
      auto expressions = rowType->names();
      expressions.push_back(fmt::format("{} AS expr", project));

      planBuilder.project(expressions);
    }
    if (addTestingPauser) {
      planBuilder.addNode([](std::string id, core::PlanNodePtr input) {
        return std::make_shared<TestingPauserNode>(id, input);
      });
    }

    return planBuilder.planNode();
  }

  // Opens a cursor and reads data. Takes action 'operation' every 'numRows'
  // rows of data. Increments the 'counter' for each successfully read row.
  void readResults(
      CursorParameters& params,
      ResultOperation operation,
      int32_t numRows,
      int32_t* counter,
      int32_t threadId = 0) {
    auto cursor = std::make_unique<RowCursor>(params);
    {
      std::lock_guard<std::mutex> l(mutex_);
      tasks_.push_back(cursor->task());
      // To be realized either after 1s wall time or when the corresponding Task
      // is no longer running.
      auto& executor = folly::QueuedImmediateExecutor::instance();
      auto future = tasks_.back()->stateChangeFuture(1'000'000).via(&executor);
      stateFutures_.emplace(threadId, std::move(future));

      EXPECT_FALSE(stateFutures_.at(threadId).isReady());
    }
    bool paused = false;
    for (;;) {
      if (operation == ResultOperation::kPause && paused) {
        if (!cursor->hasNext()) {
          paused = false;
          Task::resume(cursor->task());
        }
      }
      if (!cursor->next()) {
        break;
      }
      ++*counter;
      if (*counter % numRows == 0) {
        if (operation == ResultOperation::kDrop) {
          return;
        }
        if (operation == ResultOperation::kReadSlow) {
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
          // If this is an EXPECT this is flaky when running on a
          // noisy test cloud.
          LOG(INFO) << "Task::toString() while probably blocked: "
                    << tasks_[0]->toString();
        } else if (operation == ResultOperation::kCancel) {
          cancelFuture_ = cursor->task()->requestCancel();
        } else if (operation == ResultOperation::kTerminate) {
          cancelFuture_ = cursor->task()->requestAbort();
        } else if (operation == ResultOperation::kYield) {
          cursor->task()->requestYield();
        } else if (operation == ResultOperation::kPause) {
          auto& executor = folly::QueuedImmediateExecutor::instance();
          auto future = cursor->task()->requestPause(true).via(&executor);
          future.wait();
          paused = true;
        }
      }
    }
  }

  // Checks that Test passes within a reasonable delay. The test can
  // be flaky under indeterminate timing (heavy load) because we wait
  // for a future that is realized after all threads have acknowledged
  // a stop or pause.  Setting the next state is not in the same
  // critical section as realizing the future, hence there can be a
  // delay of some hundreds of instructions before all the consequent
  // state changes occur. For cases where we have a cursor at end and
  // the final state is set only after the cursor at end is visible to
  // the caller, we do not have a good way to combine all inside the
  // same critical section.
  template <typename Test>
  void expectWithDelay(
      Test test,
      const char* file,
      int32_t line,
      const char* message) {
    constexpr int32_t kMaxWait = 1000;

    for (auto i = 0; i < kMaxWait; ++i) {
      if (test()) {
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    FAIL() << file << ":" << line << " " << message << "not realized within 1s";
  }

 public:
  // Sets 'future' to a future that will be realized within a random
  // delay of a few ms.
  void registerForWakeup(ContinueFuture* future) {
    std::lock_guard<std::mutex> l(wakeupMutex_);
    if (!wakeupInitialized_) {
      wakeupInitialized_ = true;
      wakeupThread_ = std::thread([this]() {
        int32_t counter = 0;
        for (;;) {
          {
            std::lock_guard<std::mutex> l2(wakeupMutex_);
            if (wakeupCancelled_) {
              return;
            }
          }
          // Wait a small interval and realize a small number of queued
          // promises, if any.
          auto units = 1 + (++counter % 5);

          // NOLINT
          std::this_thread::sleep_for(std::chrono::milliseconds(units));
          {
            std::lock_guard<std::mutex> l2(wakeupMutex_);
            auto count = 1 + (++counter % 4);
            for (auto i = 0; i < count; ++i) {
              if (wakeupPromises_.empty()) {
                break;
              }
              wakeupPromises_.front().setValue();
              wakeupPromises_.pop_front();
            }
          }
        }
      });
    }
    auto [promise, semiFuture] = makeVeloxContinuePromiseContract("wakeup");
    *future = std::move(semiFuture);
    wakeupPromises_.push_back(std::move(promise));
  }

  // Registers a Task for use in randomTask().
  void registerTask(std::shared_ptr<Task> task) {
    std::lock_guard<std::mutex> l(taskMutex_);
    if (std::find(allTasks_.begin(), allTasks_.end(), task) !=
        allTasks_.end()) {
      return;
    }
    allTasks_.push_back(task);
  }

  void unregisterTask(std::shared_ptr<Task> task) {
    std::lock_guard<std::mutex> l(taskMutex_);
    auto it = std::find(allTasks_.begin(), allTasks_.end(), task);
    if (it == allTasks_.end()) {
      return;
    }
    allTasks_.erase(it);
  }

  std::shared_ptr<Task> randomTask() {
    std::lock_guard<std::mutex> l(taskMutex_);
    if (allTasks_.empty()) {
      return nullptr;
    }
    return allTasks_[folly::Random::rand32() % allTasks_.size()];
  }

 protected:
  // State for registerForWakeup().
  std::mutex wakeupMutex_;
  std::thread wakeupThread_;
  std::deque<ContinuePromise> wakeupPromises_;
  bool wakeupInitialized_{false};
  // Set to true when it is time to exit 'wakeupThread_'.
  std::atomic<bool> wakeupCancelled_{false};

  RowTypePtr rowType_;
  std::mutex mutex_;
  std::vector<std::shared_ptr<Task>> tasks_;
  ContinueFuture cancelFuture_;
  std::unordered_map<int32_t, ContinueFuture> stateFutures_;

  // Mutex for randomTask()
  std::mutex taskMutex_;
  // Tasks registered for randomTask()
  std::vector<std::shared_ptr<Task>> allTasks_;

  folly::Random::DefaultGenerator rng_;
};

#define EXPECT_WITH_DELAY(test) \
  expectWithDelay([&]() { return test; }, __FILE__, __LINE__, #test)

TEST_F(DriverTest, error) {
  CursorParameters params;
  params.planNode =
      makeValuesFilterProject(rowType_, "m1 % 0 > 0", "", 100, 10);
  params.maxDrivers = 20;
  int32_t numRead = 0;
  try {
    readResults(params, ResultOperation::kRead, 1'000'000, &numRead);
    EXPECT_TRUE(false) << "Expected exception";
  } catch (const VeloxException& e) {
    EXPECT_NE(e.message().find("Cannot divide by 0"), std::string::npos);
  }
  EXPECT_EQ(numRead, 0);
  EXPECT_TRUE(stateFutures_.at(0).isReady());
  // Realized immediately since task not running.
  EXPECT_TRUE(tasks_[0]->stateChangeFuture(1'000'000).isReady());
  EXPECT_EQ(tasks_[0]->state(), TaskState::kFailed);
}

TEST_F(DriverTest, cancel) {
  CursorParameters params;
  params.planNode = makeValuesFilterProject(
      rowType_,
      "m1 % 10 > 0",
      "m1 % 3 + m2 % 5 + m3 % 7 + m4 % 11 + m5 % 13 + m6 % 17 + m7 % 19",
      1'000,
      1'000);
  params.maxDrivers = 10;
  int32_t numRead = 0;
  try {
    readResults(params, ResultOperation::kCancel, 1'000'000, &numRead);
    FAIL() << "Expected exception";
  } catch (const VeloxRuntimeError& e) {
    EXPECT_EQ("Cancelled", e.message());
  }
  EXPECT_GE(numRead, 1'000'000);
  auto& executor = folly::QueuedImmediateExecutor::instance();
  auto future = tasks_[0]->stateChangeFuture(1'000'000).via(&executor);
  future.wait();
  EXPECT_TRUE(stateFutures_.at(0).isReady());

  std::move(cancelFuture_).via(&executor).wait();

  EXPECT_EQ(tasks_[0]->numRunningDrivers(), 0);
}

TEST_F(DriverTest, terminate) {
  CursorParameters params;
  params.planNode = makeValuesFilterProject(
      rowType_,
      "m1 % 10 > 0",
      "m1 % 3 + m2 % 5 + m3 % 7 + m4 % 11 + m5 % 13 + m6 % 17 + m7 % 19",
      1'000,
      1'000);
  params.maxDrivers = 10;
  int32_t numRead = 0;
  try {
    readResults(params, ResultOperation::kTerminate, 1'000'000, &numRead);
    // Not necessarily an exception.
  } catch (const std::exception& e) {
    // If this is an exception, it will be a cancellation.
    EXPECT_TRUE(strstr(e.what(), "Aborted") != nullptr) << e.what();
  }

  ASSERT_TRUE(cancelFuture_.valid());
  auto& executor = folly::QueuedImmediateExecutor::instance();
  std::move(cancelFuture_).via(&executor).wait();

  EXPECT_GE(numRead, 1'000'000);
  EXPECT_TRUE(stateFutures_.at(0).isReady());
  EXPECT_EQ(tasks_[0]->state(), TaskState::kAborted);
}

TEST_F(DriverTest, slow) {
  CursorParameters params;
  params.planNode = makeValuesFilterProject(
      rowType_,
      "m1 % 10 > 0",
      "m1 % 3 + m2 % 5 + m3 % 7 + m4 % 11 + m5 % 13 + m6 % 17 + m7 % 19",
      300,
      1'000);
  params.maxDrivers = 10;
  int32_t numRead = 0;
  readResults(params, ResultOperation::kReadSlow, 50'000, &numRead);
  EXPECT_GE(numRead, 50'000);
  // Sync before checking end state. The cursor is at end as soon as
  // CallbackSink::finish is called. The thread count and task state
  // are updated some tens of instructions after this. Determinism
  // requires a barrier.
  auto& executor = folly::QueuedImmediateExecutor::instance();
  auto future = tasks_[0]->stateChangeFuture(1'000'000).via(&executor);
  future.wait();
  // Note that the driver count drops after the last thread stops and
  // realizes the future.
  EXPECT_WITH_DELAY(tasks_[0]->numRunningDrivers() == 0);
  const auto stats = tasks_[0]->taskStats().pipelineStats;
  ASSERT_TRUE(!stats.empty() && !stats[0].operatorStats.empty());
  // Check that the blocking of the CallbackSink at the end of the pipeline is
  // recorded.
  EXPECT_GT(stats[0].operatorStats.back().blockedWallNanos, 0);
  EXPECT_TRUE(stateFutures_.at(0).isReady());
  // The future was realized by timeout.
  EXPECT_TRUE(stateFutures_.at(0).hasException());
}

TEST_F(DriverTest, pause) {
  CursorParameters params;
  int32_t hits;
  params.planNode = makeValuesFilterProject(
      rowType_,
      "m1 % 10 > 0",
      "m1 % 3 + m2 % 5 + m3 % 7 + m4 % 11 + m5 % 13 + m6 % 17 + m7 % 19",
      1'000,
      1'000,
      [](int64_t num) { return num % 10 > 0; },
      &hits);
  params.maxDrivers = 10;
  int32_t numRead = 0;
  readResults(params, ResultOperation::kPause, 370'000'000, &numRead);
  // Each thread will fully read the 1M rows in values.
  EXPECT_EQ(numRead, 10 * hits);
  auto stateFuture = tasks_[0]->stateChangeFuture(100'000'000);
  auto& executor = folly::QueuedImmediateExecutor::instance();
  auto state = std::move(stateFuture).via(&executor);
  state.wait();
  EXPECT_TRUE(tasks_[0]->isFinished());
  EXPECT_EQ(tasks_[0]->numRunningDrivers(), 0);
  const auto taskStats = tasks_[0]->taskStats();
  ASSERT_EQ(taskStats.pipelineStats.size(), 1);
  const auto& operators = taskStats.pipelineStats[0].operatorStats;
  EXPECT_GT(operators[1].getOutputTiming.wallNanos, 0);
  EXPECT_EQ(operators[0].outputPositions, 10000000);
  EXPECT_EQ(operators[1].inputPositions, 10000000);
  EXPECT_EQ(operators[1].outputPositions, 10 * hits);
}

TEST_F(DriverTest, yield) {
  constexpr int32_t kNumTasks = 20;
  constexpr int32_t kThreadsPerTask = 5;
  std::vector<CursorParameters> params(kNumTasks);
  int32_t hits;
  for (int32_t i = 0; i < kNumTasks; ++i) {
    params[i].planNode = makeValuesFilterProject(
        rowType_,
        "m1 % 10 > 0",
        "m1 % 3 + m2 % 5 + m3 % 7 + m4 % 11 + m5 % 13 + m6 % 17 + m7 % 19",
        200,
        2'000,
        [](int64_t num) { return num % 10 > 0; },
        &hits);
    params[i].maxDrivers = kThreadsPerTask;
  }
  std::vector<int32_t> counters(kNumTasks, 0);
  std::vector<std::thread> threads;
  threads.reserve(kNumTasks);
  for (int32_t i = 0; i < kNumTasks; ++i) {
    threads.push_back(std::thread([this, &params, &counters, i]() {
      readResults(params[i], ResultOperation::kYield, 10'000, &counters[i], i);
    }));
  }
  for (int32_t i = 0; i < kNumTasks; ++i) {
    threads[i].join();
    EXPECT_WITH_DELAY(stateFutures_.at(i).isReady());
    EXPECT_EQ(counters[i], kThreadsPerTask * hits);
  }
}

// A testing Operator that periodically does one of the following:
//
// 1. Blocks and registers a resume that continues the Driver after a timed
// pause. This simulates blocking to wait for exchange or consumer.
//
// 2. Enters a suspended section where the Driver is on thread but is not
// counted as running and is therefore instantaneously cancellable and pausable.
// Comes back on thread after a timed pause. This simulates an RPC to an out of
// process service.
//
// 3.  Enters a suspended section where this pauses and resumes random Tasks,
// including its own Task. This simulates making Tasks release memory under
// memory contention, checkpointing Tasks for migration or fault tolerance and
// other process-wide coordination activities.
//
// These situations will occur with arbitrary concurrency and sequence and must
// therefore be in one test to check against deadlocks.
class TestingPauser : public Operator {
 public:
  TestingPauser(
      DriverCtx* ctx,
      int32_t id,
      std::shared_ptr<const TestingPauserNode> node,
      DriverTest* test,
      int32_t sequence)
      : Operator(ctx, node->outputType(), id, node->id(), "Pauser"),
        test_(test),
        counter_(sequence) {
    test_->registerTask(operatorCtx_->task());
  }

  bool needsInput() const override {
    return !noMoreInput_ && !input_;
  }

  void addInput(RowVectorPtr input) override {
    input_ = std::move(input);
  }

  void noMoreInput() override {
    test_->unregisterTask(operatorCtx_->task());
    Operator::noMoreInput();
  }

  RowVectorPtr getOutput() override {
    if (!input_) {
      return nullptr;
    }
    ++counter_;
    auto label = operatorCtx_->driver()->label();
    // Block for a time quantum evern 10th time.
    if (counter_ % 10 == 0) {
      test_->registerForWakeup(&future_);
      return nullptr;
    }
    {
      SuspendedSection noCancel(operatorCtx_->driver());
      sleep(1);
      if (counter_ % 7 == 0) {
        // Every 7th time, stop and resume other Tasks. This operation is
        // globally serialized.
        std::lock_guard<std::mutex> l(pauseMutex_);

        for (auto i = 0; i <= counter_ % 3; ++i) {
          auto task = test_->randomTask();
          if (!task) {
            continue;
          }
          auto& executor = folly::QueuedImmediateExecutor::instance();
          auto future = task->requestPause(true).via(&executor);
          future.wait();
          sleep(2);
          Task::resume(task);
        }
      }
    }

    return std::move(input_);
  }

  BlockingReason isBlocked(ContinueFuture* future) override {
    VELOX_CHECK(!operatorCtx_->driver()->state().isSuspended);
    if (future_.valid()) {
      *future = std::move(future_);
      return BlockingReason::kWaitForConsumer;
    }
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return noMoreInput_ && input_ == nullptr;
  }

 private:
  void sleep(int32_t units) {
    // NOLINT
    std::this_thread::sleep_for(std::chrono::milliseconds(units));
  }

  // The DriverTest under which this is running. Used for global context.
  DriverTest* test_;

  // Mutex to serialize the pause/restart exercise so that only one instance
  // does this at a time.
  static std::mutex pauseMutex_;

  // Counter deciding the next action in getOutput().
  int32_t counter_;
  ContinueFuture future_;
};

std::mutex TestingPauser ::pauseMutex_;

namespace {

class PauserNodeFactory : public Operator::PlanNodeTranslator {
 public:
  PauserNodeFactory(
      uint32_t maxDrivers,
      std::atomic<int32_t>& sequence,
      DriverTest* testInstance)
      : maxDrivers_{maxDrivers},
        sequence_{sequence},
        testInstance_{testInstance} {}

  std::unique_ptr<Operator> toOperator(
      DriverCtx* ctx,
      int32_t id,
      const core::PlanNodePtr& node) override {
    if (auto pauser =
            std::dynamic_pointer_cast<const TestingPauserNode>(node)) {
      return std::make_unique<TestingPauser>(
          ctx, id, pauser, testInstance_, ++sequence_);
    }
    return nullptr;
  }

  std::optional<uint32_t> maxDrivers(const core::PlanNodePtr& node) override {
    if (auto pauser =
            std::dynamic_pointer_cast<const TestingPauserNode>(node)) {
      return maxDrivers_;
    }
    return std::nullopt;
  }

 private:
  uint32_t maxDrivers_;
  std::atomic<int32_t>& sequence_;
  DriverTest* testInstance_;
};

} // namespace

TEST_F(DriverTest, pauserNode) {
  constexpr int32_t kNumTasks = 20;
  constexpr int32_t kThreadsPerTask = 5;
  // Run with a fraction of the testing threads fitting in the executor.
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(20);
  static std::atomic<int32_t> sequence{0};
  // Use a static variable to pass the test instance to the create
  // function of the testing operator. The testing operator registers
  // all its Tasks in the test instance to create inter-Task pauses.
  static DriverTest* testInstance;
  testInstance = this;
  Operator::registerOperator(std::make_unique<PauserNodeFactory>(
      kThreadsPerTask, sequence, testInstance));

  std::vector<CursorParameters> params(kNumTasks);
  int32_t hits;
  for (int32_t i = 0; i < kNumTasks; ++i) {
    params[i].queryCtx = std::make_shared<core::QueryCtx>(
        executor.get(), std::make_shared<core::MemConfig>());
    params[i].planNode = makeValuesFilterProject(
        rowType_,
        "m1 % 10 > 0",
        "m1 % 3 + m2 % 5 + m3 % 7 + m4 % 11 + m5 % 13 + m6 % 17 + m7 % 19",
        200,
        2'000,
        [](int64_t num) { return num % 10 > 0; },
        &hits,
        true);
    params[i].maxDrivers =
        kThreadsPerTask * 2; // a number larger than kThreadsPerTask
  }
  std::vector<int32_t> counters(kNumTasks, 0);
  std::vector<std::thread> threads;
  threads.reserve(kNumTasks);
  for (int32_t i = 0; i < kNumTasks; ++i) {
    threads.push_back(std::thread([this, &params, &counters, i]() {
      try {
        readResults(params[i], ResultOperation::kRead, 10'000, &counters[i], i);
      } catch (const std::exception& e) {
        LOG(INFO) << "Pauser task errored out " << e.what();
      }
    }));
  }
  for (int32_t i = 0; i < kNumTasks; ++i) {
    threads[i].join();
    EXPECT_EQ(counters[i], kThreadsPerTask * hits);
    EXPECT_TRUE(stateFutures_.at(i).isReady());
  }
  tasks_.clear();
}

namespace {

// Custom node for the custom factory.
class ThrowNode : public core::PlanNode {
 public:
  ThrowNode(const core::PlanNodeId& id, core::PlanNodePtr input)
      : PlanNode(id), sources_{input} {}

  const RowTypePtr& outputType() const override {
    return sources_[0]->outputType();
  }

  const std::vector<std::shared_ptr<const PlanNode>>& sources() const override {
    return sources_;
  }

  std::string_view name() const override {
    return "Throw";
  }

 private:
  void addDetails(std::stringstream& /* stream */) const override {}

  std::vector<core::PlanNodePtr> sources_;
};

// Custom operator for the custom factory.
class ThrowOperator : public Operator {
 public:
  ThrowOperator(DriverCtx* ctx, int32_t id, core::PlanNodePtr node)
      : Operator(ctx, node->outputType(), id, node->id(), "Throw") {}

  bool needsInput() const override {
    return !noMoreInput_ && !input_;
  }

  void addInput(RowVectorPtr input) override {
    input_ = std::move(input);
  }

  void noMoreInput() override {
    Operator::noMoreInput();
  }

  RowVectorPtr getOutput() override {
    return std::move(input_);
  }

  BlockingReason isBlocked(ContinueFuture* /*future*/) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return noMoreInput_ && input_ == nullptr;
  }
};

// Custom factory that throws during driver creation.
class ThrowNodeFactory : public Operator::PlanNodeTranslator {
 public:
  ThrowNodeFactory() = default;

  std::unique_ptr<Operator> toOperator(
      DriverCtx* ctx,
      int32_t id,
      const core::PlanNodePtr& node) override {
    if (std::dynamic_pointer_cast<const ThrowNode>(node)) {
      VELOX_CHECK_EQ(driversCreated, 0, "Can only create 1 'throw driver'.");
      ++driversCreated;
      return std::make_unique<ThrowOperator>(ctx, id, node);
    }
    return nullptr;
  }

  std::optional<uint32_t> maxDrivers(const core::PlanNodePtr& node) override {
    if (std::dynamic_pointer_cast<const ThrowNode>(node)) {
      return 5;
    }
    return std::nullopt;
  }

 private:
  uint32_t driversCreated{0};
};

} // namespace

// Use a node for which driver factory would throw on any driver beyond id 0.
// This is to test that we do not crash due to early driver destruction and we
// have a proper error being propagated out.
TEST_F(DriverTest, driverCreationThrow) {
  Operator::registerOperator(std::make_unique<ThrowNodeFactory>());

  auto rows = makeRowVector({"c0"}, {makeFlatVector<int32_t>({1, 2, 3})});

  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();

  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values({rows}, true)
                  .addNode([](std::string id, core::PlanNodePtr input) {
                    return std::make_shared<ThrowNode>(id, input);
                  })
                  .planNode();

  // Ensure execution threw correct error.
  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan).maxDrivers(5).copyResults(pool()),
      "Can only create 1 'throw driver'.");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
