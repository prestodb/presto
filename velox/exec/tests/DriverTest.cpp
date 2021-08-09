/*
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
#include <folly/init/Init.h>
#include "velox/dwio/dwrf/test/utils/BatchMaker.h"
#include "velox/exec/tests/Cursor.h"
#include "velox/exec/tests/OperatorTestBase.h"
#include "velox/exec/tests/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

using facebook::velox::test::BatchMaker;

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

  std::shared_ptr<const core::PlanNode> makeValuesFilterProject(
      const std::shared_ptr<const RowType>& rowType,
      const std::string& filter,
      const std::string& project,
      int32_t numBatches,
      int32_t rowsInBatch,
      // applies to second column
      std::function<bool(int64_t)> filterFunc = nullptr,
      int32_t* filterHits = nullptr) {
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
      auto projectNames = rowType->names();
      auto expressions = projectNames;
      projectNames.push_back("expr");
      expressions.push_back(project);

      planBuilder.project(expressions, projectNames);
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
          cursor->cancelPool()->requestPause(false);
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
          cursor->cancelPool()->requestTerminate();
        } else if (operation == ResultOperation::kTerminate) {
          cursor->task()->terminate(kAborted);
        } else if (operation == ResultOperation::kYield) {
          cursor->cancelPool()->requestYield();
        } else if (operation == ResultOperation::kPause) {
          cursor->cancelPool()->requestPause(true);
          auto& executor = folly::QueuedImmediateExecutor::instance();
          auto future = cursor->cancelPool()->finishFuture().via(&executor);
          future.wait();
          paused = true;
        }
      }
    }
  }

  std::shared_ptr<const RowType> rowType_;
  std::mutex mutex_;
  std::vector<std::shared_ptr<Task>> tasks_;
  std::unordered_map<int32_t, folly::Future<bool>> stateFutures_;
};

TEST_F(DriverTest, error) {
  Driver::testingJoinAndReinitializeExecutor(10);
  CursorParameters params;
  params.planNode = makeValuesFilterProject(rowType_, "m1 % 0", "", 100, 10);
  params.numThreads = 20;
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
  EXPECT_EQ(tasks_[0]->state(), kFailed);
}

TEST_F(DriverTest, cancel) {
  CursorParameters params;
  params.queryCtx = core::QueryCtx::create();

  params.planNode = makeValuesFilterProject(
      rowType_,
      "m1 % 10 > 0",
      "m1 % 3 + m2 % 5 + m3 % 7 + m4 % 11 + m5 % 13 + m6 % 17 + m7 % 19",
      100,
      100'000);
  params.numThreads = 10;
  int32_t numRead = 0;
  try {
    readResults(params, ResultOperation::kCancel, 1'000'000, &numRead);
    EXPECT_TRUE(false) << "Expected exception";
  } catch (const VeloxRuntimeError& e) {
    EXPECT_EQ("Cancelled", e.message());
  }
  EXPECT_GE(numRead, 1'000'000);
  auto& executor = folly::QueuedImmediateExecutor::instance();
  auto future = tasks_[0]->cancelPool()->finishFuture().via(&executor);
  future.wait();
  EXPECT_TRUE(stateFutures_.at(0).isReady());
  EXPECT_EQ(tasks_[0]->numDrivers(), 0);
}

TEST_F(DriverTest, terminate) {
  CursorParameters params;
  params.queryCtx = core::QueryCtx::create();

  params.planNode = makeValuesFilterProject(
      rowType_,
      "m1 % 10 > 0",
      "m1 % 3 + m2 % 5 + m3 % 7 + m4 % 11 + m5 % 13 + m6 % 17 + m7 % 19",
      100,
      100'000);
  params.numThreads = 10;
  int32_t numRead = 0;
  try {
    readResults(params, ResultOperation::kTerminate, 1'000'000, &numRead);
    // Not necessarily an exception.
  } catch (const std::exception& e) {
    // If this is an exception, it will be a cancellation.
    EXPECT_EQ("Cancelled", std::string(e.what()));
  }
  EXPECT_GE(numRead, 1'000'000);
  EXPECT_TRUE(stateFutures_.at(0).isReady());
  EXPECT_EQ(tasks_[0]->state(), kAborted);
}

TEST_F(DriverTest, slow) {
  CursorParameters params;
  params.planNode = makeValuesFilterProject(
      rowType_,
      "m1 % 10 > 0",
      "m1 % 3 + m2 % 5 + m3 % 7 + m4 % 11 + m5 % 13 + m6 % 17 + m7 % 19",
      300,
      1'000);
  params.numThreads = 10;
  int32_t numRead = 0;
  readResults(params, ResultOperation::kReadSlow, 50'000, &numRead);
  EXPECT_GE(numRead, 50'000);
  // Sync before checking end state. The cursor is at end as soon as
  // CallbackSink::finish is called. The thread count and task state
  // are updated some tens of instructions after this. Determinism
  // requires a barrier.
  auto& executor = folly::QueuedImmediateExecutor::instance();
  auto future = tasks_[0]->cancelPool()->finishFuture().via(&executor);
  future.wait();
  EXPECT_EQ(tasks_[0]->numDrivers(), 0);
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
      100,
      10'000,
      [](int64_t num) { return num % 10 > 0; },
      &hits);
  params.numThreads = 10;
  int32_t numRead = 0;
  readResults(params, ResultOperation::kPause, 370'000'000, &numRead);
  // Each thread will fully read the 1M rows in values.
  EXPECT_EQ(numRead, 10 * hits);
  EXPECT_TRUE(stateFutures_.at(0).isReady());
  EXPECT_EQ(tasks_[0]->state(), kFinished);
  EXPECT_EQ(tasks_[0]->numDrivers(), 0);
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
  std::vector<int32_t> counters;
  counters.reserve(kNumTasks);
  std::vector<CursorParameters> params;
  params.resize(kNumTasks);
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
    params[i].numThreads = kThreadsPerTask;
  }
  std::vector<std::thread> threads;
  threads.reserve(kNumTasks);
  for (int32_t i = 0; i < kNumTasks; ++i) {
    counters.push_back(0);
    threads.push_back(std::thread([this, &params, &counters, i]() {
      readResults(params[i], ResultOperation::kYield, 10'000, &counters[i], i);
    }));
  }
  for (int32_t i = 0; i < kNumTasks; ++i) {
    threads[i].join();
    EXPECT_EQ(counters[i], kThreadsPerTask * hits);
    EXPECT_TRUE(stateFutures_.at(i).isReady());
  }
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
