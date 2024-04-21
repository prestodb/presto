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
#include "velox/exec/PartitionedOutput.h"
#include <gtest/gtest.h>
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace facebook::velox::exec::test {

class PartitionedOutputTest : public OperatorTestBase {
 protected:
  std::shared_ptr<core::QueryCtx> createQueryContext(
      std::unordered_map<std::string, std::string> config) {
    return std::make_shared<core::QueryCtx>(
        executor_.get(), core::QueryConfig(std::move(config)));
  }

  std::vector<std::unique_ptr<folly::IOBuf>>
  getData(const std::string& taskId, int destination, int64_t sequence) {
    auto [promise, semiFuture] = folly::makePromiseContract<
        std::vector<std::unique_ptr<folly::IOBuf>>>();
    VELOX_CHECK(bufferManager_->getData(
        taskId,
        destination,
        PartitionedOutput::kMinDestinationSize,
        sequence,
        [result = std::make_shared<
             folly::Promise<std::vector<std::unique_ptr<folly::IOBuf>>>>(
             std::move(promise))](
            std::vector<std::unique_ptr<folly::IOBuf>> pages,
            int64_t /*inSequence*/,
            std::vector<int64_t> /*remainingBytes*/) {
          result->setValue(std::move(pages));
        }));
    auto future = std::move(semiFuture).via(executor_.get());
    future.wait(std::chrono::seconds{10});
    VELOX_CHECK(future.isReady());
    return std::move(future).value();
  }

  std::vector<std::unique_ptr<folly::IOBuf>> getAllData(
      const std::string& taskId,
      int destination) {
    std::vector<std::unique_ptr<folly::IOBuf>> result;
    int attempts = 0;
    bool done = false;
    while (!done) {
      attempts++;
      VELOX_CHECK_LT(attempts, 100);
      std::vector<std::unique_ptr<folly::IOBuf>> pages =
          getData(taskId, destination, result.size());
      for (auto& page : pages) {
        if (page) {
          result.push_back(std::move(page));
        } else {
          bufferManager_->deleteResults(taskId, destination);
          done = true;
          break;
        }
      }
    }
    return result;
  }

 private:
  const std::shared_ptr<OutputBufferManager> bufferManager_{
      OutputBufferManager::getInstance().lock()};
};

TEST_F(PartitionedOutputTest, flush) {
  // This test verifies
  //  - Flush thresholds are respected (flush doesn't happen neither too early
  //  nor too late)
  //  - Flush is done independently for each output partition (flush for one
  //  partition doesn't trigger flush for another one)

  auto input = makeRowVector(
      {"p1", "v1"},
      {makeFlatVector<int32_t>({0, 1}),
       makeFlatVector<std::string>({
           // twice as large to make sure it is always flushed (even if
           // PartitionedOutput#setTargetSizePct rolls 120%)
           std::string(PartitionedOutput::kMinDestinationSize * 2, '0'),
           // 10 times smaller, so the data from 13 pages is always flushed as 2
           // pages
           // 130% > 120% (when PartitionedOutput#setTargetSizePct rolls 120%)
           // 130% < 140% (when PartitionedOutput#setTargetSizePct rolls 70% two
           // times in a row)
           std::string(PartitionedOutput::kMinDestinationSize / 10, '1'),
       })});

  auto plan = PlanBuilder()
                  // produce 13 pages
                  .values({input}, false, 13)
                  .partitionedOutput({"p1"}, 2, std::vector<std::string>{"v1"})
                  .planNode();

  auto taskId = "local://test-partitioned-output-flush-0";
  auto task = Task::create(
      taskId,
      core::PlanFragment{plan},
      0,
      createQueryContext(
          {{core::QueryConfig::kMaxPartitionedOutputBufferSize,
            std::to_string(PartitionedOutput::kMinDestinationSize * 2)}}),
      Task::ExecutionMode::kParallel);
  task->start(1);

  const auto partition0 = getAllData(taskId, 0);
  const auto partition1 = getAllData(taskId, 1);

  const auto taskWaitUs = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::seconds{10})
                              .count();
  auto future = task->taskCompletionFuture()
                    .within(std::chrono::microseconds(taskWaitUs))
                    .via(executor_.get());
  future.wait();

  ASSERT_TRUE(waitForTaskDriversToFinish(task.get(), taskWaitUs));

  // Since each row for partition 0 is over the flush threshold as
  // many pages as there are input pages are expected
  EXPECT_EQ(partition0.size(), 13);
  // Data for the second partition is much smaller and expected to be buffered
  // up to a defined threshold
  EXPECT_EQ(partition1.size(), 2);
}

} // namespace facebook::velox::exec::test
