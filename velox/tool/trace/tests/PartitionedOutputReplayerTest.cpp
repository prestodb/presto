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

#include <gtest/gtest.h>
#include <memory>
#include <string>

#include "folly/dynamic.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/OperatorTraceReader.h"
#include "velox/exec/PartitionFunction.h"
#include "velox/exec/TraceUtil.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/tool/trace/PartitionedOutputReplayer.h"

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::common;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace facebook::velox::tool::trace::test {
class PartitionedOutputReplayerTest
    : public HiveConnectorTestBase,
      public testing::WithParamInterface<VectorSerde::Kind> {
 public:
  static std::vector<VectorSerde::Kind> getTestParams() {
    const std::vector<VectorSerde::Kind> kinds(
        {VectorSerde::Kind::kPresto,
         VectorSerde::Kind::kCompactRow,
         VectorSerde::Kind::kUnsafeRow});
    return kinds;
  }

 protected:
  static void SetUpTestCase() {
    HiveConnectorTestBase::SetUpTestCase();
    filesystems::registerLocalFileSystem();
    if (!isRegisteredVectorSerde()) {
      serializer::presto::PrestoVectorSerde::registerVectorSerde();
    }
    Type::registerSerDe();
    common::Filter::registerSerDe();
    connector::hive::HiveTableHandle::registerSerDe();
    connector::hive::LocationHandle::registerSerDe();
    connector::hive::HiveColumnHandle::registerSerDe();
    connector::hive::HiveInsertTableHandle::registerSerDe();
    connector::hive::HiveInsertFileNameGenerator::registerSerDe();
    core::PlanNode::registerSerDe();
    core::ITypedExpr::registerSerDe();
    registerPartitionFunctionSerDe();
  }

  std::vector<RowVectorPtr> makeBatches(
      vector_size_t numBatches,
      std::function<RowVectorPtr(int32_t)> makeVector) {
    std::vector<RowVectorPtr> batches;
    batches.reserve(numBatches);
    for (int32_t i = 0; i < numBatches; ++i) {
      batches.push_back(makeVector(i));
    }
    return batches;
  }

  std::shared_ptr<core::QueryCtx> createQueryContext(
      const std::unordered_map<std::string, std::string>& config) {
    return core::QueryCtx::create(
        executor_.get(), core::QueryConfig(std::move(config)));
  }

  std::shared_ptr<exec::Task> createPartitionedOutputTask(
      const std::vector<RowVectorPtr>& inputs,
      const std::vector<std::string>& partitionKeys,
      const std::vector<std::string>& outputLayout,
      const std::string& traceRoot,
      const std::string& taskId,
      uint32_t numPartitions,
      std::string& capturedPlanNodeId) {
    VELOX_CHECK(capturedPlanNodeId.empty());
    auto plan =
        PlanBuilder()
            .values(inputs, false)
            .partitionedOutput(
                partitionKeys, numPartitions, false, outputLayout, GetParam())
            .capturePlanNodeId(capturedPlanNodeId)
            .planNode();
    auto task = Task::create(
        taskId,
        core::PlanFragment{plan},
        0,
        createQueryContext(
            {{core::QueryConfig::kQueryTraceEnabled, "true"},
             {core::QueryConfig::kQueryTraceDir, traceRoot},
             {core::QueryConfig::kQueryTraceMaxBytes,
              std::to_string(100UL << 30)},
             {core::QueryConfig::kQueryTraceTaskRegExp, ".*"},
             {core::QueryConfig::kQueryTraceNodeIds, capturedPlanNodeId},
             {core::QueryConfig::kMaxPartitionedOutputBufferSize,
              std::to_string(8UL << 20)},
             {core::QueryConfig::kMaxOutputBufferSize,
              std::to_string(8UL << 20)}}),
        Task::ExecutionMode::kParallel);
    return task;
  }

  const std::shared_ptr<OutputBufferManager> bufferManager_{
      exec::OutputBufferManager::getInstanceRef()};
};

TEST_P(PartitionedOutputReplayerTest, defaultConsumer) {
  const uint32_t numPartitions = 10;
  std::string planNodeId;
  auto input = makeRowVector(
      {"key", "value"},
      {makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
       makeFlatVector<int32_t>(1'000, [](auto row) { return row; })});
  const auto testDir = TempDirectoryPath::create();
  const auto traceRoot = fmt::format("{}/{}", testDir->getPath(), "traceRoot");
  auto originalTask = createPartitionedOutputTask(
      {input},
      {"key"},
      {"key", "value"},
      traceRoot,
      "local://test-partitioned-output-replayer-basic-defaultConsumer",
      numPartitions,
      planNodeId);

  originalTask->start(1);

  auto consumerExecutor = std::make_unique<folly::CPUThreadPoolExecutor>(
      numPartitions, std::make_shared<folly::NamedThreadFactory>("Consumer"));
  consumeAllData(
      bufferManager_,
      originalTask->taskId(),
      numPartitions,
      executor_.get(),
      consumerExecutor.get(),
      [&](auto /* unused */, auto /* unused */) {});

  ASSERT_NO_THROW(PartitionedOutputReplayer(
                      traceRoot,
                      originalTask->queryCtx()->queryId(),
                      originalTask->taskId(),
                      planNodeId,
                      GetParam(),
                      "PartitionedOutput",
                      "",
                      0,
                      executor_.get())
                      .run(false));
}

TEST_P(PartitionedOutputReplayerTest, basic) {
  struct TestParam {
    std::string testName;
    uint32_t numPartitions;
    RowVectorPtr input;
    std::string debugString() {
      return fmt::format(
          "testName {}, numPartitions {}, input type {}",
          testName,
          numPartitions,
          input->toString());
    }
  };
  std::vector<TestParam> testParams = {
      // 10 partitions, 1000 row vector[int, int]
      {"small-dataset",
       10,
       makeRowVector(
           {"key", "value"},
           {makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
            makeFlatVector<int32_t>(
                1'000, [](auto row) { return row * 2; }, nullEvery(7))})},
      // 4 partitions, 80'000 row vector[int, string] with each string being
      // 1024 bytes size
      {"large-dataset",
       4,
       makeRowVector(
           {"key", "value"},
           {makeFlatVector<int32_t>(80'000, [](auto row) { return row; }),
            makeFlatVector<std::string>(
                80'000, [](auto row) { return std::string(1024, 'x'); })})}};

  for (auto& testParam : testParams) {
    SCOPED_TRACE(testParam.debugString());
    std::string planNodeId;
    const auto testDir = TempDirectoryPath::create();
    const auto traceRoot =
        fmt::format("{}/{}", testDir->getPath(), "traceRoot");
    auto originalTask = createPartitionedOutputTask(
        {testParam.input},
        {"key"},
        {"key", "value"},
        traceRoot,
        fmt::format(
            "local://test-partitioned-output-replayer-basic-{}",
            testParam.testName),
        testParam.numPartitions,
        planNodeId);

    originalTask->start(1);

    const auto queryId = originalTask->queryCtx()->queryId();
    const auto taskId = originalTask->taskId();

    std::vector<std::vector<std::unique_ptr<folly::IOBuf>>>
        originalPartitionedResults;
    originalPartitionedResults.reserve(testParam.numPartitions);
    originalPartitionedResults.resize(testParam.numPartitions);
    auto consumerExecutor = std::make_unique<folly::CPUThreadPoolExecutor>(
        testParam.numPartitions,
        std::make_shared<folly::NamedThreadFactory>("Consumer"));
    consumeAllData(
        bufferManager_,
        taskId,
        testParam.numPartitions,
        executor_.get(),
        consumerExecutor.get(),
        [&](auto partition, auto page) {
          originalPartitionedResults[partition].push_back(std::move(page));
        });

    // Verified that the trace summary has been written properly.
    const auto taskTraceDir =
        exec::trace::getTaskTraceDirectory(traceRoot, *originalTask);
    originalTask.reset();

    {
      std::vector<std::vector<std::unique_ptr<folly::IOBuf>>>
          replayedPartitionedResults;
      replayedPartitionedResults.reserve(testParam.numPartitions);
      replayedPartitionedResults.resize(testParam.numPartitions);
      PartitionedOutputReplayer(
          traceRoot,
          queryId,
          taskId,
          planNodeId,
          GetParam(),
          "PartitionedOutput",
          "",
          0,
          executor_.get(),
          [&](auto partition, auto page) {
            replayedPartitionedResults[partition].push_back(std::move(page));
          })
          .run(false);

      ASSERT_EQ(replayedPartitionedResults.size(), testParam.numPartitions);
      for (uint32_t partition = 0; partition < testParam.numPartitions;
           partition++) {
        const auto& originalBufList = originalPartitionedResults.at(partition);
        const auto& replayedBufList = replayedPartitionedResults[partition];
        ASSERT_EQ(replayedBufList.size(), originalBufList.size());
        for (uint32_t i = 0; i < replayedBufList.size(); i++) {
          ASSERT_EQ(
              replayedBufList[i]->computeChainDataLength(),
              originalBufList[i]->computeChainDataLength());
        }
      }
    }
    originalPartitionedResults.clear();

    // Wait for the producer tasks to all finish.
    exec::test::waitForAllTasksToBeDeleted(5'000'000);

    const auto opTraceDir = exec::trace::getOpTraceDirectory(
        taskTraceDir,
        planNodeId,
        /*pipelineId=*/0,
        /*driverId=*/0);
    const auto summary =
        exec::trace::OperatorTraceSummaryReader(opTraceDir, pool()).read();
    ASSERT_GT(summary.inputRows, 0);
    ASSERT_GT(summary.peakMemory, 0);
    ASSERT_EQ(summary.opType, "PartitionedOutput");
  }
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    PartitionedOutputReplayerTest,
    PartitionedOutputReplayerTest,
    testing::ValuesIn(PartitionedOutputReplayerTest::getTestParams()));
} // namespace facebook::velox::tool::trace::test
