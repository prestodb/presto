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

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>

#include "folly/dynamic.h"
#include "velox/common/base/Fs.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/hyperloglog/SparseHll.h"
#include "velox/exec/OperatorTraceReader.h"
#include "velox/exec/PartitionFunction.h"
#include "velox/exec/TraceUtil.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/tool/trace/TableScanReplayer.h"
#include "velox/tool/trace/TraceReplayRunner.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::common;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::connector;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::common::testutil;
using namespace facebook::velox::common::hll;

namespace facebook::velox::tool::trace::test {
class TableScanReplayerTest : public HiveConnectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
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
    connector::hive::HiveConnectorSplit::registerSerDe();
    core::PlanNode::registerSerDe();
    core::ITypedExpr::registerSerDe();
    registerPartitionFunctionSerDe();
  }

  std::vector<RowVectorPtr> makeVectors(
      int32_t count,
      int32_t rowsPerVector,
      const RowTypePtr& rowType = nullptr) {
    auto inputs = rowType ? rowType : rowType_;
    return HiveConnectorTestBase::makeVectors(inputs, count, rowsPerVector);
  }

  core::PlanNodePtr tableScanNode() {
    return tableScanNode(rowType_);
  }

  core::PlanNodePtr tableScanNode(const RowTypePtr& outputType) {
    return PlanBuilder(pool_.get())
        .tableScan(outputType)
        .capturePlanNodeId(traceNodeId_)
        .planNode();
  }

  const RowTypePtr rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6"},
          {BIGINT(),
           INTEGER(),
           SMALLINT(),
           REAL(),
           DOUBLE(),
           VARCHAR(),
           TINYINT()})};
  core::PlanNodeId traceNodeId_;
};

TEST_F(TableScanReplayerTest, runner) {
  const auto vectors = makeVectors(10, 100);
  const auto testDir = TempDirectoryPath::create();
  const auto traceRoot = fmt::format("{}/{}", testDir->getPath(), "traceRoot");
  const auto fs = filesystems::getFileSystem(testDir->getPath(), nullptr);
  const int numSplits{5};
  std::vector<std::shared_ptr<TempFilePath>> splitFiles;
  for (int i = 0; i < numSplits; ++i) {
    auto filePath = TempFilePath::create();
    writeToFile(filePath->getPath(), vectors);
    splitFiles.push_back(std::move(filePath));
  }

  const auto plan = tableScanNode();
  std::shared_ptr<Task> task;
  auto traceResult =
      AssertQueryBuilder(plan)
          .maxDrivers(1)
          .config(core::QueryConfig::kQueryTraceEnabled, true)
          .config(core::QueryConfig::kQueryTraceDir, traceRoot)
          .config(core::QueryConfig::kQueryTraceMaxBytes, 100UL << 30)
          .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
          .config(core::QueryConfig::kQueryTraceNodeIds, traceNodeId_)
          .splits(makeHiveConnectorSplits(splitFiles))
          .copyResults(pool(), task);

  const auto taskTraceDir =
      exec::trace::getTaskTraceDirectory(traceRoot, *task);
  const auto taskTraceReader =
      exec::trace::TaskTraceMetadataReader(taskTraceDir, pool());
  const auto connectorId = taskTraceReader.connectorId(traceNodeId_);
  ASSERT_EQ("test-hive", connectorId);
  const auto opTraceDir = exec::trace::getOpTraceDirectory(
      taskTraceDir,
      traceNodeId_,
      /*pipelineId=*/0,
      /*driverId=*/0);
  const auto summary =
      exec::trace::OperatorTraceSummaryReader(opTraceDir, pool()).read();
  ASSERT_EQ(summary.opType, "TableScan");
  ASSERT_GT(summary.peakMemory, 0);
  const int expectedTotalRows = numSplits * 10 * 100;
  ASSERT_EQ(summary.inputRows, expectedTotalRows);
  ASSERT_GT(summary.inputBytes, 0);
  ASSERT_EQ(summary.rawInputRows, expectedTotalRows);
  ASSERT_GT(summary.rawInputBytes, 0);
  ASSERT_EQ(summary.numSplits.value(), numSplits);

  FLAGS_root_dir = traceRoot;
  FLAGS_query_id = task->queryCtx()->queryId();
  FLAGS_task_id = task->taskId();
  FLAGS_node_id = traceNodeId_;
  FLAGS_summary = true;
  {
    TraceReplayRunner runner;
    runner.init();
    runner.run();
  }

  FLAGS_task_id = task->taskId();
  FLAGS_driver_ids = "";
  FLAGS_summary = false;
  {
    TraceReplayRunner runner;
    runner.init();
    runner.run();
  }
}

TEST_F(TableScanReplayerTest, basic) {
  const auto vectors = makeVectors(10, 100);
  const auto testDir = TempDirectoryPath::create();
  const auto traceRoot = fmt::format("{}/{}", testDir->getPath(), "traceRoot");
  const auto fs = filesystems::getFileSystem(testDir->getPath(), nullptr);
  std::vector<std::shared_ptr<TempFilePath>> splitFiles;
  for (int i = 0; i < 5; ++i) {
    auto filePath = TempFilePath::create();
    writeToFile(filePath->getPath(), vectors);
    splitFiles.push_back(std::move(filePath));
  }

  const auto plan = tableScanNode();
  auto results = AssertQueryBuilder(plan)
                     .splits(makeHiveConnectorSplits(splitFiles))
                     .copyResults(pool());

  std::shared_ptr<Task> task;
  auto traceResult =
      AssertQueryBuilder(plan)
          .maxDrivers(4)
          .config(core::QueryConfig::kQueryTraceEnabled, true)
          .config(core::QueryConfig::kQueryTraceDir, traceRoot)
          .config(core::QueryConfig::kQueryTraceMaxBytes, 100UL << 30)
          .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
          .config(core::QueryConfig::kQueryTraceNodeIds, traceNodeId_)
          .splits(makeHiveConnectorSplits(splitFiles))
          .copyResults(pool(), task);

  assertEqualResults({results}, {traceResult});

  const auto taskId = task->taskId();
  const auto replayingResult = TableScanReplayer(
                                   traceRoot,
                                   task->queryCtx()->queryId(),
                                   task->taskId(),
                                   traceNodeId_,
                                   "TableScan",
                                   "",
                                   0,
                                   executor_.get())
                                   .run();
  assertEqualResults({results}, {replayingResult});

  const auto replayingResult1 = TableScanReplayer(
                                    traceRoot,
                                    task->queryCtx()->queryId(),
                                    task->taskId(),
                                    traceNodeId_,
                                    "TableScan",
                                    "0,2",
                                    0,
                                    executor_.get())
                                    .run();
  const auto replayingResult2 = TableScanReplayer(
                                    traceRoot,
                                    task->queryCtx()->queryId(),
                                    task->taskId(),
                                    traceNodeId_,
                                    "TableScan",
                                    "1,3",
                                    0,
                                    executor_.get())
                                    .run();
  assertEqualResults({results}, {replayingResult1, replayingResult2});
}

TEST_F(TableScanReplayerTest, columnPrunning) {
  const auto vectors = makeVectors(10, 100);
  const auto testDir = TempDirectoryPath::create();
  const auto traceRoot = fmt::format("{}/{}", testDir->getPath(), "traceRoot");
  const auto fs = filesystems::getFileSystem(testDir->getPath(), nullptr);
  std::vector<std::shared_ptr<TempFilePath>> splitFiles;
  for (int i = 0; i < 5; ++i) {
    auto filePath = TempFilePath::create();
    writeToFile(filePath->getPath(), vectors);
    splitFiles.push_back(std::move(filePath));
  }

  const auto plan =
      tableScanNode(ROW({"c0", "c3", "c5"}, {BIGINT(), REAL(), VARCHAR()}));

  const auto results = AssertQueryBuilder(plan)
                           .splits(makeHiveConnectorSplits(splitFiles))
                           .copyResults(pool());

  std::shared_ptr<Task> task;
  auto traceResult =
      AssertQueryBuilder(plan)
          .maxDrivers(4)
          .config(core::QueryConfig::kQueryTraceEnabled, true)
          .config(core::QueryConfig::kQueryTraceDir, traceRoot)
          .config(core::QueryConfig::kQueryTraceMaxBytes, 100UL << 30)
          .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
          .config(core::QueryConfig::kQueryTraceNodeIds, traceNodeId_)
          .splits(makeHiveConnectorSplits(splitFiles))
          .copyResults(pool(), task);

  assertEqualResults({results}, {traceResult});

  const auto taskId = task->taskId();
  const auto replayingResult = TableScanReplayer(
                                   traceRoot,
                                   task->queryCtx()->queryId(),
                                   task->taskId(),
                                   traceNodeId_,
                                   "TableScan",
                                   "",
                                   0,
                                   executor_.get())
                                   .run();
  assertEqualResults({results}, {replayingResult});
}

TEST_F(TableScanReplayerTest, subfieldPrunning) {
  // rowType: ROW
  // └── "e": ROW
  //     ├── "c": ROW
  //     │   ├── "a": BIGINT
  //     │   └── "b": DOUBLE
  //     └── "d": BIGINT
  auto innerType = ROW({"a", "b"}, {BIGINT(), DOUBLE()});
  auto columnType = ROW({"c", "d"}, {innerType, BIGINT()});
  auto rowType = ROW({"e"}, {columnType});
  auto vectors = makeVectors(10, 1'000, rowType);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->getPath(), vectors);
  std::vector<common::Subfield> requiredSubfields;
  requiredSubfields.emplace_back("e.c");
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;
  assignments["e"] = std::make_shared<HiveColumnHandle>(
      "e",
      HiveColumnHandle::ColumnType::kRegular,
      columnType,
      columnType,
      std::move(requiredSubfields));
  const auto plan = PlanBuilder()
                        .startTableScan()
                        .outputType(rowType)
                        .assignments(assignments)
                        .endTableScan()
                        .capturePlanNodeId(traceNodeId_)
                        .planNode();
  const auto split = makeHiveConnectorSplit(filePath->getPath());
  const auto results =
      AssertQueryBuilder(plan).split(split).copyResults(pool());

  const auto testDir = TempDirectoryPath::create();
  const auto traceRoot = fmt::format("{}/{}", testDir->getPath(), "traceRoot");
  std::shared_ptr<Task> task;
  auto traceResult =
      AssertQueryBuilder(plan)
          .config(core::QueryConfig::kQueryTraceEnabled, true)
          .config(core::QueryConfig::kQueryTraceDir, traceRoot)
          .config(core::QueryConfig::kQueryTraceMaxBytes, 100UL << 30)
          .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
          .config(core::QueryConfig::kQueryTraceNodeIds, traceNodeId_)
          .split(split)
          .copyResults(pool(), task);

  assertEqualResults({results}, {traceResult});

  const auto taskId = task->taskId();
  const auto replayingResult = TableScanReplayer(
                                   traceRoot,
                                   task->queryCtx()->queryId(),
                                   task->taskId(),
                                   traceNodeId_,
                                   "TableScan",
                                   "",
                                   0,
                                   executor_.get())
                                   .run();
  assertEqualResults({results}, {replayingResult});
}

TEST_F(TableScanReplayerTest, concurrent) {
  const auto vectors = makeVectors(2, 10);
  const auto testDir = TempDirectoryPath::create();
  const auto traceRoot = fmt::format("{}/{}", testDir->getPath(), "traceRoot");
  const auto fs = filesystems::getFileSystem(testDir->getPath(), nullptr);
  std::vector<std::shared_ptr<TempFilePath>> splitFiles;
  for (int i = 0; i < 2; ++i) {
    auto filePath = TempFilePath::create();
    writeToFile(filePath->getPath(), vectors);
    splitFiles.push_back(std::move(filePath));
  }

  const auto plan = tableScanNode();
  std::shared_ptr<Task> task;
  auto traceResult =
      AssertQueryBuilder(plan)
          .maxDrivers(4)
          .config(core::QueryConfig::kQueryTraceEnabled, true)
          .config(core::QueryConfig::kQueryTraceDir, traceRoot)
          .config(core::QueryConfig::kQueryTraceMaxBytes, 100UL << 30)
          .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
          .config(core::QueryConfig::kQueryTraceNodeIds, traceNodeId_)
          .splits(makeHiveConnectorSplits(splitFiles))
          .copyResults(pool(), task);

  const auto taskId = task->taskId();
  std::vector<std::thread> threads;
  threads.reserve(8);
  for (int i = 0; i < 8; ++i) {
    threads.emplace_back([&]() {
      const auto replayingResult = TableScanReplayer(
                                       traceRoot,
                                       task->queryCtx()->queryId(),
                                       task->taskId(),
                                       traceNodeId_,
                                       "TableScan",
                                       "",
                                       0,
                                       executor_.get())
                                       .run();
      assertEqualResults({traceResult}, {replayingResult});
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
}
} // namespace facebook::velox::tool::trace::test
