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

#include <boost/random/uniform_int_distribution.hpp>
#include <gtest/gtest.h>

#include <string>
#include <utility>

#include <folly/experimental/EventCount.h>

#include "velox/common/file/FileSystems.h"
#include "velox/common/hyperloglog/SparseHll.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/exec/PartitionFunction.h"
#include "velox/exec/TableWriter.h"
#include "velox/exec/TraceUtil.h"
#include "velox/exec/tests/utils/ArbitratorTestUtil.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/tool/trace/FilterProjectReplayer.h"

#include "velox/common/file/Utils.h"
#include "velox/exec/PlanNodeStats.h"

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
class FilterProjectReplayerTest : public HiveConnectorTestBase {
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

  void TearDown() override {
    input_.clear();
    HiveConnectorTestBase::TearDown();
  }

  struct PlanWithSplits {
    core::PlanNodePtr plan;
    std::vector<velox::exec::Split> splits;

    explicit PlanWithSplits(
        core::PlanNodePtr _plan,
        std::vector<velox::exec::Split> _splits = {})
        : plan(std::move(_plan)), splits(std::move(_splits)) {}
  };

  std::vector<RowVectorPtr>
  makeVectors(int32_t count, int32_t rowsPerVector, const RowTypePtr& rowType) {
    return HiveConnectorTestBase::makeVectors(rowType, count, rowsPerVector);
  }

  std::vector<Split> makeSplits(
      const std::vector<RowVectorPtr>& inputs,
      const std::string& path,
      memory::MemoryPool* writerPool) {
    std::vector<Split> splits;
    for (auto i = 0; i < 4; ++i) {
      const std::string filePath = fmt::format("{}/{}", path, i);
      writeToFile(filePath, inputs);
      splits.emplace_back(makeHiveConnectorSplit(filePath));
    }

    return splits;
  }

  enum class PlanMode {
    FilterProject = 0,
    FilterOnly = 1,
    ProjectOnly = 2,
  };

  PlanWithSplits createPlan(PlanMode planMode) {
    core::PlanNodePtr plan = nullptr;
    if (planMode == PlanMode::FilterProject) {
      plan = PlanBuilder()
                 .tableScan(inputType_)
                 .filter("c0 % 10 < 9")
                 .capturePlanNodeId(filterNodeId_)
                 .project({"c0", "c1", "c0 % 100 + c1 % 50 AS e1"})
                 .capturePlanNodeId(projectNodeId_)
                 .planNode();
    } else if (planMode == PlanMode::FilterOnly) {
      plan = PlanBuilder()
                 .tableScan(inputType_)
                 .filter("c0 % 10 < 9")
                 .capturePlanNodeId(filterNodeId_)
                 .planNode();
    } else {
      plan = PlanBuilder()
                 .tableScan(inputType_)
                 .project({"c0", "c1", "c0 % 100 + c1 % 50 AS e1"})
                 .capturePlanNodeId(projectNodeId_)
                 .planNode();
    }
    const std::vector<Split> splits = makeSplits(
        input_, fmt::format("{}/splits", testDir_->getPath()), pool());
    return PlanWithSplits{plan, splits};
  }

  core::PlanNodeId traceNodeId_;
  core::PlanNodeId filterNodeId_;
  core::PlanNodeId projectNodeId_;
  RowTypePtr inputType_{
      ROW({"c0", "c1", "c2", "c3"}, {BIGINT(), INTEGER(), SMALLINT(), REAL()})};
  std::vector<RowVectorPtr> input_ = makeVectors(5, 100, inputType_);

  const std::shared_ptr<TempDirectoryPath> testDir_ =
      TempDirectoryPath::create();
};

TEST_F(FilterProjectReplayerTest, filterProject) {
  const auto planWithSplits = createPlan(PlanMode::FilterProject);
  AssertQueryBuilder builder(planWithSplits.plan);
  const auto result = builder.splits(planWithSplits.splits).copyResults(pool());

  struct {
    uint32_t maxDrivers;

    std::string debugString() const {
      return fmt::format("maxDrivers: {}", maxDrivers);
    }
  } testSettings[]{{1}, {4}, {8}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    const auto traceRoot = fmt::format("{}/{}", testDir_->getPath(), "basic");
    const auto tracePlanWithSplits = createPlan(PlanMode::FilterProject);
    std::shared_ptr<Task> task;
    AssertQueryBuilder traceBuilder(tracePlanWithSplits.plan);
    traceBuilder.maxDrivers(4)
        .config(core::QueryConfig::kQueryTraceEnabled, true)
        .config(core::QueryConfig::kQueryTraceDir, traceRoot)
        .config(core::QueryConfig::kQueryTraceMaxBytes, 100UL << 30)
        .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
        .config(
            core::QueryConfig::kQueryTraceNodeIds,
            fmt::format("{},{}", filterNodeId_, projectNodeId_));
    auto traceResult = traceBuilder.splits(tracePlanWithSplits.splits)
                           .copyResults(pool(), task);

    assertEqualResults({result}, {traceResult});

    const auto taskId = task->taskId();
    auto replayingResult = FilterProjectReplayer(
                               traceRoot,
                               task->queryCtx()->queryId(),
                               task->taskId(),
                               projectNodeId_,
                               "FilterProject",
                               "",
                               0,
                               executor_.get())
                               .run();
    assertEqualResults({result}, {replayingResult});
  }
}

TEST_F(FilterProjectReplayerTest, filterOnly) {
  const auto planWithSplits = createPlan(PlanMode::FilterOnly);
  AssertQueryBuilder builder(planWithSplits.plan);
  const auto result = builder.splits(planWithSplits.splits).copyResults(pool());

  const auto traceRoot = fmt::format("{}/{}", testDir_->getPath(), "filter");
  const auto tracePlanWithSplits = createPlan(PlanMode::FilterOnly);
  std::shared_ptr<Task> task;
  AssertQueryBuilder traceBuilder(tracePlanWithSplits.plan);
  traceBuilder.maxDrivers(4)
      .config(core::QueryConfig::kQueryTraceEnabled, true)
      .config(core::QueryConfig::kQueryTraceDir, traceRoot)
      .config(core::QueryConfig::kQueryTraceMaxBytes, 100UL << 30)
      .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
      .config(
          core::QueryConfig::kQueryTraceNodeIds,
          fmt::format("{},{}", filterNodeId_, projectNodeId_));
  auto traceResult =
      traceBuilder.splits(tracePlanWithSplits.splits).copyResults(pool(), task);

  assertEqualResults({result}, {traceResult});

  const auto taskId = task->taskId();
  auto replayingResult = FilterProjectReplayer(
                             traceRoot,
                             task->queryCtx()->queryId(),
                             task->taskId(),
                             filterNodeId_,
                             "FilterProject",
                             "",
                             0,
                             executor_.get())
                             .run();
  assertEqualResults({result}, {replayingResult});
}

TEST_F(FilterProjectReplayerTest, projectOnly) {
  const auto planWithSplits = createPlan(PlanMode::ProjectOnly);
  AssertQueryBuilder builder(planWithSplits.plan);
  const auto result = builder.splits(planWithSplits.splits).copyResults(pool());

  const auto traceRoot = fmt::format("{}/{}", testDir_->getPath(), "project");
  const auto tracePlanWithSplits = createPlan(PlanMode::ProjectOnly);
  std::shared_ptr<Task> task;
  AssertQueryBuilder traceBuilder(tracePlanWithSplits.plan);
  traceBuilder.maxDrivers(4)
      .config(core::QueryConfig::kQueryTraceEnabled, true)
      .config(core::QueryConfig::kQueryTraceDir, traceRoot)
      .config(core::QueryConfig::kQueryTraceMaxBytes, 100UL << 30)
      .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
      .config(
          core::QueryConfig::kQueryTraceNodeIds,
          fmt::format("{},{}", filterNodeId_, projectNodeId_));
  auto traceResult =
      traceBuilder.splits(tracePlanWithSplits.splits).copyResults(pool(), task);

  assertEqualResults({result}, {traceResult});

  const auto taskId = task->taskId();
  auto replayingResult = FilterProjectReplayer(
                             traceRoot,
                             task->queryCtx()->queryId(),
                             task->taskId(),
                             projectNodeId_,
                             "FilterProject",
                             "",
                             0,
                             executor_.get())
                             .run();
  assertEqualResults({result}, {replayingResult});

  auto replayingResult1 = FilterProjectReplayer(
                              traceRoot,
                              task->queryCtx()->queryId(),
                              task->taskId(),
                              projectNodeId_,
                              "FilterProject",
                              "0,2",
                              0,
                              executor_.get())
                              .run();
  auto replayingResult2 = FilterProjectReplayer(
                              traceRoot,
                              task->queryCtx()->queryId(),
                              task->taskId(),
                              projectNodeId_,
                              "FilterProject",
                              "1,3",
                              0,
                              executor_.get())
                              .run();
  assertEqualResults({result}, {replayingResult1, replayingResult2});

  const auto taskTraceDir =
      exec::trace::getTaskTraceDirectory(traceRoot, *task);
  const auto opTraceDir =
      exec::trace::getOpTraceDirectory(taskTraceDir, projectNodeId_, 0, 0);
  const auto opTraceDataFile = exec::trace::getOpTraceInputFilePath(opTraceDir);
  auto fs = filesystems::getFileSystem(opTraceDataFile, nullptr);
  auto file = fs->openFileForWrite(
      opTraceDataFile,
      filesystems::FileOptions{
          .values = {},
          .fileSize = std::nullopt,
          .shouldThrowOnFileAlreadyExists = false});
  file->truncate(0);
  file->close();
  auto emptyResult = FilterProjectReplayer(
                         traceRoot,
                         task->queryCtx()->queryId(),
                         task->taskId(),
                         projectNodeId_,
                         "FilterProject",
                         "0",
                         0,
                         executor_.get())
                         .run();
  ASSERT_EQ(emptyResult->size(), 0);
}
} // namespace facebook::velox::tool::trace::test
