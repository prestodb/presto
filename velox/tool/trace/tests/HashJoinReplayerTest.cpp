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

#include <folly/experimental/EventCount.h>

#include "velox/common/file/FileSystems.h"
#include "velox/common/file/Utils.h"
#include "velox/common/file/tests/FaultyFileSystem.h"
#include "velox/common/hyperloglog/SparseHll.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/exec/OperatorTraceReader.h"
#include "velox/exec/PartitionFunction.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/TableWriter.h"
#include "velox/exec/TraceUtil.h"
#include "velox/exec/tests/utils/ArbitratorTestUtil.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/tool/trace/HashJoinReplayer.h"
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
using namespace facebook::velox::tests::utils;

namespace facebook::velox::tool::trace::test {
class HashJoinReplayerTest : public HiveConnectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
    HiveConnectorTestBase::SetUpTestCase();
    registerFaultyFileSystem();
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
    probeInput_.clear();
    buildInput_.clear();
    HiveConnectorTestBase::TearDown();
  }

  struct PlanWithSplits {
    core::PlanNodePtr plan;
    core::PlanNodeId probeScanId;
    core::PlanNodeId buildScanId;
    std::unordered_map<core::PlanNodeId, std::vector<exec::Split>> splits;

    explicit PlanWithSplits(
        const core::PlanNodePtr& _plan,
        const core::PlanNodeId& _probeScanId = "",
        const core::PlanNodeId& _buildScanId = "",
        const std::unordered_map<
            core::PlanNodeId,
            std::vector<velox::exec::Split>>& _splits = {})
        : plan(_plan),
          probeScanId(_probeScanId),
          buildScanId(_buildScanId),
          splits(_splits) {}
  };

  RowTypePtr concat(const RowTypePtr& a, const RowTypePtr& b) {
    std::vector<std::string> names = a->names();
    std::vector<TypePtr> types = a->children();

    for (auto i = 0; i < b->size(); ++i) {
      names.push_back(b->nameOf(i));
      types.push_back(b->childAt(i));
    }

    return ROW(std::move(names), std::move(types));
  }

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

  PlanWithSplits createPlan(
      const std::string& tableDir,
      core::JoinType joinType,
      const std::vector<std::string>& probeKeys,
      const std::vector<std::string>& buildKeys,
      const std::vector<RowVectorPtr>& probeInput,
      const std::vector<RowVectorPtr>& buildInput) {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    const std::vector<Split> probeSplits =
        makeSplits(probeInput, fmt::format("{}/probe", tableDir), pool());
    const std::vector<Split> buildSplits =
        makeSplits(buildInput, fmt::format("{}/build", tableDir), pool());
    core::PlanNodeId probeScanId;
    core::PlanNodeId buildScanId;
    const auto outputColumns = concat(
                                   asRowType(probeInput_[0]->type()),
                                   asRowType(buildInput_[0]->type()))
                                   ->names();
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .tableScan(probeType_)
                    .capturePlanNodeId(probeScanId)
                    .hashJoin(
                        probeKeys,
                        buildKeys,
                        PlanBuilder(planNodeIdGenerator)
                            .tableScan(buildType_)
                            .capturePlanNodeId(buildScanId)
                            .planNode(),
                        /*filter=*/"",
                        outputColumns,
                        joinType,
                        false)
                    .capturePlanNodeId(traceNodeId_)
                    .planNode();
    return PlanWithSplits{
        plan,
        probeScanId,
        buildScanId,
        {{probeScanId, probeSplits}, {buildScanId, buildSplits}}};
  }

  core::PlanNodeId traceNodeId_;
  RowTypePtr probeType_{
      ROW({"t0", "t1", "t2", "t3"}, {BIGINT(), VARCHAR(), SMALLINT(), REAL()})};

  RowTypePtr buildType_{
      ROW({"u0", "u1", "u2", "u3"},
          {BIGINT(), INTEGER(), SMALLINT(), VARCHAR()})};
  std::vector<RowVectorPtr> probeInput_ = makeVectors(5, 100, probeType_);
  std::vector<RowVectorPtr> buildInput_ = makeVectors(3, 100, buildType_);

  const std::vector<std::string> probeKeys_{"t0"};
  const std::vector<std::string> buildKeys_{"u0"};
  const std::shared_ptr<TempDirectoryPath> testDir_ =
      TempDirectoryPath::create();
  const std::string tableDir_ =
      fmt::format("{}/{}", testDir_->getPath(), "table");
};

TEST_F(HashJoinReplayerTest, basic) {
  const auto planWithSplits = createPlan(
      tableDir_,
      core::JoinType::kInner,
      probeKeys_,
      buildKeys_,
      probeInput_,
      buildInput_);
  AssertQueryBuilder builder(planWithSplits.plan);
  for (const auto& [planNodeId, nodeSplits] : planWithSplits.splits) {
    builder.splits(planNodeId, nodeSplits);
  }
  const auto result = builder.copyResults(pool());

  const auto traceRoot =
      fmt::format("{}/{}/traceRoot/", testDir_->getPath(), "basic");
  std::shared_ptr<Task> task;
  auto tracePlanWithSplits = createPlan(
      tableDir_,
      core::JoinType::kInner,
      probeKeys_,
      buildKeys_,
      probeInput_,
      buildInput_);
  AssertQueryBuilder traceBuilder(tracePlanWithSplits.plan);
  traceBuilder.maxDrivers(4)
      .config(core::QueryConfig::kQueryTraceEnabled, true)
      .config(core::QueryConfig::kQueryTraceDir, traceRoot)
      .config(core::QueryConfig::kQueryTraceMaxBytes, 100UL << 30)
      .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
      .config(core::QueryConfig::kQueryTraceNodeIds, traceNodeId_);
  for (const auto& [planNodeId, nodeSplits] : tracePlanWithSplits.splits) {
    traceBuilder.splits(planNodeId, nodeSplits);
  }
  auto traceResult = traceBuilder.copyResults(pool(), task);

  assertEqualResults({result}, {traceResult});

  const auto taskId = task->taskId();
  const auto replayingResult = HashJoinReplayer(
                                   traceRoot,
                                   task->queryCtx()->queryId(),
                                   task->taskId(),
                                   traceNodeId_,
                                   "HashJoin",
                                   "",
                                   0,
                                   executor_.get())
                                   .run();
  assertEqualResults({result}, {replayingResult});
}

TEST_F(HashJoinReplayerTest, partialDriverIds) {
  const std::shared_ptr<TempDirectoryPath> testDir =
      TempDirectoryPath::create(true);
  const std::string tableDir =
      fmt::format("{}/{}", testDir->getPath(), "table");
  const auto planWithSplits = createPlan(
      tableDir,
      core::JoinType::kInner,
      probeKeys_,
      buildKeys_,
      probeInput_,
      buildInput_);
  AssertQueryBuilder builder(planWithSplits.plan);
  for (const auto& [planNodeId, nodeSplits] : planWithSplits.splits) {
    builder.splits(planNodeId, nodeSplits);
  }
  const auto result = builder.copyResults(pool());

  const auto traceRoot =
      fmt::format("{}/{}/traceRoot/", testDir->getPath(), "basic");
  std::shared_ptr<Task> task;
  auto tracePlanWithSplits = createPlan(
      tableDir,
      core::JoinType::kInner,
      probeKeys_,
      buildKeys_,
      probeInput_,
      buildInput_);
  AssertQueryBuilder traceBuilder(tracePlanWithSplits.plan);
  traceBuilder.maxDrivers(4)
      .config(core::QueryConfig::kQueryTraceEnabled, true)
      .config(core::QueryConfig::kQueryTraceDir, traceRoot)
      .config(core::QueryConfig::kQueryTraceMaxBytes, 100UL << 30)
      .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
      .config(core::QueryConfig::kQueryTraceNodeIds, traceNodeId_);
  for (const auto& [planNodeId, nodeSplits] : tracePlanWithSplits.splits) {
    traceBuilder.splits(planNodeId, nodeSplits);
  }
  auto traceResult = traceBuilder.copyResults(pool(), task);

  assertEqualResults({result}, {traceResult});

  const auto taskId = task->taskId();
  const auto taskTraceDir =
      exec::trace::getTaskTraceDirectory(traceRoot, *task);
  const auto opTraceDir =
      exec::trace::getOpTraceDirectory(taskTraceDir, traceNodeId_, 0, 0);
  const auto opTraceDataFile = exec::trace::getOpTraceInputFilePath(opTraceDir);
  auto faultyFs = faultyFileSystem();
  faultyFs->setFileInjectionHook([&](FaultFileOperation* op) {
    if ((op->type == FaultFileOperation::Type::kRead ||
         op->type == FaultFileOperation::Type::kReadv) &&
        op->path == opTraceDataFile) {
      VELOX_FAIL("Read wrong data file {}", opTraceDataFile);
    }
  });

  if (faultyFs->openFileForRead(opTraceDataFile)->size() > 0) {
    VELOX_ASSERT_THROW(
        HashJoinReplayer(
            traceRoot,
            task->queryCtx()->queryId(),
            task->taskId(),
            traceNodeId_,
            "HashJoin",
            "0",
            0,
            executor_.get())
            .run(),
        "Read wrong data file");
  }
  HashJoinReplayer(
      traceRoot,
      task->queryCtx()->queryId(),
      task->taskId(),
      traceNodeId_,
      "HashJoin",
      "1,3",
      0,
      executor_.get())
      .run();
  faultyFs->clearFileFaultInjections();
}

TEST_F(HashJoinReplayerTest, runner) {
  const auto testDir = TempDirectoryPath::create();
  const auto traceRoot = fmt::format("{}/{}", testDir->getPath(), "traceRoot");
  std::shared_ptr<Task> task;
  auto tracePlanWithSplits = createPlan(
      tableDir_,
      core::JoinType::kInner,
      probeKeys_,
      buildKeys_,
      probeInput_,
      buildInput_);
  AssertQueryBuilder traceBuilder(tracePlanWithSplits.plan);
  traceBuilder.config(core::QueryConfig::kQueryTraceEnabled, true)
      .config(core::QueryConfig::kQueryTraceDir, traceRoot)
      .config(core::QueryConfig::kQueryTraceMaxBytes, 100UL << 30)
      .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
      .config(core::QueryConfig::kQueryTraceNodeIds, traceNodeId_);
  for (const auto& [planNodeId, nodeSplits] : tracePlanWithSplits.splits) {
    traceBuilder.splits(planNodeId, nodeSplits);
  }
  auto traceResult = traceBuilder.copyResults(pool(), task);

  const auto taskTraceDir =
      exec::trace::getTaskTraceDirectory(traceRoot, *task);
  const auto probeOperatorTraceDir = exec::trace::getOpTraceDirectory(
      taskTraceDir,
      traceNodeId_,
      /*pipelineId=*/0,
      /*driverId=*/0);
  const auto probeSummary =
      exec::trace::OperatorTraceSummaryReader(probeOperatorTraceDir, pool())
          .read();
  ASSERT_EQ(probeSummary.opType, "HashProbe");
  ASSERT_GT(probeSummary.peakMemory, 0);
  ASSERT_GT(probeSummary.inputRows, 0);
  ASSERT_GT(probeSummary.inputBytes, 0);
  ASSERT_EQ(probeSummary.rawInputRows, 0);
  ASSERT_EQ(probeSummary.rawInputBytes, 0);

  const auto buildOperatorTraceDir = exec::trace::getOpTraceDirectory(
      taskTraceDir,
      traceNodeId_,
      /*pipelineId=*/1,
      /*driverId=*/0);
  const auto buildSummary =
      exec::trace::OperatorTraceSummaryReader(buildOperatorTraceDir, pool())
          .read();
  ASSERT_EQ(buildSummary.opType, "HashBuild");
  ASSERT_GT(buildSummary.peakMemory, 0);
  ASSERT_GT(buildSummary.inputRows, 0);
  // NOTE: the input bytes is 0 because of the lazy materialization.
  ASSERT_EQ(buildSummary.inputBytes, 0);
  ASSERT_EQ(buildSummary.rawInputRows, 0);
  ASSERT_EQ(buildSummary.rawInputBytes, 0);

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

DEBUG_ONLY_TEST_F(HashJoinReplayerTest, hashBuildSpill) {
  const auto planWithSplits = createPlan(
      tableDir_,
      core::JoinType::kInner,
      probeKeys_,
      buildKeys_,
      probeInput_,
      buildInput_);
  AssertQueryBuilder builder(planWithSplits.plan);
  for (const auto& [planNodeId, nodeSplits] : planWithSplits.splits) {
    builder.splits(planNodeId, nodeSplits);
  }
  const auto result = builder.copyResults(pool());

  const auto traceRoot =
      fmt::format("{}/{}/traceRoot/", testDir_->getPath(), "hash_build_spill");
  const auto spillDir =
      fmt::format("{}/{}/spillDir/", testDir_->getPath(), "hash_build_spill");
  std::shared_ptr<Task> task;
  auto tracePlanWithSplits = createPlan(
      tableDir_,
      core::JoinType::kInner,
      probeKeys_,
      buildKeys_,
      probeInput_,
      buildInput_);

  std::atomic_bool injectSpillOnce{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::HashBuild::finishHashBuild",
      std::function<void(Operator*)>([&](Operator* op) {
        if (!injectSpillOnce.exchange(false)) {
          return;
        }
        Operator::ReclaimableSectionGuard guard(op);
        testingRunArbitration(op->pool());
      }));

  AssertQueryBuilder traceBuilder(tracePlanWithSplits.plan);
  traceBuilder.config(core::QueryConfig::kQueryTraceEnabled, true)
      .config(core::QueryConfig::kQueryTraceDir, traceRoot)
      .config(core::QueryConfig::kQueryTraceMaxBytes, 100UL << 30)
      .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
      .config(core::QueryConfig::kQueryTraceNodeIds, traceNodeId_)
      .config(core::QueryConfig::kSpillEnabled, true)
      .config(core::QueryConfig::kJoinSpillEnabled, true)
      .spillDirectory(spillDir);
  for (const auto& [planNodeId, nodeSplits] : tracePlanWithSplits.splits) {
    traceBuilder.splits(planNodeId, nodeSplits);
  }

  auto traceResult = traceBuilder.copyResults(pool(), task);
  auto taskStats = exec::toPlanStats(task->taskStats());
  const auto& stats = taskStats.at(traceNodeId_);
  auto opStats = toOperatorStats(task->taskStats());
  ASSERT_GT(
      opStats.at("HashBuild").runtimeStats[Operator::kSpillWrites].sum, 0);
  ASSERT_GT(stats.spilledBytes, 0);
  ASSERT_GT(stats.spilledRows, 0);
  ASSERT_GT(stats.spilledFiles, 0);
  ASSERT_GT(stats.spilledPartitions, 0);

  assertEqualResults({result}, {traceResult});

  const auto taskId = task->taskId();
  const auto replayingResult = HashJoinReplayer(
                                   traceRoot,
                                   task->queryCtx()->queryId(),
                                   task->taskId(),
                                   traceNodeId_,
                                   "HashJoin",
                                   "",
                                   0,
                                   executor_.get())
                                   .run();
  assertEqualResults({result}, {replayingResult});
}

DEBUG_ONLY_TEST_F(HashJoinReplayerTest, hashProbeSpill) {
  const auto planWithSplits = createPlan(
      tableDir_,
      core::JoinType::kInner,
      probeKeys_,
      buildKeys_,
      probeInput_,
      buildInput_);

  AssertQueryBuilder builder(planWithSplits.plan);
  for (const auto& [planNodeId, nodeSplits] : planWithSplits.splits) {
    builder.splits(planNodeId, nodeSplits);
  }
  const auto result = builder.copyResults(pool());

  const auto traceRoot =
      fmt::format("{}/{}/traceRoot/", testDir_->getPath(), "hash_probe_spill");
  const auto spillDir =
      fmt::format("{}/{}/spillDir/", testDir_->getPath(), "hash_probe_spill");
  std::shared_ptr<Task> task;
  auto tracePlanWithSplits = createPlan(
      tableDir_,
      core::JoinType::kInner,
      probeKeys_,
      buildKeys_,
      probeInput_,
      buildInput_);

  std::atomic_bool injectProbeSpillOnce{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal::getOutput",
      std::function<void(Operator*)>([&](Operator* op) {
        if (!isHashProbeMemoryPool(*op->pool())) {
          return;
        }
        if (!injectProbeSpillOnce.exchange(false)) {
          return;
        }
        testingRunArbitration(op->pool());
      }));

  AssertQueryBuilder traceBuilder(tracePlanWithSplits.plan);
  traceBuilder.config(core::QueryConfig::kQueryTraceEnabled, true)
      .config(core::QueryConfig::kQueryTraceDir, traceRoot)
      .config(core::QueryConfig::kQueryTraceMaxBytes, 100UL << 30)
      .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
      .config(core::QueryConfig::kQueryTraceNodeIds, traceNodeId_)
      .config(core::QueryConfig::kSpillEnabled, true)
      .config(core::QueryConfig::kJoinSpillEnabled, true)
      .spillDirectory(spillDir);
  for (const auto& [planNodeId, nodeSplits] : tracePlanWithSplits.splits) {
    traceBuilder.splits(planNodeId, nodeSplits);
  }

  auto traceResult = traceBuilder.copyResults(pool(), task);
  auto taskStats = exec::toPlanStats(task->taskStats());
  const auto& stats = taskStats.at(traceNodeId_);
  auto opStats = toOperatorStats(task->taskStats());
  ASSERT_GT(
      opStats.at("HashProbe").runtimeStats[Operator::kSpillWrites].sum, 0);

  ASSERT_GT(stats.spilledBytes, 0);
  ASSERT_GT(stats.spilledRows, 0);
  ASSERT_GT(stats.spilledFiles, 0);
  ASSERT_GT(stats.spilledPartitions, 0);

  assertEqualResults({result}, {traceResult});

  const auto taskId = task->taskId();
  const auto replayingResult = HashJoinReplayer(
                                   traceRoot,
                                   task->queryCtx()->queryId(),
                                   task->taskId(),
                                   traceNodeId_,
                                   "HashJoin",
                                   "",
                                   0,
                                   executor_.get())
                                   .run();
  assertEqualResults({result}, {replayingResult});
}
} // namespace facebook::velox::tool::trace::test
