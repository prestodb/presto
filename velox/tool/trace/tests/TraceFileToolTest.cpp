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

#include "velox/common/file/FileSystems.h"

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
#include "velox/tool/trace/TraceFileToolRunner.h"
#include "velox/tool/trace/TraceReplayRunner.h"

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
class TraceFileToolTest : public HiveConnectorTestBase {
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

TEST_F(TraceFileToolTest, basic) {
  const auto planWithSplits = createPlan(
      tableDir_,
      core::JoinType::kInner,
      probeKeys_,
      buildKeys_,
      probeInput_,
      buildInput_);
  const auto traceRoot =
      fmt::format("{}/{}/traceRoot", testDir_->getPath(), "basic");
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
  const auto result = traceBuilder.copyResults(pool(), task);
  const auto queryId = task->queryCtx()->queryId();
  const auto taskId = task->taskId();

  struct {
    std::string traceQueryId;
    std::string traceTaskId;

    std::string debugString() const {
      return fmt::format("queryId={}, taskId={}", traceQueryId, traceTaskId);
    }
  } testSettings[]{
      {"", ""},
      {queryId, ""},
      {queryId, taskId},
      {"", taskId},
  };

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    const auto testDir = TempDirectoryPath::create();
    const auto destRoot = testDir->getPath();
    FLAGS_source_root_dir = traceRoot;
    FLAGS_dest_root_dir = destRoot;
    FLAGS_trace_query_id = testData.traceQueryId;
    FLAGS_trace_task_id = testData.traceTaskId;
    FLAGS_trace_file_op = "copy";
    TraceFileToolRunner runner;
    if (FLAGS_trace_query_id.empty() && !FLAGS_trace_task_id.empty()) {
      VELOX_ASSERT_THROW(
          runner.init(),
          "Trace query ID is empty but trace task ID is not empty");
      continue;
    }
    runner.init();
    runner.run();

    const auto replayingResult = HashJoinReplayer(
                                     destRoot,
                                     queryId,
                                     taskId,
                                     traceNodeId_,
                                     "HashJoin",
                                     "",
                                     0,
                                     executor_.get())
                                     .run();
    assertEqualResults({result}, {replayingResult});
  }
}
} // namespace facebook::velox::tool::trace::test
