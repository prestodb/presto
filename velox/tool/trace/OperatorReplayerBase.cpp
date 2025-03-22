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

#include <folly/json.h>

#include <utility>

#include "velox/core/PlanNode.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/TaskTraceReader.h"
#include "velox/exec/TraceUtil.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/tool/trace/OperatorReplayerBase.h"

#include "velox/tool/trace/TraceReplayTaskRunner.h"

using namespace facebook::velox;

namespace facebook::velox::tool::trace {
OperatorReplayerBase::OperatorReplayerBase(
    std::string traceDir,
    std::string queryId,
    std::string taskId,
    std::string nodeId,
    std::string operatorType,
    const std::string& driverIds,
    uint64_t queryCapacity,
    folly::Executor* executor)
    : queryId_(std::string(std::move(queryId))),
      taskId_(std::move(taskId)),
      nodeId_(std::move(nodeId)),
      operatorType_(std::move(operatorType)),
      taskTraceDir_(
          exec::trace::getTaskTraceDirectory(traceDir, queryId_, taskId_)),
      nodeTraceDir_(exec::trace::getNodeTraceDirectory(taskTraceDir_, nodeId_)),
      fs_(filesystems::getFileSystem(taskTraceDir_, nullptr)),
      pipelineIds_(exec::trace::listPipelineIds(nodeTraceDir_, fs_)),
      driverIds_(
          driverIds.empty() ? exec::trace::listDriverIds(
                                  nodeTraceDir_,
                                  pipelineIds_.front(),
                                  fs_)
                            : exec::trace::extractDriverIds(driverIds)),
      queryCapacity_(queryCapacity == 0 ? memory::kMaxMemory : queryCapacity),
      executor_(executor) {
  VELOX_USER_CHECK(!taskTraceDir_.empty());
  VELOX_USER_CHECK(!taskId_.empty());
  VELOX_USER_CHECK(!nodeId_.empty());
  VELOX_USER_CHECK(!operatorType_.empty());
  if (operatorType_ == "HashJoin") {
    VELOX_USER_CHECK_EQ(pipelineIds_.size(), 2);
  } else {
    VELOX_USER_CHECK_EQ(pipelineIds_.size(), 1);
  }
  VELOX_CHECK_NOT_NULL(executor_);

  const auto taskMetaReader = exec::trace::TaskTraceMetadataReader(
      taskTraceDir_, memory::MemoryManager::getInstance()->tracePool());
  queryConfigs_ = taskMetaReader.queryConfigs();
  connectorConfigs_ = taskMetaReader.connectorProperties();
  planFragment_ = taskMetaReader.queryPlan();
  queryConfigs_[core::QueryConfig::kQueryTraceEnabled] = "false";
}

RowVectorPtr OperatorReplayerBase::run(bool copyResults) {
  auto queryCtx = createQueryCtx();
  std::shared_ptr<exec::test::TempDirectoryPath> spillDirectory;
  if (queryCtx->queryConfig().spillEnabled()) {
    spillDirectory = exec::test::TempDirectoryPath::create();
  }

  TraceReplayTaskRunner traceTaskRunner(createPlan(), std::move(queryCtx));
  auto [task, result] =
      traceTaskRunner.maxDrivers(driverIds_.size())
          .spillDirectory(spillDirectory ? spillDirectory->getPath() : "")
          .run(copyResults);
  printStats(task);
  return result;
}

core::PlanNodePtr OperatorReplayerBase::createPlan() {
  const auto* replayNode = core::PlanNode::findFirstNode(
      planFragment_.get(),
      [this](const core::PlanNode* node) { return node->id() == nodeId_; });

  if (replayNode->name() == "TableScan") {
    return exec::test::PlanBuilder()
        .addNode(replayNodeFactory(replayNode))
        .capturePlanNodeId(replayPlanNodeId_)
        .planNode();
  }

  return exec::test::PlanBuilder(planNodeIdGenerator_)
      .traceScan(
          nodeTraceDir_,
          pipelineIds_.front(),
          driverIds_,
          exec::trace::getDataType(planFragment_, nodeId_))
      .addNode(replayNodeFactory(replayNode))
      .capturePlanNodeId(replayPlanNodeId_)
      .planNode();
}

std::shared_ptr<core::QueryCtx> OperatorReplayerBase::createQueryCtx() {
  static std::atomic_uint64_t replayQueryId{0};
  auto queryPool = memory::memoryManager()->addRootPool(
      fmt::format("{}_replayer_{}", operatorType_, replayQueryId++),
      queryCapacity_);
  std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>
      connectorConfigs;
  for (auto& [connectorId, configs] : connectorConfigs_) {
    connectorConfigs.emplace(
        connectorId, std::make_shared<config::ConfigBase>(std::move(configs)));
  }
  return core::QueryCtx::create(
      executor_,
      core::QueryConfig{queryConfigs_},
      std::move(connectorConfigs),
      nullptr,
      std::move(queryPool),
      executor_);
}

std::function<core::PlanNodePtr(std::string, core::PlanNodePtr)>
OperatorReplayerBase::replayNodeFactory(const core::PlanNode* node) const {
  return [=](const core::PlanNodeId& nodeId,
             const core::PlanNodePtr& source) -> core::PlanNodePtr {
    return createPlanNode(node, nodeId, source);
  };
}

void OperatorReplayerBase::printStats(
    const std::shared_ptr<exec::Task>& task) const {
  const auto planStats = exec::toPlanStats(task->taskStats());
  const auto& stats = planStats.at(replayPlanNodeId_);
  for (const auto& [name, operatorStats] : stats.operatorStats) {
    LOG(INFO) << "Stats of replaying operator " << name << " : "
              << operatorStats->toString();
  }
  LOG(INFO) << "Memory usage: " << task->pool()->treeMemoryUsage(false);
}
} // namespace facebook::velox::tool::trace
