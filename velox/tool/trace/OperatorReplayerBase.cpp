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

#include "velox/common/serialization/Serializable.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/QueryMetadataReader.h"
#include "velox/exec/QueryTraceTraits.h"
#include "velox/exec/QueryTraceUtil.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/tool/trace/OperatorReplayerBase.h"

using namespace facebook::velox;

namespace facebook::velox::tool::trace {
OperatorReplayerBase::OperatorReplayerBase(
    std::string rootDir,
    std::string taskId,
    std::string nodeId,
    int32_t pipelineId,
    std::string operatorType)
    : rootDir_(std::move(rootDir)),
      taskId_(std::move(taskId)),
      nodeId_(std::move(nodeId)),
      pipelineId_(pipelineId),
      operatorType_(std::move(operatorType)) {
  VELOX_USER_CHECK(!rootDir_.empty());
  VELOX_USER_CHECK(!taskId_.empty());
  VELOX_USER_CHECK(!nodeId_.empty());
  VELOX_USER_CHECK_GE(pipelineId_, 0);
  VELOX_USER_CHECK(!operatorType_.empty());
  const auto traceTaskDir = fmt::format("{}/{}", rootDir_, taskId_);
  const auto metadataReader = exec::trace::QueryMetadataReader(
      traceTaskDir, memory::MemoryManager::getInstance()->tracePool());
  metadataReader.read(queryConfigs_, connectorConfigs_, planFragment_);
  queryConfigs_[core::QueryConfig::kQueryTraceEnabled] = "false";
  fs_ = filesystems::getFileSystem(rootDir_, nullptr);
  maxDrivers_ =
      exec::trace::getNumDrivers(rootDir_, taskId_, nodeId_, pipelineId_, fs_);
}

RowVectorPtr OperatorReplayerBase::run() const {
  const auto restoredPlanNode = createPlan();
  return exec::test::AssertQueryBuilder(restoredPlanNode)
      .maxDrivers(maxDrivers_)
      .configs(queryConfigs_)
      .connectorSessionProperties(connectorConfigs_)
      .copyResults(memory::MemoryManager::getInstance()->tracePool());
}

core::PlanNodePtr OperatorReplayerBase::createPlan() const {
  const auto* replayNode = core::PlanNode::findFirstNode(
      planFragment_.get(),
      [this](const core::PlanNode* node) { return node->id() == nodeId_; });
  const auto traceDir = fmt::format("{}/{}", rootDir_, taskId_);
  return exec::test::PlanBuilder()
      .traceScan(
          fmt::format("{}/{}", traceDir, nodeId_),
          exec::trace::getDataType(planFragment_, nodeId_))
      .addNode(addReplayNode(replayNode))
      .planNode();
}

std::function<core::PlanNodePtr(std::string, core::PlanNodePtr)>
OperatorReplayerBase::addReplayNode(const core::PlanNode* node) const {
  return [=](const core::PlanNodeId& nodeId,
             const core::PlanNodePtr& source) -> core::PlanNodePtr {
    return createPlanNode(node, nodeId, source);
  };
}

void OperatorReplayerBase::printSummary(
    const std::string& rootDir,
    const std::string& taskId,
    bool shortSummary) {
  const auto fs = filesystems::getFileSystem(rootDir, nullptr);
  const auto taskIds = exec::trace::getTaskIds(rootDir, fs);
  if (taskIds.empty()) {
    LOG(ERROR) << "No traced query task under " << rootDir;
    return;
  }

  std::ostringstream summary;
  summary << "\n++++++Query trace summary++++++\n";
  summary << "Number of tasks: " << taskIds.size() << "\n";
  summary << "Task ids: " << folly::join(",", taskIds);

  if (shortSummary) {
    LOG(INFO) << summary.str();
    return;
  }

  const auto summaryTaskIds =
      taskId.empty() ? taskIds : std::vector<std::string>{taskId};
  for (const auto& taskId : summaryTaskIds) {
    summary << "\n++++++Query configs and plan of task " << taskId
            << ":++++++\n";
    const auto traceTaskDir = fmt::format("{}/{}", rootDir, taskId);
    const auto queryMetaFile = fmt::format(
        "{}/{}",
        traceTaskDir,
        exec::trace::QueryTraceTraits::kQueryMetaFileName);
    const auto metaObj = exec::trace::getMetadata(queryMetaFile, fs);
    const auto& configObj =
        metaObj[exec::trace::QueryTraceTraits::kQueryConfigKey];
    summary << "++++++Query configs++++++\n";
    summary << folly::toJson(configObj) << "\n";
    summary << "++++++Query plan++++++\n";
    const auto queryPlan = ISerializable::deserialize<core::PlanNode>(
        metaObj[exec::trace::QueryTraceTraits::kPlanNodeKey],
        memory::MemoryManager::getInstance()->tracePool());
    summary << queryPlan->toString(true, true);
  }
  LOG(INFO) << summary.str();
}

std::string OperatorReplayerBase::usage() {
  std::ostringstream usage;
  usage
      << "++++++Query Trace Tool Usage++++++\n"
      << "The following options are available:\n"
      << "--usage: Show the usage\n"
      << "--root: Root dir of the query tracing, it must be set\n"
      << "--summary: Show the summary of the tracing including number of tasks"
      << "and task ids. It also print the query metadata including"
      << "query configs, connectors properties, and query plan in JSON format.\n"
      << "--short_summary: Only show number of tasks and task ids.\n"
      << "--pretty: Show the summary of the tracing in pretty JSON.\n"
      << "--task_id: Specify the target task id, if empty, show the summary of "
      << "all the traced query task.\n";
  return usage.str();
}

} // namespace facebook::velox::tool::trace
