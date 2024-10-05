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

#include "velox/tool/trace/TableWriterReplayer.h"

#include <folly/executors/IOThreadPoolExecutor.h>
#include "velox/common/memory/Memory.h"
#include "velox/exec/QueryDataReader.h"
#include "velox/exec/QueryTraceUtil.h"
#include "velox/exec/TableWriter.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace facebook::velox::tool::trace {

namespace {

std::shared_ptr<connector::hive::HiveInsertTableHandle>
makeHiveInsertTableHandle(
    const core::TableWriteNode* node,
    std::string targetDir) {
  const auto tracedHandle =
      std::dynamic_pointer_cast<connector::hive::HiveInsertTableHandle>(
          node->insertTableHandle()->connectorInsertTableHandle());
  const auto inputColumns = tracedHandle->inputColumns();
  const auto compressionKind =
      tracedHandle->compressionKind().value_or(common::CompressionKind_NONE);
  const auto storageFormat = tracedHandle->tableStorageFormat();
  const auto serdeParameters = tracedHandle->serdeParameters();
  const auto writerOptions = tracedHandle->writerOptions();
  return std::make_shared<connector::hive::HiveInsertTableHandle>(
      inputColumns,
      std::make_shared<connector::hive::LocationHandle>(
          targetDir,
          targetDir,
          connector::hive::LocationHandle::TableType::kNew),
      storageFormat,
      tracedHandle->bucketProperty() == nullptr
          ? nullptr
          : std::make_shared<connector::hive::HiveBucketProperty>(
                *tracedHandle->bucketProperty()),
      compressionKind,
      std::unordered_map<std::string, std::string>{},
      writerOptions);
}

std::shared_ptr<core::InsertTableHandle> createInsertTableHanlde(
    const std::string& connectorId,
    const core::TableWriteNode* node,
    std::string targetDir) {
  return std::make_shared<core::InsertTableHandle>(
      connectorId, makeHiveInsertTableHandle(node, std::move(targetDir)));
}

} // namespace

RowVectorPtr TableWriterReplayer::run() const {
  const auto restoredPlanNode = createPlan();

  return AssertQueryBuilder(restoredPlanNode)
      .maxDrivers(maxDrivers_)
      .configs(queryConfigs_)
      .connectorSessionProperties(connectorConfigs_)
      .copyResults(memory::MemoryManager::getInstance()->tracePool());
}

core::PlanNodePtr TableWriterReplayer::createPlan() const {
  const auto* tableWriterNode = core::PlanNode::findFirstNode(
      planFragment_.get(),
      [this](const core::PlanNode* node) { return node->id() == nodeId_; });
  const auto traceRoot = fmt::format("{}/{}", rootDir_, taskId_);
  return PlanBuilder()
      .traceScan(
          fmt::format("{}/{}", traceRoot, nodeId_),
          exec::trace::getDataType(planFragment_, nodeId_))
      .addNode(addTableWriter(
          dynamic_cast<const core::TableWriteNode*>(tableWriterNode),
          replayOutputDir_))
      .planNode();
}

core::PlanNodePtr TableWriterReplayer::createTableWrtierNode(
    const core::TableWriteNode* node,
    const std::string& targetDir,
    const core::PlanNodeId& nodeId,
    const core::PlanNodePtr& source) {
  const auto insertTableHandle =
      createInsertTableHanlde("test-hive", node, targetDir);
  return std::make_shared<core::TableWriteNode>(
      nodeId,
      node->columns(),
      node->columnNames(),
      node->aggregationNode(),
      insertTableHandle,
      node->hasPartitioningScheme(),
      TableWriteTraits::outputType(node->aggregationNode()),
      node->commitStrategy(),
      source);
}

std::function<core::PlanNodePtr(std::string, core::PlanNodePtr)>
TableWriterReplayer::addTableWriter(
    const core::TableWriteNode* node,
    const std::string& targetDir) {
  return [=](const core::PlanNodeId& nodeId,
             const core::PlanNodePtr& source) -> core::PlanNodePtr {
    return createTableWrtierNode(node, targetDir, nodeId, source);
  };
}

} // namespace facebook::velox::tool::trace
