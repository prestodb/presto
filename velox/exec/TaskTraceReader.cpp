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

#include "velox/exec/TaskTraceReader.h"

#include "velox/common/file/FileSystems.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Trace.h"
#include "velox/exec/TraceUtil.h"

namespace facebook::velox::exec::trace {

TaskTraceMetadataReader::TaskTraceMetadataReader(
    std::string traceDir,
    memory::MemoryPool* pool)
    : traceDir_(std::move(traceDir)),
      fs_(filesystems::getFileSystem(traceDir_, nullptr)),
      traceFilePath_(getTaskTraceMetaFilePath(traceDir_)),
      pool_(pool),
      metadataObj_(getTaskMetadata(traceFilePath_, fs_)),
      tracePlanNode_(ISerializable::deserialize<core::PlanNode>(
          metadataObj_[TraceTraits::kPlanNodeKey],
          pool_)) {}

std::unordered_map<std::string, std::string>
TaskTraceMetadataReader::queryConfigs() const {
  std::unordered_map<std::string, std::string> queryConfigs;
  const auto& queryConfigObj = metadataObj_[TraceTraits::kQueryConfigKey];
  for (const auto& [key, value] : queryConfigObj.items()) {
    queryConfigs[key.asString()] = value.asString();
  }
  return queryConfigs;
}

std::unordered_map<std::string, std::unordered_map<std::string, std::string>>
TaskTraceMetadataReader::connectorProperties() const {
  std::unordered_map<std::string, std::unordered_map<std::string, std::string>>
      connectorProperties;
  const auto& connectorPropertiesObj =
      metadataObj_[TraceTraits::kConnectorPropertiesKey];
  for (const auto& [connectorId, configs] : connectorPropertiesObj.items()) {
    const auto connectorIdStr = connectorId.asString();
    connectorProperties[connectorIdStr] = {};
    for (const auto& [key, value] : configs.items()) {
      connectorProperties[connectorIdStr][key.asString()] = value.asString();
    }
  }
  return connectorProperties;
}

core::PlanNodePtr TaskTraceMetadataReader::queryPlan() const {
  return tracePlanNode_;
}

std::string TaskTraceMetadataReader::nodeName(const std::string& nodeId) const {
  const auto* traceNode = core::PlanNode::findFirstNode(
      tracePlanNode_.get(),
      [&nodeId](const core::PlanNode* node) { return node->id() == nodeId; });
  return std::string(traceNode->name());
}

std::string TaskTraceMetadataReader::connectorId(
    const std::string& nodeId) const {
  const auto* traceNode = core::PlanNode::findFirstNode(
      tracePlanNode_.get(),
      [&nodeId](const core::PlanNode* node) { return node->id() == nodeId; });
  const auto* tableScanNode =
      dynamic_cast<const core::TableScanNode*>(traceNode);
  VELOX_CHECK_NOT_NULL(tableScanNode);
  const auto connectorId = tableScanNode->tableHandle()->connectorId();
  VELOX_CHECK(!connectorId.empty());
  return connectorId;
}
} // namespace facebook::velox::exec::trace
