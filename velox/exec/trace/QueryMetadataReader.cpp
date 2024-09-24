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

#include "velox/exec/trace/QueryMetadataReader.h"

#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/trace/QueryTraceTraits.h"
#include "velox/exec/trace/QueryTraceUtil.h"

namespace facebook::velox::exec::trace {

QueryMetadataReader::QueryMetadataReader(
    std::string traceDir,
    memory::MemoryPool* pool)
    : traceDir_(std::move(traceDir)),
      fs_(filesystems::getFileSystem(traceDir_, nullptr)),
      metaFilePath_(fmt::format(
          "{}/{}",
          traceDir_,
          QueryTraceTraits::kQueryMetaFileName)),
      pool_(pool) {
  VELOX_CHECK_NOT_NULL(fs_);
  VELOX_CHECK(fs_->exists(metaFilePath_));
}

void QueryMetadataReader::read(
    std::unordered_map<std::string, std::string>& queryConfigs,
    std::unordered_map<
        std::string,
        std::unordered_map<std::string, std::string>>& connectorProperties,
    core::PlanNodePtr& queryPlan) const {
  folly::dynamic metaObj = getMetadata(metaFilePath_, fs_);
  const auto& queryConfigObj = metaObj[QueryTraceTraits::kQueryConfigKey];
  for (const auto& [key, value] : queryConfigObj.items()) {
    queryConfigs[key.asString()] = value.asString();
  }

  const auto& connectorPropertiesObj =
      metaObj[QueryTraceTraits::kConnectorPropertiesKey];
  for (const auto& [connectorId, configs] : connectorPropertiesObj.items()) {
    const auto connectorIdStr = connectorId.asString();
    connectorProperties[connectorIdStr] = {};
    for (const auto& [key, value] : configs.items()) {
      connectorProperties[connectorIdStr][key.asString()] = value.asString();
    }
  }

  queryPlan = ISerializable::deserialize<core::PlanNode>(
      metaObj[QueryTraceTraits::kPlanNodeKey], pool_);
}
} // namespace facebook::velox::exec::trace
