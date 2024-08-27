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

#include "velox/exec/trace/QueryMetadataWriter.h"
#include "velox/common/config/Config.h"
#include "velox/common/file/File.h"
#include "velox/core/PlanNode.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/trace/QueryTraceTraits.h"

namespace facebook::velox::exec::trace {

QueryMetadataWriter::QueryMetadataWriter(
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
  VELOX_CHECK(!fs_->exists(metaFilePath_));
}

void QueryMetadataWriter::write(
    const std::shared_ptr<core::QueryCtx>& queryCtx,
    const core::PlanNodePtr& planNode) {
  VELOX_CHECK(!finished_, "Query metadata can only be written once");
  finished_ = true;
  folly::dynamic queryConfigObj = folly::dynamic::object;
  const auto configValues = queryCtx->queryConfig().rawConfigsCopy();
  for (const auto& [key, value] : configValues) {
    queryConfigObj[key] = value;
  }

  folly::dynamic connectorPropertiesObj = folly::dynamic::object;
  for (const auto& [connectorId, configs] :
       queryCtx->connectorSessionProperties()) {
    folly::dynamic obj = folly::dynamic::object;
    for (const auto& [key, value] : configs->rawConfigsCopy()) {
      obj[key] = value;
    }
    connectorPropertiesObj[connectorId] = obj;
  }

  folly::dynamic metaObj = folly::dynamic::object;
  metaObj[QueryTraceTraits::kQueryConfigKey] = queryConfigObj;
  metaObj[QueryTraceTraits::kConnectorPropertiesKey] = connectorPropertiesObj;
  metaObj[QueryTraceTraits::kPlanNodeKey] = planNode->serialize();

  const auto metaStr = folly::toJson(metaObj);
  const auto file = fs_->openFileForWrite(metaFilePath_);
  file->append(metaStr);
  file->close();
}

} // namespace facebook::velox::exec::trace
