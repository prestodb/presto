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

#include "velox/exec/QueryTraceUtil.h"

#include <folly/json.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/QueryTraceTraits.h"

namespace facebook::velox::exec::trace {

void createTraceDirectory(const std::string& traceDir) {
  try {
    const auto fs = filesystems::getFileSystem(traceDir, nullptr);
    if (fs->exists(traceDir)) {
      fs->rmdir(traceDir);
    }
    fs->mkdir(traceDir);
  } catch (const std::exception& e) {
    VELOX_FAIL(
        "Failed to create trace directory '{}' with error: {}",
        traceDir,
        e.what());
  }
}

std::vector<std::string> getTaskIds(
    const std::string& traceDir,
    const std::shared_ptr<filesystems::FileSystem>& fs) {
  VELOX_USER_CHECK(fs->exists(traceDir), "{} dose not exist", traceDir);
  try {
    const auto taskDirs = fs->list(traceDir);
    std::vector<std::string> taskIds;
    for (const auto& taskDir : taskDirs) {
      std::vector<std::string> pathNodes;
      folly::split("/", taskDir, pathNodes);
      taskIds.emplace_back(std::move(pathNodes.back()));
    }
    return taskIds;
  } catch (const std::exception& e) {
    VELOX_FAIL(
        "Failed to list the directory '{}' with error: {}", traceDir, e.what());
  }
}

folly::dynamic getMetadata(
    const std::string& metadataFile,
    const std::shared_ptr<filesystems::FileSystem>& fs) {
  try {
    const auto file = fs->openFileForRead(metadataFile);
    VELOX_CHECK_NOT_NULL(file);
    const auto metadata = file->pread(0, file->size());
    VELOX_USER_CHECK(!metadata.empty());
    return folly::parseJson(metadata);
  } catch (const std::exception& e) {
    VELOX_FAIL(
        "Failed to get the query metadata from '{}' with error: {}",
        metadataFile,
        e.what());
  }
}

RowTypePtr getDataType(
    const core::PlanNodePtr& tracedPlan,
    const std::string& tracedNodeId,
    size_t sourceIndex) {
  const auto* traceNode = core::PlanNode::findFirstNode(
      tracedPlan.get(), [&tracedNodeId](const core::PlanNode* node) {
        return node->id() == tracedNodeId;
      });
  VELOX_CHECK_NOT_NULL(
      traceNode,
      "traced node id {} not found in the traced plan",
      tracedNodeId);
  return traceNode->sources().at(sourceIndex)->outputType();
}

uint8_t getNumDrivers(
    const std::string& rootDir,
    const std::string& taskId,
    const std::string& nodeId,
    int32_t pipelineId,
    const std::shared_ptr<filesystems::FileSystem>& fs) {
  const auto traceDir =
      fmt::format("{}/{}/{}/{}", rootDir, taskId, nodeId, pipelineId);
  const auto driverDirs = fs->list(traceDir);
  return driverDirs.size();
}

std::string
getDataDir(const std::string& traceDir, int pipelineId, int driverId) {
  return fmt::format("{}/{}/{}/data", traceDir, pipelineId, driverId);
}

} // namespace facebook::velox::exec::trace
