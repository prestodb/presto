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

#include "velox/exec/TraceUtil.h"

#include <folly/json.h>

#include <numeric>
#include "velox/common/base/Exceptions.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/Trace.h"

namespace facebook::velox::exec::trace {
namespace {
std::string findLastPathNode(const std::string& path) {
  std::vector<std::string> pathNodes;
  folly::split("/", path, pathNodes);
  while (!pathNodes.empty() && pathNodes.back().empty()) {
    pathNodes.pop_back();
  }
  VELOX_CHECK(!pathNodes.empty(), "No valid path nodes found from {}", path);
  return pathNodes.back();
}
} // namespace

void createTraceDirectory(
    const std::string& traceDir,
    const std::string& directoryConfig) {
  try {
    const auto fs = filesystems::getFileSystem(traceDir, nullptr);
    if (fs->exists(traceDir)) {
      fs->rmdir(traceDir);
    }

    filesystems::DirectoryOptions options;
    // If the trace directory config is set, we shall create the directory with
    // the provided config.
    if (!directoryConfig.empty()) {
      options.values.emplace(
          filesystems::DirectoryOptions::kMakeDirectoryConfig.toString(),
          directoryConfig);
    }
    fs->mkdir(traceDir, options);
  } catch (const std::exception& e) {
    VELOX_FAIL(
        "Failed to create trace directory '{}' with error: {}",
        traceDir,
        e.what());
  }
}

std::string getQueryTraceDirectory(
    const std::string& traceDir,
    const std::string& queryId) {
  return fmt::format("{}/{}", traceDir, queryId);
}

std::string getTaskTraceDirectory(
    const std::string& traceDir,
    const Task& task) {
  return getTaskTraceDirectory(
      traceDir, task.queryCtx()->queryId(), task.taskId());
}

std::string getTaskTraceDirectory(
    const std::string& traceDir,
    const std::string& queryId,
    const std::string& taskId) {
  return fmt::format(
      "{}/{}", getQueryTraceDirectory(traceDir, queryId), taskId);
}

std::string getTaskTraceMetaFilePath(const std::string& taskTraceDir) {
  return fmt::format("{}/{}", taskTraceDir, TraceTraits::kTaskMetaFileName);
}

std::string getNodeTraceDirectory(
    const std::string& taskTraceDir,
    const std::string& nodeId) {
  return fmt::format("{}/{}", taskTraceDir, nodeId);
}

std::string getPipelineTraceDirectory(
    const std::string& nodeTraceDir,
    uint32_t pipelineId) {
  return fmt::format("{}/{}", nodeTraceDir, pipelineId);
}

std::string getOpTraceDirectory(
    const std::string& taskTraceDir,
    const std::string& nodeId,
    uint32_t pipelineId,
    uint32_t driverId) {
  return getOpTraceDirectory(
      getNodeTraceDirectory(taskTraceDir, nodeId), pipelineId, driverId);
}

std::string getOpTraceDirectory(
    const std::string& nodeTraceDir,
    uint32_t pipelineId,
    uint32_t driverId) {
  return fmt::format("{}/{}/{}", nodeTraceDir, pipelineId, driverId);
}

std::string getOpTraceInputFilePath(const std::string& opTraceDir) {
  return fmt::format("{}/{}", opTraceDir, OperatorTraceTraits::kInputFileName);
}

std::string getOpTraceSplitFilePath(const std::string& opTraceDir) {
  return fmt::format("{}/{}", opTraceDir, OperatorTraceTraits::kSplitFileName);
}

std::string getOpTraceSummaryFilePath(const std::string& opTraceDir) {
  return fmt::format(
      "{}/{}", opTraceDir, OperatorTraceTraits::kSummaryFileName);
}

std::vector<std::string> getTaskIds(
    const std::string& traceDir,
    const std::string& queryId,
    const std::shared_ptr<filesystems::FileSystem>& fs) {
  const auto queryTraceDir = getQueryTraceDirectory(traceDir, queryId);
  VELOX_USER_CHECK(
      fs->exists(queryTraceDir), "{} dose not exist", queryTraceDir);
  const auto taskDirs = fs->list(queryTraceDir);
  std::vector<std::string> taskIds;
  for (const auto& taskDir : taskDirs) {
    taskIds.emplace_back(findLastPathNode(taskDir));
  }
  return taskIds;
}

folly::dynamic getTaskMetadata(
    const std::string& taskMetaFilePath,
    const std::shared_ptr<filesystems::FileSystem>& fs) {
  try {
    VELOX_CHECK_NOT_NULL(fs);
    const auto file = fs->openFileForRead(taskMetaFilePath);
    VELOX_CHECK_NOT_NULL(file);
    const auto taskMeta = file->pread(0, file->size());
    VELOX_USER_CHECK(!taskMeta.empty());
    return folly::parseJson(taskMeta);
  } catch (const std::exception& e) {
    VELOX_FAIL(
        "Failed to get the query metadata from '{}' with error: {}",
        taskMetaFilePath,
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

std::vector<uint32_t> listPipelineIds(
    const std::string& nodeTraceDir,
    const std::shared_ptr<filesystems::FileSystem>& fs) {
  const auto pipelineDirs = fs->list(nodeTraceDir);
  std::vector<uint32_t> pipelineIds;
  pipelineIds.reserve(pipelineDirs.size());
  try {
    for (const auto& pipelineDir : pipelineDirs) {
      pipelineIds.emplace_back(
          folly::to<uint32_t>(findLastPathNode(pipelineDir)));
    }
  } catch (std::exception& e) {
    VELOX_FAIL(
        "Failed to list pipeline IDs in '{}' with error: {}",
        nodeTraceDir,
        e.what());
  }
  std::sort(pipelineIds.begin(), pipelineIds.end());
  return pipelineIds;
}

std::vector<uint32_t> listDriverIds(
    const std::string& nodeTraceDir,
    uint32_t pipelineId,
    const std::shared_ptr<filesystems::FileSystem>& fs) {
  const auto pipelineDir = getPipelineTraceDirectory(nodeTraceDir, pipelineId);
  const auto driverDirs = fs->list(pipelineDir);
  std::vector<uint32_t> driverIds;
  driverIds.reserve(driverDirs.size());
  try {
    for (const auto& driverDir : driverDirs) {
      driverIds.emplace_back(folly::to<uint32_t>(findLastPathNode(driverDir)));
    }
  } catch (std::exception& e) {
    VELOX_FAIL(
        "Failed to list driver IDs in '{}' with error: {}",
        pipelineDir,
        e.what());
  }
  std::sort(driverIds.begin(), driverIds.end());
  return driverIds;
}

std::vector<uint32_t> extractDriverIds(const std::string& driverIds) {
  std::vector<uint32_t> driverIdList;
  if (driverIds.empty()) {
    return driverIdList;
  }
  folly::split(",", driverIds, driverIdList);
  return driverIdList;
}

bool canTrace(const std::string& operatorType) {
  static const std::unordered_set<std::string> kSupportedOperatorTypes{
      "Aggregation",
      "FilterProject",
      "HashBuild",
      "HashProbe",
      "PartialAggregation",
      "PartitionedOutput",
      "TableScan",
      "TableWrite"};
  return kSupportedOperatorTypes.count(operatorType) > 0;
}
} // namespace facebook::velox::exec::trace
