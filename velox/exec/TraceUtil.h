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

#pragma once

#include <string>
#include <vector>
#include "velox/common/file/FileSystems.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Task.h"
#include "velox/type/Type.h"

#include <folly/dynamic.h>

namespace facebook::velox::exec::trace {

/// Creates a directory to store the query trace metdata and data.
void createTraceDirectory(
    const std::string& traceDir,
    const std::string& directoryConfig = "");

/// Returns the trace directory for a given query.
std::string getQueryTraceDirectory(
    const std::string& traceDir,
    const std::string& queryId);

/// Returns the trace directory for a given query task.
std::string getTaskTraceDirectory(
    const std::string& traceDir,
    const Task& task);

std::string getTaskTraceDirectory(
    const std::string& traceDir,
    const std::string& queryId,
    const std::string& taskId);

/// Returns the file path for a given task's metadata trace file.
std::string getTaskTraceMetaFilePath(const std::string& taskTraceDir);

/// Returns the trace directory for a given traced plan node.
std::string getNodeTraceDirectory(
    const std::string& taskTraceDir,
    const std::string& nodeId);

/// Returns the trace directory for a given traced pipeline.
std::string getPipelineTraceDirectory(
    const std::string& nodeTraceDir,
    uint32_t pipelineId);

/// Returns the trace directory for a given traced operator.
std::string getOpTraceDirectory(
    const std::string& taskTraceDir,
    const std::string& nodeId,
    uint32_t pipelineId,
    uint32_t driverId);

std::string getOpTraceDirectory(
    const std::string& nodeTraceDir,
    uint32_t pipelineId,
    uint32_t driverId);

/// Returns the file path for a given operator's traced input file.
std::string getOpTraceInputFilePath(const std::string& opTraceDir);

/// Returns the file path for a given operator's traced split file.
std::string getOpTraceSplitFilePath(const std::string& opTraceDir);

/// Returns the file path for a given operator's traced input file.
std::string getOpTraceSummaryFilePath(const std::string& opTraceDir);

/// Extracts the input data type for the trace scan operator. The function first
/// uses the traced node id to find traced operator's plan node from the traced
/// plan fragment. Then it uses the specified source node index to find the
/// output data type from its source node plans as the input data type of the
/// traced plan node.
///
/// For hash join plan node, there are two source nodes, the  output data type
/// of the first node is the input data type of the 'HashProbe' operator, and
/// the output data type of the second one is the input data type of the
/// 'HashBuild' operator.
///
/// @param tracedPlan The root node of the trace plan fragment.
/// @param tracedNodeId The node id of the trace node.
/// @param sourceIndex The source index of the specific traced operator.
RowTypePtr getDataType(
    const core::PlanNodePtr& tracedPlan,
    const std::string& tracedNodeId,
    size_t sourceIndex = 0);

/// Extracts pipeline IDs in ascending order by listing the trace directory,
/// then decoding the names of the subdirectories to obtain the pipeline IDs,
/// and finally sorting them. 'nodeTraceDir' corresponds to the trace directory
/// of the plan node.
std::vector<uint32_t> listPipelineIds(
    const std::string& nodeTraceDir,
    const std::shared_ptr<filesystems::FileSystem>& fs);

/// Extracts driver IDs in ascending order by listing the trace directory for a
/// given pipeline then decoding the names of the subdirectories to obtain the
/// driver IDs, and finally sorting them. 'nodeTraceDir' corresponds to the
/// trace directory of the plan node.
std::vector<uint32_t> listDriverIds(
    const std::string& nodeTraceDir,
    uint32_t pipelineId,
    const std::shared_ptr<filesystems::FileSystem>& fs);

/// Extracts the driver IDs from the comma-separated list of driver IDs string.
std::vector<uint32_t> extractDriverIds(const std::string& driverIds);

/// Extracts task ids of the query tracing by listing the query trace directory.
/// 'traceDir' is the root trace directory. 'queryId' is the query id.
std::vector<std::string> getTaskIds(
    const std::string& traceDir,
    const std::string& queryId,
    const std::shared_ptr<filesystems::FileSystem>& fs);

/// Gets the metadata from a given task metadata file which includes query plan,
/// configs and connector properties.
folly::dynamic getTaskMetadata(
    const std::string& taskMetaFilePath,
    const std::shared_ptr<filesystems::FileSystem>& fs);

/// Checks whether the operator can be traced.
bool canTrace(const std::string& operatorType);
} // namespace facebook::velox::exec::trace
