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
#include "velox/type/Type.h"

#include <folly/dynamic.h>

namespace facebook::velox::exec::trace {

/// Creates a directory to store the query trace metdata and data.
void createTraceDirectory(const std::string& traceDir);

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

/// Extracts the number of drivers by listing the number of sub-directors under
/// the trace directory for a given pipeline.
uint8_t getNumDrivers(
    const std::string& rootDir,
    const std::string& taskId,
    const std::string& nodeId,
    int32_t pipelineId,
    const std::shared_ptr<filesystems::FileSystem>& fs);

/// Extracts task ids of the query tracing by listing the trace directory.
std::vector<std::string> getTaskIds(
    const std::string& traceDir,
    const std::shared_ptr<filesystems::FileSystem>& fs);

/// Gets the metadata from a given task metadata file which includes query plan,
/// configs and connector properties.
folly::dynamic getMetadata(
    const std::string& metadataFile,
    const std::shared_ptr<filesystems::FileSystem>& fs);

/// Gets the traced data directory. 'traceaDir' is the trace directory for a
/// given plan node, which is $traceRoot/$taskId/$nodeId.
std::string
getDataDir(const std::string& traceDir, int pipelineId, int driverId);
} // namespace facebook::velox::exec::trace
