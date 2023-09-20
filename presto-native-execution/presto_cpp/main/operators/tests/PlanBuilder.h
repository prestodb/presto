/*
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

#include "velox/core/PlanNode.h"

namespace facebook::presto::operators {

// Helper functions to use with PlanBuilder::addNode.

std::function<
    velox::core::PlanNodePtr(std::string nodeId, velox::core::PlanNodePtr)>

addPartitionAndSerializeNode(
    uint32_t numPartitions,
    bool replicateNullsAndAny,
    const std::vector<std::string>& serializedColumns = {});

std::function<
    velox::core::PlanNodePtr(std::string nodeId, velox::core::PlanNodePtr)>
addShuffleReadNode(const velox::RowTypePtr& outputType);

std::function<
    velox::core::PlanNodePtr(std::string nodeId, velox::core::PlanNodePtr)>
addShuffleWriteNode(
    uint32_t numPartitions,
    const std::string& shuffleName,
    const std::string& serializedWriteInfo);

std::function<velox::core::PlanNodePtr(std::string, velox::core::PlanNodePtr)>

/// Add BroadcastWriteNode for writing broadcast data to files under
/// specified basePath
/// @param outputLayout Optional ordered list of input column names to use as
/// serde layout. Input columns may appear in different order, some columns may
/// be missing and other columns may be duplicated.
addBroadcastWriteNode(
    const std::string& basePath,
    const std::optional<std::vector<std::string>>& outputLayout = std::nullopt);
} // namespace facebook::presto::operators
