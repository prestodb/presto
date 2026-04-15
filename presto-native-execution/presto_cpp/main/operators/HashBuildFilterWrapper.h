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

#include <string>
#include <unordered_set>
#include "presto_cpp/main/operators/DynamicFilterSource.h"
#include "velox/exec/HashBuild.h"
#include "velox/exec/Operator.h"

namespace facebook::presto::operators {

/// Registers the HashBuildFilterWrapper translator. Must be called once
/// during worker initialization, BEFORE Velox's built-in translators
/// are used (first non-null result wins in the translator chain).
void registerHashBuildFilterWrapper();

/// Called during plan conversion (per task) to record which HashJoinNode
/// IDs have distributed dynamic filters. The translator checks this set
/// to decide whether to wrap a HashBuild.
void setDynamicFilterJoinNodeIds(
    const std::string& taskId,
    std::unordered_map<
        std::string,
        std::vector<DynamicFilterChannel>> joinNodeChannels);

/// Called on task deletion to clean up the per-task state.
void clearDynamicFilterJoinNodeIds(const std::string& taskId);

} // namespace facebook::presto::operators
