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

#include <functional>
#include <string>
#include <vector>
#include "presto_cpp/main/operators/DynamicFilterSource.h"
#include "velox/common/memory/Memory.h"
#include "velox/exec/HashTable.h"
#include "velox/type/Filter.h"
#include "velox/type/Variant.h"

namespace facebook::presto::operators {

/// Optional callback for reporting per-channel extraction errors.
/// Receives the error message string.
using DppErrorCallback = std::function<void(const std::string&)>;

/// Extracts filter data from a common::Filter and adds to the accumulators.
/// Populates discreteValues with individual values and/or updates
/// minValue/maxValue with range bounds.
void convertFilter(
    const velox::common::Filter& filter,
    const velox::TypePtr& type,
    std::vector<velox::variant>& discreteValues,
    std::optional<velox::variant>& minValue,
    std::optional<velox::variant>& maxValue);

/// Extracts dynamic filters from pre-merge hash tables' VectorHasher objects
/// and delivers them to the PrestoTask via DynamicFilterCallbackRegistry.
///
/// Called before prepareJoinTable() so VectorHasher discrete values are still
/// intact. Unions values across all per-driver hash tables.
///
/// @param errorCallback Optional callback invoked with the error message
///   when buildTupleDomain fails for a channel. The channel is skipped but
///   other channels continue. If null, errors are only logged.
void extractAndDeliverFilters(
    const std::string& taskId,
    const std::vector<DynamicFilterChannel>& channels,
    const velox::exec::BaseHashTable& mainTable,
    const std::vector<std::unique_ptr<velox::exec::BaseHashTable>>&
        otherTables,
    velox::memory::MemoryPool* pool,
    DppErrorCallback errorCallback = nullptr);

} // namespace facebook::presto::operators
