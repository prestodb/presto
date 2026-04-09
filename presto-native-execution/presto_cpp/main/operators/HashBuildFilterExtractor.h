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
#include <vector>
#include "presto_cpp/main/operators/DynamicFilterSource.h"
#include "velox/common/memory/Memory.h"
#include "velox/exec/HashTable.h"

namespace facebook::presto::operators {

/// Extracts dynamic filters from pre-merge hash tables' VectorHasher objects
/// and delivers them to the PrestoTask via DynamicFilterCallbackRegistry.
///
/// Called before prepareJoinTable() so VectorHasher discrete values are still
/// intact. Unions values across all per-driver hash tables.
void extractAndDeliverFilters(
    const std::string& taskId,
    const std::vector<DynamicFilterChannel>& channels,
    const velox::exec::BaseHashTable& mainTable,
    const std::vector<std::unique_ptr<velox::exec::BaseHashTable>>&
        otherTables,
    velox::memory::MemoryPool* pool);

} // namespace facebook::presto::operators
