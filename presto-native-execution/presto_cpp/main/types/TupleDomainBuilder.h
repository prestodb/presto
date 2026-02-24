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

#include <optional>
#include <string>
#include <vector>
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/common/memory/Memory.h"
#include "velox/type/Type.h"
#include "velox/type/Variant.h"

namespace facebook::presto {

/// Serializes a single Velox value to a Base64-encoded Block string suitable
/// for the Presto protocol Marker.valueBlock field.
std::string serializeValueToBlock(
    const velox::TypePtr& type,
    const velox::variant& value,
    velox::memory::MemoryPool* pool);

/// Builds a protocol::TupleDomain<String> from collected filter data.
///
/// If discreteValues is non-empty, builds a SortedRangeSet with one
/// Range{EXACTLY(v), EXACTLY(v)} per value.
/// Otherwise, if minValue and maxValue are set, builds a SortedRangeSet with
/// one Range{EXACTLY(min), EXACTLY(max)}.
protocol::TupleDomain<std::string> buildTupleDomain(
    const std::string& columnName,
    const velox::TypePtr& type,
    const std::vector<velox::variant>& discreteValues,
    const std::optional<velox::variant>& minValue,
    const std::optional<velox::variant>& maxValue,
    bool nullAllowed,
    velox::memory::MemoryPool* pool);

/// Returns TupleDomain representing none() -- empty build side.
/// Serialized as {"columnDomains": null} (domains pointer is nullptr).
protocol::TupleDomain<std::string> buildNoneTupleDomain();

/// Maps a Velox type to the Presto type signature string used in protocol
/// Marker and SortedRangeSet type fields.
std::string toPrestoTypeName(const velox::TypePtr& type);

} // namespace facebook::presto
