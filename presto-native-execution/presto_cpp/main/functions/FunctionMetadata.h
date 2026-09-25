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
#include "presto_cpp/external/json/nlohmann/json.hpp"

namespace facebook::presto {

/// Returns metadata for all functions this worker can run as json, keyed by
/// function name: the Velox function registries plus the async RPC registry.
/// 'namespacePrefix' qualifies the plain names the RPC registry holds, e.g.
/// "presto.default.".  Functions outside 'catalog' are omitted when it is set.
nlohmann::json getFunctionsMetadata(
    const std::string& namespacePrefix,
    const std::optional<std::string>& catalog);

} // namespace facebook::presto
