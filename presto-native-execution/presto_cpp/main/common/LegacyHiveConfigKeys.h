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
#include <unordered_map>

namespace facebook::presto::util {

/// Transitional. Rewrites legacy Hive Parquet connector property keys to
/// their post-rename equivalents from Velox commit
/// 1f90087b4e0431578777eba68e6dccc2155df3ee. No-op unless `connectorName`
/// is `hive` or `iceberg`. `max-target-file-size` is fanned out to all
/// three new format-specific keys to preserve its previous format-agnostic
/// behavior. On conflict, the existing new key wins.
void migrateLegacyHiveParquetKeys(
    const std::string& connectorName,
    std::unordered_map<std::string, std::string>& configs);

/// Transitional. Same as above for connector session properties. Rewrites
/// `allow_int32_narrowing` and fans `max_target_file_size` out across the
/// three format-specific session keys. Logs a deprecation warning once per
/// process (called per task update).
void migrateLegacyHiveParquetSessionKeys(
    std::unordered_map<std::string, std::string>& sessionProperties);

} // namespace facebook::presto::util
