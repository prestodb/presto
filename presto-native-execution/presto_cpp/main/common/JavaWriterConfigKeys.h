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

/// Adds the Velox connector config keys for the writer sizes that a catalog
/// file sets under their Presto Java names, so that native workers honor the
/// same catalog file as Java workers:
/// - `iceberg.target-max-file-size`, in an Iceberg catalog, applies to every
///   file format. It becomes the Parquet, ORC and Nimble max target file size
///   keys.
/// - `hive.parquet.writer.block-size`, in a Hive or Iceberg catalog, becomes
///   the Parquet row group size key.
/// A Velox key that is already set wins. No-op for other connectors.
void mapJavaWriterConnectorConfigs(
    const std::string& connectorName,
    std::unordered_map<std::string, std::string>& configs);

} // namespace facebook::presto::util
