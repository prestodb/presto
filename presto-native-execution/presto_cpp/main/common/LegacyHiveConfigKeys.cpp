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
#include "presto_cpp/main/common/LegacyHiveConfigKeys.h"

#include <array>
#include <string_view>
#include <utility>

#include "presto_cpp/main/common/Utils.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/iceberg/IcebergConnector.h"

namespace facebook::presto::util {

void migrateLegacyHiveParquetKeys(
    const std::string& connectorName,
    std::unordered_map<std::string, std::string>& configs) {
  if (connectorName !=
          velox::connector::hive::HiveConnectorFactory::kHiveConnectorName &&
      connectorName !=
          velox::connector::hive::iceberg::IcebergConnectorFactory::
              kIcebergConnectorName) {
    return;
  }

  static constexpr std::array<std::pair<std::string_view, std::string_view>, 4>
      kRenames = {{
          {"parquet.use-column-names", "hive.parquet.use-column-names"},
          {"parquet.allow-int32-narrowing",
           "hive.parquet.allow-int32-narrowing"},
          {"parquet.footer-speculative-io-size",
           "hive.parquet.footer-speculative-io-size"},
          {"parquet-footer-memory-tracking-threshold",
           "hive.parquet.footer-memory-tracking-threshold"},
      }};
  for (const auto& [oldKey, newKey] : kRenames) {
    auto node = configs.extract(std::string{oldKey});
    if (!node) {
      continue;
    }
    if (configs.find(std::string{newKey}) == configs.end()) {
      PRESTO_STARTUP_LOG(WARNING)
          << "Hive connector config '" << oldKey
          << "' is deprecated; rewriting to '" << newKey << "'.";
      configs.emplace(std::string{newKey}, std::move(node.mapped()));
    } else {
      PRESTO_STARTUP_LOG(WARNING) << "Hive connector config '" << oldKey
                                  << "' is deprecated and ignored because '"
                                  << newKey << "' is also set.";
    }
  }

  static constexpr std::string_view kLegacyMaxTargetFileSize =
      "max-target-file-size";
  static constexpr std::array<std::string_view, 3>
      kPerFormatMaxTargetFileSizeKeys = {
          "hive.parquet.writer.max-target-file-size",
          "hive.orc.writer.max-target-file-size",
          "hive.nimble.writer.max-target-file-size",
      };
  auto legacyNode = configs.extract(std::string{kLegacyMaxTargetFileSize});
  if (legacyNode) {
    PRESTO_STARTUP_LOG(WARNING)
        << "Hive connector config '" << kLegacyMaxTargetFileSize
        << "' is deprecated and has been split into per-format keys; "
        << "fanning out value to all of: " << kPerFormatMaxTargetFileSizeKeys[0]
        << ", " << kPerFormatMaxTargetFileSizeKeys[1] << ", "
        << kPerFormatMaxTargetFileSizeKeys[2] << ".";
    const auto& value = legacyNode.mapped();
    for (const auto newKey : kPerFormatMaxTargetFileSizeKeys) {
      if (configs.find(std::string{newKey}) == configs.end()) {
        configs.emplace(std::string{newKey}, value);
      } else {
        PRESTO_STARTUP_LOG(WARNING)
            << "  '" << newKey << "' is already set; not overwriting.";
      }
    }
  }
}

void migrateLegacyHiveParquetSessionKeys(
    std::unordered_map<std::string, std::string>& sessionProperties) {
  static constexpr std::pair<std::string_view, std::string_view> kRename = {
      "allow_int32_narrowing", "parquet_allow_int32_narrowing"};
  if (auto node = sessionProperties.extract(std::string{kRename.first})) {
    LOG_FIRST_N(WARNING, 1)
        << "Hive connector session property '" << kRename.first
        << "' is deprecated; rewriting to '" << kRename.second
        << "'. (Logged once per process.)";
    if (sessionProperties.find(std::string{kRename.second}) ==
        sessionProperties.end()) {
      sessionProperties.emplace(
          std::string{kRename.second}, std::move(node.mapped()));
    }
  }

  static constexpr std::string_view kLegacyMaxTargetFileSize =
      "max_target_file_size";
  static constexpr std::array<std::string_view, 3>
      kPerFormatMaxTargetFileSizeSessionKeys = {
          "parquet_writer_max_target_file_size",
          "orc_writer_max_target_file_size",
          "nimble_writer_max_target_file_size",
      };
  if (auto legacyNode =
          sessionProperties.extract(std::string{kLegacyMaxTargetFileSize})) {
    LOG_FIRST_N(WARNING, 1)
        << "Hive connector session property '" << kLegacyMaxTargetFileSize
        << "' is deprecated; fanning out to format-specific keys. "
        << "(Logged once per process.)";
    const auto& value = legacyNode.mapped();
    for (const auto newKey : kPerFormatMaxTargetFileSizeSessionKeys) {
      if (sessionProperties.find(std::string{newKey}) ==
          sessionProperties.end()) {
        sessionProperties.emplace(std::string{newKey}, value);
      }
    }
  }
}

} // namespace facebook::presto::util
