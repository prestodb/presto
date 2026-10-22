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
#include "presto_cpp/main/common/JavaWriterConfigKeys.h"

#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/iceberg/IcebergConnector.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/parquet/common/ParquetConfig.h"

namespace facebook::presto::util {

void mapJavaWriterConnectorConfigs(
    const std::string& connectorName,
    std::unordered_map<std::string, std::string>& configs) {
  using velox::connector::hive::HiveConfig;
  const bool isIceberg = connectorName ==
      velox::connector::hive::iceberg::IcebergConnectorFactory::
          kIcebergConnectorName;
  if (!isIceberg &&
      connectorName !=
          velox::connector::hive::HiveConnectorFactory::kHiveConnectorName) {
    return;
  }

  if (isIceberg) {
    if (auto it = configs.find("iceberg.target-max-file-size");
        it != configs.end()) {
      const auto value = it->second;
      for (const char* veloxKey :
           {HiveConfig::kParquetMaxTargetFileSize,
            HiveConfig::kOrcMaxTargetFileSize,
            HiveConfig::kNimbleMaxTargetFileSize}) {
        configs.emplace(veloxKey, value);
      }
    }
  }

  if (auto it = configs.find("hive.parquet.writer.block-size");
      it != configs.end()) {
    // Both connectors read Velox connector configs under the Hive prefix.
    const auto value = it->second;
    configs.emplace(
        HiveConfig::makeConnectorConfigPrefix(HiveConfig::kConnectorName) +
            velox::dwio::common::formatConfigPrefix(
                velox::dwio::common::FileFormat::PARQUET, ".") +
            std::string(velox::parquet::ParquetConfig::kWriterRowGroupSize),
        value);
  }
}

} // namespace facebook::presto::util
