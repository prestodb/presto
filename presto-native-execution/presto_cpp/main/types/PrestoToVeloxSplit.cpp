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
#include "presto_cpp/main/types/PrestoToVeloxSplit.h"
#include <optional>
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/tpch/TpchConnectorSplit.h"
#include "velox/exec/Exchange.h"

using namespace facebook::velox;

namespace facebook::presto {

namespace {

dwio::common::FileFormat toVeloxFileFormat(
    const presto::protocol::StorageFormat& format) {
  if (format.inputFormat == "com.facebook.hive.orc.OrcInputFormat") {
    return dwio::common::FileFormat::DWRF;
  } else if (
      format.inputFormat ==
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat") {
    return dwio::common::FileFormat::PARQUET;
  } else if (format.inputFormat == "org.apache.hadoop.mapred.TextInputFormat") {
    if (format.serDe == "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe") {
      return dwio::common::FileFormat::TEXT;
    } else if (format.serDe == "org.apache.hive.hcatalog.data.JsonSerDe") {
      return dwio::common::FileFormat::JSON;
    }
  } else if (format.inputFormat == "com.facebook.alpha.AlphaInputFormat") {
    return dwio::common::FileFormat::ALPHA;
  }
  VELOX_UNSUPPORTED(
      "Unsupported file format: {} {}", format.inputFormat, format.serDe);
}

dwio::common::FileFormat toVeloxFileFormat(
    const presto::protocol::FileFormat format) {
  if (format == protocol::FileFormat::ORC) {
    return dwio::common::FileFormat::DWRF;
  } else if (format == protocol::FileFormat::PARQUET) {
    return dwio::common::FileFormat::PARQUET;
  }
  VELOX_UNSUPPORTED("Unsupported file format: {}", fmt::underlying(format));
}

} // anonymous namespace

velox::exec::Split toVeloxSplit(
    const presto::protocol::ScheduledSplit& scheduledSplit) {
  const auto& connectorSplit = scheduledSplit.split.connectorSplit;
  const auto splitGroupId = scheduledSplit.split.lifespan.isgroup
      ? scheduledSplit.split.lifespan.groupid
      : -1;
  if (auto hiveSplit = std::dynamic_pointer_cast<const protocol::HiveSplit>(
          connectorSplit)) {
    std::unordered_map<std::string, std::optional<std::string>> partitionKeys;
    for (const auto& entry : hiveSplit->partitionKeys) {
      partitionKeys.emplace(
          entry.name,
          entry.value == nullptr ? std::nullopt
                                 : std::optional<std::string>{*entry.value});
    }
    std::unordered_map<std::string, std::string> customSplitInfo;
    for (const auto& [key, value] : hiveSplit->fileSplit.customSplitInfo) {
      customSplitInfo[key] = value;
    }
    std::shared_ptr<std::string> extraFileInfo;
    if (hiveSplit->fileSplit.extraFileInfo) {
      extraFileInfo = std::make_shared<std::string>(
          velox::encoding::Base64::decode(*hiveSplit->fileSplit.extraFileInfo));
    }
    std::unordered_map<std::string, std::string> serdeParameters;
    serdeParameters.reserve(hiveSplit->storage.serdeParameters.size());
    for (const auto& [key, value] : hiveSplit->storage.serdeParameters) {
      serdeParameters[key] = value;
    }
    return velox::exec::Split(
        std::make_shared<connector::hive::HiveConnectorSplit>(
            scheduledSplit.split.connectorId,
            hiveSplit->fileSplit.path,
            toVeloxFileFormat(hiveSplit->storage.storageFormat),
            hiveSplit->fileSplit.start,
            hiveSplit->fileSplit.length,
            partitionKeys,
            hiveSplit->tableBucketNumber
                ? std::optional<int>(*hiveSplit->tableBucketNumber)
                : std::nullopt,
            customSplitInfo,
            extraFileInfo,
            serdeParameters),
        splitGroupId);
  }

  if (auto icebergSplit =
          std::dynamic_pointer_cast<const protocol::IcebergSplit>(
              connectorSplit)) {
    std::unordered_map<std::string, std::optional<std::string>> partitionKeys;
    for (const auto& entry : icebergSplit->partitionKeys) {
      partitionKeys.emplace(
          entry.second.name,
          entry.second.value == nullptr
              ? std::nullopt
              : std::optional<std::string>{*entry.second.value});
    }

    return velox::exec::Split(
        std::make_shared<connector::hive::HiveConnectorSplit>(
            scheduledSplit.split.connectorId,
            icebergSplit->path,
            toVeloxFileFormat(icebergSplit->fileFormat),
            icebergSplit->start,
            icebergSplit->length,
            partitionKeys),
        splitGroupId);
  }

  if (auto remoteSplit = std::dynamic_pointer_cast<const protocol::RemoteSplit>(
          connectorSplit)) {
    return velox::exec::Split(
        std::make_shared<exec::RemoteConnectorSplit>(
            remoteSplit->location.location),
        splitGroupId);
  }
  if (auto tpchSplit = std::dynamic_pointer_cast<const protocol::TpchSplit>(
          connectorSplit)) {
    return velox::exec::Split(
        std::make_shared<connector::tpch::TpchConnectorSplit>(
            scheduledSplit.split.connectorId,
            tpchSplit->totalParts,
            tpchSplit->partNumber),
        splitGroupId);
  }
  if (std::dynamic_pointer_cast<const protocol::EmptySplit>(connectorSplit)) {
    // We return NULL for empty splits to signal to do nothing.
    return velox::exec::Split(nullptr, splitGroupId);
  }

  VELOX_CHECK(false, "Unknown split type {}", connectorSplit->_type);
}

} // namespace facebook::presto
