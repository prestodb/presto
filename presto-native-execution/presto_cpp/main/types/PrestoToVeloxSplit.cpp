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
#include "velox/exec/Exchange.h"

using namespace facebook::velox;

namespace facebook::presto {

namespace {

dwio::common::FileFormat toVeloxFileFormat(
    const facebook::presto::protocol::String& format) {
  if (format == "com.facebook.hive.orc.OrcInputFormat") {
    return dwio::common::FileFormat::DWRF;
  } else if (
      format ==
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat") {
    return dwio::common::FileFormat::PARQUET;
  } else {
    VELOX_FAIL("Unknown file format {}", format);
  }
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

    return velox::exec::Split(
        std::make_shared<connector::hive::HiveConnectorSplit>(
            scheduledSplit.split.connectorId,
            hiveSplit->path,
            toVeloxFileFormat(hiveSplit->storage.storageFormat.inputFormat),
            hiveSplit->start,
            hiveSplit->length,
            partitionKeys,
            hiveSplit->tableBucketNumber
                ? std::optional<int>(*hiveSplit->tableBucketNumber)
                : std::nullopt),
        splitGroupId);
  }
  if (auto remoteSplit = std::dynamic_pointer_cast<const protocol::RemoteSplit>(
          connectorSplit)) {
    return velox::exec::Split(
        std::make_shared<exec::RemoteConnectorSplit>(
            remoteSplit->location.location),
        splitGroupId);
  }
  if (std::dynamic_pointer_cast<const protocol::EmptySplit>(connectorSplit)) {
    // We return NULL for empty splits to signal to do nothing.
    return velox::exec::Split(nullptr, splitGroupId);
  }

  VELOX_CHECK(false, "Unknown split type {}", connectorSplit->_type);
}

} // namespace facebook::presto
