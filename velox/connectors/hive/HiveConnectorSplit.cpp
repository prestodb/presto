/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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

#include "velox/connectors/hive/HiveConnectorSplit.h"

namespace facebook::velox::connector::hive {

std::string HiveConnectorSplit::toString() const {
  if (tableBucketNumber.has_value()) {
    return fmt::format(
        "Hive: {} {} - {} {}",
        filePath,
        start,
        length,
        tableBucketNumber.value());
  }
  return fmt::format("Hive: {} {} - {}", filePath, start, length);
}

std::string HiveConnectorSplit::getFileName() const {
  const auto i = filePath.rfind('/');
  return i == std::string::npos ? filePath : filePath.substr(i + 1);
}

folly::dynamic HiveConnectorSplit::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "HiveConnectorSplit";
  obj["connectorId"] = connectorId;
  obj["splitWeight"] = splitWeight;
  obj["filePath"] = filePath;
  obj["fileFormat"] = dwio::common::toString(fileFormat);
  obj["start"] = start;
  obj["length"] = length;

  folly::dynamic partitionKeysObj = folly::dynamic::object;
  for (const auto& [key, value] : partitionKeys) {
    partitionKeysObj[key] =
        value.has_value() ? folly::dynamic(value.value()) : nullptr;
  }
  obj["partitionKeys"] = partitionKeysObj;

  obj["tableBucketNumber"] = tableBucketNumber.has_value()
      ? folly::dynamic(tableBucketNumber.value())
      : nullptr;

  folly::dynamic customSplitInfoObj = folly::dynamic::object;
  for (const auto& [key, value] : customSplitInfo) {
    customSplitInfoObj[key] = value;
  }
  obj["customSplitInfo"] = customSplitInfoObj;
  obj["extraFileInfo"] = *extraFileInfo;

  folly::dynamic serdeParametersObj = folly::dynamic::object;
  for (const auto& [key, value] : serdeParameters) {
    serdeParametersObj[key] = value;
  }
  obj["serdeParameters"] = serdeParametersObj;

  folly::dynamic infoColumnsObj = folly::dynamic::object;
  for (const auto& [key, value] : infoColumns) {
    infoColumnsObj[key] = value;
  }
  obj["infoColumns"] = infoColumnsObj;

  if (properties.has_value()) {
    folly::dynamic propertiesObj = folly::dynamic::object;
    propertiesObj["fileSize"] = properties->fileSize.has_value()
        ? folly::dynamic(properties->fileSize.value())
        : nullptr;
    propertiesObj["modificationTime"] = properties->modificationTime.has_value()
        ? folly::dynamic(properties->modificationTime.value())
        : nullptr;
    obj["properties"] = propertiesObj;
  } else {
    obj["properties"] = nullptr;
  }

  return obj;
}

// static
std::shared_ptr<HiveConnectorSplit> HiveConnectorSplit::create(
    const folly::dynamic& obj) {
  const auto connectorId = obj["connectorId"].asString();
  const auto splitWeight = obj["splitWeight"].asInt();
  const auto filePath = obj["filePath"].asString();
  const auto fileFormat =
      dwio::common::toFileFormat(obj["fileFormat"].asString());
  const auto start = static_cast<uint64_t>(obj["start"].asInt());
  const auto length = static_cast<uint64_t>(obj["length"].asInt());

  std::unordered_map<std::string, std::optional<std::string>> partitionKeys;
  for (const auto& [key, value] : obj["partitionKeys"].items()) {
    partitionKeys[key.asString()] = value.isNull()
        ? std::nullopt
        : std::optional<std::string>(value.asString());
  }

  const auto tableBucketNumber = obj["tableBucketNumber"].isNull()
      ? std::nullopt
      : std::optional<int32_t>(obj["tableBucketNumber"].asInt());

  std::unordered_map<std::string, std::string> customSplitInfo;
  for (const auto& [key, value] : obj["customSplitInfo"].items()) {
    customSplitInfo[key.asString()] = value.asString();
  }

  std::shared_ptr<std::string> extraFileInfo =
      std::make_shared<std::string>(obj["extraFileInfo"].asString());
  std::unordered_map<std::string, std::string> serdeParameters;
  for (const auto& [key, value] : obj["serdeParameters"].items()) {
    serdeParameters[key.asString()] = value.asString();
  }

  std::unordered_map<std::string, std::string> infoColumns;
  for (const auto& [key, value] : obj["infoColumns"].items()) {
    infoColumns[key.asString()] = value.asString();
  }

  std::optional<FileProperties> properties = std::nullopt;
  const auto propertiesObj = obj["properties"];
  if (!propertiesObj.isNull()) {
    properties = FileProperties{
        propertiesObj["fileSize"].isNull()
            ? std::nullopt
            : std::optional(propertiesObj["fileSize"].asInt()),
        propertiesObj["modificationTime"].isNull()
            ? std::nullopt
            : std::optional(propertiesObj["modificationTime"].asInt())};
  }

  return std::make_shared<HiveConnectorSplit>(
      connectorId,
      filePath,
      fileFormat,
      start,
      length,
      partitionKeys,
      tableBucketNumber,
      customSplitInfo,
      extraFileInfo,
      serdeParameters,
      splitWeight,
      infoColumns,
      properties);
}

// static
void HiveConnectorSplit::registerSerDe() {
  auto& registry = DeserializationRegistryForSharedPtr();
  registry.Register("HiveConnectorSplit", HiveConnectorSplit::create);
}
} // namespace facebook::velox::connector::hive
