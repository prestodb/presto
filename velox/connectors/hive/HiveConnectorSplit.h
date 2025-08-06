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
#pragma once

#include <optional>
#include <unordered_map>
#include "velox/connectors/Connector.h"
#include "velox/connectors/hive/FileProperties.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/dwio/common/Options.h"

namespace facebook::velox::connector::hive {

/// A bucket conversion that should happen on the split.  This happens when we
/// increase the bucket count of a table, but the old partitions are still
/// generated using the old bucket count, so that multiple new buckets can exist
/// in the same file, and we need to apply extra filter when we read these files
/// to make sure we read the rows corresponding to the selected bucket number
/// only.
struct HiveBucketConversion {
  int32_t tableBucketCount;
  int32_t partitionBucketCount;
  std::vector<std::shared_ptr<HiveColumnHandle>> bucketColumnHandles;
};

struct RowIdProperties {
  int64_t metadataVersion;
  int64_t partitionId;
  std::string tableGuid;
};

struct HiveConnectorSplit : public connector::ConnectorSplit {
  const std::string filePath;
  dwio::common::FileFormat fileFormat;
  const uint64_t start;
  const uint64_t length;

  /// Mapping from partition keys to values. Values are specified as strings
  /// formatted the same way as CAST(x as VARCHAR). Null values are specified as
  /// std::nullopt. Date values must be formatted using ISO 8601 as YYYY-MM-DD.
  /// All scalar types and date type are supported.
  const std::unordered_map<std::string, std::optional<std::string>>
      partitionKeys;
  std::optional<int32_t> tableBucketNumber;
  std::unordered_map<std::string, std::string> customSplitInfo;
  std::shared_ptr<std::string> extraFileInfo;
  // Parameters that are provided as the serialization options.
  std::unordered_map<std::string, std::string> serdeParameters;

  /// These represent columns like $file_size, $file_modified_time that are
  /// associated with the HiveSplit.
  std::unordered_map<std::string, std::string> infoColumns;

  /// These represent file properties like file size that are used while opening
  /// the file handle.
  std::optional<FileProperties> properties;

  std::optional<RowIdProperties> rowIdProperties;

  std::optional<HiveBucketConversion> bucketConversion;

  HiveConnectorSplit(
      const std::string& connectorId,
      const std::string& _filePath,
      dwio::common::FileFormat _fileFormat,
      uint64_t _start = 0,
      uint64_t _length = std::numeric_limits<uint64_t>::max(),
      const std::unordered_map<std::string, std::optional<std::string>>&
          _partitionKeys = {},
      std::optional<int32_t> _tableBucketNumber = std::nullopt,
      const std::unordered_map<std::string, std::string>& _customSplitInfo = {},
      const std::shared_ptr<std::string>& _extraFileInfo = {},
      const std::unordered_map<std::string, std::string>& _serdeParameters = {},
      int64_t splitWeight = 0,
      bool cacheable = true,
      const std::unordered_map<std::string, std::string>& _infoColumns = {},
      std::optional<FileProperties> _properties = std::nullopt,
      std::optional<RowIdProperties> _rowIdProperties = std::nullopt,
      const std::optional<HiveBucketConversion>& _bucketConversion =
          std::nullopt)
      : ConnectorSplit(connectorId, splitWeight, cacheable),
        filePath(_filePath),
        fileFormat(_fileFormat),
        start(_start),
        length(_length),
        partitionKeys(_partitionKeys),
        tableBucketNumber(_tableBucketNumber),
        customSplitInfo(_customSplitInfo),
        extraFileInfo(_extraFileInfo),
        serdeParameters(_serdeParameters),
        infoColumns(_infoColumns),
        properties(_properties),
        rowIdProperties(_rowIdProperties),
        bucketConversion(_bucketConversion) {}

  ~HiveConnectorSplit() = default;

  uint64_t size() const override;

  std::string toString() const override;

  std::string getFileName() const;

  folly::dynamic serialize() const override;

  static std::shared_ptr<HiveConnectorSplit> create(const folly::dynamic& obj);

  static void registerSerDe();
};

class HiveConnectorSplitBuilder {
 public:
  explicit HiveConnectorSplitBuilder(std::string filePath)
      : filePath_{std::move(filePath)} {
    infoColumns_["$path"] = filePath_;
  }

  HiveConnectorSplitBuilder& start(uint64_t start) {
    start_ = start;
    return *this;
  }

  HiveConnectorSplitBuilder& length(uint64_t length) {
    length_ = length;
    return *this;
  }

  HiveConnectorSplitBuilder& splitWeight(int64_t splitWeight) {
    splitWeight_ = splitWeight;
    return *this;
  }

  HiveConnectorSplitBuilder& cacheable(bool cacheable) {
    cacheable_ = cacheable;
    return *this;
  }

  HiveConnectorSplitBuilder& fileFormat(dwio::common::FileFormat format) {
    fileFormat_ = format;
    return *this;
  }

  HiveConnectorSplitBuilder& infoColumn(
      const std::string& name,
      const std::string& value) {
    infoColumns_.emplace(std::move(name), std::move(value));
    return *this;
  }

  HiveConnectorSplitBuilder& partitionKey(
      std::string name,
      std::optional<std::string> value) {
    partitionKeys_.emplace(std::move(name), std::move(value));
    return *this;
  }

  HiveConnectorSplitBuilder& tableBucketNumber(int32_t bucket) {
    tableBucketNumber_ = bucket;
    infoColumns_["$bucket"] = std::to_string(bucket);
    return *this;
  }

  HiveConnectorSplitBuilder& bucketConversion(
      const HiveBucketConversion& bucketConversion) {
    bucketConversion_ = bucketConversion;
    return *this;
  }

  HiveConnectorSplitBuilder& customSplitInfo(
      const std::unordered_map<std::string, std::string>& customSplitInfo) {
    customSplitInfo_ = customSplitInfo;
    return *this;
  }

  HiveConnectorSplitBuilder& extraFileInfo(
      const std::shared_ptr<std::string>& extraFileInfo) {
    extraFileInfo_ = extraFileInfo;
    return *this;
  }

  HiveConnectorSplitBuilder& serdeParameters(
      const std::unordered_map<std::string, std::string>& serdeParameters) {
    serdeParameters_ = serdeParameters;
    return *this;
  }

  HiveConnectorSplitBuilder& connectorId(const std::string& connectorId) {
    connectorId_ = connectorId;
    return *this;
  }

  HiveConnectorSplitBuilder& fileProperties(FileProperties fileProperties) {
    fileProperties_ = fileProperties;
    return *this;
  }

  HiveConnectorSplitBuilder& rowIdProperties(
      const RowIdProperties& rowIdProperties) {
    rowIdProperties_ = rowIdProperties;
    return *this;
  }

  std::shared_ptr<connector::hive::HiveConnectorSplit> build() const {
    return std::make_shared<connector::hive::HiveConnectorSplit>(
        connectorId_,
        filePath_,
        fileFormat_,
        start_,
        length_,
        partitionKeys_,
        tableBucketNumber_,
        customSplitInfo_,
        extraFileInfo_,
        serdeParameters_,
        splitWeight_,
        cacheable_,
        infoColumns_,
        fileProperties_,
        rowIdProperties_,
        bucketConversion_);
  }

 private:
  const std::string filePath_;
  dwio::common::FileFormat fileFormat_{dwio::common::FileFormat::DWRF};
  uint64_t start_{0};
  uint64_t length_{std::numeric_limits<uint64_t>::max()};
  std::unordered_map<std::string, std::optional<std::string>> partitionKeys_;
  std::optional<int32_t> tableBucketNumber_;
  std::optional<HiveBucketConversion> bucketConversion_;
  std::unordered_map<std::string, std::string> customSplitInfo_ = {};
  std::shared_ptr<std::string> extraFileInfo_ = {};
  std::unordered_map<std::string, std::string> serdeParameters_ = {};
  std::unordered_map<std::string, std::string> infoColumns_ = {};
  std::string connectorId_;
  int64_t splitWeight_{0};
  bool cacheable_{true};
  std::optional<FileProperties> fileProperties_;
  std::optional<RowIdProperties> rowIdProperties_ = std::nullopt;
};

} // namespace facebook::velox::connector::hive
