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
#include "velox/dwio/common/Options.h"

namespace facebook::velox::connector::hive {

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
  std::unordered_map<std::string, std::string> serdeParameters;

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
      const std::unordered_map<std::string, std::string>& _serdeParameters = {})
      : ConnectorSplit(connectorId),
        filePath(_filePath),
        fileFormat(_fileFormat),
        start(_start),
        length(_length),
        partitionKeys(_partitionKeys),
        tableBucketNumber(_tableBucketNumber),
        customSplitInfo(_customSplitInfo),
        extraFileInfo(_extraFileInfo),
        serdeParameters(_serdeParameters) {}

  std::string toString() const override {
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

  std::string getFileName() const {
    auto i = filePath.rfind('/');
    return i == std::string::npos ? filePath : filePath.substr(i + 1);
  }
};

} // namespace facebook::velox::connector::hive
