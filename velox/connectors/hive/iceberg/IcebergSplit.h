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

#include <string>

#include "velox/connectors/hive/HiveConnectorSplit.h"

namespace facebook::velox::connector::hive::iceberg {

class IcebergDeleteFile;

struct HiveIcebergSplit : public connector::hive::HiveConnectorSplit {
  std::vector<IcebergDeleteFile> deleteFiles;

  HiveIcebergSplit(
      const std::string& connectorId,
      const std::string& _filePath,
      dwio::common::FileFormat _fileFormat,
      uint64_t _start = 0,
      uint64_t _length = std::numeric_limits<uint64_t>::max(),
      const std::unordered_map<std::string, std::optional<std::string>>&
          _partitionKeys = {},
      std::optional<int32_t> _tableBucketNumber = std::nullopt,
      const std::unordered_map<std::string, std::string>& _customSplitInfo = {},
      const std::shared_ptr<std::string>& _extraFileInfo = {});

  // For tests only
  HiveIcebergSplit(
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
      std::vector<IcebergDeleteFile> deletes = {});
};

} // namespace facebook::velox::connector::hive::iceberg
