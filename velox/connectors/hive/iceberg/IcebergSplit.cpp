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

#include "velox/connectors/hive/iceberg/IcebergSplit.h"

#include "velox/connectors/hive/iceberg/IcebergDeleteFile.h"

namespace facebook::velox::connector::hive::iceberg {

HiveIcebergSplit::HiveIcebergSplit(
    const std::string& _connectorId,
    const std::string& _filePath,
    dwio::common::FileFormat _fileFormat,
    uint64_t _start,
    uint64_t _length,
    const std::unordered_map<std::string, std::optional<std::string>>&
        _partitionKeys,
    std::optional<int32_t> _tableBucketNumber,
    const std::unordered_map<std::string, std::string>& _customSplitInfo,
    const std::shared_ptr<std::string>& _extraFileInfo,
    const std::unordered_map<std::string, std::string>& _infoColumns,
    std::optional<FileProperties> _properties)
    : HiveConnectorSplit(
          _connectorId,
          _filePath,
          _fileFormat,
          _start,
          _length,
          _partitionKeys,
          _tableBucketNumber,
          _customSplitInfo,
          _extraFileInfo,
          {},
          0,
          _infoColumns,
          _properties) {
  // TODO: Deserialize _extraFileInfo to get deleteFiles;
}

// For tests only
HiveIcebergSplit::HiveIcebergSplit(
    const std::string& _connectorId,
    const std::string& _filePath,
    dwio::common::FileFormat _fileFormat,
    uint64_t _start,
    uint64_t _length,
    const std::unordered_map<std::string, std::optional<std::string>>&
        _partitionKeys,
    std::optional<int32_t> _tableBucketNumber,
    const std::unordered_map<std::string, std::string>& _customSplitInfo,
    const std::shared_ptr<std::string>& _extraFileInfo,
    std::vector<IcebergDeleteFile> _deletes,
    const std::unordered_map<std::string, std::string>& _infoColumns,
    std::optional<FileProperties> _properties)
    : HiveConnectorSplit(
          _connectorId,
          _filePath,
          _fileFormat,
          _start,
          _length,
          _partitionKeys,
          _tableBucketNumber,
          _customSplitInfo,
          _extraFileInfo,
          {},
          0,
          _infoColumns,
          _properties),
      deleteFiles(_deletes) {}
} // namespace facebook::velox::connector::hive::iceberg
