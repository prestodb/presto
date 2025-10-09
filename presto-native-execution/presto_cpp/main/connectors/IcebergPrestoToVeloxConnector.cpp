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

#include "presto_cpp/main/connectors/IcebergPrestoToVeloxConnector.h"

#include "presto_cpp/presto_protocol/connector/iceberg/IcebergConnectorProtocol.h"
#include "velox/connectors/hive/iceberg/IcebergSplit.h"
#include "velox/type/fbhive/HiveTypeParser.h"

namespace facebook::presto {

namespace {

velox::connector::hive::iceberg::FileContent toVeloxFileContent(
    const presto::protocol::iceberg::FileContent content) {
  if (content == protocol::iceberg::FileContent::DATA) {
    return velox::connector::hive::iceberg::FileContent::kData;
  } else if (content == protocol::iceberg::FileContent::POSITION_DELETES) {
    return velox::connector::hive::iceberg::FileContent::kPositionalDeletes;
  }
  VELOX_UNSUPPORTED("Unsupported file content: {}", fmt::underlying(content));
}

velox::dwio::common::FileFormat toVeloxFileFormat(
    const presto::protocol::iceberg::FileFormat format) {
  if (format == protocol::iceberg::FileFormat::ORC) {
    return velox::dwio::common::FileFormat::ORC;
  } else if (format == protocol::iceberg::FileFormat::PARQUET) {
    return velox::dwio::common::FileFormat::PARQUET;
  }
  VELOX_UNSUPPORTED("Unsupported file format: {}", fmt::underlying(format));
}

} // namespace

std::unique_ptr<velox::connector::ConnectorSplit>
IcebergPrestoToVeloxConnector::toVeloxSplit(
    const protocol::ConnectorId& catalogId,
    const protocol::ConnectorSplit* connectorSplit,
    const protocol::SplitContext* splitContext) const {
  auto icebergSplit =
      dynamic_cast<const protocol::iceberg::IcebergSplit*>(connectorSplit);
  VELOX_CHECK_NOT_NULL(
      icebergSplit, "Unexpected split type {}", connectorSplit->_type);

  std::unordered_map<std::string, std::optional<std::string>> partitionKeys;
  for (const auto& entry : icebergSplit->partitionKeys) {
    partitionKeys.emplace(
        entry.second.name,
        entry.second.value == nullptr
            ? std::nullopt
            : std::optional<std::string>{*entry.second.value});
  }

  std::unordered_map<std::string, std::string> customSplitInfo;
  customSplitInfo["table_format"] = "hive-iceberg";

  std::vector<velox::connector::hive::iceberg::IcebergDeleteFile> deletes;
  deletes.reserve(icebergSplit->deletes.size());
  for (const auto& deleteFile : icebergSplit->deletes) {
    std::unordered_map<int32_t, std::string> lowerBounds(
        deleteFile.lowerBounds.begin(), deleteFile.lowerBounds.end());

    std::unordered_map<int32_t, std::string> upperBounds(
        deleteFile.upperBounds.begin(), deleteFile.upperBounds.end());

    velox::connector::hive::iceberg::IcebergDeleteFile icebergDeleteFile(
        toVeloxFileContent(deleteFile.content),
        deleteFile.path,
        toVeloxFileFormat(deleteFile.format),
        deleteFile.recordCount,
        deleteFile.fileSizeInBytes,
        std::vector(deleteFile.equalityFieldIds),
        lowerBounds,
        upperBounds);

    deletes.emplace_back(icebergDeleteFile);
  }

  std::unordered_map<std::string, std::string> infoColumns = {
      {"$data_sequence_number",
       std::to_string(icebergSplit->dataSequenceNumber)},
      {"$path", icebergSplit->path}};

  return std::make_unique<velox::connector::hive::iceberg::HiveIcebergSplit>(
      catalogId,
      icebergSplit->path,
      toVeloxFileFormat(icebergSplit->fileFormat),
      icebergSplit->start,
      icebergSplit->length,
      partitionKeys,
      std::nullopt,
      customSplitInfo,
      nullptr,
      splitContext->cacheable,
      deletes,
      infoColumns);
}

std::unique_ptr<velox::connector::ColumnHandle>
IcebergPrestoToVeloxConnector::toVeloxColumnHandle(
    const protocol::ColumnHandle* column,
    const TypeParser& typeParser) const {
  auto icebergColumn =
      dynamic_cast<const protocol::iceberg::IcebergColumnHandle*>(column);
  VELOX_CHECK_NOT_NULL(
      icebergColumn, "Unexpected column handle type {}", column->_type);
  // TODO(imjalpreet): Modify 'hiveType' argument of the 'HiveColumnHandle'
  //  constructor similar to how Hive Connector is handling for bucketing
  velox::type::fbhive::HiveTypeParser hiveTypeParser;
  auto type = stringToType(icebergColumn->type, typeParser);
  velox::connector::hive::HiveColumnHandle::ColumnParseParameters
      columnParseParameters;
  if (type->isDate()) {
    columnParseParameters.partitionDateValueFormat = velox::connector::hive::
        HiveColumnHandle::ColumnParseParameters::kDaysSinceEpoch;
  }
  return std::make_unique<velox::connector::hive::HiveColumnHandle>(
      icebergColumn->columnIdentity.name,
      toHiveColumnType(icebergColumn->columnType),
      type,
      type,
      toRequiredSubfields(icebergColumn->requiredSubfields),
      columnParseParameters);
}

std::unique_ptr<velox::connector::ConnectorTableHandle>
IcebergPrestoToVeloxConnector::toVeloxTableHandle(
    const protocol::TableHandle& tableHandle,
    const VeloxExprConverter& exprConverter,
    const TypeParser& typeParser,
    velox::connector::ColumnHandleMap& assignments) const {
  auto addSynthesizedColumn = [&](const std::string& name,
                                  protocol::hive::ColumnType columnType,
                                  const protocol::ColumnHandle& column) {
    if (toHiveColumnType(columnType) ==
        velox::connector::hive::HiveColumnHandle::ColumnType::kSynthesized) {
      if (assignments.count(name) == 0) {
        assignments.emplace(name, toVeloxColumnHandle(&column, typeParser));
      }
    }
  };

  auto icebergLayout = std::dynamic_pointer_cast<
      const protocol::iceberg::IcebergTableLayoutHandle>(
      tableHandle.connectorTableLayout);
  VELOX_CHECK_NOT_NULL(
      icebergLayout,
      "Unexpected layout type {}",
      tableHandle.connectorTableLayout->_type);

  for (const auto& entry : icebergLayout->partitionColumns) {
    assignments.emplace(
        entry.columnIdentity.name, toVeloxColumnHandle(&entry, typeParser));
  }

  // Add synthesized columns to the TableScanNode columnHandles as well.
  for (const auto& entry : icebergLayout->predicateColumns) {
    addSynthesizedColumn(entry.first, entry.second.columnType, entry.second);
  }

  auto icebergTableHandle =
      std::dynamic_pointer_cast<const protocol::iceberg::IcebergTableHandle>(
          tableHandle.connectorHandle);
  VELOX_CHECK_NOT_NULL(
      icebergTableHandle,
      "Unexpected table handle type {}",
      tableHandle.connectorHandle->_type);

  // Use fully qualified name if available.
  std::string tableName = icebergTableHandle->schemaName.empty()
      ? icebergTableHandle->icebergTableName.tableName
      : fmt::format(
            "{}.{}",
            icebergTableHandle->schemaName,
            icebergTableHandle->icebergTableName.tableName);

  return toHiveTableHandle(
      icebergLayout->domainPredicate,
      icebergLayout->remainingPredicate,
      icebergLayout->pushdownFilterEnabled,
      tableName,
      icebergLayout->dataColumns,
      tableHandle,
      {},
      exprConverter,
      typeParser);
}

std::unique_ptr<protocol::ConnectorProtocol>
IcebergPrestoToVeloxConnector::createConnectorProtocol() const {
  return std::make_unique<protocol::iceberg::IcebergConnectorProtocol>();
}

} // namespace facebook::presto
