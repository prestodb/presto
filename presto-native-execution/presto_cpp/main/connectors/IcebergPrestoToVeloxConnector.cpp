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

using namespace velox;

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

  std::function<connector::hive::iceberg::IcebergNestedField(
      const protocol::iceberg::ColumnIdentity*)>
      collectNestedField = [&](const protocol::iceberg::ColumnIdentity* column)
      -> connector::hive::iceberg::IcebergNestedField {
    std::vector<connector::hive::iceberg::IcebergNestedField> children;
    if (!column->children.empty()) {
      children.reserve(column->children.size());
      for (const auto& child : column->children) {
        children.push_back(collectNestedField(&child));
      }
    }
    auto type = stringToType(icebergColumn->type, typeParser);
    return connector::hive::iceberg::IcebergNestedField(column->id, children);
  };

  auto nestedField = collectNestedField(&icebergColumn->columnIdentity);

  return std::make_unique<connector::hive::iceberg::IcebergColumnHandle>(
      icebergColumn->columnIdentity.name,
      toHiveColumnType(icebergColumn->columnType),
      type,
      type,
      nestedField,
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

std::unique_ptr<velox::connector::ConnectorInsertTableHandle>
IcebergPrestoToVeloxConnector::toVeloxInsertTableHandle(
    const protocol::CreateHandle* createHandle,
    const TypeParser& typeParser,
    velox::memory::MemoryPool* pool) const {
  auto icebergOutputTableHandle =
      std::dynamic_pointer_cast<protocol::iceberg::IcebergOutputTableHandle>(
          createHandle->handle.connectorHandle);

  VELOX_CHECK_NOT_NULL(
      icebergOutputTableHandle,
      "Unexpected output table handle type {}",
      createHandle->handle.connectorHandle->_type);

  const auto inputColumns =
      toIcebergColumns(icebergOutputTableHandle->inputColumns, typeParser);

  auto sortedBy = toIcebergSortingColumns(
      icebergOutputTableHandle->sortOrder, icebergOutputTableHandle->schema);

  return std::make_unique<
      velox::connector::hive::iceberg::IcebergInsertTableHandle>(
      inputColumns,
      std::make_shared<connector::hive::LocationHandle>(
          fmt::format("{}/data", icebergOutputTableHandle->outputPath),
          fmt::format("{}/data", icebergOutputTableHandle->outputPath),
          connector::hive::LocationHandle::TableType::kNew),
      toVeloxIcebergPartitionSpec(
          icebergOutputTableHandle->partitionSpec, typeParser),
      pool,
      toVeloxFileFormat(icebergOutputTableHandle->fileFormat),
      std::move(sortedBy),
      std::optional(
          toFileCompressionKind(icebergOutputTableHandle->compressionCodec)));
}

std::vector<velox::connector::hive::iceberg::IcebergSortingColumn>
IcebergPrestoToVeloxConnector::toIcebergSortingColumns(
    protocol::List<protocol::iceberg::SortField> sortFields,
    const protocol::iceberg::PrestoIcebergSchema& schema) const {
  std::vector<connector::hive::iceberg::IcebergSortingColumn> sortedBy;
  sortedBy.reserve(sortFields.size());
  for (const auto& sortField : sortFields) {
    velox::core::SortOrder veloxSortOrder(
        sortField.sortOrder == protocol::SortOrder::ASC_NULLS_LAST ||
            sortField.sortOrder == protocol::SortOrder::ASC_NULLS_FIRST,
        sortField.sortOrder == protocol::SortOrder::DESC_NULLS_FIRST ||
            sortField.sortOrder == protocol::SortOrder::ASC_NULLS_FIRST);

    for (const auto& column : schema.columns) {
      if (column.id == sortField.sourceColumnId) {
        sortedBy.emplace_back(
            connector::hive::iceberg::IcebergSortingColumn(
                column.name, veloxSortOrder));
        break;
      }
    }
  }
  return sortedBy;
}

std::unique_ptr<velox::connector::ConnectorInsertTableHandle>
IcebergPrestoToVeloxConnector::toVeloxInsertTableHandle(
    const protocol::InsertHandle* insertHandle,
    const TypeParser& typeParser,
    velox::memory::MemoryPool* pool) const {
  auto icebergInsertTableHandle =
      std::dynamic_pointer_cast<protocol::iceberg::IcebergInsertTableHandle>(
          insertHandle->handle.connectorHandle);

  VELOX_CHECK_NOT_NULL(
      icebergInsertTableHandle,
      "Unexpected insert table handle type {}",
      insertHandle->handle.connectorHandle->_type);

  const auto inputColumns =
      toIcebergColumns(icebergInsertTableHandle->inputColumns, typeParser);

  auto sortedBy = toIcebergSortingColumns(
      icebergInsertTableHandle->sortOrder, icebergInsertTableHandle->schema);

  return std::make_unique<connector::hive::iceberg::IcebergInsertTableHandle>(
      inputColumns,
      std::make_shared<connector::hive::LocationHandle>(
          fmt::format("{}/data", icebergInsertTableHandle->outputPath),
          fmt::format("{}/data", icebergInsertTableHandle->outputPath),
          connector::hive::LocationHandle::TableType::kExisting),
      toVeloxIcebergPartitionSpec(
          icebergInsertTableHandle->partitionSpec, typeParser),
      pool,
      toVeloxFileFormat(icebergInsertTableHandle->fileFormat),
      std::move(sortedBy),
      std::optional(
          toFileCompressionKind(icebergInsertTableHandle->compressionCodec)));
}

std::vector<
    std::shared_ptr<const connector::hive::iceberg::IcebergColumnHandle>>
IcebergPrestoToVeloxConnector::toIcebergColumns(
    const protocol::List<protocol::iceberg::IcebergColumnHandle>& inputColumns,
    const TypeParser& typeParser) const {
  std::vector<
      std::shared_ptr<const connector::hive::iceberg::IcebergColumnHandle>>
      icebergColumns;
  icebergColumns.reserve(inputColumns.size());
  for (const auto& columnHandle : inputColumns) {
    icebergColumns.emplace_back(
        std::dynamic_pointer_cast<
            connector::hive::iceberg::IcebergColumnHandle>(
            std::shared_ptr(toVeloxColumnHandle(&columnHandle, typeParser))));
  }
  return icebergColumns;
}

connector::hive::iceberg::IcebergPartitionSpec::Field
IcebergPrestoToVeloxConnector::toVeloxIcebergPartitionField(
    const protocol::iceberg::IcebergPartitionField& field,
    const facebook::presto::TypeParser& typeParser,
    const protocol::iceberg::PrestoIcebergSchema& schema) const {
  std::string type;
  for (const auto& column : schema.columns) {
    if (column.name == field.name) {
      type = column.prestoType;
    }
  }
  return connector::hive::iceberg::IcebergPartitionSpec::Field(
      field.name,
      stringToType(type, typeParser),
      static_cast<connector::hive::iceberg::TransformType>(field.transform),
      field.parameter ? *field.parameter : std::optional<int32_t>());
}

std::unique_ptr<velox::connector::hive::iceberg::IcebergPartitionSpec>
IcebergPrestoToVeloxConnector::toVeloxIcebergPartitionSpec(
    const protocol::iceberg::PrestoIcebergPartitionSpec& spec,
    const facebook::presto::TypeParser& typeParser) const {
  std::vector<connector::hive::iceberg::IcebergPartitionSpec::Field> fields;
  fields.reserve(spec.fields.size());
  for (auto field : spec.fields) {
    fields.emplace_back(
        toVeloxIcebergPartitionField(field, typeParser, spec.schema));
  }
  return std::make_unique<connector::hive::iceberg::IcebergPartitionSpec>(
      spec.specId, fields);
}

} // namespace facebook::presto
