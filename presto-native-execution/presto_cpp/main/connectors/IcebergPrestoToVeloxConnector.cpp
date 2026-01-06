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
#include "presto_cpp/main/connectors/PrestoToVeloxConnectorUtils.h"

#include "presto_cpp/presto_protocol/connector/iceberg/IcebergConnectorProtocol.h"
#include "velox/connectors/hive/iceberg/IcebergDataSink.h"
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

std::unique_ptr<velox::connector::ConnectorTableHandle> toIcebergTableHandle(
    const protocol::TupleDomain<protocol::Subfield>& domainPredicate,
    const std::shared_ptr<protocol::RowExpression>& remainingPredicate,
    bool isPushdownFilterEnabled,
    const std::string& tableName,
    const protocol::List<protocol::Column>& dataColumns,
    const protocol::TableHandle& tableHandle,
    const std::vector<velox::connector::hive::HiveColumnHandlePtr>&
        columnHandles,
    const VeloxExprConverter& exprConverter,
    const TypeParser& typeParser) {
  velox::common::SubfieldFilters subfieldFilters;
  auto domains = domainPredicate.domains;
  for (const auto& domain : *domains) {
    auto filter = domain.second;
    subfieldFilters[velox::common::Subfield(domain.first)] =
        toFilter(domain.second, exprConverter, typeParser);
  }

  auto remainingFilter = exprConverter.toVeloxExpr(remainingPredicate);
  if (auto constant =
          std::dynamic_pointer_cast<const velox::core::ConstantTypedExpr>(
              remainingFilter)) {
    bool value = constant->value().value<bool>();
    VELOX_CHECK(value, "Unexpected always-false remaining predicate");

    // Use null for always-true filter.
    remainingFilter = nullptr;
  }

  velox::RowTypePtr finalDataColumns;
  if (!dataColumns.empty()) {
    std::vector<std::string> names;
    std::vector<velox::TypePtr> types;
    velox::type::fbhive::HiveTypeParser hiveTypeParser;
    names.reserve(dataColumns.size());
    types.reserve(dataColumns.size());
    for (auto& column : dataColumns) {
      // For iceberg, the column name should be consistent with
      // names in iceberg manifest file. The names in iceberg
      // manifest file are consistent with the field names in
      // parquet data file.
      names.emplace_back(column.name);
      auto parsedType = hiveTypeParser.parse(column.type);
      // The type from the metastore may have upper case letters
      // in field names, convert them all to lower case to be
      // compatible with Presto.
      types.push_back(VELOX_DYNAMIC_TYPE_DISPATCH(
          fieldNamesToLowerCase, parsedType->kind(), parsedType));
    }
    finalDataColumns = ROW(std::move(names), std::move(types));
  }

  return std::make_unique<velox::connector::hive::HiveTableHandle>(
      tableHandle.connectorId,
      tableName,
      isPushdownFilterEnabled,
      std::move(subfieldFilters),
      remainingFilter,
      finalDataColumns,
      std::unordered_map<std::string, std::string>{},
      columnHandles);
}

velox::connector::hive::iceberg::IcebergPartitionSpec::Field
toVeloxIcebergPartitionField(
    const protocol::iceberg::IcebergPartitionField& field,
    const TypeParser& typeParser,
    const protocol::iceberg::PrestoIcebergSchema& schema) {
  std::string type;
  for (const auto& column : schema.columns) {
    if (column.name == field.name) {
      type = column.prestoType;
      break;
    }
  }

  VELOX_USER_CHECK(
      !type.empty(),
      "Partition column not found in table schema: {}",
      field.name);

  return velox::connector::hive::iceberg::IcebergPartitionSpec::Field{
      field.name,
      stringToType(type, typeParser),
      static_cast<velox::connector::hive::iceberg::TransformType>(
          field.transform),
      field.parameter ? *field.parameter : std::optional<int32_t>()};
}

std::unique_ptr<velox::connector::hive::iceberg::IcebergPartitionSpec>
toVeloxIcebergPartitionSpec(
    const protocol::iceberg::PrestoIcebergPartitionSpec& spec,
    const TypeParser& typeParser) {
  std::vector<velox::connector::hive::iceberg::IcebergPartitionSpec::Field>
      fields;
  fields.reserve(spec.fields.size());
  for (const auto& field : spec.fields) {
    fields.emplace_back(
        toVeloxIcebergPartitionField(field, typeParser, spec.schema));
  }
  return std::make_unique<
      velox::connector::hive::iceberg::IcebergPartitionSpec>(
      spec.specId, fields);
}

velox::parquet::ParquetFieldId toParquetField(
    const protocol::iceberg::ColumnIdentity& column) {
  std::vector<velox::parquet::ParquetFieldId> children;
  if (!column.children.empty()) {
    children.reserve(column.children.size());
    for (const auto& child : column.children) {
      children.push_back(toParquetField(child));
    }
  }
  return velox::parquet::ParquetFieldId{column.id, std::move(children)};
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

  return std::make_unique<velox::connector::hive::iceberg::IcebergColumnHandle>(
      icebergColumn->columnIdentity.name,
      toHiveColumnType(icebergColumn->columnType),
      type,
      toParquetField(icebergColumn->columnIdentity),
      toRequiredSubfields(icebergColumn->requiredSubfields));
}

std::unique_ptr<velox::connector::ConnectorTableHandle>
IcebergPrestoToVeloxConnector::toVeloxTableHandle(
    const protocol::TableHandle& tableHandle,
    const VeloxExprConverter& exprConverter,
    const TypeParser& typeParser) const {
  auto icebergLayout = std::dynamic_pointer_cast<
      const protocol::iceberg::IcebergTableLayoutHandle>(
      tableHandle.connectorTableLayout);
  VELOX_CHECK_NOT_NULL(
      icebergLayout,
      "Unexpected layout type {}",
      tableHandle.connectorTableLayout->_type);

  std::unordered_set<std::string> columnNames;
  std::vector<velox::connector::hive::HiveColumnHandlePtr> columnHandles;
  for (const auto& entry : icebergLayout->partitionColumns) {
    if (columnNames.emplace(entry.columnIdentity.name).second) {
      columnHandles.emplace_back(
          std::dynamic_pointer_cast<
              const velox::connector::hive::HiveColumnHandle>(
              std::shared_ptr(toVeloxColumnHandle(&entry, typeParser))));
    }
  }

  // Add synthesized columns to the TableScanNode columnHandles as well.
  for (const auto& entry : icebergLayout->predicateColumns) {
    if (columnNames.emplace(entry.second.columnIdentity.name).second) {
      columnHandles.emplace_back(
          std::dynamic_pointer_cast<
              const velox::connector::hive::HiveColumnHandle>(
              std::shared_ptr(toVeloxColumnHandle(&entry.second, typeParser))));
    }
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

  return toIcebergTableHandle(
      icebergLayout->domainPredicate,
      icebergLayout->remainingPredicate,
      icebergLayout->pushdownFilterEnabled,
      tableName,
      icebergLayout->dataColumns,
      tableHandle,
      columnHandles,
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
    const TypeParser& typeParser) const {
  auto icebergOutputTableHandle =
      std::dynamic_pointer_cast<protocol::iceberg::IcebergOutputTableHandle>(
          createHandle->handle.connectorHandle);

  VELOX_CHECK_NOT_NULL(
      icebergOutputTableHandle,
      "Unexpected output table handle type {}",
      createHandle->handle.connectorHandle->_type);

  const auto inputColumns =
      toIcebergColumns(icebergOutputTableHandle->inputColumns, typeParser);

  return std::make_unique<
      velox::connector::hive::iceberg::IcebergInsertTableHandle>(
      inputColumns,
      std::make_shared<velox::connector::hive::LocationHandle>(
          fmt::format("{}/data", icebergOutputTableHandle->outputPath),
          fmt::format("{}/data", icebergOutputTableHandle->outputPath),
          velox::connector::hive::LocationHandle::TableType::kNew),
      toVeloxFileFormat(icebergOutputTableHandle->fileFormat),
      toVeloxIcebergPartitionSpec(
          icebergOutputTableHandle->partitionSpec, typeParser),
      std::optional(
          toFileCompressionKind(icebergOutputTableHandle->compressionCodec)));
}

std::unique_ptr<velox::connector::ConnectorInsertTableHandle>
IcebergPrestoToVeloxConnector::toVeloxInsertTableHandle(
    const protocol::InsertHandle* insertHandle,
    const TypeParser& typeParser) const {
  auto icebergInsertTableHandle =
      std::dynamic_pointer_cast<protocol::iceberg::IcebergInsertTableHandle>(
          insertHandle->handle.connectorHandle);

  VELOX_CHECK_NOT_NULL(
      icebergInsertTableHandle,
      "Unexpected insert table handle type {}",
      insertHandle->handle.connectorHandle->_type);

  const auto inputColumns =
      toIcebergColumns(icebergInsertTableHandle->inputColumns, typeParser);

  return std::make_unique<
      velox::connector::hive::iceberg::IcebergInsertTableHandle>(
      inputColumns,
      std::make_shared<velox::connector::hive::LocationHandle>(
          fmt::format("{}/data", icebergInsertTableHandle->outputPath),
          fmt::format("{}/data", icebergInsertTableHandle->outputPath),
          velox::connector::hive::LocationHandle::TableType::kExisting),
      toVeloxFileFormat(icebergInsertTableHandle->fileFormat),
      toVeloxIcebergPartitionSpec(
          icebergInsertTableHandle->partitionSpec, typeParser),
      std::optional(
          toFileCompressionKind(icebergInsertTableHandle->compressionCodec)));
}

std::vector<velox::connector::hive::iceberg::IcebergColumnHandlePtr>
IcebergPrestoToVeloxConnector::toIcebergColumns(
    const protocol::List<protocol::iceberg::IcebergColumnHandle>& inputColumns,
    const TypeParser& typeParser) const {
  std::vector<velox::connector::hive::iceberg::IcebergColumnHandlePtr>
      icebergColumns;
  icebergColumns.reserve(inputColumns.size());
  for (const auto& columnHandle : inputColumns) {
    icebergColumns.emplace_back(
        std::dynamic_pointer_cast<
            velox::connector::hive::iceberg::IcebergColumnHandle>(
            std::shared_ptr(toVeloxColumnHandle(&columnHandle, typeParser))));
  }
  return icebergColumns;
}

} // namespace facebook::presto
