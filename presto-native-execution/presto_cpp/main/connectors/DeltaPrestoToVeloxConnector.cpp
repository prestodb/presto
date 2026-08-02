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

#include "presto_cpp/main/connectors/DeltaPrestoToVeloxConnector.h"
#include "presto_cpp/main/connectors/PrestoToVeloxConnectorUtils.h"

#include <folly/String.h>
#include "presto_cpp/presto_protocol/connector/delta/DeltaConnectorProtocol.h"
#include "velox/connectors/hive/delta/HiveDeltaSplit.h"

namespace facebook::presto {

std::unique_ptr<velox::connector::ConnectorSplit>
DeltaPrestoToVeloxConnector::toVeloxSplit(
    const protocol::ConnectorId& catalogId,
    const protocol::ConnectorSplit* connectorSplit,
    const protocol::SplitContext* splitContext) const {
  auto deltaSplit =
      dynamic_cast<const protocol::delta::DeltaSplit*>(connectorSplit);
  VELOX_CHECK_NOT_NULL(
      deltaSplit, "Unexpected split type {}", connectorSplit->_type);

  // Convert partition values to the format expected by Velox
  // For Delta Lake, partition values should be in ISO8601 format for dates
  std::unordered_map<std::string, std::optional<std::string>> partitionKeys;
  for (const auto& entry : deltaSplit->partitionValues) {
    partitionKeys.emplace(
        entry.first,
        entry.second.empty() ? std::nullopt
                             : std::optional<std::string>{entry.second});
  }

  // Add Delta-specific metadata to custom split info
  std::unordered_map<std::string, std::string> customSplitInfo;
  customSplitInfo["table_format"] = "hive-delta";
  customSplitInfo["schema"] = deltaSplit->schemaName;
  customSplitInfo["table"] = deltaSplit->tableName;

  // Construct full file path from tableLocation and filePath
  // If the file path is already absolute (contains scheme), use it as is
  // Otherwise, combine table location with the relative file path
  std::string fullFilePath;
  const std::string& path = deltaSplit->filePath;
  if (path.find("://") != std::string::npos || path.starts_with("file:/")) {
    fullFilePath = path;
  } else {
    // Remove trailing slash from table location if present
    std::string tableLocation = deltaSplit->tableLocation;
    if (!tableLocation.empty() && tableLocation.back() == '/') {
      tableLocation.pop_back();
    }
    // Ensure file path starts with /
    std::string filePath = deltaSplit->filePath;
    if (!filePath.empty() && filePath.front() != '/') {
      filePath = "/" + filePath;
    }
    fullFilePath = tableLocation + filePath;
  }

  // Add info columns for Delta Lake metadata
  std::unordered_map<std::string, std::string> infoColumns = {
      {"$path", fullFilePath},
      {"$file_size", std::to_string(deltaSplit->fileSize)}};

  // Delta Lake uses Parquet by default
  auto fileFormat = velox::dwio::common::FileFormat::PARQUET;

  return std::make_unique<velox::connector::hive::delta::HiveDeltaSplit>(
      catalogId,
      fullFilePath,
      fileFormat,
      deltaSplit->start,
      deltaSplit->length,
      partitionKeys,
      std::nullopt,
      customSplitInfo,
      nullptr,
      splitContext->cacheable,
      infoColumns);
}

std::unique_ptr<velox::connector::ColumnHandle>
DeltaPrestoToVeloxConnector::toVeloxColumnHandle(
    const protocol::ColumnHandle* column,
    const TypeParser& typeParser) const {
  auto deltaColumn =
      dynamic_cast<const protocol::delta::DeltaColumnHandle*>(column);
  VELOX_CHECK_NOT_NULL(
      deltaColumn, "Unexpected column handle type {}", column->_type);

  auto type = stringToType(deltaColumn->dataType, typeParser);

  velox::connector::hive::HiveColumnHandle::ColumnParseParameters
      columnParseParameters;
  if (type->isDate()) {
    // Delta Lake stores date partition values in ISO8601 format (YYYY-MM-DD)
    columnParseParameters.partitionDateValueFormat = velox::connector::hive::
        HiveColumnHandle::ColumnParseParameters::kISO8601;
  }

  // Convert Delta column type to Hive column type
  // Note: The actual enum values will be available after protocol generation
  velox::connector::hive::HiveColumnHandle::ColumnType hiveColumnType =
      velox::connector::hive::HiveColumnHandle::ColumnType::kRegular;

  if (deltaColumn->columnType == protocol::delta::ColumnType::PARTITION) {
    hiveColumnType =
        velox::connector::hive::HiveColumnHandle::ColumnType::kPartitionKey;
  }

  // Convert subfield if present
  std::vector<velox::common::Subfield> requiredSubfields;
  if (deltaColumn->subfield) {
    requiredSubfields.push_back(
        velox::common::Subfield(*deltaColumn->subfield));
  }

  // Use physicalName if present (for column mapping), otherwise use logical name
  std::string columnName = (deltaColumn->physicalName && !deltaColumn->physicalName->empty())
      ? *deltaColumn->physicalName
      : deltaColumn->name;

  return std::unique_ptr<velox::connector::ColumnHandle>(
      new velox::connector::hive::HiveColumnHandle(
          columnName,
          hiveColumnType,
          type,
          type,
          std::move(requiredSubfields),
          columnParseParameters,
          {})); // empty postProcessor
}

std::unique_ptr<velox::connector::ConnectorTableHandle>
DeltaPrestoToVeloxConnector::toVeloxTableHandle(
    const protocol::TableHandle& tableHandle,
    const VeloxExprConverter& exprConverter,
    const TypeParser& typeParser) const {
  // Note: After protocol generation, cast to DeltaTableLayoutHandle
  // For now, we'll work with the basic table handle structure

  auto deltaTableHandle =
      std::dynamic_pointer_cast<const protocol::delta::DeltaTableHandle>(
          tableHandle.connectorHandle);
  VELOX_CHECK_NOT_NULL(
      deltaTableHandle,
      "Unexpected table handle type {}",
      tableHandle.connectorHandle->_type);

  // Use fully qualified table name
  std::string tableName = fmt::format(
      "{}.{}",
      deltaTableHandle->deltaTable.schemaName,
      deltaTableHandle->deltaTable.tableName);

  // Build column handles from Delta table columns
  std::vector<velox::connector::hive::HiveColumnHandlePtr> columnHandles;
  for (const auto& deltaColumn : deltaTableHandle->deltaTable.columns) {
    auto type = stringToType(deltaColumn.type, typeParser);

    velox::connector::hive::HiveColumnHandle::ColumnParseParameters
        columnParseParameters;
    if (type->isDate()) {
      columnParseParameters.partitionDateValueFormat = velox::connector::hive::
          HiveColumnHandle::ColumnParseParameters::kISO8601;
    }

    velox::connector::hive::HiveColumnHandle::ColumnType hiveColumnType =
        deltaColumn.partition
        ? velox::connector::hive::HiveColumnHandle::ColumnType::kPartitionKey
        : velox::connector::hive::HiveColumnHandle::ColumnType::kRegular;

    // Use physicalName if present (for column mapping), otherwise use logical name
    std::string columnName = (deltaColumn.physicalName && !deltaColumn.physicalName->empty())
        ? *deltaColumn.physicalName
        : deltaColumn.logicalName;

    columnHandles.emplace_back(
        std::make_shared<velox::connector::hive::HiveColumnHandle>(
            columnName,
            hiveColumnType,
            type,
            type,
            std::vector<velox::common::Subfield>{},
            columnParseParameters));
  }

  // Build dataColumns from columnHandles, excluding partition columns.
  // This matches Hive's behavior where dataColumns only contains non-partition
  // columns that are actually stored in the data files. Partition columns are
  // handled separately as constants during reading.
  velox::RowTypePtr dataColumns;
  if (!columnHandles.empty()) {
    std::vector<std::string> names;
    std::vector<velox::TypePtr> types;
    names.reserve(columnHandles.size());
    types.reserve(columnHandles.size());

    // Add only non-partition columns (regular columns that exist in data files)
    for (const auto& columnHandle : columnHandles) {
      // Skip partition columns - they're not in the data files
      if (columnHandle->columnType() ==
          velox::connector::hive::HiveColumnHandle::ColumnType::kPartitionKey) {
        continue;
      }

      // For Delta, the column name should be consistent with
      // names in Delta manifest file. The names in Delta
      // manifest file are consistent with the field names in
      // parquet data file.
      names.emplace_back(columnHandle->name());
      auto type = columnHandle->hiveType() ? columnHandle->hiveType()
                                           : columnHandle->dataType();
      // The type from the metastore may have upper case letters
      // in field names, convert them all to lower case to be
      // compatible with Presto.
      types.push_back(VELOX_DYNAMIC_TYPE_DISPATCH(
          fieldNamesToLowerCase, type->kind(), type));
    }

    if (!names.empty()) {
      dataColumns = ROW(std::move(names), std::move(types));
    }
  }

  // Create basic table handle without predicates for now
  // TODO: After protocol generation, extract predicates from
  // DeltaTableLayoutHandle
  return std::make_unique<velox::connector::hive::HiveTableHandle>(
      tableHandle.connectorId,
      tableName,
      velox::common::SubfieldFilters{}, // subfieldFilters
      nullptr, // remainingFilter
      dataColumns, // dataColumns
      std::unordered_map<std::string, std::string>{}, // tableParameters
      columnHandles); // filterColumnHandles
}

std::unique_ptr<protocol::ConnectorProtocol>
DeltaPrestoToVeloxConnector::createConnectorProtocol() const {
  return std::make_unique<protocol::delta::DeltaConnectorProtocol>();
}

std::vector<velox::connector::hive::HiveColumnHandlePtr>
DeltaPrestoToVeloxConnector::toHiveColumns(
    const protocol::List<protocol::delta::DeltaColumnHandle>& inputColumns,
    const TypeParser& typeParser) const {
  std::vector<velox::connector::hive::HiveColumnHandlePtr> hiveColumns;
  hiveColumns.reserve(inputColumns.size());
  for (const auto& columnHandle : inputColumns) {
    hiveColumns.emplace_back(
        std::dynamic_pointer_cast<velox::connector::hive::HiveColumnHandle>(
            std::shared_ptr(toVeloxColumnHandle(&columnHandle, typeParser))));
  }
  return hiveColumns;
}

} // namespace facebook::presto
