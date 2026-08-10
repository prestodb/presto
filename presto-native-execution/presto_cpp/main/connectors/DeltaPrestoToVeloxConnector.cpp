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

#include "presto_cpp/presto_protocol/connector/delta/DeltaConnectorProtocol.h"
#include "velox/connectors/hive/delta/HiveDeltaSplit.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"

namespace facebook::presto {

namespace {

/// Returns the name the data files use for a column. Delta column mapping
/// stores a column under a physical name that differs from its logical (table)
/// name, in which case the reader must be given the physical name. Mirrors
/// DeltaColumnHandle.getSourceName() on the coordinator.
const std::string& sourceName(
    const std::shared_ptr<std::string>& physicalName,
    const std::string& logicalName) {
  return (physicalName && !physicalName->empty()) ? *physicalName : logicalName;
}

/// Builds the Hive column handle Delta reads a column through. Shared by the
/// column handle and table handle conversions so both agree on the column
/// name, the column type and the partition date format.
std::unique_ptr<velox::connector::hive::HiveColumnHandle> makeHiveColumnHandle(
    const std::string& name,
    bool isPartitionKey,
    const velox::TypePtr& type,
    std::vector<velox::common::Subfield> requiredSubfields = {}) {
  velox::connector::hive::HiveColumnHandle::ColumnParseParameters
      columnParseParameters;
  if (type->isDate()) {
    // Delta Lake stores date partition values in ISO8601 format (YYYY-MM-DD).
    columnParseParameters.partitionDateValueFormat = velox::connector::hive::
        HiveColumnHandle::ColumnParseParameters::kISO8601;
  }

  return std::make_unique<velox::connector::hive::HiveColumnHandle>(
      name,
      isPartitionKey
          ? velox::connector::hive::HiveColumnHandle::ColumnType::kPartitionKey
          : velox::connector::hive::HiveColumnHandle::ColumnType::kRegular,
      type,
      type,
      std::move(requiredSubfields),
      columnParseParameters);
}

/// A domain can only become a reader filter when the value the reader decodes
/// compares the way the coordinator's domain does. Timestamps are excluded:
/// the reader adjusts them to the session timezone while decoding
/// (legacy_timestamp) and TIMESTAMP WITH TIME ZONE is a packed value, so a
/// raw-value range filter would not match Presto's semantics. Complex types
/// carry no range domain to begin with.
bool canFilterOnFileValues(const velox::TypePtr& type) {
  return type->isPrimitiveType() && !type->isTimestamp() &&
      !velox::isTimestampWithTimeZoneType(type);
}

/// Converts the layout's domain predicate into reader filters. The Delta
/// predicate is keyed by column handle rather than by subfield (unlike Hive and
/// Iceberg), so the filter key is the column's source name.
///
/// The coordinator enforces the partition part of the predicate when it
/// generates splits and leaves the rest as an unenforced constraint, i.e. as a
/// filter above the scan (see DeltaMetadata.getTableLayoutForConstraint), so
/// this pushdown only lets the reader skip row groups and rows earlier. Entries
/// that cannot be mapped to a file column are therefore safe to drop:
///  - partition columns: already enforced by split generation;
///  - pushed-down subfields: the path is expressed in logical names and column
///    mapping renames every level, so there is no physical path to key on;
///  - columns whose file values are not directly comparable, see
///    canFilterOnFileValues().
velox::common::SubfieldFilters toSubfieldFilters(
    const protocol::TupleDomain<protocol::delta::DeltaColumnHandle>& predicate,
    const VeloxExprConverter& exprConverter,
    const TypeParser& typeParser) {
  velox::common::SubfieldFilters subfieldFilters;
  if (predicate.domains == nullptr) {
    return subfieldFilters;
  }

  for (const auto& [column, domain] : *predicate.domains) {
    if (column.columnType != protocol::delta::ColumnType::REGULAR) {
      continue;
    }
    if (!canFilterOnFileValues(stringToType(column.dataType, typeParser))) {
      continue;
    }
    subfieldFilters[velox::common::Subfield(
        sourceName(column.physicalName, column.name))] =
        toFilter(domain, exprConverter, typeParser);
  }
  return subfieldFilters;
}

} // namespace

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

  std::vector<velox::common::Subfield> requiredSubfields;
  if (deltaColumn->subfield) {
    requiredSubfields.emplace_back(*deltaColumn->subfield);
  }

  return makeHiveColumnHandle(
      sourceName(deltaColumn->physicalName, deltaColumn->name),
      deltaColumn->columnType == protocol::delta::ColumnType::PARTITION,
      type,
      std::move(requiredSubfields));
}

std::unique_ptr<velox::connector::ConnectorTableHandle>
DeltaPrestoToVeloxConnector::toVeloxTableHandle(
    const protocol::TableHandle& tableHandle,
    const VeloxExprConverter& exprConverter,
    const TypeParser& typeParser) const {
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
    columnHandles.emplace_back(makeHiveColumnHandle(
        sourceName(deltaColumn.physicalName, deltaColumn.logicalName),
        deltaColumn.partition,
        stringToType(deltaColumn.type, typeParser)));
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

  // Push the layout's domain predicate into the reader so it can skip row
  // groups and rows. The layout is absent for plans that do not go through
  // getTableLayoutForConstraint, in which case there is nothing to push down.
  velox::common::SubfieldFilters subfieldFilters;
  if (tableHandle.connectorTableLayout != nullptr) {
    auto deltaLayout = std::dynamic_pointer_cast<
        const protocol::delta::DeltaTableLayoutHandle>(
        tableHandle.connectorTableLayout);
    VELOX_CHECK_NOT_NULL(
        deltaLayout,
        "Unexpected table layout type {}",
        tableHandle.connectorTableLayout->_type);
    subfieldFilters =
        toSubfieldFilters(deltaLayout->predicate, exprConverter, typeParser);
  }

  return std::make_unique<velox::connector::hive::HiveTableHandle>(
      tableHandle.connectorId,
      tableName,
      std::move(subfieldFilters),
      nullptr, // remainingFilter: Delta has no remaining predicate on the wire;
               // it stays in the plan as a filter above the scan.
      dataColumns, // dataColumns
      std::unordered_map<std::string, std::string>{}, // tableParameters
      columnHandles); // filterColumnHandles
}

std::unique_ptr<protocol::ConnectorProtocol>
DeltaPrestoToVeloxConnector::createConnectorProtocol() const {
  return std::make_unique<protocol::delta::DeltaConnectorProtocol>();
}

} // namespace facebook::presto
