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

#include "presto_cpp/main/connectors/HivePrestoToVeloxConnector.h"

#include "presto_cpp/main/connectors/PrestoToVeloxConnectorUtils.h"
#include "presto_cpp/main/types/PrestoToVeloxExpr.h"
#include "presto_cpp/main/types/TypeParser.h"
#include "presto_cpp/presto_protocol/connector/hive/HiveConnectorProtocol.h"

#include <velox/type/fbhive/HiveTypeParser.h>
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/hive/HiveDataSink.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/type/Filter.h"

namespace facebook::presto {
using namespace velox;

namespace {

connector::hive::LocationHandle::TableType toTableType(
    protocol::hive::TableType tableType) {
  switch (tableType) {
    case protocol::hive::TableType::NEW:
    // Temporary tables are written and read by the SPI in a single pipeline.
    // So they can be treated as New. They do not require Append or Overwrite
    // semantics as applicable for regular tables.
    case protocol::hive::TableType::TEMPORARY:
      return connector::hive::LocationHandle::TableType::kNew;
    case protocol::hive::TableType::EXISTING:
      return connector::hive::LocationHandle::TableType::kExisting;
    default:
      VELOX_UNSUPPORTED("Unsupported table type: {}.", toJsonString(tableType));
  }
}

std::shared_ptr<connector::hive::LocationHandle> toLocationHandle(
    const protocol::hive::LocationHandle& locationHandle) {
  return std::make_shared<connector::hive::LocationHandle>(
      locationHandle.targetPath,
      locationHandle.writePath,
      toTableType(locationHandle.tableType));
}

velox::connector::hive::HiveBucketProperty::Kind toHiveBucketPropertyKind(
    protocol::hive::BucketFunctionType bucketFuncType) {
  switch (bucketFuncType) {
    case protocol::hive::BucketFunctionType::PRESTO_NATIVE:
      return velox::connector::hive::HiveBucketProperty::Kind::kPrestoNative;
    case protocol::hive::BucketFunctionType::HIVE_COMPATIBLE:
      return velox::connector::hive::HiveBucketProperty::Kind::kHiveCompatible;
    default:
      VELOX_USER_FAIL(
          "Unknown hive bucket function: {}", toJsonString(bucketFuncType));
  }
}

dwio::common::FileFormat toFileFormat(
    const protocol::hive::HiveStorageFormat storageFormat,
    const char* usage) {
  switch (storageFormat) {
    case protocol::hive::HiveStorageFormat::DWRF:
      return dwio::common::FileFormat::DWRF;
    case protocol::hive::HiveStorageFormat::PARQUET:
      return dwio::common::FileFormat::PARQUET;
    case protocol::hive::HiveStorageFormat::ALPHA:
      // This has been renamed in Velox from ALPHA to NIMBLE.
      return dwio::common::FileFormat::NIMBLE;
    case protocol::hive::HiveStorageFormat::TEXTFILE:
      return dwio::common::FileFormat::TEXT;
    default:
      VELOX_UNSUPPORTED(
          "Unsupported file format in {}: {}.",
          usage,
          toJsonString(storageFormat));
  }
}

std::vector<TypePtr> stringToTypes(
    const std::shared_ptr<protocol::List<protocol::Type>>& typeStrings,
    const TypeParser& typeParser) {
  std::vector<TypePtr> types;
  types.reserve(typeStrings->size());
  for (const auto& typeString : *typeStrings) {
    types.push_back(stringToType(typeString, typeParser));
  }
  return types;
}

core::SortOrder toSortOrder(protocol::hive::Order order) {
  switch (order) {
    case protocol::hive::Order::ASCENDING:
      return core::SortOrder(true, true);
    case protocol::hive::Order::DESCENDING:
      return core::SortOrder(false, false);
    default:
      VELOX_USER_FAIL("Unknown sort order: {}", toJsonString(order));
  }
}

std::shared_ptr<velox::connector::hive::HiveSortingColumn> toHiveSortingColumn(
    const protocol::hive::SortingColumn& sortingColumn) {
  return std::make_shared<velox::connector::hive::HiveSortingColumn>(
      sortingColumn.columnName, toSortOrder(sortingColumn.order));
}

std::vector<std::shared_ptr<const velox::connector::hive::HiveSortingColumn>>
toHiveSortingColumns(
    const protocol::List<protocol::hive::SortingColumn>& sortedBy) {
  std::vector<std::shared_ptr<const velox::connector::hive::HiveSortingColumn>>
      sortingColumns;
  sortingColumns.reserve(sortedBy.size());
  for (const auto& sortingColumn : sortedBy) {
    sortingColumns.push_back(toHiveSortingColumn(sortingColumn));
  }
  return sortingColumns;
}

std::shared_ptr<velox::connector::hive::HiveBucketProperty>
toHiveBucketProperty(
    const std::vector<std::shared_ptr<const connector::hive::HiveColumnHandle>>&
        inputColumns,
    const std::shared_ptr<protocol::hive::HiveBucketProperty>& bucketProperty,
    const TypeParser& typeParser) {
  if (bucketProperty == nullptr) {
    return nullptr;
  }

  VELOX_USER_CHECK_GT(
      bucketProperty->bucketCount, 0, "Bucket count must be a positive value");

  VELOX_USER_CHECK(
      !bucketProperty->bucketedBy.empty(),
      "Bucketed columns must be set: {}",
      toJsonString(*bucketProperty));

  const velox::connector::hive::HiveBucketProperty::Kind kind =
      toHiveBucketPropertyKind(bucketProperty->bucketFunctionType);
  std::vector<TypePtr> bucketedTypes;
  if (kind ==
      velox::connector::hive::HiveBucketProperty::Kind::kHiveCompatible) {
    VELOX_USER_CHECK_NULL(
        bucketProperty->types,
        "Unexpected bucketed types set for hive compatible bucket function: {}",
        toJsonString(*bucketProperty));
    bucketedTypes.reserve(bucketProperty->bucketedBy.size());
    for (const auto& bucketedColumn : bucketProperty->bucketedBy) {
      TypePtr bucketedType{nullptr};
      for (const auto& inputColumn : inputColumns) {
        if (inputColumn->name() != bucketedColumn) {
          continue;
        }
        VELOX_USER_CHECK_NOT_NULL(inputColumn->hiveType());
        bucketedType = inputColumn->hiveType();
        break;
      }
      VELOX_USER_CHECK_NOT_NULL(
          bucketedType, "Bucketed column {} not found", bucketedColumn);
      bucketedTypes.push_back(std::move(bucketedType));
    }
  } else {
    VELOX_USER_CHECK_EQ(
        bucketProperty->types->size(),
        bucketProperty->bucketedBy.size(),
        "Bucketed types is not set properly for presto native bucket function: {}",
        toJsonString(*bucketProperty));
    bucketedTypes = stringToTypes(bucketProperty->types, typeParser);
  }

  const auto sortedBy = toHiveSortingColumns(bucketProperty->sortedBy);

  return std::make_shared<velox::connector::hive::HiveBucketProperty>(
      toHiveBucketPropertyKind(bucketProperty->bucketFunctionType),
      bucketProperty->bucketCount,
      bucketProperty->bucketedBy,
      bucketedTypes,
      sortedBy);
}

std::unique_ptr<velox::connector::hive::HiveColumnHandle>
toVeloxHiveColumnHandle(
    const protocol::ColumnHandle* column,
    const TypeParser& typeParser) {
  auto* hiveColumn =
      dynamic_cast<const protocol::hive::HiveColumnHandle*>(column);
  VELOX_CHECK_NOT_NULL(
      hiveColumn, "Unexpected column handle type {}", column->_type);
  velox::type::fbhive::HiveTypeParser hiveTypeParser;
  // TODO(spershin): Should we pass something different than 'typeSignature'
  // to 'hiveType' argument of the 'HiveColumnHandle' constructor?
  return std::make_unique<velox::connector::hive::HiveColumnHandle>(
      hiveColumn->name,
      toHiveColumnType(hiveColumn->columnType),
      stringToType(hiveColumn->typeSignature, typeParser),
      hiveTypeParser.parse(hiveColumn->hiveType),
      toRequiredSubfields(hiveColumn->requiredSubfields));
}

velox::connector::hive::HiveBucketConversion toVeloxBucketConversion(
    const protocol::hive::BucketConversion& bucketConversion) {
  velox::connector::hive::HiveBucketConversion veloxBucketConversion;
  // Current table bucket count (new).
  veloxBucketConversion.tableBucketCount = bucketConversion.tableBucketCount;
  // Partition bucket count (old).
  veloxBucketConversion.partitionBucketCount =
      bucketConversion.partitionBucketCount;
  TypeParser typeParser;
  for (const auto& column : bucketConversion.bucketColumnHandles) {
    // Columns used as bucket input.
    veloxBucketConversion.bucketColumnHandles.push_back(
        toVeloxHiveColumnHandle(&column, typeParser));
  }
  return veloxBucketConversion;
}

dwio::common::FileFormat toVeloxFileFormat(
    const presto::protocol::hive::StorageFormat& format) {
  if (format.inputFormat == "com.facebook.hive.orc.OrcInputFormat") {
    return dwio::common::FileFormat::DWRF;
  } else if (
      format.inputFormat == "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat") {
    return dwio::common::FileFormat::ORC;
  } else if (
      format.inputFormat ==
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat") {
    return dwio::common::FileFormat::PARQUET;
  } else if (format.inputFormat == "org.apache.hadoop.mapred.TextInputFormat") {
    if (format.serDe == "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe") {
      return dwio::common::FileFormat::TEXT;
    } else if (format.serDe == "org.apache.hive.hcatalog.data.JsonSerDe") {
      return dwio::common::FileFormat::JSON;
    }
  } else if (
      format.inputFormat ==
      "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat") {
    if (format.serDe ==
        "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe") {
      return dwio::common::FileFormat::PARQUET;
    }
  } else if (format.inputFormat == "com.facebook.alpha.AlphaInputFormat") {
    // ALPHA has been renamed in Velox to NIMBLE.
    return dwio::common::FileFormat::NIMBLE;
  }
  VELOX_UNSUPPORTED(
      "Unsupported file format: {} {}", format.inputFormat, format.serDe);
}

} // namespace

std::unique_ptr<velox::connector::ConnectorSplit>
HivePrestoToVeloxConnector::toVeloxSplit(
    const protocol::ConnectorId& catalogId,
    const protocol::ConnectorSplit* connectorSplit,
    const protocol::SplitContext* splitContext) const {
  auto hiveSplit =
      dynamic_cast<const protocol::hive::HiveSplit*>(connectorSplit);
  VELOX_CHECK_NOT_NULL(
      hiveSplit, "Unexpected split type {}", connectorSplit->_type);
  std::unordered_map<std::string, std::optional<std::string>> partitionKeys;
  for (const auto& entry : hiveSplit->partitionKeys) {
    partitionKeys.emplace(
        entry.name,
        entry.value == nullptr ? std::nullopt
                               : std::optional<std::string>{*entry.value});
  }
  std::unordered_map<std::string, std::string> customSplitInfo;
  for (const auto& [key, value] : hiveSplit->fileSplit.customSplitInfo) {
    customSplitInfo[key] = value;
  }
  std::shared_ptr<std::string> extraFileInfo;
  if (hiveSplit->fileSplit.extraFileInfo) {
    extraFileInfo = std::make_shared<std::string>(
        velox::encoding::Base64::decode(*hiveSplit->fileSplit.extraFileInfo));
  }
  std::unordered_map<std::string, std::string> serdeParameters;
  serdeParameters.reserve(hiveSplit->storage.serdeParameters.size());
  for (const auto& [key, value] : hiveSplit->storage.serdeParameters) {
    serdeParameters[key] = value;
  }
  std::unordered_map<std::string, std::string> infoColumns = {
      {"$path", hiveSplit->fileSplit.path},
      {"$file_size", std::to_string(hiveSplit->fileSplit.fileSize)},
      {"$file_modified_time",
       std::to_string(hiveSplit->fileSplit.fileModifiedTime)},
  };
  if (hiveSplit->tableBucketNumber) {
    infoColumns["$bucket"] = std::to_string(*hiveSplit->tableBucketNumber);
  }
  auto veloxSplit =
      std::make_unique<velox::connector::hive::HiveConnectorSplit>(
          catalogId,
          hiveSplit->fileSplit.path,
          toVeloxFileFormat(hiveSplit->storage.storageFormat),
          hiveSplit->fileSplit.start,
          hiveSplit->fileSplit.length,
          partitionKeys,
          hiveSplit->tableBucketNumber
              ? std::optional<int>(*hiveSplit->tableBucketNumber)
              : std::nullopt,
          customSplitInfo,
          extraFileInfo,
          serdeParameters,
          hiveSplit->splitWeight,
          splitContext->cacheable,
          infoColumns);
  if (hiveSplit->bucketConversion) {
    VELOX_CHECK_NOT_NULL(hiveSplit->tableBucketNumber);
    veloxSplit->bucketConversion =
        toVeloxBucketConversion(*hiveSplit->bucketConversion);
  }
  return veloxSplit;
}

std::unique_ptr<velox::connector::ColumnHandle>
HivePrestoToVeloxConnector::toVeloxColumnHandle(
    const protocol::ColumnHandle* column,
    const TypeParser& typeParser) const {
  return toVeloxHiveColumnHandle(column, typeParser);
}

std::unique_ptr<velox::connector::ConnectorTableHandle>
HivePrestoToVeloxConnector::toVeloxTableHandle(
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
  auto hiveLayout =
      std::dynamic_pointer_cast<const protocol::hive::HiveTableLayoutHandle>(
          tableHandle.connectorTableLayout);
  VELOX_CHECK_NOT_NULL(
      hiveLayout,
      "Unexpected layout type {}",
      tableHandle.connectorTableLayout->_type);
  for (const auto& entry : hiveLayout->partitionColumns) {
    assignments.emplace(entry.name, toVeloxColumnHandle(&entry, typeParser));
  }

  // Add synthesized columns to the TableScanNode columnHandles as well.
  for (const auto& entry : hiveLayout->predicateColumns) {
    addSynthesizedColumn(entry.first, entry.second.columnType, entry.second);
  }

  auto hiveTableHandle =
      std::dynamic_pointer_cast<const protocol::hive::HiveTableHandle>(
          tableHandle.connectorHandle);
  VELOX_CHECK_NOT_NULL(
      hiveTableHandle,
      "Unexpected table handle type {}",
      tableHandle.connectorHandle->_type);

  // Use fully qualified name if available.
  std::string tableName = hiveTableHandle->schemaName.empty()
      ? hiveTableHandle->tableName
      : fmt::format(
            "{}.{}", hiveTableHandle->schemaName, hiveTableHandle->tableName);

  return toHiveTableHandle(
      hiveLayout->domainPredicate,
      hiveLayout->remainingPredicate,
      hiveLayout->pushdownFilterEnabled,
      tableName,
      hiveLayout->dataColumns,
      tableHandle,
      hiveLayout->tableParameters,
      exprConverter,
      typeParser);
}

std::unique_ptr<velox::connector::ConnectorInsertTableHandle>
HivePrestoToVeloxConnector::toVeloxInsertTableHandle(
    const protocol::CreateHandle* createHandle,
    const TypeParser& typeParser) const {
  auto hiveOutputTableHandle =
      std::dynamic_pointer_cast<protocol::hive::HiveOutputTableHandle>(
          createHandle->handle.connectorHandle);
  VELOX_CHECK_NOT_NULL(
      hiveOutputTableHandle,
      "Unexpected output table handle type {}",
      createHandle->handle.connectorHandle->_type);
  bool isPartitioned{false};
  const auto inputColumns = toHiveColumns(
      hiveOutputTableHandle->inputColumns, typeParser, isPartitioned);
  return std::make_unique<velox::connector::hive::HiveInsertTableHandle>(
      inputColumns,
      toLocationHandle(hiveOutputTableHandle->locationHandle),
      toFileFormat(hiveOutputTableHandle->actualStorageFormat, "TableWrite"),
      toHiveBucketProperty(
          inputColumns, hiveOutputTableHandle->bucketProperty, typeParser),
      std::optional(
          toFileCompressionKind(hiveOutputTableHandle->compressionCodec)));
}

std::unique_ptr<velox::connector::ConnectorInsertTableHandle>
HivePrestoToVeloxConnector::toVeloxInsertTableHandle(
    const protocol::InsertHandle* insertHandle,
    const TypeParser& typeParser) const {
  auto hiveInsertTableHandle =
      std::dynamic_pointer_cast<protocol::hive::HiveInsertTableHandle>(
          insertHandle->handle.connectorHandle);
  VELOX_CHECK_NOT_NULL(
      hiveInsertTableHandle,
      "Unexpected insert table handle type {}",
      insertHandle->handle.connectorHandle->_type);
  bool isPartitioned{false};
  const auto inputColumns = toHiveColumns(
      hiveInsertTableHandle->inputColumns, typeParser, isPartitioned);

  const auto table = hiveInsertTableHandle->pageSinkMetadata.table;
  VELOX_USER_CHECK_NOT_NULL(table, "Table must not be null for insert query");
  return std::make_unique<connector::hive::HiveInsertTableHandle>(
      inputColumns,
      toLocationHandle(hiveInsertTableHandle->locationHandle),
      toFileFormat(hiveInsertTableHandle->actualStorageFormat, "TableWrite"),
      toHiveBucketProperty(
          inputColumns, hiveInsertTableHandle->bucketProperty, typeParser),
      std::optional(
          toFileCompressionKind(hiveInsertTableHandle->compressionCodec)),
      std::unordered_map<std::string, std::string>(
          table->storage.serdeParameters.begin(),
          table->storage.serdeParameters.end()));
}

std::vector<std::shared_ptr<const connector::hive::HiveColumnHandle>>
HivePrestoToVeloxConnector::toHiveColumns(
    const protocol::List<protocol::hive::HiveColumnHandle>& inputColumns,
    const TypeParser& typeParser,
    bool& hasPartitionColumn) const {
  hasPartitionColumn = false;
  std::vector<std::shared_ptr<const connector::hive::HiveColumnHandle>>
      hiveColumns;
  hiveColumns.reserve(inputColumns.size());
  for (const auto& columnHandle : inputColumns) {
    hasPartitionColumn |=
        columnHandle.columnType == protocol::hive::ColumnType::PARTITION_KEY;
    hiveColumns.emplace_back(
        std::dynamic_pointer_cast<connector::hive::HiveColumnHandle>(
            std::shared_ptr(toVeloxColumnHandle(&columnHandle, typeParser))));
  }
  return hiveColumns;
}

std::unique_ptr<velox::core::PartitionFunctionSpec>
HivePrestoToVeloxConnector::createVeloxPartitionFunctionSpec(
    const protocol::ConnectorPartitioningHandle* partitioningHandle,
    const std::vector<int>& bucketToPartition,
    const std::vector<velox::column_index_t>& channels,
    const std::vector<velox::VectorPtr>& constValues,
    bool& effectivelyGather) const {
  auto hivePartitioningHandle =
      dynamic_cast<const protocol::hive::HivePartitioningHandle*>(
          partitioningHandle);
  VELOX_CHECK_NOT_NULL(
      hivePartitioningHandle,
      "Unexpected partitioning handle type {}",
      partitioningHandle->_type);
  VELOX_USER_CHECK(
      hivePartitioningHandle->bucketFunctionType ==
          protocol::hive::BucketFunctionType::HIVE_COMPATIBLE,
      "Unsupported Hive bucket function type: {}",
      toJsonString(hivePartitioningHandle->bucketFunctionType));
  effectivelyGather = hivePartitioningHandle->bucketCount == 1;
  return std::make_unique<connector::hive::HivePartitionFunctionSpec>(
      hivePartitioningHandle->bucketCount,
      bucketToPartition,
      channels,
      constValues);
}

std::unique_ptr<protocol::ConnectorProtocol>
HivePrestoToVeloxConnector::createConnectorProtocol() const {
  return std::make_unique<protocol::hive::HiveConnectorProtocol>();
}

} // namespace facebook::presto
