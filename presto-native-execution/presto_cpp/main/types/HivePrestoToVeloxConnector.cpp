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

#include "presto_cpp/main/types/HivePrestoToVeloxConnector.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/hive/HiveDataSink.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/connectors/hive/iceberg/IcebergDeleteFile.h"
#include "velox/core/PlanNode.h"
#include "velox/type/fbhive/HiveTypeParser.h"

using namespace facebook::velox;

namespace facebook::presto {
namespace {

template <typename T>
std::string toJsonString(const T& value) {
  return ((json)value).dump();
}

TypePtr stringToType(
    const std::string& typeString,
    const TypeParser& typeParser) {
  return typeParser.parse(typeString);
}

velox::connector::hive::LocationHandle::TableType toTableType(
    protocol::TableType tableType) {
  switch (tableType) {
    case protocol::TableType::NEW:
    // Temporary tables are written and read by the SPI in a single pipeline.
    // So they can be treated as New. They do not require Append or Overwrite
    // semantics as applicable for regular tables.
    case protocol::TableType::TEMPORARY:
      return velox::connector::hive::LocationHandle::TableType::kNew;
    case protocol::TableType::EXISTING:
      return velox::connector::hive::LocationHandle::TableType::kExisting;
    default:
      VELOX_UNSUPPORTED("Unsupported table type: {}.", toJsonString(tableType));
  }
}

std::shared_ptr<velox::connector::hive::LocationHandle> toLocationHandle(
    const protocol::LocationHandle& locationHandle) {
  return std::make_shared<velox::connector::hive::LocationHandle>(
      locationHandle.targetPath,
      locationHandle.writePath,
      toTableType(locationHandle.tableType));
}

dwio::common::FileFormat toFileFormat(
    const protocol::HiveStorageFormat storageFormat,
    const char* usage) {
  switch (storageFormat) {
    case protocol::HiveStorageFormat::DWRF:
      return dwio::common::FileFormat::DWRF;
    case protocol::HiveStorageFormat::PARQUET:
      return dwio::common::FileFormat::PARQUET;
    case protocol::HiveStorageFormat::ALPHA:
      // This has been renamed in Velox from ALPHA to NIMBLE.
      return dwio::common::FileFormat::NIMBLE;
    default:
      VELOX_UNSUPPORTED(
          "Unsupported file format in {}: {}.",
          usage,
          toJsonString(storageFormat));
  }
}

velox::common::CompressionKind toFileCompressionKind(
    const protocol::HiveCompressionCodec& hiveCompressionCodec) {
  switch (hiveCompressionCodec) {
    case protocol::HiveCompressionCodec::SNAPPY:
      return velox::common::CompressionKind::CompressionKind_SNAPPY;
    case protocol::HiveCompressionCodec::GZIP:
      return velox::common::CompressionKind::CompressionKind_GZIP;
    case protocol::HiveCompressionCodec::LZ4:
      return velox::common::CompressionKind::CompressionKind_LZ4;
    case protocol::HiveCompressionCodec::ZSTD:
      return velox::common::CompressionKind::CompressionKind_ZSTD;
    case protocol::HiveCompressionCodec::NONE:
      return velox::common::CompressionKind::CompressionKind_NONE;
    default:
      VELOX_UNSUPPORTED(
          "Unsupported file compression format: {}.",
          toJsonString(hiveCompressionCodec));
  }
}

velox::connector::hive::HiveBucketProperty::Kind toHiveBucketPropertyKind(
    protocol::BucketFunctionType bucketFuncType) {
  switch (bucketFuncType) {
    case protocol::BucketFunctionType::PRESTO_NATIVE:
      return velox::connector::hive::HiveBucketProperty::Kind::kPrestoNative;
    case protocol::BucketFunctionType::HIVE_COMPATIBLE:
      return velox::connector::hive::HiveBucketProperty::Kind::kHiveCompatible;
    default:
      VELOX_USER_FAIL(
          "Unknown hive bucket function: {}", toJsonString(bucketFuncType));
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

core::SortOrder toSortOrder(protocol::Order order) {
  switch (order) {
    case protocol::Order::ASCENDING:
      return core::SortOrder(true, true);
    case protocol::Order::DESCENDING:
      return core::SortOrder(false, false);
    default:
      VELOX_USER_FAIL("Unknown sort order: {}", toJsonString(order));
  }
}

std::shared_ptr<velox::connector::hive::HiveSortingColumn> toHiveSortingColumn(
    const protocol::SortingColumn& sortingColumn) {
  return std::make_shared<velox::connector::hive::HiveSortingColumn>(
      sortingColumn.columnName, toSortOrder(sortingColumn.order));
}

std::vector<std::shared_ptr<const velox::connector::hive::HiveSortingColumn>>
toHiveSortingColumns(const protocol::List<protocol::SortingColumn>& sortedBy) {
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
    const std::shared_ptr<protocol::HiveBucketProperty>& bucketProperty,
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
  auto* hiveColumn = dynamic_cast<const protocol::HiveColumnHandle*>(column);
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
    const protocol::BucketConversion& bucketConversion) {
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
    const presto::protocol::StorageFormat& format) {
  if (format.inputFormat == "com.facebook.hive.orc.OrcInputFormat") {
    return dwio::common::FileFormat::DWRF;
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
  } else if (format.inputFormat == "com.facebook.alpha.AlphaInputFormat") {
    // ALPHA has been renamed in Velox to NIMBLE.
    return dwio::common::FileFormat::NIMBLE;
  }
  VELOX_UNSUPPORTED(
      "Unsupported file format: {} {}", format.inputFormat, format.serDe);
}

void validateHiveConfigs(
    const std::unordered_map<std::string, std::string>& configs) {
  // Set of valid velox hive connector configs.
  static const std::unordered_set<std::string> kHiveConfigs = {
      "connector.name",
      velox::connector::hive::HiveConfig::kInsertExistingPartitionsBehavior,
      velox::connector::hive::HiveConfig::kMaxPartitionsPerWriters,
      velox::connector::hive::HiveConfig::kImmutablePartitions,
      velox::connector::hive::HiveConfig::kS3PathStyleAccess,
      velox::connector::hive::HiveConfig::kS3LogLevel,
      velox::connector::hive::HiveConfig::kS3SSLEnabled,
      velox::connector::hive::HiveConfig::kS3UseInstanceCredentials,
      velox::connector::hive::HiveConfig::kS3Endpoint,
      velox::connector::hive::HiveConfig::kS3AwsAccessKey,
      velox::connector::hive::HiveConfig::kS3AwsSecretKey,
      velox::connector::hive::HiveConfig::kS3IamRole,
      velox::connector::hive::HiveConfig::kS3IamRoleSessionName,
      velox::connector::hive::HiveConfig::kS3ConnectTimeout,
      velox::connector::hive::HiveConfig::kS3SocketTimeout,
      velox::connector::hive::HiveConfig::kS3MaxConnections,
      velox::connector::hive::HiveConfig::kS3MaxAttempts,
      velox::connector::hive::HiveConfig::kS3RetryMode,
      velox::connector::hive::HiveConfig::kGCSEndpoint,
      velox::connector::hive::HiveConfig::kGCSScheme,
      velox::connector::hive::HiveConfig::kGCSCredentials,
      velox::connector::hive::HiveConfig::kGCSMaxRetryCount,
      velox::connector::hive::HiveConfig::kGCSMaxRetryTime,
      velox::connector::hive::HiveConfig::kOrcUseColumnNames,
      velox::connector::hive::HiveConfig::kFileColumnNamesReadAsLowerCase,
      velox::connector::hive::HiveConfig::kMaxCoalescedBytes,
      velox::connector::hive::HiveConfig::kMaxCoalescedDistanceBytes,
      velox::connector::hive::HiveConfig::kPrefetchRowGroups,
      velox::connector::hive::HiveConfig::kLoadQuantum,
      velox::connector::hive::HiveConfig::kNumCacheFileHandles,
      velox::connector::hive::HiveConfig::kEnableFileHandleCache,
      velox::connector::hive::HiveConfig::kFooterEstimatedSize,
      velox::connector::hive::HiveConfig::kFilePreloadThreshold,
      velox::connector::hive::HiveConfig::kOrcWriterMaxStripeSize,
      velox::connector::hive::HiveConfig::kOrcWriterMaxDictionaryMemory,
      velox::connector::hive::HiveConfig::
          kOrcWriterIntegerDictionaryEncodingEnabled,
      velox::connector::hive::HiveConfig::
          kOrcWriterStringDictionaryEncodingEnabled,
      velox::connector::hive::HiveConfig::kOrcWriterLinearStripeSizeHeuristics,
      velox::connector::hive::HiveConfig::kOrcWriterMinCompressionSize,
      velox::connector::hive::HiveConfig::kOrcWriterCompressionLevel,
      velox::connector::hive::HiveConfig::kWriteFileCreateConfig,
      velox::connector::hive::HiveConfig::kSortWriterMaxOutputRows,
      velox::connector::hive::HiveConfig::kSortWriterMaxOutputBytes,
      velox::connector::hive::HiveConfig::kS3UseProxyFromEnv,
      velox::connector::hive::HiveConfig::kReadTimestampUnit,
      velox::connector::hive::HiveConfig::kCacheNoRetention,
  };

  for (const auto entry : configs) {
    if (!kHiveConfigs.count(entry.first)) {
      LOG(WARNING) << "Hive config: " << entry.first << " not recognized";
    }
  }
}
} // namespace

HivePrestoToVeloxConnector::HivePrestoToVeloxConnector(
    std::string connectorName,
    std::optional<std::vector<std::unordered_map<std::string, std::string>>>
        hiveConfigs)
    : PrestoToVeloxConnector(std::move(connectorName)) {
  if (hiveConfigs.has_value()) {
    for (const auto& map : hiveConfigs.value()) {
      validateHiveConfigs(map);
    }
  }
}

std::unique_ptr<facebook::velox::connector::ConnectorSplit>
HivePrestoToVeloxConnector::toVeloxSplit(
    const protocol::ConnectorId& catalogId,
    const protocol::ConnectorSplit* const connectorSplit) const {
  auto hiveSplit = dynamic_cast<const protocol::HiveSplit*>(connectorSplit);
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
  std::unordered_map<std::string, std::string> infoColumns;
  infoColumns.reserve(2);
  infoColumns.insert(
      {"$file_size", std::to_string(hiveSplit->fileSplit.fileSize)});
  infoColumns.insert(
      {"$file_modified_time",
       std::to_string(hiveSplit->fileSplit.fileModifiedTime)});
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
    std::unordered_map<
        std::string,
        std::shared_ptr<velox::connector::ColumnHandle>>& assignments) const {
  auto addSynthesizedColumn = [&](const std::string& name,
                                  protocol::ColumnType columnType,
                                  const protocol::ColumnHandle& column) {
    if (toHiveColumnType(columnType) ==
        velox::connector::hive::HiveColumnHandle::ColumnType::kSynthesized) {
      if (assignments.count(name) == 0) {
        assignments.emplace(name, toVeloxColumnHandle(&column, typeParser));
      }
    }
  };
  auto hiveLayout =
      std::dynamic_pointer_cast<const protocol::HiveTableLayoutHandle>(
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
      std::dynamic_pointer_cast<const protocol::HiveTableHandle>(
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
      std::dynamic_pointer_cast<protocol::HiveOutputTableHandle>(
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
      toFileFormat(hiveOutputTableHandle->tableStorageFormat, "TableWrite"),
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
      std::dynamic_pointer_cast<protocol::HiveInsertTableHandle>(
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
      toFileFormat(hiveInsertTableHandle->tableStorageFormat, "TableWrite"),
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
    const protocol::List<protocol::HiveColumnHandle>& inputColumns,
    const TypeParser& typeParser,
    bool& hasPartitionColumn) const {
  hasPartitionColumn = false;
  std::vector<std::shared_ptr<const connector::hive::HiveColumnHandle>>
      hiveColumns;
  hiveColumns.reserve(inputColumns.size());
  for (const auto& columnHandle : inputColumns) {
    hasPartitionColumn |=
        columnHandle.columnType == protocol::ColumnType::PARTITION_KEY;
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
      dynamic_cast<const protocol::HivePartitioningHandle*>(partitioningHandle);
  VELOX_CHECK_NOT_NULL(
      hivePartitioningHandle,
      "Unexpected partitioning handle type {}",
      partitioningHandle->_type);
  VELOX_USER_CHECK(
      hivePartitioningHandle->bucketFunctionType ==
          protocol::BucketFunctionType::HIVE_COMPATIBLE,
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
  return std::make_unique<protocol::HiveConnectorProtocol>();
}
} // namespace facebook::presto
