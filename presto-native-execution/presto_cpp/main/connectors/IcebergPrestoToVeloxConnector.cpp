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

#include <folly/json.h>
#include <algorithm>
#include <unordered_set>

#include "presto_cpp/presto_protocol/connector/iceberg/IcebergConnectorProtocol.h"
#include "velox/connectors/hive/iceberg/IcebergDataSink.h"
#include "velox/connectors/hive/iceberg/IcebergMetadataColumns.h"
#include "velox/connectors/hive/iceberg/IcebergSplit.h"
#include "velox/type/fbhive/HiveTypeParser.h"

namespace facebook::presto {

namespace {

// Row-lineage columns are not included in a table's dataColumns but may exist
// physically in written files. Centralizing the names here means adding a new
// lineage column only requires updating this one set.
const std::unordered_set<std::string> kRowLineageColumnNames = {
    velox::connector::hive::iceberg::IcebergMetadataColumn::kRowIdColumnName,
    velox::connector::hive::iceberg::IcebergMetadataColumn::
        kLastUpdatedSequenceNumberColumnName,
};

velox::connector::hive::iceberg::FileContent toVeloxFileContent(
    const presto::protocol::iceberg::FileContent content) {
  if (content == protocol::iceberg::FileContent::DATA) {
    return velox::connector::hive::iceberg::FileContent::kData;
  } else if (content == protocol::iceberg::FileContent::POSITION_DELETES) {
    return velox::connector::hive::iceberg::FileContent::kPositionalDeletes;
  } else if (content == protocol::iceberg::FileContent::EQUALITY_DELETES) {
    return velox::connector::hive::iceberg::FileContent::kEqualityDeletes;
  } else if (content == protocol::iceberg::FileContent::DELETION_VECTOR) {
    return velox::connector::hive::iceberg::FileContent::kDeletionVector;
  }
  VELOX_UNSUPPORTED("Unsupported file content: {}", fmt::underlying(content));
}

velox::dwio::common::FileFormat toVeloxFileFormat(
    const presto::protocol::iceberg::FileFormat format) {
  if (format == protocol::iceberg::FileFormat::ORC) {
    // Iceberg manifests have no DWRF enum, so Meta's DWRF files (and
    // genuine ORC files, which DWRF is a superset of) are reported as
    // "ORC" on the wire per the cross-engine convention shared with the
    // Java planner (FileFormat.DWRF.toIceberg() in
    // presto-facebook-iceberg). Velox only registers a writer for
    // FileFormat::DWRF, so map protocol ORC -> velox DWRF here to unify
    // the two views and let the DWRF writer/reader (which handles both
    // formats) take over.
    return velox::dwio::common::FileFormat::DWRF;
  } else if (format == protocol::iceberg::FileFormat::PARQUET) {
    return velox::dwio::common::FileFormat::PARQUET;
  } else if (format == protocol::iceberg::FileFormat::DWRF) {
    // The protocol enum now carries DWRF directly (post-regen alignment
    // with the Java FileFormat enum). Java's `format='DWRF'` write paths
    // (InsertTableHandle, MergeTableHandle, etc.) send "DWRF" on the wire
    // for tables created with `WITH (format='DWRF', ...)`. Map straight
    // through to Velox's DWRF.
    return velox::dwio::common::FileFormat::DWRF;
  } else if (format == protocol::iceberg::FileFormat::NIMBLE) {
    // Same rationale as DWRF — the regen added NIMBLE to the protocol
    // enum, so Java's `format='NIMBLE'` write paths now reach the bridge
    // directly instead of being silently coerced to ORC by nlohmann's
    // unknown-enum-string-falls-back-to-first behavior.
    return velox::dwio::common::FileFormat::NIMBLE;
  } else if (format == protocol::iceberg::FileFormat::PUFFIN) {
    // Iceberg V3 deletion-vector files are stored in Puffin format. The
    // Velox DV branch in IcebergSplitReader keys off
    // FileContent::kDeletionVector rather than this format, but map it
    // through anyway so the value round-trips correctly.
    return velox::dwio::common::FileFormat::PUFFIN;
  }
  VELOX_UNSUPPORTED("Unsupported file format: {}", fmt::underlying(format));
}

// Read-path file-format mapping. A file reported as Iceberg "ORC" on the wire
// may be a genuine ORC file (written by the Java/presto-orc writer, the default
// for table data in tests) whose footer follows the ORC proto schema. The Velox
// reader selects its PostScript proto schema purely from the FileFormat it is
// handed (ReaderBase: FileFormat::ORC -> proto::orc::PostScript, otherwise the
// DWRF proto::PostScript) rather than sniffing magic bytes. Mapping ORC ->
// Velox DWRF on read (as toVeloxFileFormat does for the writer's sake) makes
// the reader parse a genuine-ORC PostScript with the DWRF schema, where the
// compression enum ordinals diverge (ORC ZSTD=5 collides with DWRF LZ4=5),
// yielding "lz4 failed to decompress" on ZSTD-compressed streams. Map ORC ->
// Velox ORC here so the ORC reader (registered via registerOrcReaderFactory)
// parses the correct schema; all other formats reuse the write mapping.
velox::dwio::common::FileFormat toVeloxReadFileFormat(
    const presto::protocol::iceberg::FileFormat format) {
  if (format == protocol::iceberg::FileFormat::ORC) {
    return velox::dwio::common::FileFormat::ORC;
  }
  return toVeloxFileFormat(format);
}

std::unique_ptr<velox::connector::ConnectorTableHandle> toIcebergTableHandle(
    const protocol::TupleDomain<protocol::Subfield>& domainPredicate,
    const std::shared_ptr<protocol::RowExpression>& remainingPredicate,
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

    // Row-lineage columns are not included in the table's dataColumns but may
    // exist physically in the written files (e.g. after MERGE/UPDATE). Add any
    // requested lineage columns to finalDataColumns so the reader can match
    // them with the Parquet schema.
    for (const auto& handle : columnHandles) {
      if (kRowLineageColumnNames.count(handle->name()) &&
          std::find(names.begin(), names.end(), handle->name()) ==
              names.end()) {
        names.emplace_back(handle->name());
        types.push_back(handle->dataType());
      }
    }

    finalDataColumns = ROW(std::move(names), std::move(types));
  }

  return std::make_unique<velox::connector::hive::HiveTableHandle>(
      tableHandle.connectorId,
      tableName,
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
  // ParquetFieldId does not declare a constructor that takes fieldId and
  // children, so we use aggregate initialization to make it work for compilers
  // that don't create the necessary constructors by default (e.g clang-15).
  velox::parquet::ParquetFieldId pf{.fieldId = column.id, .children = children};
  return pf;
}

// Extracts the Iceberg partition spec ID from the JSON-encoded spec carried on
// each split (IcebergSplit.partitionSpecAsJson on the Java side). The JSON
// always has a top-level "specId" integer per Iceberg's PartitionSpecParser.
// Returns std::nullopt only when parsing fails so callers can decide whether
// to omit the resulting info column (synthesis paths that depend on a real
// spec_id should treat absence as a hard error).
std::optional<int32_t> tryParsePartitionSpecId(
    const std::string& partitionSpecAsJson) {
  if (partitionSpecAsJson.empty()) {
    return std::nullopt;
  }
  try {
    const auto parsed = folly::parseJson(partitionSpecAsJson);
    if (!parsed.isObject()) {
      return std::nullopt;
    }
    const auto* specIdField = parsed.get_ptr("specId");
    if (specIdField == nullptr || !specIdField->isInt()) {
      return std::nullopt;
    }
    return static_cast<int32_t>(specIdField->asInt());
  } catch (const folly::json::parse_error&) {
    return std::nullopt;
  } catch (const folly::TypeError&) {
    return std::nullopt;
  }
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

    // Iceberg V3 backward-compat: older iceberg-api releases report DV
    // files with content()=POSITION_DELETES and format()=PUFFIN (the
    // DELETION_VECTOR enum value was added later). Treat any Puffin delete
    // file as a DV here so it routes to the Velox DeletionVectorReader
    // rather than PositionalDeleteFileReader (which would call
    // getReaderFactory(PUFFIN) and fail with "ReaderFactory not registered
    // for format puffin").
    auto veloxContent =
        (deleteFile.format == protocol::iceberg::FileFormat::PUFFIN)
        ? velox::connector::hive::iceberg::FileContent::kDeletionVector
        : toVeloxFileContent(deleteFile.content);

    velox::connector::hive::iceberg::IcebergDeleteFile icebergDeleteFile(
        veloxContent,
        deleteFile.path,
        toVeloxReadFileFormat(deleteFile.format),
        deleteFile.recordCount,
        deleteFile.fileSizeInBytes,
        std::vector(deleteFile.equalityFieldIds),
        lowerBounds,
        upperBounds,
        // The delete file carries its own dataSequenceNumber (the sequence
        // number of the delete operation), which the Velox DV applier uses for
        // sequence-number-based conflict resolution.
        deleteFile.dataSequenceNumber,
        // The three V3 Optional<> fields are absent for V2 splits — fall back
        // to 0 / empty when the shared_ptr is null.
        deleteFile.contentOffset ? *deleteFile.contentOffset : 0,
        deleteFile.contentSizeInBytes ? *deleteFile.contentSizeInBytes : 0,
        deleteFile.referencedDataFile ? *deleteFile.referencedDataFile
                                      : std::string{});

    deletes.emplace_back(icebergDeleteFile);
  }

  std::unordered_map<std::string, std::string> infoColumns = {
      {"$path", icebergSplit->path},
      {velox::connector::hive::iceberg::IcebergMetadataColumn::
           kDataSequenceNumberInfoColumn,
       std::to_string(icebergSplit->dataSequenceNumber)}};
  if (icebergSplit->firstRowId >= 0) {
    infoColumns[velox::connector::hive::iceberg::IcebergMetadataColumn::
                    kFirstRowIdInfoColumn] =
        std::to_string(icebergSplit->firstRowId);
  }

  // Iceberg MERGE INTO row-id synthesis: feed the split's partition spec ID
  // and the per-file PartitionData JSON down to the Velox split reader so it
  // can populate the spec_id and partition_data fields of the synthetic
  // $target_table_row_id ROW column. partitionSpecAsJson is required on every
  // Iceberg split; partitionDataJson is optional (absent for unpartitioned
  // tables, in which case Java emits an empty string — match that here).
  if (auto specId = tryParsePartitionSpecId(icebergSplit->partitionSpecAsJson);
      specId.has_value()) {
    infoColumns.emplace(
        velox::connector::hive::iceberg::IcebergMetadataColumn::
            kSpecIdInfoColumn,
        fmt::to_string(*specId));
  }
  infoColumns.emplace(
      velox::connector::hive::iceberg::IcebergMetadataColumn::
          kPartitionDataInfoColumn,
      icebergSplit->partitionDataJson ? *icebergSplit->partitionDataJson : "");

  return std::make_unique<velox::connector::hive::iceberg::HiveIcebergSplit>(
      catalogId,
      icebergSplit->path,
      toVeloxReadFileFormat(icebergSplit->fileFormat),
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

  std::optional<std::string> defaultValue;
  if (icebergColumn->defaultValue) {
    defaultValue = *icebergColumn->defaultValue;
  }

  return std::make_unique<velox::connector::hive::iceberg::IcebergColumnHandle>(
      icebergColumn->columnIdentity.name,
      toHiveColumnType(icebergColumn->columnType),
      type,
      toParquetField(icebergColumn->columnIdentity),
      toRequiredSubfields(icebergColumn->requiredSubfields),
      defaultValue);
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
      tableName,
      icebergLayout->dataColumns,
      tableHandle,
      columnHandles,
      exprConverter,
      typeParser);
}

std::unique_ptr<velox::connector::ConnectorInsertTableHandle>
IcebergPrestoToVeloxConnector::toVeloxInsertTableHandle(
    const protocol::ExecuteProcedureHandle* executeProcedureHandle,
    const TypeParser& typeParser) const {
  auto icebergDistributedProcedureHandle = std::dynamic_pointer_cast<
      protocol::iceberg::IcebergDistributedProcedureHandle>(
      executeProcedureHandle->handle.connectorHandle);

  VELOX_CHECK_NOT_NULL(
      icebergDistributedProcedureHandle,
      "Unexpected call distributed procedure handle type {}",
      executeProcedureHandle->handle.connectorHandle->_type);

  const auto inputColumns = toIcebergColumns(
      icebergDistributedProcedureHandle->inputColumns, typeParser);

  return std::make_unique<
      velox::connector::hive::iceberg::IcebergInsertTableHandle>(
      inputColumns,
      std::make_shared<velox::connector::hive::LocationHandle>(
          fmt::format("{}/data", icebergDistributedProcedureHandle->outputPath),
          fmt::format("{}/data", icebergDistributedProcedureHandle->outputPath),
          velox::connector::hive::LocationHandle::TableType::kExisting),
      toVeloxFileFormat(icebergDistributedProcedureHandle->fileFormat),
      toVeloxIcebergPartitionSpec(
          icebergDistributedProcedureHandle->partitionSpec, typeParser),
      std::optional(toFileCompressionKind(
          icebergDistributedProcedureHandle->compressionCodec)));
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

std::unique_ptr<velox::connector::ConnectorInsertTableHandle>
IcebergPrestoToVeloxConnector::toVeloxInsertTableHandle(
    const protocol::DeleteHandle* deleteHandle,
    const TypeParser& typeParser) const {
  auto icebergDeleteTableHandle =
      std::dynamic_pointer_cast<protocol::iceberg::IcebergDeleteTableHandle>(
          deleteHandle->handle.connectorHandle);

  VELOX_CHECK_NOT_NULL(
      icebergDeleteTableHandle,
      "Unexpected delete table handle type {}",
      deleteHandle->handle.connectorHandle->_type);

  const auto inputColumns =
      toIcebergColumns(icebergDeleteTableHandle->inputColumns, typeParser);

  // Derive Velox WriteKind from the protocol's fileContent. Only the V3
  // deletion-vector branch routes through this bridge today; V2
  // POSITION_DELETES flows through the Java row-id-rewrite path on the
  // coordinator and never reaches the C++ worker as a typed DeleteHandle.
  // If we do see a non-DELETION_VECTOR fileContent here we fall back to
  // kData so the existing IcebergDataSink raises a clear error rather
  // than silently emitting a deletion vector for the wrong format.
  const auto writeKind = icebergDeleteTableHandle->fileContent ==
          protocol::iceberg::FileContent::DELETION_VECTOR
      ? velox::connector::hive::iceberg::IcebergInsertTableHandle::WriteKind::
            kDeletionVector
      : velox::connector::hive::iceberg::IcebergInsertTableHandle::WriteKind::
            kData;

  return std::make_unique<
      velox::connector::hive::iceberg::IcebergInsertTableHandle>(
      inputColumns,
      std::make_shared<velox::connector::hive::LocationHandle>(
          fmt::format("{}/data", icebergDeleteTableHandle->outputPath),
          fmt::format("{}/data", icebergDeleteTableHandle->outputPath),
          velox::connector::hive::LocationHandle::TableType::kExisting),
      toVeloxFileFormat(icebergDeleteTableHandle->fileFormat),
      toVeloxIcebergPartitionSpec(
          icebergDeleteTableHandle->partitionSpec, typeParser),
      std::optional(
          toFileCompressionKind(icebergDeleteTableHandle->compressionCodec)),
      /*serdeParameters=*/std::unordered_map<std::string, std::string>{},
      writeKind);
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

// Layer 3b: MergeHandle → IcebergInsertTableHandle (with WriteKind::kMerge).
// MergeHandle.connectorMergeTableHandle is the IcebergMergeTableHandle which
// wraps an IcebergInsertTableHandle (the same protocol struct used by plain
// INSERT). We unwrap it and forward to the IcebergInsertTableHandle build
// path, then tag the Velox handle with WriteKind::kMerge so
// IcebergConnector::createDataSink dispatches to IcebergMergeSink (Layer 2).
std::unique_ptr<velox::connector::ConnectorInsertTableHandle>
IcebergPrestoToVeloxConnector::toVeloxInsertTableHandle(
    const protocol::MergeHandle* mergeHandle,
    const TypeParser& typeParser) const {
  auto icebergMergeTableHandle =
      std::dynamic_pointer_cast<protocol::iceberg::IcebergMergeTableHandle>(
          mergeHandle->connectorMergeTableHandle);

  VELOX_CHECK_NOT_NULL(
      icebergMergeTableHandle,
      "Unexpected merge table handle type {}",
      mergeHandle->connectorMergeTableHandle->_type);

  const auto& innerInsert = icebergMergeTableHandle->insertTableHandle;
  const auto inputColumns =
      toIcebergColumns(innerInsert.inputColumns, typeParser);

  return std::make_unique<
      velox::connector::hive::iceberg::IcebergInsertTableHandle>(
      inputColumns,
      std::make_shared<velox::connector::hive::LocationHandle>(
          fmt::format("{}/data", innerInsert.outputPath),
          fmt::format("{}/data", innerInsert.outputPath),
          velox::connector::hive::LocationHandle::TableType::kExisting),
      toVeloxFileFormat(innerInsert.fileFormat),
      toVeloxIcebergPartitionSpec(innerInsert.partitionSpec, typeParser),
      std::optional(toFileCompressionKind(innerInsert.compressionCodec)),
      std::unordered_map<std::string, std::string>{},
      velox::connector::hive::iceberg::IcebergInsertTableHandle::WriteKind::
          kMerge);
}

} // namespace facebook::presto
