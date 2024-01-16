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

#include "velox/connectors/hive/HiveConnectorUtil.h"

#include "velox/connectors/hive/FileHandle.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/CachedBufferedInput.h"
#include "velox/dwio/common/DirectBufferedInput.h"
#include "velox/dwio/common/Reader.h"

namespace facebook::velox::connector::hive {

namespace {

struct SubfieldSpec {
  const common::Subfield* subfield;
  bool filterOnly;
};

template <typename T>
void deduplicate(std::vector<T>& values) {
  std::sort(values.begin(), values.end());
  values.erase(std::unique(values.begin(), values.end()), values.end());
}

// Floating point map key subscripts are truncated toward 0 in Presto.  For
// example given `a' as a map with floating point key, if user queries a[0.99],
// Presto coordinator will generate a required subfield a[0]; for a[-1.99] it
// will generate a[-1]; for anything larger than 9223372036854775807, it
// generates a[9223372036854775807]; for anything smaller than
// -9223372036854775808 it generates a[-9223372036854775808].
template <typename T>
std::unique_ptr<common::Filter> makeFloatingPointMapKeyFilter(
    const std::vector<int64_t>& subscripts) {
  std::vector<std::unique_ptr<common::Filter>> filters;
  for (auto subscript : subscripts) {
    T lower = subscript;
    T upper = subscript;
    bool lowerUnbounded = subscript == std::numeric_limits<int64_t>::min();
    bool upperUnbounded = subscript == std::numeric_limits<int64_t>::max();
    bool lowerExclusive = false;
    bool upperExclusive = false;
    if (lower <= 0 && !lowerUnbounded) {
      if (lower > subscript - 1) {
        lower = subscript - 1;
      } else {
        lower = std::nextafter(lower, -std::numeric_limits<T>::infinity());
      }
      lowerExclusive = true;
    }
    if (upper >= 0 && !upperUnbounded) {
      if (upper < subscript + 1) {
        upper = subscript + 1;
      } else {
        upper = std::nextafter(upper, std::numeric_limits<T>::infinity());
      }
      upperExclusive = true;
    }
    if (lowerUnbounded && upperUnbounded) {
      continue;
    }
    filters.push_back(std::make_unique<common::FloatingPointRange<T>>(
        lower,
        lowerUnbounded,
        lowerExclusive,
        upper,
        upperUnbounded,
        upperExclusive,
        false));
  }
  if (filters.size() == 1) {
    return std::move(filters[0]);
  }
  return std::make_unique<common::MultiRange>(std::move(filters), false, false);
}

// Recursively add subfields to scan spec.
void addSubfields(
    const Type& type,
    std::vector<SubfieldSpec>& subfields,
    int level,
    memory::MemoryPool* pool,
    common::ScanSpec& spec) {
  int newSize = 0;
  for (int i = 0; i < subfields.size(); ++i) {
    if (level < subfields[i].subfield->path().size()) {
      subfields[newSize++] = subfields[i];
    } else if (!subfields[i].filterOnly) {
      spec.addAllChildFields(type);
      return;
    }
  }
  subfields.resize(newSize);
  switch (type.kind()) {
    case TypeKind::ROW: {
      folly::F14FastMap<std::string, std::vector<SubfieldSpec>> required;
      for (auto& subfield : subfields) {
        auto* element = subfield.subfield->path()[level].get();
        auto* nestedField =
            dynamic_cast<const common::Subfield::NestedField*>(element);
        VELOX_CHECK(
            nestedField,
            "Unsupported for row subfields pruning: {}",
            element->toString());
        required[nestedField->name()].push_back(subfield);
      }
      auto& rowType = type.asRow();
      for (int i = 0; i < rowType.size(); ++i) {
        auto& childName = rowType.nameOf(i);
        auto& childType = rowType.childAt(i);
        auto* child = spec.addField(childName, i);
        auto it = required.find(childName);
        if (it == required.end()) {
          child->setConstantValue(
              BaseVector::createNullConstant(childType, 1, pool));
        } else {
          addSubfields(*childType, it->second, level + 1, pool, *child);
        }
      }
      break;
    }
    case TypeKind::MAP: {
      auto& keyType = type.childAt(0);
      auto* keys = spec.addMapKeyFieldRecursively(*keyType);
      addSubfields(
          *type.childAt(1),
          subfields,
          level + 1,
          pool,
          *spec.addMapValueField());
      if (subfields.empty()) {
        return;
      }
      bool stringKey = keyType->isVarchar() || keyType->isVarbinary();
      std::vector<std::string> stringSubscripts;
      std::vector<int64_t> longSubscripts;
      for (auto& subfield : subfields) {
        auto* element = subfield.subfield->path()[level].get();
        if (dynamic_cast<const common::Subfield::AllSubscripts*>(element)) {
          return;
        }
        if (stringKey) {
          auto* subscript =
              dynamic_cast<const common::Subfield::StringSubscript*>(element);
          VELOX_CHECK(
              subscript,
              "Unsupported for string map pruning: {}",
              element->toString());
          stringSubscripts.push_back(subscript->index());
        } else {
          auto* subscript =
              dynamic_cast<const common::Subfield::LongSubscript*>(element);
          VELOX_CHECK(
              subscript,
              "Unsupported for long map pruning: {}",
              element->toString());
          longSubscripts.push_back(subscript->index());
        }
      }
      std::unique_ptr<common::Filter> filter;
      if (stringKey) {
        deduplicate(stringSubscripts);
        filter = std::make_unique<common::BytesValues>(stringSubscripts, false);
      } else {
        deduplicate(longSubscripts);
        if (keyType->isReal()) {
          filter = makeFloatingPointMapKeyFilter<float>(longSubscripts);
        } else if (keyType->isDouble()) {
          filter = makeFloatingPointMapKeyFilter<double>(longSubscripts);
        } else {
          filter = common::createBigintValues(longSubscripts, false);
        }
      }
      keys->setFilter(std::move(filter));
      break;
    }
    case TypeKind::ARRAY: {
      addSubfields(
          *type.childAt(0),
          subfields,
          level + 1,
          pool,
          *spec.addArrayElementField());
      if (subfields.empty()) {
        return;
      }
      constexpr long kMaxIndex = std::numeric_limits<vector_size_t>::max();
      long maxIndex = -1;
      for (auto& subfield : subfields) {
        auto* element = subfield.subfield->path()[level].get();
        if (dynamic_cast<const common::Subfield::AllSubscripts*>(element)) {
          return;
        }
        auto* subscript =
            dynamic_cast<const common::Subfield::LongSubscript*>(element);
        VELOX_CHECK(
            subscript,
            "Unsupported for array pruning: {}",
            element->toString());
        VELOX_USER_CHECK_GT(
            subscript->index(),
            0,
            "Non-positive array subscript cannot be push down");
        maxIndex = std::max(maxIndex, std::min(kMaxIndex, subscript->index()));
      }
      spec.setMaxArrayElementsCount(maxIndex);
      break;
    }
    default:
      break;
  }
}

inline uint8_t parseDelimiter(const std::string& delim) {
  for (char const& ch : delim) {
    if (!std::isdigit(ch)) {
      return delim[0];
    }
  }
  return stoi(delim);
}

} // namespace

const std::string& getColumnName(const common::Subfield& subfield) {
  VELOX_CHECK_GT(subfield.path().size(), 0);
  auto* field = dynamic_cast<const common::Subfield::NestedField*>(
      subfield.path()[0].get());
  VELOX_CHECK(field);
  return field->name();
}

void checkColumnNameLowerCase(const std::shared_ptr<const Type>& type) {
  switch (type->kind()) {
    case TypeKind::ARRAY:
      checkColumnNameLowerCase(type->asArray().elementType());
      break;
    case TypeKind::MAP: {
      checkColumnNameLowerCase(type->asMap().keyType());
      checkColumnNameLowerCase(type->asMap().valueType());

    } break;
    case TypeKind::ROW: {
      for (auto& outputName : type->asRow().names()) {
        VELOX_CHECK(
            !std::any_of(outputName.begin(), outputName.end(), isupper));
      }
      for (auto& childType : type->asRow().children()) {
        checkColumnNameLowerCase(childType);
      }
    } break;
    default:
      VLOG(1) << "No need to check type lowercase mode" << type->toString();
  }
}

void checkColumnNameLowerCase(const SubfieldFilters& filters) {
  for (auto& pair : filters) {
    if (auto name = pair.first.toString(); name == kPath || name == kBucket) {
      continue;
    }
    auto& path = pair.first.path();

    for (int i = 0; i < path.size(); ++i) {
      auto nestedField =
          dynamic_cast<const common::Subfield::NestedField*>(path[i].get());
      if (nestedField == nullptr) {
        continue;
      }
      VELOX_CHECK(!std::any_of(
          nestedField->name().begin(), nestedField->name().end(), isupper));
    }
  }
}

void checkColumnNameLowerCase(const core::TypedExprPtr& typeExpr) {
  if (typeExpr == nullptr) {
    return;
  }
  checkColumnNameLowerCase(typeExpr->type());
  for (auto& type : typeExpr->inputs()) {
    checkColumnNameLowerCase(type);
  }
}

std::shared_ptr<common::ScanSpec> makeScanSpec(
    const RowTypePtr& rowType,
    const folly::F14FastMap<std::string, std::vector<const common::Subfield*>>&
        outputSubfields,
    const SubfieldFilters& filters,
    const RowTypePtr& dataColumns,
    const std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>&
        partitionKeys,
    memory::MemoryPool* pool) {
  auto spec = std::make_shared<common::ScanSpec>("root");
  folly::F14FastMap<std::string, std::vector<const common::Subfield*>>
      filterSubfields;
  std::vector<SubfieldSpec> subfieldSpecs;
  for (auto& [subfield, _] : filters) {
    if (auto name = subfield.toString();
        name != kPath && name != kBucket && partitionKeys.count(name) == 0) {
      filterSubfields[getColumnName(subfield)].push_back(&subfield);
    }
  }

  // Process columns that will be projected out.
  for (int i = 0; i < rowType->size(); ++i) {
    auto& name = rowType->nameOf(i);
    auto& type = rowType->childAt(i);
    auto it = outputSubfields.find(name);
    if (it == outputSubfields.end()) {
      spec->addFieldRecursively(name, *type, i);
      filterSubfields.erase(name);
      continue;
    }
    for (auto* subfield : it->second) {
      subfieldSpecs.push_back({subfield, false});
    }
    it = filterSubfields.find(name);
    if (it != filterSubfields.end()) {
      for (auto* subfield : it->second) {
        subfieldSpecs.push_back({subfield, true});
      }
      filterSubfields.erase(it);
    }
    addSubfields(*type, subfieldSpecs, 1, pool, *spec->addField(name, i));
    subfieldSpecs.clear();
  }

  // Now process the columns that will not be projected out.
  if (!filterSubfields.empty()) {
    VELOX_CHECK_NOT_NULL(dataColumns);
    for (auto& [fieldName, subfields] : filterSubfields) {
      for (auto* subfield : subfields) {
        subfieldSpecs.push_back({subfield, true});
      }
      auto& type = dataColumns->findChild(fieldName);
      auto* fieldSpec = spec->getOrCreateChild(common::Subfield(fieldName));
      addSubfields(*type, subfieldSpecs, 1, pool, *fieldSpec);
      subfieldSpecs.clear();
    }
  }

  for (auto& pair : filters) {
    // SelectiveColumnReader doesn't support constant columns with filters,
    // hence, we can't have a filter for a $path or $bucket column.
    //
    // Unfortunately, Presto happens to specify a filter for $path or
    // $bucket column. This filter is redundant and needs to be removed.
    // TODO Remove this check when Presto is fixed to not specify a filter
    // on $path and $bucket column.
    if (auto name = pair.first.toString(); name == kPath || name == kBucket) {
      continue;
    }
    auto fieldSpec = spec->getOrCreateChild(pair.first);
    fieldSpec->addFilter(*pair.second);
  }

  return spec;
}

std::unique_ptr<dwio::common::SerDeOptions> parseSerdeParameters(
    const std::unordered_map<std::string, std::string>& serdeParameters) {
  auto fieldIt = serdeParameters.find(dwio::common::SerDeOptions::kFieldDelim);
  if (fieldIt == serdeParameters.end()) {
    fieldIt = serdeParameters.find("serialization.format");
  }
  auto collectionIt =
      serdeParameters.find(dwio::common::SerDeOptions::kCollectionDelim);
  if (collectionIt == serdeParameters.end()) {
    // For collection delimiter, Hive 1.x, 2.x uses "colelction.delim", but
    // Hive 3.x uses "collection.delim".
    // See: https://issues.apache.org/jira/browse/HIVE-16922)
    collectionIt = serdeParameters.find("colelction.delim");
  }
  auto mapKeyIt =
      serdeParameters.find(dwio::common::SerDeOptions::kMapKeyDelim);

  if (fieldIt == serdeParameters.end() &&
      collectionIt == serdeParameters.end() &&
      mapKeyIt == serdeParameters.end()) {
    return nullptr;
  }

  uint8_t fieldDelim = '\1';
  uint8_t collectionDelim = '\2';
  uint8_t mapKeyDelim = '\3';
  if (fieldIt != serdeParameters.end()) {
    fieldDelim = parseDelimiter(fieldIt->second);
  }
  if (collectionIt != serdeParameters.end()) {
    collectionDelim = parseDelimiter(collectionIt->second);
  }
  if (mapKeyIt != serdeParameters.end()) {
    mapKeyDelim = parseDelimiter(mapKeyIt->second);
  }
  auto serDeOptions = std::make_unique<dwio::common::SerDeOptions>(
      fieldDelim, collectionDelim, mapKeyDelim);
  return serDeOptions;
}

void configureReaderOptions(
    dwio::common::ReaderOptions& readerOptions,
    const std::shared_ptr<HiveConfig>& hiveConfig,
    const Config* sessionProperties,
    const RowTypePtr& fileSchema,
    std::shared_ptr<HiveConnectorSplit> hiveSplit) {
  readerOptions.setMaxCoalesceBytes(hiveConfig->maxCoalescedBytes());
  readerOptions.setMaxCoalesceDistance(hiveConfig->maxCoalescedDistanceBytes());
  readerOptions.setFileColumnNamesReadAsLowerCase(
      hiveConfig->isFileColumnNamesReadAsLowerCase(sessionProperties));
  readerOptions.setUseColumnNamesForColumnMapping(
      hiveConfig->isOrcUseColumnNames(sessionProperties));
  readerOptions.setFileSchema(fileSchema);
  readerOptions.setFooterEstimatedSize(hiveConfig->footerEstimatedSize());
  readerOptions.setFilePreloadThreshold(hiveConfig->filePreloadThreshold());

  if (readerOptions.getFileFormat() != dwio::common::FileFormat::UNKNOWN) {
    VELOX_CHECK(
        readerOptions.getFileFormat() == hiveSplit->fileFormat,
        "HiveDataSource received splits of different formats: {} and {}",
        dwio::common::toString(readerOptions.getFileFormat()),
        dwio::common::toString(hiveSplit->fileFormat));
  } else {
    auto serDeOptions = parseSerdeParameters(hiveSplit->serdeParameters);
    if (serDeOptions) {
      readerOptions.setSerDeOptions(*serDeOptions);
    }

    readerOptions.setFileFormat(hiveSplit->fileFormat);
  }
}

void configureRowReaderOptions(
    dwio::common::RowReaderOptions& rowReaderOptions,
    const std::unordered_map<std::string, std::string>& tableParameters,
    std::shared_ptr<common::ScanSpec> scanSpec,
    std::shared_ptr<common::MetadataFilter> metadataFilter,
    const RowTypePtr& rowType,
    std::shared_ptr<HiveConnectorSplit> hiveSplit) {
  auto skipRowsIt =
      tableParameters.find(dwio::common::TableParameter::kSkipHeaderLineCount);
  if (skipRowsIt != tableParameters.end()) {
    rowReaderOptions.setSkipRows(folly::to<uint64_t>(skipRowsIt->second));
  }

  rowReaderOptions.setScanSpec(scanSpec);
  rowReaderOptions.setMetadataFilter(metadataFilter);

  std::vector<std::string> columnNames;
  for (auto& spec : scanSpec->children()) {
    if (!spec->isConstant()) {
      columnNames.push_back(spec->fieldName());
    }
  }
  std::shared_ptr<dwio::common::ColumnSelector> cs;
  if (columnNames.empty()) {
    static const RowTypePtr kEmpty{ROW({}, {})};
    cs = std::make_shared<dwio::common::ColumnSelector>(kEmpty);
  } else {
    cs = std::make_shared<dwio::common::ColumnSelector>(rowType, columnNames);
  }
  rowReaderOptions.select(cs).range(hiveSplit->start, hiveSplit->length);
}

bool applyPartitionFilter(
    TypeKind kind,
    const std::string& partitionValue,
    common::Filter* filter) {
  switch (kind) {
    case TypeKind::BIGINT:
    case TypeKind::INTEGER:
    case TypeKind::SMALLINT:
    case TypeKind::TINYINT: {
      return applyFilter(*filter, folly::to<int64_t>(partitionValue));
    }
    case TypeKind::REAL:
    case TypeKind::DOUBLE: {
      return applyFilter(*filter, folly::to<double>(partitionValue));
    }
    case TypeKind::BOOLEAN: {
      return applyFilter(*filter, folly::to<bool>(partitionValue));
    }
    case TypeKind::VARCHAR: {
      return applyFilter(*filter, partitionValue);
    }
    default:
      VELOX_FAIL("Bad type {} for partition value: {}", kind, partitionValue);
  }
}

bool testFilters(
    common::ScanSpec* scanSpec,
    dwio::common::Reader* reader,
    const std::string& filePath,
    const std::unordered_map<std::string, std::optional<std::string>>&
        partitionKey,
    std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>*
        partitionKeysHandle) {
  auto totalRows = reader->numberOfRows();
  const auto& fileTypeWithId = reader->typeWithId();
  const auto& rowType = reader->rowType();
  for (const auto& child : scanSpec->children()) {
    if (child->filter()) {
      const auto& name = child->fieldName();
      if (!rowType->containsChild(name)) {
        // If missing column is partition key.
        auto iter = partitionKey.find(name);
        if (iter != partitionKey.end() && iter->second.has_value()) {
          return applyPartitionFilter(
              (*partitionKeysHandle)[name]->dataType()->kind(),
              iter->second.value(),
              child->filter());
        }
        // Column is missing. Most likely due to schema evolution.
        if (child->filter()->isDeterministic() &&
            !child->filter()->testNull()) {
          return false;
        }
      } else {
        const auto& typeWithId = fileTypeWithId->childByName(name);
        auto columnStats = reader->columnStatistics(typeWithId->id());
        if (columnStats != nullptr &&
            !testFilter(
                child->filter(),
                columnStats.get(),
                totalRows.value(),
                typeWithId->type())) {
          VLOG(1) << "Skipping " << filePath
                  << " based on stats and filter for column "
                  << child->fieldName();
          return false;
        }
      }
    }
  }

  return true;
}

std::unique_ptr<dwio::common::BufferedInput> createBufferedInput(
    const FileHandle& fileHandle,
    const dwio::common::ReaderOptions& readerOpts,
    const ConnectorQueryCtx* connectorQueryCtx,
    std::shared_ptr<io::IoStatistics> ioStats,
    folly::Executor* executor) {
  if (connectorQueryCtx->cache()) {
    return std::make_unique<dwio::common::CachedBufferedInput>(
        fileHandle.file,
        dwio::common::MetricsLog::voidLog(),
        fileHandle.uuid.id(),
        connectorQueryCtx->cache(),
        Connector::getTracker(
            connectorQueryCtx->scanId(), readerOpts.loadQuantum()),
        fileHandle.groupId.id(),
        ioStats,
        executor,
        readerOpts);
  }
  return std::make_unique<dwio::common::DirectBufferedInput>(
      fileHandle.file,
      dwio::common::MetricsLog::voidLog(),
      fileHandle.uuid.id(),
      Connector::getTracker(
          connectorQueryCtx->scanId(), readerOpts.loadQuantum()),
      fileHandle.groupId.id(),
      ioStats,
      executor,
      readerOpts);
}

} // namespace facebook::velox::connector::hive
