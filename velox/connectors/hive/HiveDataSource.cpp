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

#include "velox/connectors/hive/HiveDataSource.h"

#include <string>
#include <unordered_map>

#include "velox/common/caching/AsyncDataCache.h"
#include "velox/dwio/common/CachedBufferedInput.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/expression/ExprToSubfieldFilter.h"
#include "velox/expression/FieldReference.h"

namespace facebook::velox::connector::hive {

class HiveTableHandle;
class HiveColumnHandle;

namespace {

static const char* kPath = "$path";
static const char* kBucket = "$bucket";

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
      break;
  }
}

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
        maxIndex = std::max(maxIndex, std::min(kMaxIndex, subscript->index()));
      }
      spec.setMaxArrayElementsCount(maxIndex);
      break;
    }
    default:
      break;
  }
}

bool testFilters(
    common::ScanSpec* scanSpec,
    dwio::common::Reader* reader,
    const std::string& filePath,
    const std::unordered_map<std::string, std::optional<std::string>>&
        partitionKey,
    std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>&
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
              partitionKeysHandle[name]->dataType()->kind(),
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

template <TypeKind ToKind>
velox::variant convertFromString(const std::optional<std::string>& value) {
  if (value.has_value()) {
    if constexpr (ToKind == TypeKind::VARCHAR) {
      return velox::variant(value.value());
    }
    if constexpr (ToKind == TypeKind::VARBINARY) {
      return velox::variant::binary((value.value()));
    }
    auto result = velox::util::Converter<ToKind>::cast(value.value());

    return velox::variant(result);
  }
  return velox::variant(ToKind);
}

core::CallTypedExprPtr replaceInputs(
    const core::CallTypedExpr* call,
    std::vector<core::TypedExprPtr>&& inputs) {
  return std::make_shared<core::CallTypedExpr>(
      call->type(), std::move(inputs), call->name());
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

const std::string& getColumnName(const common::Subfield& subfield) {
  VELOX_CHECK_GT(subfield.path().size(), 0);
  auto* field = dynamic_cast<const common::Subfield::NestedField*>(
      subfield.path()[0].get());
  VELOX_CHECK(field);
  return field->name();
}

} // namespace

core::TypedExprPtr HiveDataSource::extractFiltersFromRemainingFilter(
    const core::TypedExprPtr& expr,
    core::ExpressionEvaluator* evaluator,
    bool negated,
    SubfieldFilters& filters) {
  auto* call = dynamic_cast<const core::CallTypedExpr*>(expr.get());
  if (!call) {
    return expr;
  }
  common::Filter* oldFilter = nullptr;
  try {
    common::Subfield subfield;
    if (auto filter = exec::leafCallToSubfieldFilter(
            *call, subfield, evaluator, negated)) {
      if (auto it = filters.find(subfield); it != filters.end()) {
        oldFilter = it->second.get();
        filter = filter->mergeWith(oldFilter);
      }
      filters.insert_or_assign(std::move(subfield), std::move(filter));
      return nullptr;
    }
  } catch (const VeloxException&) {
    LOG(WARNING) << "Unexpected failure when extracting filter for: "
                 << expr->toString();
    if (oldFilter) {
      LOG(WARNING) << "Merging with " << oldFilter->toString();
    }
  }
  if (call->name() == "not") {
    auto inner = extractFiltersFromRemainingFilter(
        call->inputs()[0], evaluator, !negated, filters);
    return inner ? replaceInputs(call, {inner}) : nullptr;
  }
  if ((call->name() == "and" && !negated) ||
      (call->name() == "or" && negated)) {
    auto lhs = extractFiltersFromRemainingFilter(
        call->inputs()[0], evaluator, negated, filters);
    auto rhs = extractFiltersFromRemainingFilter(
        call->inputs()[1], evaluator, negated, filters);
    if (!lhs) {
      return rhs;
    }
    if (!rhs) {
      return lhs;
    }
    return replaceInputs(call, {lhs, rhs});
  }
  return expr;
}

HiveDataSource::HiveDataSource(
    const RowTypePtr& outputType,
    const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& columnHandles,
    FileHandleFactory* fileHandleFactory,
    core::ExpressionEvaluator* expressionEvaluator,
    cache::AsyncDataCache* cache,
    const std::string& scanId,
    folly::Executor* executor,
    const dwio::common::ReaderOptions& options)
    : fileHandleFactory_(fileHandleFactory),
      readerOpts_(options),
      pool_(&options.getMemoryPool()),
      outputType_(outputType),
      expressionEvaluator_(expressionEvaluator),
      cache_(cache),
      scanId_(scanId),
      executor_(executor) {
  // Column handled keyed on the column alias, the name used in the query.
  for (const auto& [canonicalizedName, columnHandle] : columnHandles) {
    auto handle = std::dynamic_pointer_cast<HiveColumnHandle>(columnHandle);
    VELOX_CHECK(
        handle != nullptr,
        "ColumnHandle must be an instance of HiveColumnHandle for {}",
        canonicalizedName);

    if (handle->columnType() == HiveColumnHandle::ColumnType::kPartitionKey) {
      partitionKeys_.emplace(handle->name(), handle);
    }
  }

  std::vector<std::string> readerRowNames;
  auto readerRowTypes = outputType_->children();
  folly::F14FastMap<std::string, std::vector<const common::Subfield*>>
      subfields;
  for (auto& outputName : outputType_->names()) {
    auto it = columnHandles.find(outputName);
    VELOX_CHECK(
        it != columnHandles.end(),
        "ColumnHandle is missing for output column: {}",
        outputName);

    auto* handle = static_cast<const HiveColumnHandle*>(it->second.get());
    readerRowNames.push_back(handle->name());
    for (auto& subfield : handle->requiredSubfields()) {
      VELOX_USER_CHECK_EQ(
          getColumnName(subfield),
          handle->name(),
          "Required subfield does not match column name");
      subfields[handle->name()].push_back(&subfield);
    }
  }

  auto hiveTableHandle =
      std::dynamic_pointer_cast<HiveTableHandle>(tableHandle);
  VELOX_CHECK(
      hiveTableHandle != nullptr,
      "TableHandle must be an instance of HiveTableHandle");
  if (readerOpts_.isFileColumnNamesReadAsLowerCase()) {
    checkColumnNameLowerCase(outputType_);
    checkColumnNameLowerCase(hiveTableHandle->subfieldFilters());
    checkColumnNameLowerCase(hiveTableHandle->remainingFilter());
  }

  SubfieldFilters filters;
  core::TypedExprPtr remainingFilter;
  if (hiveTableHandle->isFilterPushdownEnabled()) {
    for (auto& [k, v] : hiveTableHandle->subfieldFilters()) {
      filters.emplace(k.clone(), v->clone());
    }
    remainingFilter = extractFiltersFromRemainingFilter(
        hiveTableHandle->remainingFilter(),
        expressionEvaluator_,
        false,
        filters);
  } else {
    for (auto& [field, _] : hiveTableHandle->subfieldFilters()) {
      VELOX_USER_CHECK_EQ(
          field.path().size(),
          1,
          "Unexpected filter on table {}, field {}",
          hiveTableHandle->tableName(),
          field.toString());
      auto* nestedField = dynamic_cast<const common::Subfield::NestedField*>(
          field.path()[0].get());
      VELOX_USER_CHECK_NOT_NULL(
          nestedField,
          "Unexpected filter on table {}, field {}",
          hiveTableHandle->tableName(),
          field.toString());
      VELOX_USER_CHECK_GT(
          partitionKeys_.count(nestedField->name()),
          0,
          "Unexpected filter on table {}, field {}",
          hiveTableHandle->tableName(),
          field.toString());
    }
    remainingFilter = hiveTableHandle->remainingFilter();
  }

  std::vector<common::Subfield> remainingFilterSubfields;
  if (remainingFilter) {
    remainingFilterExprSet_ = expressionEvaluator_->compile(remainingFilter);
    auto& remainingFilterExpr = remainingFilterExprSet_->expr(0);
    folly::F14FastSet<std::string> columnNames(
        readerRowNames.begin(), readerRowNames.end());
    for (auto& input : remainingFilterExpr->distinctFields()) {
      if (columnNames.count(input->field()) > 0) {
        continue;
      }
      // Remaining filter may reference columns that are not used otherwise,
      // e.g. are not being projected out and are not used in range filters.
      // Make sure to add these columns to readerOutputType_.
      readerRowNames.push_back(input->field());
      readerRowTypes.push_back(input->type());
    }
    remainingFilterSubfields = remainingFilterExpr->extractSubfields();
    if (VLOG_IS_ON(1)) {
      VLOG(1) << fmt::format(
          "Extracted subfields from remaining filter: [{}]",
          fmt::join(remainingFilterSubfields, ", "));
    }
    for (auto& subfield : remainingFilterSubfields) {
      auto& name = getColumnName(subfield);
      auto it = subfields.find(name);
      if (it != subfields.end()) {
        // Only subfields of the column are projected out.
        it->second.push_back(&subfield);
      } else if (columnNames.count(name) == 0) {
        // Column appears only in remaining filter.
        subfields[name].push_back(&subfield);
      }
    }
  }

  readerOutputType_ = ROW(std::move(readerRowNames), std::move(readerRowTypes));
  scanSpec_ = makeScanSpec(
      readerOutputType_,
      subfields,
      filters,
      hiveTableHandle->dataColumns(),
      pool_);
  if (remainingFilter) {
    metadataFilter_ = std::make_shared<common::MetadataFilter>(
        *scanSpec_, *remainingFilter, expressionEvaluator_);
  }

  readerOpts_.setFileSchema(hiveTableHandle->dataColumns());
  rowReaderOpts_.setScanSpec(scanSpec_);
  rowReaderOpts_.setMetadataFilter(metadataFilter_);

  auto skipRowsIt = hiveTableHandle->tableParameters().find(
      dwio::common::TableParameter::kSkipHeaderLineCount);
  if (skipRowsIt != hiveTableHandle->tableParameters().end()) {
    rowReaderOpts_.setSkipRows(folly::to<uint64_t>(skipRowsIt->second));
  }

  ioStats_ = std::make_shared<io::IoStatistics>();
}

inline uint8_t parseDelimiter(const std::string& delim) {
  for (char const& ch : delim) {
    if (!std::isdigit(ch)) {
      return delim[0];
    }
  }
  return stoi(delim);
}

void HiveDataSource::parseSerdeParameters(
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
    return;
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
  dwio::common::SerDeOptions serDeOptions(
      fieldDelim, collectionDelim, mapKeyDelim);
  readerOpts_.setSerDeOptions(serDeOptions);
}

void HiveDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  VELOX_CHECK(
      split_ == nullptr,
      "Previous split has not been processed yet. Call next to process the split.");
  split_ = std::dynamic_pointer_cast<HiveConnectorSplit>(split);
  VELOX_CHECK(split_, "Wrong type of split");

  VLOG(1) << "Adding split " << split_->toString();

  fileHandle_ = fileHandleFactory_->generate(split_->filePath).second;
  auto input = createBufferedInput(*fileHandle_, readerOpts_);

  if (readerOpts_.getFileFormat() != dwio::common::FileFormat::UNKNOWN) {
    VELOX_CHECK(
        readerOpts_.getFileFormat() == split_->fileFormat,
        "HiveDataSource received splits of different formats: {} and {}",
        toString(readerOpts_.getFileFormat()),
        toString(split_->fileFormat));
  } else {
    parseSerdeParameters(split_->serdeParameters);
    readerOpts_.setFileFormat(split_->fileFormat);
  }

  reader_ = dwio::common::getReaderFactory(readerOpts_.getFileFormat())
                ->createReader(std::move(input), readerOpts_);

  emptySplit_ = false;
  if (reader_->numberOfRows() == 0) {
    emptySplit_ = true;
    return;
  }

  // Check filters and see if the whole split can be skipped.
  if (!testFilters(
          scanSpec_.get(),
          reader_.get(),
          split_->filePath,
          split_->partitionKeys,
          partitionKeys_)) {
    emptySplit_ = true;
    ++runtimeStats_.skippedSplits;
    runtimeStats_.skippedSplitBytes += split_->length;
    return;
  }

  auto& fileType = reader_->rowType();
  // Keep track of schema types for columns in file, used by ColumnSelector.
  std::vector<TypePtr> columnTypes = fileType->children();

  auto& childrenSpecs = scanSpec_->children();
  for (size_t i = 0; i < childrenSpecs.size(); ++i) {
    auto* childSpec = childrenSpecs[i].get();
    const std::string& fieldName = childSpec->fieldName();

    auto iter = split_->partitionKeys.find(fieldName);
    if (iter != split_->partitionKeys.end()) {
      setPartitionValue(childSpec, fieldName, iter->second);
    } else if (fieldName == kPath) {
      setConstantValue(childSpec, VARCHAR(), velox::variant(split_->filePath));
    } else if (fieldName == kBucket) {
      if (split_->tableBucketNumber.has_value()) {
        setConstantValue(
            childSpec,
            INTEGER(),
            velox::variant(split_->tableBucketNumber.value()));
      }
    } else {
      auto fileTypeIdx = fileType->getChildIdxIfExists(fieldName);
      if (!fileTypeIdx.has_value()) {
        // Column is missing. Most likely due to schema evolution.
        VELOX_CHECK(readerOpts_.getFileSchema());
        setNullConstantValue(
            childSpec, readerOpts_.getFileSchema()->findChild(fieldName));
      } else {
        // Column no longer missing, reset constant value set on the spec.
        childSpec->setConstantValue(nullptr);
        auto outputTypeIdx = readerOutputType_->getChildIdxIfExists(fieldName);
        if (outputTypeIdx.has_value()) {
          // We know the fieldName exists in the file, make the type at that
          // position match what we expect in the output.
          columnTypes[fileTypeIdx.value()] =
              readerOutputType_->childAt(*outputTypeIdx);
        }
      }
    }
  }

  scanSpec_->resetCachedValues(false);
  configureRowReaderOptions(
      rowReaderOpts_,
      ROW(std::vector<std::string>(fileType->names()), std::move(columnTypes)));
  // NOTE: we firstly reset the finished 'rowReader_' of previous split before
  // setting up for the next one to avoid doubling the peak memory usage.
  rowReader_.reset();
  rowReader_ = createRowReader(rowReaderOpts_);
}

std::optional<RowVectorPtr> HiveDataSource::next(
    uint64_t size,
    velox::ContinueFuture& /*future*/) {
  VELOX_CHECK(split_ != nullptr, "No split to process. Call addSplit first.");
  if (emptySplit_) {
    resetSplit();
    return nullptr;
  }

  if (!output_) {
    output_ = BaseVector::create(readerOutputType_, 0, pool_);
  }

  // TODO Check if remaining filter has a conjunct that doesn't depend on
  // any column, e.g. rand() < 0.1. Evaluate that conjunct first, then scan
  // only rows that passed.

  auto rowsScanned = readNext(size);
  completedRows_ += rowsScanned;

  if (rowsScanned) {
    VELOX_CHECK(
        !output_->mayHaveNulls(), "Top-level row vector cannot have nulls");
    auto rowsRemaining = output_->size();
    if (rowsRemaining == 0) {
      // no rows passed the pushed down filters.
      return getEmptyOutput();
    }

    auto rowVector = std::dynamic_pointer_cast<RowVector>(output_);

    // In case there is a remaining filter that excludes some but not all
    // rows, collect the indices of the passing rows. If there is no filter,
    // or it passes on all rows, leave this as null and let exec::wrap skip
    // wrapping the results.
    BufferPtr remainingIndices;
    if (remainingFilterExprSet_) {
      rowsRemaining = evaluateRemainingFilter(rowVector);
      VELOX_CHECK_LE(rowsRemaining, rowsScanned);
      if (rowsRemaining == 0) {
        // No rows passed the remaining filter.
        return getEmptyOutput();
      }

      if (rowsRemaining < rowVector->size()) {
        // Some, but not all rows passed the remaining filter.
        remainingIndices = filterEvalCtx_.selectedIndices;
      }
    }

    if (outputType_->size() == 0) {
      return exec::wrap(rowsRemaining, remainingIndices, rowVector);
    }

    std::vector<VectorPtr> outputColumns;
    outputColumns.reserve(outputType_->size());
    for (int i = 0; i < outputType_->size(); i++) {
      auto& child = rowVector->childAt(i);
      if (remainingIndices) {
        // Disable dictionary values caching in expression eval so that we
        // don't need to reallocate the result for every batch.
        child->disableMemo();
      }
      outputColumns.emplace_back(
          exec::wrapChild(rowsRemaining, remainingIndices, child));
    }

    return std::make_shared<RowVector>(
        pool_, outputType_, BufferPtr(nullptr), rowsRemaining, outputColumns);
  }

  rowReader_->updateRuntimeStats(runtimeStats_);

  resetSplit();
  return nullptr;
}

void HiveDataSource::addDynamicFilter(
    column_index_t outputChannel,
    const std::shared_ptr<common::Filter>& filter) {
  auto& fieldSpec = scanSpec_->getChildByChannel(outputChannel);
  fieldSpec.addFilter(*filter);
  scanSpec_->resetCachedValues(true);
  if (rowReader_) {
    rowReader_->resetFilterCaches();
  }
}

std::unordered_map<std::string, RuntimeCounter> HiveDataSource::runtimeStats() {
  auto res = runtimeStats_.toMap();
  res.insert(
      {{"numPrefetch", RuntimeCounter(ioStats_->prefetch().count())},
       {"prefetchBytes",
        RuntimeCounter(
            ioStats_->prefetch().sum(), RuntimeCounter::Unit::kBytes)},
       {"numStorageRead", RuntimeCounter(ioStats_->read().count())},
       {"storageReadBytes",
        RuntimeCounter(ioStats_->read().sum(), RuntimeCounter::Unit::kBytes)},
       {"numLocalRead", RuntimeCounter(ioStats_->ssdRead().count())},
       {"localReadBytes",
        RuntimeCounter(
            ioStats_->ssdRead().sum(), RuntimeCounter::Unit::kBytes)},
       {"numRamRead", RuntimeCounter(ioStats_->ramHit().count())},
       {"ramReadBytes",
        RuntimeCounter(ioStats_->ramHit().sum(), RuntimeCounter::Unit::kBytes)},
       {"totalScanTime",
        RuntimeCounter(
            ioStats_->totalScanTime(), RuntimeCounter::Unit::kNanos)},
       {"ioWaitNanos",
        RuntimeCounter(
            ioStats_->queryThreadIoLatency().sum() * 1000,
            RuntimeCounter::Unit::kNanos)},
       {"overreadBytes",
        RuntimeCounter(
            ioStats_->rawOverreadBytes(), RuntimeCounter::Unit::kBytes)},
       {"queryThreadIoLatency",
        RuntimeCounter(ioStats_->queryThreadIoLatency().count())}});
  return res;
}

void HiveDataSource::setFromDataSource(
    std::unique_ptr<DataSource> sourceUnique) {
  auto source = dynamic_cast<HiveDataSource*>(sourceUnique.get());
  VELOX_CHECK(source, "Bad DataSource type");
  emptySplit_ = source->emptySplit_;
  split_ = std::move(source->split_);
  if (emptySplit_) {
    return;
  }
  source->scanSpec_->moveAdaptationFrom(*scanSpec_);
  scanSpec_ = std::move(source->scanSpec_);
  reader_ = std::move(source->reader_);
  rowReader_ = std::move(source->rowReader_);
  // New io will be accounted on the stats of 'source'. Add the existing
  // balance to that.
  source->ioStats_->merge(*ioStats_);
  ioStats_ = std::move(source->ioStats_);
}

int64_t HiveDataSource::estimatedRowSize() {
  if (!rowReader_) {
    return kUnknownRowSize;
  }
  auto size = rowReader_->estimatedRowSize();
  if (size.has_value()) {
    return size.value();
  }
  return kUnknownRowSize;
}

std::shared_ptr<common::ScanSpec> HiveDataSource::makeScanSpec(
    const RowTypePtr& rowType,
    const folly::F14FastMap<std::string, std::vector<const common::Subfield*>>&
        outputSubfields,
    const SubfieldFilters& filters,
    const RowTypePtr& dataColumns,
    memory::MemoryPool* pool) {
  auto spec = std::make_shared<common::ScanSpec>("root");
  folly::F14FastMap<std::string, std::vector<const common::Subfield*>>
      filterSubfields;
  std::vector<SubfieldSpec> subfieldSpecs;
  for (auto& [subfield, _] : filters) {
    if (auto name = subfield.toString(); name != kPath && name != kBucket) {
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

std::unique_ptr<dwio::common::BufferedInput>
HiveDataSource::createBufferedInput(
    const FileHandle& fileHandle,
    const dwio::common::ReaderOptions& readerOpts) {
  if (cache_) {
    return std::make_unique<dwio::common::CachedBufferedInput>(
        fileHandle.file,
        dwio::common::MetricsLog::voidLog(),
        fileHandle.uuid.id(),
        cache_,
        Connector::getTracker(scanId_, readerOpts.loadQuantum()),
        fileHandle.groupId.id(),
        ioStats_,
        executor_,
        readerOpts);
  }
  return std::make_unique<dwio::common::BufferedInput>(
      fileHandle.file,
      readerOpts.getMemoryPool(),
      dwio::common::MetricsLog::voidLog(),
      ioStats_.get());
}

vector_size_t HiveDataSource::evaluateRemainingFilter(RowVectorPtr& rowVector) {
  filterRows_.resize(output_->size());

  expressionEvaluator_->evaluate(
      remainingFilterExprSet_.get(), filterRows_, *rowVector, filterResult_);
  return exec::processFilterResults(
      filterResult_, filterRows_, filterEvalCtx_, pool_);
}

void HiveDataSource::setConstantValue(
    common::ScanSpec* spec,
    const TypePtr& type,
    const velox::variant& value) const {
  spec->setConstantValue(BaseVector::createConstant(type, value, 1, pool_));
}

void HiveDataSource::setNullConstantValue(
    common::ScanSpec* spec,
    const TypePtr& type) const {
  spec->setConstantValue(BaseVector::createNullConstant(type, 1, pool_));
}

void HiveDataSource::setPartitionValue(
    common::ScanSpec* spec,
    const std::string& partitionKey,
    const std::optional<std::string>& value) const {
  auto it = partitionKeys_.find(partitionKey);
  VELOX_CHECK(
      it != partitionKeys_.end(),
      "ColumnHandle is missing for partition key {}",
      partitionKey);
  auto constValue = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      convertFromString, it->second->dataType()->kind(), value);
  setConstantValue(spec, it->second->dataType(), constValue);
}

void HiveDataSource::resetSplit() {
  split_.reset();
  // Keep readers around to hold adaptation.
}

void HiveDataSource::configureRowReaderOptions(
    dwio::common::RowReaderOptions& options,
    const RowTypePtr& rowType) const {
  std::vector<std::string> columnNames;
  for (auto& spec : scanSpec_->children()) {
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
  options.select(cs).range(split_->start, split_->length);
}

} // namespace facebook::velox::connector::hive
