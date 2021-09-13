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
#include "velox/connectors/hive/HiveConnector.h"
#include <velox/dwio/dwrf/reader/SelectiveColumnReader.h>
#include "velox/dwio/common/InputStream.h"
#include "velox/expression/ControlExpr.h"

using namespace facebook::velox::dwrf;
using WriterConfig = facebook::velox::dwrf::Config;

DEFINE_int32(
    file_handle_cache_mb,
    1024,
    "Amount of space for the file handle cache in mb.");

namespace facebook::velox::connector::hive {

namespace {
static const char* kPath = "$path";
static const char* kBucket = "$bucket";
} // namespace

HiveDataSink::HiveDataSink(
    std::shared_ptr<const RowType> inputType,
    const std::string& filePath,
    velox::memory::MemoryPool* memoryPool)
    : inputType_(inputType) {
  auto config = std::make_shared<WriterConfig>();
  // TODO: Wire up serde properties to writer configs.

  facebook::velox::dwrf::WriterOptions options;
  options.config = config;
  options.schema = inputType;
  // Without explicitly setting flush policy, the default memory based flush
  // policy is used.

  auto sink = facebook::dwio::common::DataSink::create(filePath);
  writer_ = std::make_unique<Writer>(options, std::move(sink), *memoryPool);
}

void HiveDataSink::appendData(VectorPtr input) {
  writer_->write(input);
}

void HiveDataSink::close() {
  writer_->close();
}

namespace {
static void makeFieldSpecs(
    const std::string& pathPrefix,
    int32_t level,
    const std::shared_ptr<const RowType>& type,
    common::ScanSpec* spec) {
  for (auto i = 0; i < type->size(); ++i) {
    std::string path =
        level == 0 ? type->nameOf(i) : pathPrefix + "." + type->nameOf(i);
    common::Subfield subfield(path);
    common::ScanSpec* fieldSpec = spec->getOrCreateChild(subfield);
    fieldSpec->setProjectOut(true);
    if (level == 0) {
      fieldSpec->setChannel(i);
    }
    auto fieldType = type->childAt(i);
    if (fieldType->kind() == TypeKind::ROW) {
      makeFieldSpecs(
          path,
          level + 1,
          std::static_pointer_cast<const RowType>(fieldType),
          spec);
    }
  }
}

std::unique_ptr<common::ScanSpec> makeScanSpec(
    const SubfieldFilters& filters,
    const std::shared_ptr<const RowType>& rowType) {
  auto spec = std::make_unique<common::ScanSpec>("root");
  makeFieldSpecs("", 0, rowType, spec.get());

  for (auto& pair : filters) {
    // SelectiveColumnReader doesn't support constant columns with filters,
    // hence, we can't have a filter for a $path or $bucket column.
    //
    // Unfortunately, Presto happens to specify a filter for $path or $bucket
    // column. This filter is redundant and needs to be removed.
    // TODO Remove this check when Presto is fixed to not specify a filter
    // on $path and $bucket column.
    if (pair.first.toString() == kPath || pair.first.toString() == kBucket) {
      continue;
    }
    auto fieldSpec = spec->getOrCreateChild(pair.first);
    fieldSpec->setFilter(pair.second->clone());
  }
  return spec;
}
} // namespace

HiveDataSource::HiveDataSource(
    const std::shared_ptr<const RowType>& outputType,
    const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& columnHandles,
    FileHandleFactory* fileHandleFactory,
    velox::memory::MemoryPool* pool,
    DataCache* dataCache,
    ExpressionEvaluator* expressionEvaluator)
    : outputType_(outputType),
      fileHandleFactory_(fileHandleFactory),
      pool_(pool),
      readerOpts_(pool),
      dataCache_(dataCache),
      expressionEvaluator_(expressionEvaluator) {
  regularColumns_.reserve(outputType->size());

  std::vector<std::string> columnNames;
  columnNames.reserve(outputType->size());
  for (auto& name : outputType->names()) {
    auto it = columnHandles.find(name);
    VELOX_CHECK(
        it != columnHandles.end(),
        "ColumnHandle is missing for output column {}",
        name);

    auto handle = std::dynamic_pointer_cast<HiveColumnHandle>(it->second);
    VELOX_CHECK(
        handle != nullptr,
        "ColumnHandle must be an instance of HiveColumnHandle for {}",
        name);

    columnNames.emplace_back(handle->name());
    if (handle->columnType() == HiveColumnHandle::ColumnType::kRegular) {
      regularColumns_.emplace_back(handle->name());
    }
  }

  auto hiveTableHandle =
      std::dynamic_pointer_cast<HiveTableHandle>(tableHandle);
  VELOX_CHECK(
      hiveTableHandle != nullptr,
      "TableHandle must be an instance of HiveTableHandle");
  VELOX_CHECK(
      hiveTableHandle->isFilterPushdownEnabled(),
      "Filter pushdown must be enabled");

  auto outputTypes = outputType_->children();
  readerOutputType_ = ROW(std::move(columnNames), std::move(outputTypes));
  scanSpec_ =
      makeScanSpec(hiveTableHandle->subfieldFilters(), readerOutputType_);

  const auto& remainingFilter = hiveTableHandle->remainingFilter();
  if (remainingFilter) {
    remainingFilterExprSet_ = expressionEvaluator_->compile(remainingFilter);

    // Remaining filter may reference columns that are not used otherwise,
    // e.g. are not being projected out and are not used in range filters.
    // Make sure to add these columns to scanSpec_.

    auto filterInputs = remainingFilterExprSet_->expr(0)->distinctFields();
    ChannelIndex channel = outputType_->size();
    auto names = readerOutputType_->names();
    auto types = readerOutputType_->children();
    for (auto& input : filterInputs) {
      if (readerOutputType_->containsChild(input->field())) {
        continue;
      }
      names.emplace_back(input->field());
      types.emplace_back(input->type());

      common::Subfield subfield(input->field());
      auto fieldSpec = scanSpec_->getOrCreateChild(subfield);
      fieldSpec->setProjectOut(true);
      fieldSpec->setChannel(channel++);
    }
    readerOutputType_ = ROW(std::move(names), std::move(types));
  }

  columnReaderFactory_ =
      std::make_unique<dwrf::SelectiveColumnReaderFactory>(scanSpec_.get());
  rowReaderOpts_.setColumnReaderFactory(columnReaderFactory_.get());

  ioStats_ = std::make_unique<dwio::common::IoStatistics>();
}

namespace {

bool testFilters(
    common::ScanSpec* scanSpec,
    dwrf::DwrfReader* reader,
    const std::string& filePath) {
  auto stats = reader->getStatistics();
  auto totalRows = reader->getFooter().numberofrows();
  const auto& fileTypeWithId = reader->getTypeWithId();
  const auto& rowType = reader->getType();
  for (const auto& child : scanSpec->children()) {
    if (child->filter()) {
      const auto& name = child->fieldName();
      if (!rowType->containsChild(name)) {
        // Column is missing. Most likely due to schema evolution.
        if (child->filter()->isDeterministic() &&
            !child->filter()->testNull()) {
          return false;
        }
      } else {
        const auto& typeWithId = fileTypeWithId->childByName(name);
        auto columnStats = reader->getColumnStatistics(typeWithId->id);
        if (!testFilter(
                child->filter(),
                columnStats.get(),
                totalRows,
                typeWithId->type)) {
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
} // namespace

void HiveDataSource::addDynamicFilter(
    ChannelIndex outputChannel,
    const std::shared_ptr<common::Filter>& filter) {
  auto& fieldSpec = scanSpec_->getChildByChannel(outputChannel);
  if (fieldSpec.filter()) {
    fieldSpec.setFilter(fieldSpec.filter()->mergeWith(filter.get()));
  } else {
    fieldSpec.setFilter(filter->clone());
  }
  scanSpec_->resetCachedValues();

  auto columnReader =
      dynamic_cast<SelectiveColumnReader*>(rowReader_->columnReader());
  assert(columnReader);
  columnReader->resetFilterCaches();
}

void HiveDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  VELOX_CHECK(
      split_ == nullptr,
      "Previous split has not been processed yet. Call next to process the split.");
  split_ = std::dynamic_pointer_cast<HiveConnectorSplit>(split);
  VELOX_CHECK(split_, "Wrong type of split");

  VLOG(1) << "Adding split " << split_->toString();

  fileHandle_ = fileHandleFactory_->generate(split_->filePath);
  if (dataCache_) {
    auto dataCacheConfig = std::make_shared<dwio::common::DataCacheConfig>();
    dataCacheConfig->cache = dataCache_;
    dataCacheConfig->filenum = fileHandle_->uuid.id();
    readerOpts_.setDataCacheConfig(std::move(dataCacheConfig));
  }

  reader_ = dwrf::DwrfReader::create(
      std::make_unique<dwio::common::ReadFileInputStream>(
          fileHandle_->file.get(),
          dwio::common::MetricsLog::voidLog(),
          ioStats_.get()),
      readerOpts_);

  emptySplit_ = false;
  if (reader_->getFooter().has_numberofrows() &&
      reader_->getFooter().numberofrows() == 0) {
    emptySplit_ = true;
    return;
  }

  // Check filters and see if the whole split can be skipped
  if (!testFilters(scanSpec_.get(), reader_.get(), split_->filePath)) {
    emptySplit_ = true;
    ++skippedSplits_;
    skippedSplitBytes_ += split_->length;
    return;
  }

  auto fileType = reader_->getType();

  for (int i = 0; i < readerOutputType_->size(); i++) {
    auto fieldName = readerOutputType_->nameOf(i);
    auto scanChildSpec = scanSpec_->childByName(fieldName);

    auto keyIt = split_->partitionKeys.find(fieldName);
    if (keyIt != split_->partitionKeys.end()) {
      setConstantValue(scanChildSpec, velox::variant(keyIt->second));
    } else if (fieldName == kPath) {
      setConstantValue(scanChildSpec, velox::variant(split_->filePath));
    } else if (fieldName == kBucket) {
      if (split_->tableBucketNumber.has_value()) {
        setConstantValue(
            scanChildSpec, velox::variant(split_->tableBucketNumber.value()));
      }
    } else if (!fileType->containsChild(fieldName)) {
      // Column is missing. Most likely due to schema evolution.
      setNullConstantValue(scanChildSpec, readerOutputType_->childAt(i));
    } else {
      scanChildSpec->setConstantValue(nullptr);
    }
  }

  // Set constant values for partition keys and $path column. If these are
  // used in filters only, the loop above will miss them.
  for (const auto& entry : split_->partitionKeys) {
    auto childSpec = scanSpec_->childByName(entry.first);
    if (childSpec) {
      setConstantValue(childSpec, velox::variant(entry.second));
    }
  }

  auto pathSpec = scanSpec_->childByName(kPath);
  if (pathSpec) {
    setConstantValue(pathSpec, velox::variant(split_->filePath));
  }

  auto bucketSpec = scanSpec_->childByName(kBucket);
  if (bucketSpec && split_->tableBucketNumber.has_value()) {
    setConstantValue(
        bucketSpec, velox::variant(split_->tableBucketNumber.value()));
  }

  std::vector<std::string> columnNames;
  for (auto& spec : scanSpec_->children()) {
    if (!spec->isConstant()) {
      columnNames.push_back(spec->fieldName());
    }
  }

  std::shared_ptr<dwio::common::ColumnSelector> cs;
  if (columnNames.empty()) {
    static const std::shared_ptr<const RowType> kEmpty{ROW({}, {})};
    cs = std::make_shared<dwio::common::ColumnSelector>(kEmpty);
  } else {
    cs = std::make_shared<dwio::common::ColumnSelector>(fileType, columnNames);
  }

  rowReader_ = reader_->createRowReader(
      rowReaderOpts_.select(cs).range(split_->start, split_->length));
}

RowVectorPtr HiveDataSource::next(uint64_t size) {
  VELOX_CHECK(split_ != nullptr, "No split to process. Call addSplit first.");
  if (emptySplit_) {
    split_.reset();
    reader_.reset();
    rowReader_.reset();
    return nullptr;
  }

  if (!output_) {
    output_ = BaseVector::create(readerOutputType_, 0, pool_);
  }

  // TODO Check if remaining filter has a conjunct that doesn't depend on any
  // column, e.g. rand() < 0.1. Evaluate that conjunct first, then scan only
  // rows that passed.

  auto rowsScanned = rowReader_->next(size, output_);
  completedRows_ += rowsScanned;

  if (rowsScanned) {
    VELOX_CHECK(
        !output_->mayHaveNulls(), "Top-level row vector cannot have nulls");
    auto rowsRemaining = output_->size();
    if (rowsRemaining == 0) {
      // no rows passed the pushed down filters
      return RowVector::createEmpty(outputType_, pool_);
    }

    auto rowVector = std::dynamic_pointer_cast<RowVector>(output_);

    BufferPtr remainingIndices;
    if (remainingFilterExprSet_) {
      rowsRemaining = evaluateRemainingFilter(rowVector);
      VELOX_CHECK_LE(rowsRemaining, rowsScanned);
      if (rowsRemaining == 0) {
        // no rows passed the remaining filter
        return RowVector::createEmpty(outputType_, pool_);
      }

      if (rowsRemaining < rowsScanned) {
        remainingIndices = filterEvalCtx_.selectedIndices;
      }
    }

    if (outputType_->size() == 0) {
      return exec::wrap(rowsRemaining, remainingIndices, rowVector);
    }

    std::vector<VectorPtr> outputColumns;
    outputColumns.reserve(outputType_->size());
    for (int i = 0; i < outputType_->size(); i++) {
      outputColumns.emplace_back(exec::wrapChild(
          rowsRemaining, remainingIndices, rowVector->childAt(i)));
    }

    return std::make_shared<RowVector>(
        pool_, outputType_, BufferPtr(nullptr), rowsRemaining, outputColumns);
  }

  skippedStrides_ += rowReader_->skippedStrides();

  split_.reset();
  reader_.reset();
  rowReader_.reset();
  return nullptr;
}

vector_size_t HiveDataSource::evaluateRemainingFilter(RowVectorPtr& rowVector) {
  filterRows_.resize(output_->size());

  expressionEvaluator_->evaluate(
      remainingFilterExprSet_.get(), filterRows_, rowVector, &filterResult_);
  return exec::processFilterResults(
      filterResult_, filterRows_, filterEvalCtx_, pool_);
}

void HiveDataSource::setConstantValue(
    common::ScanSpec* spec,
    const velox::variant& value) const {
  spec->setConstantValue(BaseVector::createConstant(value, 1, pool_));
}

void HiveDataSource::setNullConstantValue(
    common::ScanSpec* spec,
    const TypePtr& type) const {
  spec->setConstantValue(BaseVector::createNullConstant(type, 1, pool_));
}

HiveConnector::HiveConnector(
    const std::string& id,
    std::unique_ptr<DataCache> dataCache)
    : Connector(id),
      dataCache_(std::move(dataCache)),
      fileHandleFactory_(
          std::make_unique<SimpleLRUCache<std::string, FileHandle>>(
              FLAGS_file_handle_cache_mb << 20),
          std::make_unique<FileHandleGenerator>()) {}

VELOX_REGISTER_CONNECTOR_FACTORY(std::make_shared<HiveConnectorFactory>())

} // namespace facebook::velox::connector::hive
