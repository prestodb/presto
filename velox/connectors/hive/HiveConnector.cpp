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

#include <memory>

#include "velox/dwio/common/InputStream.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/dwio/dwrf/common/CachedBufferedInput.h"
#include "velox/dwio/dwrf/reader/SelectiveColumnReader.h"
#include "velox/expression/ControlExpr.h"
#include "velox/type/Conversions.h"
#include "velox/type/Type.h"
#include "velox/type/Variant.h"

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

HiveTableHandle::HiveTableHandle(
    const std::string& tableName,
    bool filterPushdownEnabled,
    SubfieldFilters subfieldFilters,
    const std::shared_ptr<const core::ITypedExpr>& remainingFilter)
    : tableName_(tableName),
      filterPushdownEnabled_(filterPushdownEnabled),
      subfieldFilters_(std::move(subfieldFilters)),
      remainingFilter_(remainingFilter) {}

HiveTableHandle::~HiveTableHandle() {}

std::string HiveTableHandle::toString() const {
  std::stringstream out;
  out << "Table: " << tableName_;
  if (!subfieldFilters_.empty()) {
    // Sort filters by subfield for deterministic output.
    std::map<std::string, common::Filter*> orderedFilters;
    for (const auto& [field, filter] : subfieldFilters_) {
      orderedFilters[field.toString()] = filter.get();
    }
    out << ", Filters: [";
    bool notFirstFilter = false;
    for (const auto& [field, filter] : orderedFilters) {
      if (notFirstFilter) {
        out << ", ";
      }
      out << "(" << field << ", " << filter->toString() << ")";
      notFirstFilter = true;
    }
    out << "]";
  }
  return out.str();
}

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

  auto sink = facebook::velox::dwio::common::DataSink::create(filePath);
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
    const TypePtr& type,
    common::ScanSpec* spec) {
  constexpr int32_t kNoChannel = -1;
  auto makeNestedSpec = [&](const std::string& name, int32_t channel) {
    std::string path = level == 0 ? name : pathPrefix + "." + name;
    auto fieldSpec = spec->getOrCreateChild(common::Subfield(path));
    fieldSpec->setProjectOut(true);
    if (channel != kNoChannel) {
      fieldSpec->setChannel(channel);
    }
    return path;
  };

  switch (type->kind()) {
    case TypeKind::ROW: {
      auto rowType = type->as<TypeKind::ROW>();
      for (auto i = 0; i < type->size(); ++i) {
        makeFieldSpecs(
            makeNestedSpec(rowType.nameOf(i), i),
            level + 1,
            type->childAt(i),
            spec);
      }
      break;
    }
    case TypeKind::MAP: {
      makeFieldSpecs(
          makeNestedSpec("keys", kNoChannel),
          level + 1,
          type->childAt(0),
          spec);
      makeFieldSpecs(
          makeNestedSpec("elements", kNoChannel),
          level + 1,
          type->childAt(1),
          spec);
      break;
    }
    case TypeKind::ARRAY: {
      makeFieldSpecs(
          makeNestedSpec("elements", kNoChannel),
          level + 1,
          type->childAt(0),
          spec);
      break;
    }

    default:
      break;
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
    ExpressionEvaluator* expressionEvaluator,
    memory::MappedMemory* mappedMemory,
    const std::string& scanId,
    folly::Executor* executor)
    : outputType_(outputType),
      fileHandleFactory_(fileHandleFactory),
      pool_(pool),
      readerOpts_(pool),
      dataCache_(dataCache),
      expressionEvaluator_(expressionEvaluator),
      mappedMemory_(mappedMemory),
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

  std::vector<std::string> columnNames;
  columnNames.reserve(outputType->size());
  for (auto& outputName : outputType->names()) {
    auto it = columnHandles.find(outputName);
    VELOX_CHECK(
        it != columnHandles.end(),
        "ColumnHandle is missing for output column: {}",
        outputName);

    const auto& handle = static_cast<HiveColumnHandle&>(*it->second);
    columnNames.emplace_back(handle.name());
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

  rowReaderOpts_.setScanSpec(scanSpec_.get());

  ioStats_ = std::make_shared<dwio::common::IoStatistics>();
}

namespace {
bool testFilters(
    common::ScanSpec* scanSpec,
    dwio::common::Reader* reader,
    const std::string& filePath) {
  auto totalRows = reader->numberOfRows();
  const auto& fileTypeWithId = reader->typeWithId();
  const auto& rowType = reader->rowType();
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
        auto columnStats = reader->columnStatistics(typeWithId->id);
        if (!testFilter(
                child->filter(),
                columnStats.get(),
                totalRows.value(),
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

class InputStreamHolder : public dwrf::AbstractInputStreamHolder {
 public:
  InputStreamHolder(
      FileHandleCachedPtr fileHandle,
      std::shared_ptr<dwio::common::IoStatistics> stats)
      : fileHandle_(std::move(fileHandle)), stats_(std::move(stats)) {
    input_ = std::make_unique<dwio::common::ReadFileInputStream>(
        fileHandle_->file.get(), dwio::common::MetricsLog::voidLog(), nullptr);
  }

  dwio::common::InputStream& get() override {
    return *input_;
  }

 private:
  FileHandleCachedPtr fileHandle_;
  // Keeps the pointer alive also in case of cancellation while reads
  // proceeding on different threads.
  std::shared_ptr<dwio::common::IoStatistics> stats_;
  std::unique_ptr<dwio::common::InputStream> input_;
};

std::unique_ptr<InputStreamHolder> makeStreamHolder(
    FileHandleFactory* factory,
    const std::string& path) {
  return std::make_unique<InputStreamHolder>(factory->generate(path), nullptr);
}

template <TypeKind ToKind>
velox::variant convertFromString(const std::optional<std::string>& value) {
  if (value.has_value()) {
    if constexpr (ToKind == TypeKind::VARCHAR) {
      return velox::variant(value.value());
    }
    bool nullOutput = false;
    auto result =
        velox::util::Converter<ToKind>::cast(value.value(), nullOutput);
    VELOX_CHECK(
        not nullOutput, "Failed to cast {} to {}", value.value(), ToKind)
    return velox::variant(result);
  }
  return velox::variant(ToKind);
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
}

void HiveDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  VELOX_CHECK(
      split_ == nullptr,
      "Previous split has not been processed yet. Call next to process the split.");
  split_ = std::dynamic_pointer_cast<HiveConnectorSplit>(split);
  VELOX_CHECK(split_, "Wrong type of split");

  VLOG(1) << "Adding split " << split_->toString();

  fileHandle_ = fileHandleFactory_->generate(split_->filePath);
  // For DataCache and no cache, the stream keeps track of IO.
  auto asyncCache = dynamic_cast<cache::AsyncDataCache*>(mappedMemory_);
  // Decide between AsyncDataCache, legacy DataCache and no cache. All
  // three are supported to enable comparison.
  if (asyncCache) {
    VELOX_CHECK(
        !dataCache_,
        "DataCache should not be present if the MappedMemory is AsyncDataCache");
    // Make DataCacheConfig to pass the filenum and a null DataCache.
    if (!readerOpts_.getDataCacheConfig()) {
      auto dataCacheConfig = std::make_shared<dwio::common::DataCacheConfig>();
      readerOpts_.setDataCacheConfig(std::move(dataCacheConfig));
    }
    readerOpts_.getDataCacheConfig()->filenum = fileHandle_->uuid.id();
    bufferedInputFactory_ = std::make_unique<dwrf::CachedBufferedInputFactory>(
        (asyncCache),
        Connector::getTracker(scanId_, readerOpts_.loadQuantum()),
        fileHandle_->groupId.id(),
        [factory = fileHandleFactory_, path = split_->filePath]() {
          return makeStreamHolder(factory, path);
        },
        ioStats_,
        executor_,
        readerOpts_);
    readerOpts_.setBufferedInputFactory(bufferedInputFactory_.get());
  } else if (dataCache_) {
    auto dataCacheConfig = std::make_shared<dwio::common::DataCacheConfig>();
    dataCacheConfig->cache = dataCache_;
    dataCacheConfig->filenum = fileHandle_->uuid.id();
    readerOpts_.setDataCacheConfig(std::move(dataCacheConfig));
  }
  if (readerOpts_.getFileFormat() != dwio::common::FileFormat::UNKNOWN) {
    VELOX_CHECK(
        readerOpts_.getFileFormat() == split_->fileFormat,
        "HiveDataSource received splits of different formats: {} and {}",
        toString(readerOpts_.getFileFormat()),
        toString(split_->fileFormat));
  } else {
    readerOpts_.setFileFormat(split_->fileFormat);
  }

  // We run with the default BufferedInputFactory and no DataCacheConfig if
  // there is no DataCache and the MappedMemory is not an AsyncDataCache.
  reader_ = dwio::common::getReaderFactory(readerOpts_.getFileFormat())
                ->createReader(
                    std::make_unique<dwio::common::ReadFileInputStream>(
                        fileHandle_->file.get(),
                        dwio::common::MetricsLog::voidLog(),
                        asyncCache ? nullptr : ioStats_.get()),
                    readerOpts_);

  emptySplit_ = false;
  if (reader_->numberOfRows() == 0) {
    emptySplit_ = true;
    return;
  }

  // Check filters and see if the whole split can be skipped
  if (!testFilters(scanSpec_.get(), reader_.get(), split_->filePath)) {
    emptySplit_ = true;
    ++runtimeStats_.skippedSplits;
    runtimeStats_.skippedSplitBytes += split_->length;
    return;
  }

  auto fileType = reader_->rowType();

  for (int i = 0; i < readerOutputType_->size(); i++) {
    auto fieldName = readerOutputType_->nameOf(i);
    auto scanChildSpec = scanSpec_->childByName(fieldName);

    auto keyIt = split_->partitionKeys.find(fieldName);
    if (keyIt != split_->partitionKeys.end()) {
      setPartitionValue(scanChildSpec, fieldName, keyIt->second);
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
      setPartitionValue(childSpec, entry.first, entry.second);
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
    resetSplit();
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

  rowReader_->updateRuntimeStats(runtimeStats_);

  resetSplit();
  return nullptr;
}

void HiveDataSource::resetSplit() {
  split_.reset();
  // Make sure to destroy Reader and RowReader in the opposite order of
  // creation, e.g. destroy RowReader first, then destroy Reader.
  rowReader_.reset();
  reader_.reset();
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
  setConstantValue(spec, constValue);
}

std::unordered_map<std::string, RuntimeCounter> HiveDataSource::runtimeStats() {
  auto res = runtimeStats_.toMap();
  res.insert(
      {{"numPrefetch", RuntimeCounter(ioStats_->prefetch().count())},
       {"prefetchBytes",
        RuntimeCounter(
            ioStats_->prefetch().bytes(), RuntimeCounter::Unit::kBytes)},
       {"numStorageRead", RuntimeCounter(ioStats_->read().count())},
       {"storageReadBytes",
        RuntimeCounter(ioStats_->read().bytes(), RuntimeCounter::Unit::kBytes)},
       {"numLocalRead", RuntimeCounter(ioStats_->ssdRead().count())},
       {"localReadBytes",
        RuntimeCounter(
            ioStats_->ssdRead().bytes(), RuntimeCounter::Unit::kBytes)},
       {"numRamRead", RuntimeCounter(ioStats_->ramHit().count())},
       {"ramReadBytes",
        RuntimeCounter(
            ioStats_->ramHit().bytes(), RuntimeCounter::Unit::kBytes)}});
  return res;
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

HiveConnector::HiveConnector(
    const std::string& id,
    std::shared_ptr<const Config> properties,
    std::unique_ptr<DataCache> dataCache,
    folly::Executor* FOLLY_NULLABLE executor)
    : Connector(id, properties),
      dataCache_(std::move(dataCache)),
      fileHandleFactory_(
          std::make_unique<SimpleLRUCache<std::string, FileHandle>>(
              FLAGS_file_handle_cache_mb << 20),
          std::make_unique<FileHandleGenerator>(std::move(properties))),
      executor_(executor) {}

VELOX_REGISTER_CONNECTOR_FACTORY(std::make_shared<HiveConnectorFactory>())
VELOX_REGISTER_CONNECTOR_FACTORY(
    std::make_shared<HiveHadoop2ConnectorFactory>())
} // namespace facebook::velox::connector::hive
