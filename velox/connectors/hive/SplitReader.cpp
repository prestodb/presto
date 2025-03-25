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

#include "velox/connectors/hive/SplitReader.h"

#include "velox/common/caching/CacheTTLController.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/hive/HiveConnectorUtil.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/connectors/hive/iceberg/IcebergSplitReader.h"
#include "velox/dwio/common/CachedBufferedInput.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/type/TimestampConversion.h"

namespace facebook::velox::connector::hive {
namespace {

template <TypeKind kind>
VectorPtr newConstantFromString(
    const TypePtr& type,
    const std::optional<std::string>& value,
    vector_size_t size,
    velox::memory::MemoryPool* pool,
    const std::string& sessionTimezone,
    bool asLocalTime,
    bool isPartitionDateDaysSinceEpoch = false) {
  using T = typename TypeTraits<kind>::NativeType;
  if (!value.has_value()) {
    return std::make_shared<ConstantVector<T>>(pool, size, true, type, T());
  }

  if (type->isDate()) {
    int32_t days = 0;
    // For Iceberg, the date partition values are already in daysSinceEpoch
    // form.
    if (isPartitionDateDaysSinceEpoch) {
      days = folly::to<int32_t>(value.value());
    } else {
      days = DATE()->toDays(static_cast<folly::StringPiece>(value.value()));
    }
    return std::make_shared<ConstantVector<int32_t>>(
        pool, size, false, type, std::move(days));
  }

  if constexpr (std::is_same_v<T, StringView>) {
    return std::make_shared<ConstantVector<StringView>>(
        pool, size, false, type, StringView(value.value()));
  } else {
    auto copy = velox::util::Converter<kind>::tryCast(value.value())
                    .thenOrThrow(folly::identity, [&](const Status& status) {
                      VELOX_USER_FAIL("{}", status.message());
                    });
    if constexpr (kind == TypeKind::TIMESTAMP) {
      if (asLocalTime) {
        copy.toGMT(Timestamp::defaultTimezone());
      }
    }
    return std::make_shared<ConstantVector<T>>(
        pool, size, false, type, std::move(copy));
  }
}
} // namespace

std::unique_ptr<SplitReader> SplitReader::create(
    const std::shared_ptr<hive::HiveConnectorSplit>& hiveSplit,
    const std::shared_ptr<const HiveTableHandle>& hiveTableHandle,
    const std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>*
        partitionKeys,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<const HiveConfig>& hiveConfig,
    const RowTypePtr& readerOutputType,
    const std::shared_ptr<io::IoStatistics>& ioStats,
    const std::shared_ptr<filesystems::File::IoStats>& fsStats,
    FileHandleFactory* fileHandleFactory,
    folly::Executor* executor,
    const std::shared_ptr<common::ScanSpec>& scanSpec) {
  //  Create the SplitReader based on hiveSplit->customSplitInfo["table_format"]
  if (hiveSplit->customSplitInfo.count("table_format") > 0 &&
      hiveSplit->customSplitInfo["table_format"] == "hive-iceberg") {
    return std::make_unique<iceberg::IcebergSplitReader>(
        hiveSplit,
        hiveTableHandle,
        partitionKeys,
        connectorQueryCtx,
        hiveConfig,
        readerOutputType,
        ioStats,
        fsStats,
        fileHandleFactory,
        executor,
        scanSpec);
  } else {
    return std::unique_ptr<SplitReader>(new SplitReader(
        hiveSplit,
        hiveTableHandle,
        partitionKeys,
        connectorQueryCtx,
        hiveConfig,
        readerOutputType,
        ioStats,
        fsStats,
        fileHandleFactory,
        executor,
        scanSpec));
  }
}

SplitReader::SplitReader(
    const std::shared_ptr<const hive::HiveConnectorSplit>& hiveSplit,
    const std::shared_ptr<const HiveTableHandle>& hiveTableHandle,
    const std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>*
        partitionKeys,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<const HiveConfig>& hiveConfig,
    const RowTypePtr& readerOutputType,
    const std::shared_ptr<io::IoStatistics>& ioStats,
    const std::shared_ptr<filesystems::File::IoStats>& fsStats,
    FileHandleFactory* fileHandleFactory,
    folly::Executor* executor,
    const std::shared_ptr<common::ScanSpec>& scanSpec)
    : hiveSplit_(hiveSplit),
      hiveTableHandle_(hiveTableHandle),
      partitionKeys_(partitionKeys),
      connectorQueryCtx_(connectorQueryCtx),
      hiveConfig_(hiveConfig),
      readerOutputType_(readerOutputType),
      ioStats_(ioStats),
      fsStats_(fsStats),
      fileHandleFactory_(fileHandleFactory),
      executor_(executor),
      pool_(connectorQueryCtx->memoryPool()),
      scanSpec_(scanSpec),
      baseReaderOpts_(connectorQueryCtx->memoryPool()),
      emptySplit_(false) {}

void SplitReader::configureReaderOptions(
    std::shared_ptr<velox::random::RandomSkipTracker> randomSkip) {
  hive::configureReaderOptions(
      hiveConfig_,
      connectorQueryCtx_,
      hiveTableHandle_,
      hiveSplit_,
      baseReaderOpts_);
  baseReaderOpts_.setRandomSkip(std::move(randomSkip));
  baseReaderOpts_.setScanSpec(scanSpec_);
  baseReaderOpts_.setFileFormat(hiveSplit_->fileFormat);
}

void SplitReader::prepareSplit(
    std::shared_ptr<common::MetadataFilter> metadataFilter,
    dwio::common::RuntimeStatistics& runtimeStats) {
  createReader();
  if (emptySplit_) {
    return;
  }
  auto rowType = getAdaptedRowType();

  if (checkIfSplitIsEmpty(runtimeStats)) {
    VELOX_CHECK(emptySplit_);
    return;
  }

  createRowReader(std::move(metadataFilter), std::move(rowType));
}

uint64_t SplitReader::next(uint64_t size, VectorPtr& output) {
  if (!baseReaderOpts_.randomSkip()) {
    return baseRowReader_->next(size, output);
  }
  dwio::common::Mutation mutation;
  mutation.randomSkip = baseReaderOpts_.randomSkip().get();
  return baseRowReader_->next(size, output, &mutation);
}

void SplitReader::resetFilterCaches() {
  if (baseRowReader_) {
    baseRowReader_->resetFilterCaches();
  }
}

bool SplitReader::emptySplit() const {
  return emptySplit_;
}

void SplitReader::resetSplit() {
  hiveSplit_.reset();
}

int64_t SplitReader::estimatedRowSize() const {
  if (!baseRowReader_) {
    return DataSource::kUnknownRowSize;
  }

  const auto size = baseRowReader_->estimatedRowSize();
  return size.value_or(DataSource::kUnknownRowSize);
}

void SplitReader::updateRuntimeStats(
    dwio::common::RuntimeStatistics& stats) const {
  if (baseRowReader_) {
    baseRowReader_->updateRuntimeStats(stats);
  }
}

bool SplitReader::allPrefetchIssued() const {
  return baseRowReader_ && baseRowReader_->allPrefetchIssued();
}

void SplitReader::setConnectorQueryCtx(
    const ConnectorQueryCtx* connectorQueryCtx) {
  connectorQueryCtx_ = connectorQueryCtx;
}

std::string SplitReader::toString() const {
  std::string partitionKeys;
  std::for_each(
      partitionKeys_->begin(), partitionKeys_->end(), [&](const auto& column) {
        partitionKeys += " " + column.second->toString();
      });
  return fmt::format(
      "SplitReader: hiveSplit_{} scanSpec_{} readerOutputType_{} partitionKeys_{} reader{} rowReader{}",
      hiveSplit_->toString(),
      scanSpec_->toString(),
      readerOutputType_->toString(),
      partitionKeys,
      static_cast<const void*>(baseReader_.get()),
      static_cast<const void*>(baseRowReader_.get()));
}

void SplitReader::createReader() {
  VELOX_CHECK_NE(
      baseReaderOpts_.fileFormat(), dwio::common::FileFormat::UNKNOWN);

  FileHandleCachedPtr fileHandleCachePtr;
  try {
    fileHandleCachePtr = fileHandleFactory_->generate(
        hiveSplit_->filePath,
        hiveSplit_->properties.has_value() ? &*hiveSplit_->properties : nullptr,
        fsStats_ ? fsStats_.get() : nullptr);
    VELOX_CHECK_NOT_NULL(fileHandleCachePtr.get());
  } catch (const VeloxRuntimeError& e) {
    if (e.errorCode() == error_code::kFileNotFound &&
        hiveConfig_->ignoreMissingFiles(
            connectorQueryCtx_->sessionProperties())) {
      emptySplit_ = true;
      return;
    }
    throw;
  }

  // Here we keep adding new entries to CacheTTLController when new fileHandles
  // are generated, if CacheTTLController was created. Creator of
  // CacheTTLController needs to make sure a size control strategy was available
  // such as removing aged out entries.
  if (auto* cacheTTLController = cache::CacheTTLController::getInstance()) {
    cacheTTLController->addOpenFileInfo(fileHandleCachePtr->uuid.id());
  }
  auto baseFileInput = createBufferedInput(
      *fileHandleCachePtr,
      baseReaderOpts_,
      connectorQueryCtx_,
      ioStats_,
      fsStats_,
      executor_);

  baseReader_ = dwio::common::getReaderFactory(baseReaderOpts_.fileFormat())
                    ->createReader(std::move(baseFileInput), baseReaderOpts_);
}

RowTypePtr SplitReader::getAdaptedRowType() const {
  auto& fileType = baseReader_->rowType();
  auto columnTypes = adaptColumns(fileType, baseReaderOpts_.fileSchema());
  auto columnNames = fileType->names();
  return ROW(std::move(columnNames), std::move(columnTypes));
}

bool SplitReader::filterOnStats(
    dwio::common::RuntimeStatistics& runtimeStats) const {
  if (testFilters(
          scanSpec_.get(),
          baseReader_.get(),
          hiveSplit_->filePath,
          hiveSplit_->partitionKeys,
          *partitionKeys_,
          hiveConfig_->readTimestampPartitionValueAsLocalTime(
              connectorQueryCtx_->sessionProperties()))) {
    return true;
  }
  ++runtimeStats.skippedSplits;
  runtimeStats.skippedSplitBytes += hiveSplit_->length;
  return false;
}

bool SplitReader::checkIfSplitIsEmpty(
    dwio::common::RuntimeStatistics& runtimeStats) {
  // emptySplit_ may already be set if the data file is not found. In this case
  // we don't need to test further.
  if (emptySplit_) {
    return true;
  }
  if (!baseReader_ || baseReader_->numberOfRows() == 0 ||
      !filterOnStats(runtimeStats)) {
    emptySplit_ = true;
  }
  return emptySplit_;
}

void SplitReader::createRowReader(
    std::shared_ptr<common::MetadataFilter> metadataFilter,
    RowTypePtr rowType) {
  VELOX_CHECK_NULL(baseRowReader_);
  configureRowReaderOptions(
      hiveTableHandle_->tableParameters(),
      scanSpec_,
      std::move(metadataFilter),
      std::move(rowType),
      hiveSplit_,
      hiveConfig_,
      connectorQueryCtx_->sessionProperties(),
      baseRowReaderOpts_);
  baseRowReader_ = baseReader_->createRowReader(baseRowReaderOpts_);
}

std::vector<TypePtr> SplitReader::adaptColumns(
    const RowTypePtr& fileType,
    const std::shared_ptr<const velox::RowType>& tableSchema) const {
  // Keep track of schema types for columns in file, used by ColumnSelector.
  std::vector<TypePtr> columnTypes = fileType->children();

  auto& childrenSpecs = scanSpec_->children();
  for (size_t i = 0; i < childrenSpecs.size(); ++i) {
    auto* childSpec = childrenSpecs[i].get();
    const std::string& fieldName = childSpec->fieldName();

    if (auto it = hiveSplit_->partitionKeys.find(fieldName);
        it != hiveSplit_->partitionKeys.end()) {
      setPartitionValue(childSpec, fieldName, it->second);
    } else if (auto iter = hiveSplit_->infoColumns.find(fieldName);
               iter != hiveSplit_->infoColumns.end()) {
      auto infoColumnType =
          readerOutputType_->childAt(readerOutputType_->getChildIdx(fieldName));
      auto constant = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
          newConstantFromString,
          infoColumnType->kind(),
          infoColumnType,
          iter->second,
          1,
          connectorQueryCtx_->memoryPool(),
          connectorQueryCtx_->sessionTimezone(),
          hiveConfig_->readTimestampPartitionValueAsLocalTime(
              connectorQueryCtx_->sessionProperties()));
      childSpec->setConstantValue(constant);
    } else if (
        childSpec->columnType() == common::ScanSpec::ColumnType::kRegular) {
      auto fileTypeIdx = fileType->getChildIdxIfExists(fieldName);
      if (!fileTypeIdx.has_value()) {
        // Column is missing. Most likely due to schema evolution.
        VELOX_CHECK(tableSchema, "Unable to resolve column '{}'", fieldName);
        childSpec->setConstantValue(BaseVector::createNullConstant(
            tableSchema->findChild(fieldName),
            1,
            connectorQueryCtx_->memoryPool()));
      } else {
        // Column no longer missing, reset constant value set on the spec.
        childSpec->setConstantValue(nullptr);
        auto outputTypeIdx = readerOutputType_->getChildIdxIfExists(fieldName);
        if (outputTypeIdx.has_value()) {
          auto& outputType = readerOutputType_->childAt(*outputTypeIdx);
          auto& columnType = columnTypes[*fileTypeIdx];
          if (childSpec->isFlatMapAsStruct()) {
            // Flat map column read as struct.  Leave the schema type as MAP.
            VELOX_CHECK(outputType->isRow() && columnType->isMap());
          } else {
            // We know the fieldName exists in the file, make the type at that
            // position match what we expect in the output.
            columnType = outputType;
          }
        }
      }
    }
  }

  scanSpec_->resetCachedValues(false);

  return columnTypes;
}

void SplitReader::setPartitionValue(
    common::ScanSpec* spec,
    const std::string& partitionKey,
    const std::optional<std::string>& value) const {
  auto it = partitionKeys_->find(partitionKey);
  VELOX_CHECK(
      it != partitionKeys_->end(),
      "ColumnHandle is missing for partition key {}",
      partitionKey);
  auto type = it->second->dataType();
  auto constant = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
      newConstantFromString,
      type->kind(),
      type,
      value,
      1,
      connectorQueryCtx_->memoryPool(),
      connectorQueryCtx_->sessionTimezone(),
      hiveConfig_->readTimestampPartitionValueAsLocalTime(
          connectorQueryCtx_->sessionProperties()),
      it->second->isPartitionDateValueDaysSinceEpoch());
  spec->setConstantValue(constant);
}

} // namespace facebook::velox::connector::hive
