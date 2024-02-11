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
#include "velox/dwio/common/ReaderFactory.h"

namespace facebook::velox::connector::hive {

std::unique_ptr<SplitReader> SplitReader::create(
    const std::shared_ptr<velox::connector::hive::HiveConnectorSplit>&
        hiveSplit,
    const std::shared_ptr<HiveTableHandle>& hiveTableHandle,
    const std::shared_ptr<common::ScanSpec>& scanSpec,
    const RowTypePtr& readerOutputType,
    std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>*
        partitionKeys,
    FileHandleFactory* fileHandleFactory,
    folly::Executor* executor,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<HiveConfig>& hiveConfig,
    const std::shared_ptr<io::IoStatistics>& ioStats) {
  return std::make_unique<SplitReader>(
      hiveSplit,
      hiveTableHandle,
      scanSpec,
      readerOutputType,
      partitionKeys,
      fileHandleFactory,
      executor,
      connectorQueryCtx,
      hiveConfig,
      ioStats);
}

SplitReader::SplitReader(
    const std::shared_ptr<velox::connector::hive::HiveConnectorSplit>&
        hiveSplit,
    const std::shared_ptr<HiveTableHandle>& hiveTableHandle,
    const std::shared_ptr<common::ScanSpec>& scanSpec,
    const RowTypePtr& readerOutputType,
    std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>*
        partitionKeys,
    FileHandleFactory* fileHandleFactory,
    folly::Executor* executor,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<HiveConfig>& hiveConfig,
    const std::shared_ptr<io::IoStatistics>& ioStats)
    : hiveSplit_(hiveSplit),
      hiveTableHandle_(hiveTableHandle),
      scanSpec_(scanSpec),
      readerOutputType_(readerOutputType),
      partitionKeys_(partitionKeys),
      pool_(connectorQueryCtx->memoryPool()),
      fileHandleFactory_(fileHandleFactory),
      executor_(executor),
      connectorQueryCtx_(connectorQueryCtx),
      hiveConfig_(hiveConfig),
      ioStats_(ioStats),
      baseReaderOpts_(connectorQueryCtx->memoryPool()) {}

void SplitReader::configureReaderOptions() {
  hive::configureReaderOptions(
      baseReaderOpts_,
      hiveConfig_,
      connectorQueryCtx_->sessionProperties(),
      hiveTableHandle_,
      hiveSplit_);
}

void SplitReader::prepareSplit(
    std::shared_ptr<common::MetadataFilter> metadataFilter,
    dwio::common::RuntimeStatistics& runtimeStats) {
  VELOX_CHECK_NE(
      baseReaderOpts_.getFileFormat(), dwio::common::FileFormat::UNKNOWN);

  std::shared_ptr<FileHandle> fileHandle;
  try {
    fileHandle = fileHandleFactory_->generate(hiveSplit_->filePath).second;
  } catch (VeloxRuntimeError& e) {
    if (e.errorCode() == error_code::kFileNotFound.c_str() &&
        hiveConfig_->ignoreMissingFiles(
            connectorQueryCtx_->sessionProperties())) {
      emptySplit_ = true;
      return;
    } else {
      throw;
    }
  }
  // Here we keep adding new entries to CacheTTLController when new fileHandles
  // are generated, if CacheTTLController was created. Creator of
  // CacheTTLController needs to make sure a size control strategy was available
  // such as removing aged out entries.
  if (auto* cacheTTLController = cache::CacheTTLController::getInstance()) {
    cacheTTLController->addOpenFileInfo(fileHandle->uuid.id());
  }
  auto baseFileInput = createBufferedInput(
      *fileHandle, baseReaderOpts_, connectorQueryCtx_, ioStats_, executor_);

  baseReader_ = dwio::common::getReaderFactory(baseReaderOpts_.getFileFormat())
                    ->createReader(std::move(baseFileInput), baseReaderOpts_);

  // Note that this doesn't apply to Hudi tables.
  emptySplit_ = false;
  if (baseReader_->numberOfRows() == 0) {
    emptySplit_ = true;
    return;
  }

  // Check filters and see if the whole split can be skipped. Note that this
  // doesn't apply to Hudi tables.
  if (!testFilters(
          scanSpec_.get(),
          baseReader_.get(),
          hiveSplit_->filePath,
          hiveSplit_->partitionKeys,
          partitionKeys_)) {
    emptySplit_ = true;
    ++runtimeStats.skippedSplits;
    runtimeStats.skippedSplitBytes += hiveSplit_->length;
    return;
  }

  auto& fileType = baseReader_->rowType();
  auto columnTypes = adaptColumns(fileType, baseReaderOpts_.getFileSchema());

  configureRowReaderOptions(
      baseRowReaderOpts_,
      hiveTableHandle_->tableParameters(),
      scanSpec_,
      metadataFilter,
      ROW(std::vector<std::string>(fileType->names()), std::move(columnTypes)),
      hiveSplit_);
  // NOTE: we firstly reset the finished 'baseRowReader_' of previous split
  // before setting up for the next one to avoid doubling the peak memory usage.
  baseRowReader_.reset();
  baseRowReader_ = baseReader_->createRowReader(baseRowReaderOpts_);
}

std::vector<TypePtr> SplitReader::adaptColumns(
    const RowTypePtr& fileType,
    const std::shared_ptr<const velox::RowType>& tableSchema) {
  // Keep track of schema types for columns in file, used by ColumnSelector.
  std::vector<TypePtr> columnTypes = fileType->children();

  auto& childrenSpecs = scanSpec_->children();
  for (size_t i = 0; i < childrenSpecs.size(); ++i) {
    auto* childSpec = childrenSpecs[i].get();
    const std::string& fieldName = childSpec->fieldName();

    auto iter = hiveSplit_->partitionKeys.find(fieldName);
    if (iter != hiveSplit_->partitionKeys.end()) {
      setPartitionValue(childSpec, fieldName, iter->second);
    } else if (fieldName == kPath) {
      setConstantValue(
          childSpec, VARCHAR(), velox::variant(hiveSplit_->filePath));
    } else if (fieldName == kBucket) {
      if (hiveSplit_->tableBucketNumber.has_value()) {
        setConstantValue(
            childSpec,
            INTEGER(),
            velox::variant(hiveSplit_->tableBucketNumber.value()));
      }
    } else {
      auto fileTypeIdx = fileType->getChildIdxIfExists(fieldName);
      if (!fileTypeIdx.has_value()) {
        // Column is missing. Most likely due to schema evolution.
        VELOX_CHECK(tableSchema);
        setNullConstantValue(childSpec, tableSchema->findChild(fieldName));
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

  return columnTypes;
}

uint64_t SplitReader::next(int64_t size, VectorPtr& output) {
  return baseRowReader_->next(size, output);
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

  auto size = baseRowReader_->estimatedRowSize();
  if (size.has_value()) {
    return size.value();
  }
  return DataSource::kUnknownRowSize;
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

void SplitReader::setConstantValue(
    common::ScanSpec* spec,
    const TypePtr& type,
    const velox::variant& value) const {
  spec->setConstantValue(BaseVector::createConstant(
      type, value, 1, connectorQueryCtx_->memoryPool()));
}

void SplitReader::setNullConstantValue(
    common::ScanSpec* spec,
    const TypePtr& type) const {
  spec->setConstantValue(BaseVector::createNullConstant(
      type, 1, connectorQueryCtx_->memoryPool()));
}

namespace {

template <TypeKind ToKind>
velox::variant convertFromString(
    const std::optional<std::string>& value,
    const TypePtr& toType) {
  if (value.has_value()) {
    if constexpr (ToKind == TypeKind::VARCHAR) {
      return velox::variant(value.value());
    }
    if constexpr (ToKind == TypeKind::VARBINARY) {
      return velox::variant::binary((value.value()));
    }
    if (toType->isDate()) {
      return velox::variant(util::castFromDateString(
          StringView(value.value()), true /*isIso8601*/));
    }
    auto result = velox::util::Converter<ToKind>::cast(value.value());
    if constexpr (ToKind == TypeKind::TIMESTAMP) {
      result.toGMT(Timestamp::defaultTimezone());
    }
    return velox::variant(result);
  }
  return velox::variant(ToKind);
}

} // namespace

void SplitReader::setPartitionValue(
    common::ScanSpec* spec,
    const std::string& partitionKey,
    const std::optional<std::string>& value) const {
  auto it = partitionKeys_->find(partitionKey);
  VELOX_CHECK(
      it != partitionKeys_->end(),
      "ColumnHandle is missing for partition key {}",
      partitionKey);
  auto constValue = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      convertFromString,
      it->second->dataType()->kind(),
      value,
      it->second->dataType());
  setConstantValue(spec, it->second->dataType(), constValue);
}

std::string SplitReader::toString() const {
  std::string partitionKeys;
  std::for_each(
      partitionKeys_->begin(),
      partitionKeys_->end(),
      [&](std::pair<
          const std::string,
          std::shared_ptr<facebook::velox::connector::hive::HiveColumnHandle>>
              column) { partitionKeys += " " + column.second->toString(); });
  return fmt::format(
      "SplitReader: hiveSplit_{} scanSpec_{} readerOutputType_{} partitionKeys_{} reader{} rowReader{}",
      hiveSplit_->toString(),
      scanSpec_->toString(),
      readerOutputType_->toString(),
      partitionKeys,
      static_cast<const void*>(baseReader_.get()),
      static_cast<const void*>(baseRowReader_.get()));
}

} // namespace facebook::velox::connector::hive

template <>
struct fmt::formatter<facebook::velox::dwio::common::FileFormat>
    : formatter<std::string> {
  auto format(
      facebook::velox::dwio::common::FileFormat fmt,
      format_context& ctx) {
    return formatter<std::string>::format(
        facebook::velox::dwio::common::toString(fmt), ctx);
  }
};
