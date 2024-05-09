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

#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveConnectorUtil.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/expression/FieldReference.h"

namespace facebook::velox::connector::hive {

class HiveTableHandle;
class HiveColumnHandle;

HiveDataSource::HiveDataSource(
    const RowTypePtr& outputType,
    const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& columnHandles,
    FileHandleFactory* fileHandleFactory,
    folly::Executor* executor,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<HiveConfig>& hiveConfig)
    : pool_(connectorQueryCtx->memoryPool()),
      fileHandleFactory_(fileHandleFactory),
      executor_(executor),
      connectorQueryCtx_(connectorQueryCtx),
      hiveConfig_(hiveConfig),
      outputType_(outputType),
      expressionEvaluator_(connectorQueryCtx->expressionEvaluator()) {
  // Column handled keyed on the column alias, the name used in the query.
  for (const auto& [canonicalizedName, columnHandle] : columnHandles) {
    auto handle = std::dynamic_pointer_cast<HiveColumnHandle>(columnHandle);
    VELOX_CHECK_NOT_NULL(
        handle,
        "ColumnHandle must be an instance of HiveColumnHandle for {}",
        canonicalizedName);

    if (handle->columnType() == HiveColumnHandle::ColumnType::kPartitionKey) {
      partitionKeys_.emplace(handle->name(), handle);
    }

    if (handle->columnType() == HiveColumnHandle::ColumnType::kSynthesized) {
      infoColumns_.emplace(handle->name(), handle);
    }

    if (handle->columnType() == HiveColumnHandle::ColumnType::kRowIndex) {
      rowIndexColumn_ = handle;
    }
  }

  std::vector<std::string> readerRowNames;
  auto readerRowTypes = outputType_->children();
  folly::F14FastMap<std::string, std::vector<const common::Subfield*>>
      subfields;
  for (const auto& outputName : outputType_->names()) {
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

  hiveTableHandle_ = std::dynamic_pointer_cast<HiveTableHandle>(tableHandle);
  VELOX_CHECK_NOT_NULL(
      hiveTableHandle_, "TableHandle must be an instance of HiveTableHandle");
  if (hiveConfig_->isFileColumnNamesReadAsLowerCase(
          connectorQueryCtx->sessionProperties())) {
    checkColumnNameLowerCase(outputType_);
    checkColumnNameLowerCase(hiveTableHandle_->subfieldFilters(), infoColumns_);
    checkColumnNameLowerCase(hiveTableHandle_->remainingFilter());
  }

  SubfieldFilters filters;
  for (const auto& [k, v] : hiveTableHandle_->subfieldFilters()) {
    filters.emplace(k.clone(), v->clone());
  }
  double sampleRate = 1;
  auto remainingFilter = extractFiltersFromRemainingFilter(
      hiveTableHandle_->remainingFilter(),
      expressionEvaluator_,
      false,
      filters,
      sampleRate);
  if (sampleRate != 1) {
    randomSkip_ = std::make_shared<random::RandomSkipTracker>(sampleRate);
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
      hiveTableHandle_->dataColumns(),
      partitionKeys_,
      infoColumns_,
      rowIndexColumn_,
      pool_);
  if (remainingFilter) {
    metadataFilter_ = std::make_shared<common::MetadataFilter>(
        *scanSpec_, *remainingFilter, expressionEvaluator_);
  }

  ioStats_ = std::make_shared<io::IoStatistics>();
}

std::unique_ptr<SplitReader> HiveDataSource::createSplitReader() {
  return SplitReader::create(
      split_,
      hiveTableHandle_,
      &partitionKeys_,
      connectorQueryCtx_,
      hiveConfig_,
      readerOutputType_,
      ioStats_,
      fileHandleFactory_,
      executor_,
      scanSpec_);
}

void HiveDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  VELOX_CHECK_NULL(
      split_,
      "Previous split has not been processed yet. Call next to process the split.");
  split_ = std::dynamic_pointer_cast<HiveConnectorSplit>(split);
  VELOX_CHECK_NOT_NULL(split_, "Wrong type of split");

  VLOG(1) << "Adding split " << split_->toString();

  if (splitReader_) {
    splitReader_.reset();
  }

  splitReader_ = createSplitReader();
  // Split reader subclasses may need to use the reader options in prepareSplit
  // so we initialize it beforehand.
  splitReader_->configureReaderOptions(randomSkip_);
  splitReader_->prepareSplit(metadataFilter_, runtimeStats_, rowIndexColumn_);
}

std::optional<RowVectorPtr> HiveDataSource::next(
    uint64_t size,
    velox::ContinueFuture& /*future*/) {
  VELOX_CHECK(split_ != nullptr, "No split to process. Call addSplit first.");
  VELOX_CHECK_NOT_NULL(splitReader_, "No split reader present");

  if (splitReader_->emptySplit()) {
    resetSplit();
    return nullptr;
  }

  if (!output_) {
    output_ = BaseVector::create(readerOutputType_, 0, pool_);
  }

  auto rowsScanned = splitReader_->next(size, output_);
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

  splitReader_->updateRuntimeStats(runtimeStats_);
  resetSplit();
  return nullptr;
}

void HiveDataSource::addDynamicFilter(
    column_index_t outputChannel,
    const std::shared_ptr<common::Filter>& filter) {
  auto& fieldSpec = scanSpec_->getChildByChannel(outputChannel);
  fieldSpec.addFilter(*filter);
  scanSpec_->resetCachedValues(true);
  if (splitReader_) {
    splitReader_->resetFilterCaches();
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
       {"totalRemainingFilterTime",
        RuntimeCounter(
            totalRemainingFilterTime_.load(std::memory_order_relaxed),
            RuntimeCounter::Unit::kNanos)},
       {"ioWaitNanos",
        RuntimeCounter(
            ioStats_->queryThreadIoLatency().sum() * 1000,
            RuntimeCounter::Unit::kNanos)},
       {"maxSingleIoWaitNanos",
        RuntimeCounter(
            ioStats_->queryThreadIoLatency().max() * 1000,
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
  VELOX_CHECK_NOT_NULL(source, "Bad DataSource type");

  split_ = std::move(source->split_);
  runtimeStats_.skippedSplits += source->runtimeStats_.skippedSplits;
  runtimeStats_.skippedSplitBytes += source->runtimeStats_.skippedSplitBytes;
  source->scanSpec_->moveAdaptationFrom(*scanSpec_);
  scanSpec_ = std::move(source->scanSpec_);
  splitReader_ = std::move(source->splitReader_);
  splitReader_->setConnectorQueryCtx(connectorQueryCtx_);
  // New io will be accounted on the stats of 'source'. Add the existing
  // balance to that.
  source->ioStats_->merge(*ioStats_);
  ioStats_ = std::move(source->ioStats_);
}

int64_t HiveDataSource::estimatedRowSize() {
  if (!splitReader_) {
    return kUnknownRowSize;
  }
  return splitReader_->estimatedRowSize();
}

vector_size_t HiveDataSource::evaluateRemainingFilter(RowVectorPtr& rowVector) {
  auto filterStartMicros = getCurrentTimeMicro();
  filterRows_.resize(output_->size());

  expressionEvaluator_->evaluate(
      remainingFilterExprSet_.get(), filterRows_, *rowVector, filterResult_);
  auto res = exec::processFilterResults(
      filterResult_, filterRows_, filterEvalCtx_, pool_);
  totalRemainingFilterTime_.fetch_add(
      (getCurrentTimeMicro() - filterStartMicros) * 1000,
      std::memory_order_relaxed);
  return res;
}

void HiveDataSource::resetSplit() {
  split_.reset();
  splitReader_->resetSplit();
  // Keep readers around to hold adaptation.
}

HiveDataSource::WaveDelegateHookFunction HiveDataSource::waveDelegateHook_;

std::shared_ptr<wave::WaveDataSource> HiveDataSource::toWaveDataSource() {
  VELOX_CHECK_NOT_NULL(waveDelegateHook_);
  if (!waveDataSource_) {
    waveDataSource_ = waveDelegateHook_(
        hiveTableHandle_,
        scanSpec_,
        readerOutputType_,
        &partitionKeys_,
        fileHandleFactory_,
        executor_,
        connectorQueryCtx_,
        hiveConfig_,
        ioStats_,
        remainingFilterExprSet_.get(),
        metadataFilter_);
  }
  return waveDataSource_;
}

//  static
void HiveDataSource::registerWaveDelegateHook(WaveDelegateHookFunction hook) {
  waveDelegateHook_ = hook;
}
std::shared_ptr<wave::WaveDataSource> toWaveDataSource();

} // namespace facebook::velox::connector::hive
