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

#include "velox/dwio/dwrf/reader/DwrfReader.h"

#include <chrono>

#include "velox/dwio/common/OnDemandUnitLoader.h"
#include "velox/dwio/common/TypeUtils.h"
#include "velox/dwio/common/exception/Exception.h"
#include "velox/dwio/dwrf/reader/ColumnReader.h"
#include "velox/dwio/dwrf/reader/StreamLabels.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::dwrf {

using dwio::common::ColumnSelector;
using dwio::common::FileFormat;
using dwio::common::LoadUnit;
using dwio::common::ReaderOptions;
using dwio::common::RowReaderOptions;
using dwio::common::UnitLoader;
using dwio::common::UnitLoaderFactory;

class DwrfUnit : public LoadUnit {
 public:
  DwrfUnit(
      const StripeReaderBase& stripeReaderBase,
      const StrideIndexProvider& strideIndexProvider,
      dwio::common::ColumnReaderStatistics& columnReaderStatistics,
      uint32_t stripeIndex,
      std::shared_ptr<dwio::common::ColumnSelector> columnSelector,
      const std::shared_ptr<BitSet>& projectedNodes,
      RowReaderOptions options)
      : stripeReaderBase_{stripeReaderBase},
        strideIndexProvider_{strideIndexProvider},
        columnReaderStatistics_{&columnReaderStatistics},
        stripeIndex_{stripeIndex},
        columnSelector_{std::move(columnSelector)},
        projectedNodes_{projectedNodes},
        options_{std::move(options)},
        stripeInfo_{
            stripeReaderBase.getReader().footer().stripes(stripeIndex_)} {}

  ~DwrfUnit() override = default;

  /// Performs the IO (read)
  void load() override;

  /// Unloads the unit to free memory
  void unload() override;

  /// Number of rows in the unit
  uint64_t getNumRows() override;

  /// Number of bytes that the IO will read
  uint64_t getIoSize() override;

  std::unique_ptr<ColumnReader>& getColumnReader() {
    return columnReader_;
  }

  std::unique_ptr<dwio::common::SelectiveColumnReader>&
  getSelectiveColumnReader() {
    return selectiveColumnReader_;
  }

 private:
  void ensureDecoders();
  void loadDecoders();

  // Immutables
  const StripeReaderBase& stripeReaderBase_;
  const StrideIndexProvider& strideIndexProvider_;
  dwio::common::ColumnReaderStatistics* const columnReaderStatistics_;
  const uint32_t stripeIndex_;
  const std::shared_ptr<dwio::common::ColumnSelector> columnSelector_;
  const std::shared_ptr<BitSet> projectedNodes_;
  const RowReaderOptions options_;
  const StripeInformationWrapper stripeInfo_;

  // Mutables
  bool preloaded_;
  std::optional<uint64_t> cachedIoSize_;
  std::shared_ptr<StripeReadState> stripeReadState_;
  std::unique_ptr<StripeStreamsImpl> stripeStreams_;
  std::unique_ptr<ColumnReader> columnReader_;
  std::unique_ptr<dwio::common::SelectiveColumnReader> selectiveColumnReader_;
  std::shared_ptr<StripeDictionaryCache> stripeDictionaryCache_;
};

void DwrfUnit::load() {
  ensureDecoders();
  loadDecoders();
}

void DwrfUnit::unload() {
  cachedIoSize_.reset();
  stripeStreams_.reset();
  columnReader_.reset();
  selectiveColumnReader_.reset();
  stripeDictionaryCache_.reset();
  stripeReadState_.reset();
}

uint64_t DwrfUnit::getNumRows() {
  return stripeInfo_.numberOfRows();
}

uint64_t DwrfUnit::getIoSize() {
  if (cachedIoSize_) {
    return *cachedIoSize_;
  }
  ensureDecoders();
  cachedIoSize_ =
      stripeReadState_->stripeMetadata->stripeInput->nextFetchSize();
  return *cachedIoSize_;
}

void DwrfUnit::ensureDecoders() {
  if (columnReader_ || selectiveColumnReader_) {
    return;
  }

  preloaded_ = options_.preloadStripe();

  stripeReadState_ = std::make_shared<StripeReadState>(
      stripeReaderBase_.readerBaseShared(),
      stripeReaderBase_.fetchStripe(stripeIndex_, preloaded_));

  stripeStreams_ = std::make_unique<StripeStreamsImpl>(
      stripeReadState_,
      columnSelector_.get(),
      projectedNodes_,
      options_,
      stripeInfo_.offset(),
      stripeInfo_.numberOfRows(),
      strideIndexProvider_,
      stripeIndex_);

  auto* scanSpec = options_.scanSpec().get();
  const auto& fileType = stripeReaderBase_.getReader().schemaWithId();
  FlatMapContext flatMapContext;
  flatMapContext.keySelectionCallback = options_.keySelectionCallback();
  memory::AllocationPool pool(&stripeReaderBase_.getReader().memoryPool());
  StreamLabels streamLabels(pool);

  if (scanSpec) {
    selectiveColumnReader_ = SelectiveDwrfReader::build(
        options_.requestedType() ? options_.requestedType() : fileType->type(),
        fileType,
        *stripeStreams_,
        streamLabels,
        *columnReaderStatistics_,
        scanSpec,
        flatMapContext,
        /*isRoot=*/true);
    selectiveColumnReader_->setIsTopLevel();
    selectiveColumnReader_->setFillMutatedOutputRows(
        options_.rowNumberColumnInfo().has_value());
  } else {
    auto requestedType = columnSelector_->getSchemaWithId();
    auto factory = &ColumnReaderFactory::defaultFactory();
    if (auto formatOptions = std::dynamic_pointer_cast<DwrfOptions>(
            options_.formatSpecificOptions())) {
      factory = formatOptions->columnReaderFactory().get();
    }
    columnReader_ = factory->build(
        requestedType,
        fileType,
        *stripeStreams_,
        streamLabels,
        options_.decodingExecutor().get(),
        options_.decodingParallelismFactor(),
        flatMapContext);
  }

  VELOX_CHECK_NE(
      columnReader_ != nullptr,
      selectiveColumnReader_ != nullptr,
      "ColumnReader was not created");
}

void DwrfUnit::loadDecoders() {
  // load data plan according to its updated selector during column reader
  // construction if planReads is off which means stripe data loaded as whole.
  if (!preloaded_) {
    VLOG(1) << "[DWRF] Load read plan for stripe " << stripeIndex_;
    stripeStreams_->loadReadPlan();
  }

  stripeDictionaryCache_ = stripeStreams_->getStripeDictionaryCache();
}

namespace {

DwrfUnit* castDwrfUnit(LoadUnit* unit) {
  VELOX_CHECK_NOT_NULL(unit);
  auto* dwrfUnit = dynamic_cast<DwrfUnit*>(unit);
  VELOX_CHECK_NOT_NULL(dwrfUnit);
  return dwrfUnit;
}

void makeProjectedNodes(
    const dwio::common::TypeWithId& fileType,
    BitSet& projectedNodes) {
  projectedNodes.insert(fileType.id());
  for (auto& child : fileType.getChildren()) {
    if (child) {
      makeProjectedNodes(*child, projectedNodes);
    }
  }
}

} // namespace

DwrfRowReader::DwrfRowReader(
    const std::shared_ptr<ReaderBase>& reader,
    const RowReaderOptions& opts)
    : StripeReaderBase(reader),
      options_(opts),
      columnSelector_{
          options_.scanSpec() != nullptr
              ? nullptr
              : std::make_shared<ColumnSelector>(ColumnSelector::apply(
                    options_.selector(),
                    reader->schema()))},
      decodingTimeCallback_{options_.decodingTimeCallback()},
      strideIndex_{0},
      currentUnit_{nullptr} {
  const auto& fileFooter = getReader().footer();
  const uint32_t numberOfStripes = fileFooter.stripesSize();
  currentStripe_ = numberOfStripes;
  stripeCeiling_ = 0;
  currentRowInStripe_ = 0;
  rowsInCurrentStripe_ = 0;

  uint64_t numRows{0};
  firstRowOfStripe_.reserve(numberOfStripes);
  for (uint32_t i = 0; i < numberOfStripes; ++i) {
    firstRowOfStripe_.push_back(numRows);
    const auto stripeInfo = fileFooter.stripes(i);
    numRows += stripeInfo.numberOfRows();
    if ((stripeInfo.offset() >= options_.offset()) &&
        (stripeInfo.offset() < options_.limit())) {
      currentStripe_ = std::min(currentStripe_, i);
      stripeCeiling_ = std::max(stripeCeiling_, i + 1);
    }
  }
  firstStripe_ = currentStripe_;

  // stripeCeiling_ will only be 0 here in cases where the passed
  // RowReaderOptions has an [offset,length] not found in the file. In this
  // case, set stripeCeiling_ == firstStripe_ == numberOfStripes
  if (stripeCeiling_ == 0) {
    stripeCeiling_ = firstStripe_;
  }

  const auto stripeCountCallback = options_.stripeCountCallback();
  if (stripeCountCallback) {
    stripeCountCallback(stripeCeiling_ - firstStripe_);
  }

  if (currentStripe_ == 0) {
    previousRow_ = std::numeric_limits<uint64_t>::max();
  } else if (currentStripe_ == numberOfStripes) {
    previousRow_ = fileFooter.numberOfRows();
  } else {
    previousRow_ = firstRowOfStripe_[firstStripe_] - 1;
  }

  // Validate the requested type is compatible with what's in the file
  std::function<std::string()> createExceptionContext = [&]() {
    return fmt::format(
        "The schema loaded in the reader does not match the schema in the file footer."
        "Input Name: {},\n"
        "File Footer Schema (without partition columns): {},\n"
        "Input Table Schema (with partition columns): {}\n",
        getReader().bufferedInput().getName(),
        getReader().schema()->toString(),
        type()->toString());
  };

  if (columnSelector_) {
    dwio::common::typeutils::checkTypeCompatibility(
        *getReader().schema(), *columnSelector_, createExceptionContext);
  } else {
    projectedNodes_ = std::make_shared<BitSet>(0);
    makeProjectedNodes(*getReader().schemaWithId(), *projectedNodes_);
  }

  unitLoader_ = getUnitLoader();
}

std::unique_ptr<ColumnReader>& DwrfRowReader::getColumnReader() {
  VELOX_DCHECK_NOT_NULL(currentUnit_);
  return currentUnit_->getColumnReader();
}

std::unique_ptr<dwio::common::SelectiveColumnReader>&
DwrfRowReader::getSelectiveColumnReader() {
  VELOX_DCHECK(currentUnit_ != nullptr);
  return currentUnit_->getSelectiveColumnReader();
}

std::unique_ptr<dwio::common::UnitLoader> DwrfRowReader::getUnitLoader() {
  std::vector<std::unique_ptr<LoadUnit>> loadUnits;
  loadUnits.reserve(stripeCeiling_ - firstStripe_);
  for (auto stripe = firstStripe_; stripe < stripeCeiling_; ++stripe) {
    loadUnits.emplace_back(std::make_unique<DwrfUnit>(
        /*stripeReaderBase=*/*this,
        /*strideIndexProvider=*/*this,
        columnReaderStatistics_,
        stripe,
        columnSelector_,
        projectedNodes_,
        options_));
  }
  std::shared_ptr<UnitLoaderFactory> unitLoaderFactory =
      options_.unitLoaderFactory();
  if (!unitLoaderFactory) {
    unitLoaderFactory =
        std::make_shared<dwio::common::OnDemandUnitLoaderFactory>(
            options_.blockedOnIoCallback());
  }
  return unitLoaderFactory->create(std::move(loadUnits), 0);
}

uint64_t DwrfRowReader::seekToRow(uint64_t rowNumber) {
  // Empty file
  if (isEmptyFile()) {
    return 0;
  }
  nextRowNumber_.reset();

  // If we are reading only a portion of the file (bounded by firstStripe_ and
  // stripeCeiling_), seeking before or after the portion of interest should
  // return no data. Implement this by setting previousRow_ to the number of
  // rows in the file.

  // seeking past stripeCeiling_.
  const auto& fileFooter = getReader().footer();
  const uint32_t numStripes = fileFooter.stripesSize();
  if ((stripeCeiling_ == numStripes &&
       rowNumber >= fileFooter.numberOfRows()) ||
      (stripeCeiling_ < numStripes &&
       rowNumber >= firstRowOfStripe_[stripeCeiling_])) {
    VLOG(1) << "Trying to seek past stripeCeiling_, total rows: "
            << fileFooter.numberOfRows() << " num_stripes: " << numStripes;

    currentStripe_ = numStripes;
    previousRow_ = fileFooter.numberOfRows();
    return previousRow_;
  }

  uint32_t seekToStripe = 0;
  while (seekToStripe + 1 < stripeCeiling_ &&
         firstRowOfStripe_[seekToStripe + 1] <= rowNumber) {
    ++seekToStripe;
  }

  // seeking before the first stripe
  if (seekToStripe < firstStripe_) {
    currentStripe_ = numStripes;
    previousRow_ = fileFooter.numberOfRows();
    return previousRow_;
  }

  const auto previousStripe = currentStripe_;
  const auto previousRowInStripe = currentRowInStripe_;
  currentStripe_ = seekToStripe;
  currentRowInStripe_ = rowNumber - firstRowOfStripe_[currentStripe_];
  previousRow_ = rowNumber;

  const auto loadUnitIdx = currentStripe_ - firstStripe_;
  unitLoader_->onSeek(loadUnitIdx, currentRowInStripe_);

  if (currentStripe_ != previousStripe) {
    // Different stripe. Let's load the new stripe.
    currentUnit_ = nullptr;
    loadCurrentStripe();
    if (currentRowInStripe_ > 0) {
      skip(currentRowInStripe_);
    }
  } else if (currentRowInStripe_ < previousRowInStripe) {
    // Same stripe but we have to seek backwards.
    if (currentUnit_) {
      // We had a loaded stripe, we have to reload.
      LOG(WARNING) << "Reloading stripe " << currentStripe_
                   << " because we have to seek backwards on it from row "
                   << previousRowInStripe << " to row " << currentRowInStripe_;
      currentUnit_->unload();
      currentUnit_->load();
    } else {
      // We had no stripe loaded. Let's load the current one.
      loadCurrentStripe();
    }
    if (currentRowInStripe_ > 0) {
      skip(currentRowInStripe_);
    }
  } else if (currentRowInStripe_ > previousRowInStripe) {
    // We have to seek forward on the same stripe. We can just skip.
    if (!currentUnit_) {
      // Load the current stripe if no stripe was loaded.
      loadCurrentStripe();
    }
    skip(currentRowInStripe_ - previousRowInStripe);
  } // otherwise the seek ended on the same stripe, same row

  return previousRow_;
}

uint64_t DwrfRowReader::skipRows(uint64_t numberOfRowsToSkip) {
  if (isEmptyFile()) {
    VLOG(1) << "Empty file, nothing to skip";
    return 0;
  }

  // When no rows to skip - just return 0
  if (numberOfRowsToSkip == 0) {
    VLOG(1) << "No skipping is needed";
    return 0;
  }

  // when we skipped or exhausted the whole file we can return 0
  const auto& fileFooter = getReader().footer();
  if (previousRow_ == fileFooter.numberOfRows()) {
    VLOG(1) << "previousRow_ is beyond EOF, nothing to skip";
    return 0;
  }

  if (previousRow_ == std::numeric_limits<uint64_t>::max()) {
    VLOG(1) << "Start of the file, skipping: " << numberOfRowsToSkip;
    seekToRow(numberOfRowsToSkip);
    if (previousRow_ == fileFooter.numberOfRows()) {
      VLOG(1) << "Reached end of the file, returning: " << previousRow_;
      return previousRow_;
    } else {
      VLOG(1) << "previousRow_ after skipping: " << previousRow_;
      return previousRow_;
    }
  }

  VLOG(1) << "Previous row: " << previousRow_
          << " Skipping: " << numberOfRowsToSkip;

  const uint64_t initialRow = previousRow_;
  seekToRow(previousRow_ + numberOfRowsToSkip);

  VLOG(1) << "After skipping: " << previousRow_
          << " InitialRow: " << initialRow;

  if (previousRow_ == fileFooter.numberOfRows()) {
    VLOG(1) << "When seeking past stripeCeiling_";
    return previousRow_ - initialRow - 1;
  }
  return previousRow_ - initialRow;
}

void DwrfRowReader::checkSkipStrides(uint64_t strideSize) {
  if (!getSelectiveColumnReader() || strideSize == 0 ||
      currentRowInStripe_ % strideSize != 0) {
    return;
  }

  if (currentRowInStripe_ == 0 || recomputeStridesToSkip_) {
    StatsContext context(getReader().writerName(), getReader().writerVersion());
    DwrfData::FilterRowGroupsResult res;
    getSelectiveColumnReader()->filterRowGroups(strideSize, context, res);
    if (auto& metadataFilter = options_.metadataFilter()) {
      metadataFilter->eval(res.metadataFilterResults, res.filterResult);
    }
    stridesToSkip_ = res.filterResult.data();
    stridesToSkipSize_ = res.totalCount;
    stripeStridesToSkip_[currentStripe_] = std::move(res.filterResult);
    recomputeStridesToSkip_ = false;
  }

  bool foundStridesToSkip = false;
  auto currentStride = currentRowInStripe_ / strideSize;
  while (currentStride < stridesToSkipSize_ &&
         bits::isBitSet(stridesToSkip_, currentStride)) {
    foundStridesToSkip = true;
    currentRowInStripe_ =
        std::min(currentRowInStripe_ + strideSize, rowsInCurrentStripe_);
    ++currentStride;
    ++skippedStrides_;
  }
  if (foundStridesToSkip && currentRowInStripe_ < rowsInCurrentStripe_) {
    getSelectiveColumnReader()->seekToRowGroup(currentStride);
  }
}

void DwrfRowReader::readNext(
    uint64_t rowsToRead,
    const dwio::common::Mutation* mutation,
    VectorPtr& result) {
  if (!getSelectiveColumnReader()) {
    std::optional<std::chrono::steady_clock::time_point> startTime;
    if (decodingTimeCallback_) {
      // We'll use wall time since we have parallel decoding.
      // If we move to sequential decoding only, we can use CPU time.
      startTime.emplace(std::chrono::steady_clock::now());
    }
    // TODO: Move row number appending logic here.  Currently this is done in
    // the wrapper reader.
    VELOX_CHECK_NULL(
        mutation, "Mutation pushdown is only supported in selective reader");
    getColumnReader()->next(rowsToRead, result);
    if (startTime.has_value()) {
      decodingTimeCallback_(
          std::chrono::steady_clock::now() - startTime.value());
    }
    return;
  }

  if (!options_.rowNumberColumnInfo().has_value()) {
    getSelectiveColumnReader()->next(rowsToRead, result, mutation);
    return;
  }

  readWithRowNumber(
      getSelectiveColumnReader(),
      options_,
      previousRow_,
      rowsToRead,
      mutation,
      result);
}

uint64_t DwrfRowReader::skip(uint64_t numValues) {
  if (getSelectiveColumnReader()) {
    return getSelectiveColumnReader()->skip(numValues);
  } else {
    return getColumnReader()->skip(numValues);
  }
}

int64_t DwrfRowReader::nextRowNumber() {
  if (nextRowNumber_.has_value()) {
    return *nextRowNumber_;
  }

  const auto strideSize = getReader().footer().rowIndexStride();
  while (currentStripe_ < stripeCeiling_) {
    if (currentRowInStripe_ == 0) {
      if (getReader().randomSkip()) {
        const auto numStripeRows =
            getReader().footer().stripes(currentStripe_).numberOfRows();
        const auto skipRows = getReader().randomSkip()->nextSkip();
        if (skipRows >= numStripeRows) {
          getReader().randomSkip()->consume(numStripeRows);
          const auto numStrides = bits::divRoundUp(numStripeRows, strideSize);
          skippedStrides_ += numStrides;
          goto advanceToNextStripe;
        }
      }
      loadCurrentStripe();
      VELOX_CHECK_NOT_NULL(currentUnit_);
    }

    checkSkipStrides(strideSize);
    if (currentRowInStripe_ < rowsInCurrentStripe_) {
      nextRowNumber_ = firstRowOfStripe_[currentStripe_] + currentRowInStripe_;
      return *nextRowNumber_;
    }

  advanceToNextStripe:
    ++currentStripe_;
    currentRowInStripe_ = 0;
    currentUnit_ = nullptr;
  }

  nextRowNumber_ = kAtEnd;
  return kAtEnd;
}

int64_t DwrfRowReader::nextReadSize(uint64_t size) {
  VELOX_DCHECK_GT(size, 0);
  if (nextRowNumber() == kAtEnd) {
    return kAtEnd;
  }

  auto rowsToRead = std::min(size, rowsInCurrentStripe_ - currentRowInStripe_);
  const auto strideSize = getReader().footer().rowIndexStride();
  if (LIKELY(strideSize > 0)) {
    // Don't allow read to cross stride.
    rowsToRead =
        std::min(rowsToRead, strideSize - currentRowInStripe_ % strideSize);
  }
  VELOX_DCHECK_GT(rowsToRead, 0);
  return rowsToRead;
}

uint64_t DwrfRowReader::next(
    uint64_t size,
    velox::VectorPtr& result,
    const dwio::common::Mutation* mutation) {
  const auto nextRow = nextRowNumber();
  if (nextRow == kAtEnd) {
    if (!isEmptyFile()) {
      previousRow_ = firstRowOfStripe_[stripeCeiling_ - 1] +
          getReader().footer().stripes(stripeCeiling_ - 1).numberOfRows();
    } else {
      previousRow_ = 0;
    }
    return 0;
  }

  const auto rowsToRead = nextReadSize(size);
  nextRowNumber_.reset();
  previousRow_ = nextRow;
  // Record strideIndex for use by the columnReader_ which may delay actual
  // reading of the data.
  const auto strideSize = getReader().footer().rowIndexStride();
  strideIndex_ = strideSize > 0 ? currentRowInStripe_ / strideSize : 0;
  const auto loadUnitIdx = currentStripe_ - firstStripe_;
  unitLoader_->onRead(loadUnitIdx, currentRowInStripe_, rowsToRead);
  readNext(rowsToRead, mutation, result);
  currentRowInStripe_ += rowsToRead;
  return rowsToRead;
}

void DwrfRowReader::resetFilterCaches() {
  if (getSelectiveColumnReader()) {
    getSelectiveColumnReader()->resetFilterCaches();
    recomputeStridesToSkip_ = true;
  }

  // For columnReader_, this is no-op.
}

void DwrfRowReader::loadCurrentStripe() {
  if (currentUnit_ || currentStripe_ >= stripeCeiling_) {
    return;
  }

  VELOX_CHECK_GE(currentStripe_, firstStripe_);
  strideIndex_ = 0;
  const auto loadUnitIdx = currentStripe_ - firstStripe_;
  currentUnit_ = castDwrfUnit(&unitLoader_->getLoadedUnit(loadUnitIdx));
  rowsInCurrentStripe_ = currentUnit_->getNumRows();
}

size_t DwrfRowReader::estimatedReaderMemory() const {
  VELOX_CHECK_NOT_NULL(columnSelector_);
  return 2 * DwrfReader::getMemoryUse(getReader(), -1, *columnSelector_);
}

bool DwrfRowReader::shouldReadNode(uint32_t nodeId) const {
  if (columnSelector_) {
    return columnSelector_->shouldReadNode(nodeId);
  }
  return projectedNodes_->contains(nodeId);
}

namespace {

template <typename T>
std::optional<uint64_t> getStringOrBinaryColumnSize(
    const dwio::common::ColumnStatistics& stats) {
  if (auto* typedStats = dynamic_cast<const T*>(&stats)) {
    if (typedStats->getTotalLength().has_value()) {
      return typedStats->getTotalLength();
    }
  }
  // Sometimes the column statistics are not typed and we don't have total
  // length, use raw size as an estimation.
  return stats.getRawSize();
}

} // namespace

std::optional<size_t> DwrfRowReader::estimatedRowSizeHelper(
    const FooterWrapper& fileFooter,
    const dwio::common::Statistics& stats,
    uint32_t nodeId) const {
  VELOX_CHECK_LT(nodeId, fileFooter.typesSize(), "Types missing in footer");

  const auto& nodeStats = stats.getColumnStatistics(nodeId);
  const auto& nodeType = fileFooter.types(nodeId);
  if (!nodeStats.getNumberOfValues()) {
    return std::nullopt;
  }
  const auto valueCount = nodeStats.getNumberOfValues().value();
  if (valueCount < 1) {
    return 0;
  }
  switch (nodeType.kind()) {
    case TypeKind::BOOLEAN: {
      return valueCount * sizeof(uint8_t);
    }
    case TypeKind::TINYINT: {
      return valueCount * sizeof(uint8_t);
    }
    case TypeKind::SMALLINT: {
      return valueCount * sizeof(uint16_t);
    }
    case TypeKind::INTEGER: {
      return valueCount * sizeof(uint32_t);
    }
    case TypeKind::BIGINT: {
      return valueCount * sizeof(uint64_t);
    }
    case TypeKind::HUGEINT: {
      return valueCount * sizeof(uint128_t);
    }
    case TypeKind::REAL: {
      return valueCount * sizeof(float);
    }
    case TypeKind::DOUBLE: {
      return valueCount * sizeof(double);
    }
    case TypeKind::VARCHAR:
      return getStringOrBinaryColumnSize<dwio::common::StringColumnStatistics>(
          nodeStats);
    case TypeKind::VARBINARY:
      return getStringOrBinaryColumnSize<dwio::common::BinaryColumnStatistics>(
          nodeStats);
    case TypeKind::TIMESTAMP: {
      return valueCount * sizeof(uint64_t) * 2;
    }
    case TypeKind::ARRAY:
    case TypeKind::MAP:
    case TypeKind::ROW: {
      // start the estimate with the offsets and hasNulls vectors sizes
      size_t totalEstimate = valueCount * (sizeof(uint8_t) + sizeof(uint64_t));
      for (int32_t i = 0; i < nodeType.subtypesSize(); ++i) {
        if (!shouldReadNode(nodeType.subtypes(i))) {
          continue;
        }
        const auto subtypeEstimate =
            estimatedRowSizeHelper(fileFooter, stats, nodeType.subtypes(i));
        if (subtypeEstimate.has_value()) {
          totalEstimate += subtypeEstimate.value();
        } else {
          return std::nullopt;
        }
      }
      return totalEstimate;
    }
    default:
      return std::nullopt;
  }
}

std::optional<size_t> DwrfRowReader::estimatedRowSize() const {
  const auto& reader = getReader();
  const auto& fileFooter = reader.footer();

  if (!fileFooter.hasNumberOfRows()) {
    return std::nullopt;
  }

  if (fileFooter.numberOfRows() < 1) {
    return 0;
  }

  // Estimate with projections.
  constexpr uint32_t ROOT_NODE_ID = 0;
  const auto stats = reader.statistics();
  const auto projectedSize =
      estimatedRowSizeHelper(fileFooter, *stats, ROOT_NODE_ID);
  if (projectedSize.has_value()) {
    return projectedSize.value() / fileFooter.numberOfRows();
  }
  return std::nullopt;
}

DwrfReader::DwrfReader(
    const ReaderOptions& options,
    std::unique_ptr<dwio::common::BufferedInput> input)
    : readerBase_(std::make_unique<ReaderBase>(
          options.memoryPool(),
          std::move(input),
          options.decrypterFactory(),
          options.footerEstimatedSize(),
          options.filePreloadThreshold(),
          options.fileFormat() == FileFormat::ORC ? FileFormat::ORC
                                                  : FileFormat::DWRF,
          options.fileColumnNamesReadAsLowerCase(),
          options.randomSkip(),
          options.scanSpec())),
      options_(options) {
  // If we are not using column names to map table columns to file columns,
  // then we use indices. In that case we need to ensure the names completely
  // match, because we are still mapping columns by names further down the
  // code. So we rename column names in the file schema to match table schema.
  // We test the options to have 'fileSchema' (actually table schema) as most
  // of the unit tests fail to provide it.
  if ((!options_.useColumnNamesForColumnMapping()) &&
      (options_.fileSchema() != nullptr)) {
    updateColumnNamesFromTableSchema();
  }
}

namespace {
void logTypeInequality(
    const Type& fileType,
    const Type& tableType,
    const std::string& fileFieldName,
    const std::string& tableFieldName) {
  VLOG(1) << "Type of the File field '" << fileFieldName
          << "' does not match the type of the Table field '" << tableFieldName
          << "': [" << fileType.toString() << "] vs [" << tableType.toString()
          << "]";
}

// Forward declaration for general type tree recursion function.
TypePtr updateColumnNames(
    const TypePtr& fileType,
    const TypePtr& tableType,
    const std::string& fileFieldName,
    const std::string& tableFieldName);

// Non-primitive type tree recursion function.
template <typename T>
TypePtr updateColumnNames(const TypePtr& fileType, const TypePtr& tableType) {
  const auto fileRowType = std::dynamic_pointer_cast<const T>(fileType);
  const auto tableRowType = std::dynamic_pointer_cast<const T>(tableType);

  std::vector<std::string> newFileFieldNames;
  newFileFieldNames.reserve(fileRowType->size());
  std::vector<TypePtr> newFileFieldTypes;
  newFileFieldTypes.reserve(fileRowType->size());

  for (auto childIdx = 0; childIdx < tableRowType->size(); ++childIdx) {
    if (childIdx >= fileRowType->size()) {
      break;
    }

    newFileFieldTypes.push_back(updateColumnNames(
        fileRowType->childAt(childIdx),
        tableRowType->childAt(childIdx),
        fileRowType->nameOf(childIdx),
        tableRowType->nameOf(childIdx)));

    newFileFieldNames.push_back(tableRowType->nameOf(childIdx));
  }

  for (auto childIdx = tableRowType->size(); childIdx < fileRowType->size();
       ++childIdx) {
    newFileFieldTypes.push_back(fileRowType->childAt(childIdx));
    newFileFieldNames.push_back(fileRowType->nameOf(childIdx));
  }

  return std::make_shared<const T>(
      std::move(newFileFieldNames), std::move(newFileFieldTypes));
}

// General type tree recursion function.
TypePtr updateColumnNames(
    const TypePtr& fileType,
    const TypePtr& tableType,
    const std::string& fileFieldName,
    const std::string& tableFieldName) {
  // Check type kind equality. If not equal, no point to continue down the
  // tree.
  if (fileType->kind() != tableType->kind()) {
    logTypeInequality(*fileType, *tableType, fileFieldName, tableFieldName);
    return fileType;
  }

  // For leaf types we return type as is.
  if (fileType->isPrimitiveType()) {
    return fileType;
  }

  if (fileType->isRow()) {
    return updateColumnNames<RowType>(fileType, tableType);
  }

  if (fileType->isMap()) {
    return updateColumnNames<MapType>(fileType, tableType);
  }

  if (fileType->isArray()) {
    return updateColumnNames<ArrayType>(fileType, tableType);
  }

  // We should not be here.
  VLOG(1) << "Unexpected table type during column names update for File field '"
          << fileFieldName << "': [" << fileType->toString() << "]";
  return fileType;
}
} // namespace

void DwrfReader::updateColumnNamesFromTableSchema() {
  const auto& tableSchema = options_.fileSchema();
  const auto& fileSchema = readerBase_->schema();
  readerBase_->setSchema(std::dynamic_pointer_cast<const RowType>(
      updateColumnNames(fileSchema, tableSchema, "", "")));
}

std::unique_ptr<StripeInformation> DwrfReader::getStripe(
    uint32_t stripeIndex) const {
  VELOX_CHECK_LT(
      stripeIndex, getNumberOfStripes(), "stripe index out of range");
  auto stripeInfo = readerBase_->footer().stripes(stripeIndex);

  return std::make_unique<StripeInformationImpl>(
      stripeInfo.offset(),
      stripeInfo.indexLength(),
      stripeInfo.dataLength(),
      stripeInfo.footerLength(),
      stripeInfo.numberOfRows());
}

std::vector<std::string> DwrfReader::getMetadataKeys() const {
  std::vector<std::string> result;
  auto& fileFooter = readerBase_->footer();
  result.reserve(fileFooter.metadataSize());
  for (int32_t i = 0; i < fileFooter.metadataSize(); ++i) {
    result.push_back(fileFooter.metadata(i).name());
  }
  return result;
}

std::string DwrfReader::getMetadataValue(const std::string& key) const {
  auto& fileFooter = readerBase_->footer();
  for (int32_t i = 0; i < fileFooter.metadataSize(); ++i) {
    if (fileFooter.metadata(i).name() == key) {
      return fileFooter.metadata(i).value();
    }
  }
  VELOX_FAIL("key not found: {}", key);
}

bool DwrfReader::hasMetadataValue(const std::string& key) const {
  auto& fileFooter = readerBase_->footer();
  for (int32_t i = 0; i < fileFooter.metadataSize(); ++i) {
    if (fileFooter.metadata(i).name() == key) {
      return true;
    }
  }
  return false;
}

uint64_t maxStreamsForType(const TypeWrapper& type) {
  if (type.format() == DwrfFormat::kOrc) {
    switch (type.kind()) {
      case TypeKind::ROW:
        return 1;
      case TypeKind::SMALLINT:
      case TypeKind::INTEGER:
      case TypeKind::BIGINT:
      case TypeKind::REAL:
      case TypeKind::DOUBLE:
      case TypeKind::BOOLEAN:
      case TypeKind::ARRAY:
      case TypeKind::MAP:
        return 2;
      case TypeKind::VARBINARY:
      case TypeKind::TIMESTAMP:
        return 3;
      case TypeKind::TINYINT:
      case TypeKind::VARCHAR:
        return 4;
      default:
        return 0;
    }
  }

  // DWRF
  switch (type.kind()) {
    case TypeKind::ROW:
      return 1;
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::ARRAY:
    case TypeKind::MAP:
      return 2;
    case TypeKind::VARBINARY:
    case TypeKind::TIMESTAMP:
      return 3;
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
      return 4;
    case TypeKind::VARCHAR:
      return 7;
    default:
      return 0;
  }
}

uint64_t DwrfReader::getMemoryUse(int32_t stripeIx) {
  ColumnSelector cs(readerBase_->schema());
  return getMemoryUse(*readerBase_, stripeIx, cs);
}

uint64_t DwrfReader::getMemoryUseByFieldId(
    const std::vector<uint64_t>& include,
    int32_t stripeIx) {
  ColumnSelector cs(readerBase_->schema(), include);
  return getMemoryUse(*readerBase_, stripeIx, cs);
}

uint64_t DwrfReader::getMemoryUseByName(
    const std::vector<std::string>& names,
    int32_t stripeIx) {
  ColumnSelector cs(readerBase_->schema(), names);
  return getMemoryUse(*readerBase_, stripeIx, cs);
}

uint64_t DwrfReader::getMemoryUseByTypeId(
    const std::vector<uint64_t>& include,
    int32_t stripeIx) {
  ColumnSelector cs(readerBase_->schema(), include, true);
  return getMemoryUse(*readerBase_, stripeIx, cs);
}

uint64_t DwrfReader::getMemoryUse(
    ReaderBase& readerBase,
    int32_t stripeIx,
    const ColumnSelector& cs) {
  uint64_t maxDataLength = 0;
  const auto& fileFooter = readerBase.footer();
  if (stripeIx >= 0 && stripeIx < fileFooter.stripesSize()) {
    const uint64_t stripeLength = fileFooter.stripes(stripeIx).dataLength();
    maxDataLength = std::max(maxDataLength, stripeLength);
  } else {
    for (int32_t i = 0; i < fileFooter.stripesSize(); ++i) {
      const uint64_t stripeLength = fileFooter.stripes(i).dataLength();
      maxDataLength = std::max(maxDataLength, stripeLength);
    }
  }

  bool hasStringColumn = false;
  uint64_t numSelectedStreams = 0;
  for (int32_t i = 0; !hasStringColumn && i < fileFooter.typesSize(); ++i) {
    if (cs.shouldReadNode(i)) {
      const auto type = fileFooter.types(i);
      numSelectedStreams += maxStreamsForType(type);
      switch (type.kind()) {
        case TypeKind::VARCHAR:
        case TypeKind::VARBINARY: {
          hasStringColumn = true;
          break;
        }
        default: {
          break;
        }
      }
    }
  }

  // If a string column is read, use stripe datalength as a memory estimate
  // because we don't know the dictionary size. Multiply by 2 because a string
  // column requires two buffers: in the input stream and in the seekable
  // input stream. If no string column is read, estimate from the number of
  // streams.
  uint64_t memoryBytes = hasStringColumn
      ? 2 * maxDataLength
      : std::min(
            uint64_t(maxDataLength),
            numSelectedStreams *
                readerBase.bufferedInput().getReadFile()->getNaturalReadSize());

  // Do we need even more memory to read the footer or the metadata?
  const auto footerLength = readerBase.postScript().footerLength();
  if (memoryBytes < footerLength + readerBase.footerEstimatedSize()) {
    memoryBytes = footerLength + readerBase.footerEstimatedSize();
  }

  // Account for firstRowOfStripe.
  memoryBytes +=
      static_cast<uint64_t>(fileFooter.stripesSize()) * sizeof(uint64_t);

  // Decompressors need buffers for each stream
  uint64_t decompressorMemoryBytes = 0;
  const auto compressionKind = readerBase.compressionKind();
  if (compressionKind != common::CompressionKind_NONE) {
    for (int32_t i = 0; i < fileFooter.typesSize(); ++i) {
      if (cs.shouldReadNode(i)) {
        const auto type = fileFooter.types(i);
        decompressorMemoryBytes +=
            maxStreamsForType(type) * readerBase.compressionBlockSize();
      }
    }
    if (compressionKind == common::CompressionKind_SNAPPY) {
      decompressorMemoryBytes *= 2; // Snappy decompressor uses a second buffer
    }
  }

  return memoryBytes + decompressorMemoryBytes;
}

std::unique_ptr<dwio::common::RowReader> DwrfReader::createRowReader(
    const RowReaderOptions& opts) const {
  return createDwrfRowReader(opts);
}

std::unique_ptr<DwrfRowReader> DwrfReader::createDwrfRowReader(
    const RowReaderOptions& opts) const {
  auto rowReader = std::make_unique<DwrfRowReader>(readerBase_, opts);
  if (opts.eagerFirstStripeLoad()) {
    // Load the first stripe on construction so that readers created in
    // background have a reader tree and can preload the first
    // stripe. Also the reader tree needs to exist in order to receive
    // adaptation from a previous reader.
    rowReader->nextRowNumber();
  }
  return rowReader;
}

std::unique_ptr<DwrfReader> DwrfReader::create(
    std::unique_ptr<dwio::common::BufferedInput> input,
    const ReaderOptions& options) {
  return std::make_unique<DwrfReader>(options, std::move(input));
}

void registerDwrfReaderFactory() {
  dwio::common::registerReaderFactory(std::make_shared<DwrfReaderFactory>());
}

void unregisterDwrfReaderFactory() {
  dwio::common::unregisterReaderFactory(dwio::common::FileFormat::DWRF);
}

} // namespace facebook::velox::dwrf
