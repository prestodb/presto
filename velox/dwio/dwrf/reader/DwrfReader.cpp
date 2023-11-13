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
#include "velox/dwio/common/TypeUtils.h"
#include "velox/dwio/common/exception/Exception.h"
#include "velox/dwio/dwrf/reader/ColumnReader.h"
#include "velox/dwio/dwrf/reader/StreamLabels.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::dwrf {

using dwio::common::ColumnSelector;
using dwio::common::FileFormat;
using dwio::common::InputStream;
using dwio::common::ReaderOptions;
using dwio::common::RowReaderOptions;

DwrfRowReader::DwrfRowReader(
    const std::shared_ptr<ReaderBase>& reader,
    const RowReaderOptions& opts)
    : StripeReaderBase(reader),
      options_(opts),
      executor_{options_.getDecodingExecutor()},
      columnSelector_{std::make_shared<ColumnSelector>(
          ColumnSelector::apply(opts.getSelector(), reader->getSchema()))} {
  auto& footer = getReader().getFooter();
  uint32_t numberOfStripes = footer.stripesSize();
  currentStripe = numberOfStripes;
  stripeCeiling = 0;
  currentRowInStripe = 0;
  newStripeReadyForRead = false;
  rowsInCurrentStripe = 0;
  uint64_t rowTotal = 0;

  firstRowOfStripe.reserve(numberOfStripes);
  for (uint32_t i = 0; i < numberOfStripes; ++i) {
    firstRowOfStripe.push_back(rowTotal);
    auto stripeInfo = footer.stripes(i);
    rowTotal += stripeInfo.numberOfRows();
    if ((stripeInfo.offset() >= opts.getOffset()) &&
        (stripeInfo.offset() < opts.getLimit())) {
      if (i < currentStripe) {
        currentStripe = i;
      }
      if (i >= stripeCeiling) {
        stripeCeiling = i + 1;
      }
    }
  }
  firstStripe = currentStripe;

  // stripeCeiling will only be 0 here in cases where the passed
  // RowReaderOptions has an [offset,length] not found in the file. In this
  // case, set stripeCeiling == firstStripe == numberOfStripes
  if (stripeCeiling == 0) {
    stripeCeiling = firstStripe;
  }

  if (currentStripe == 0) {
    previousRow = std::numeric_limits<uint64_t>::max();
  } else if (currentStripe == numberOfStripes) {
    previousRow = footer.numberOfRows();
  } else {
    previousRow = firstRowOfStripe[firstStripe] - 1;
  }

  // Validate the requested type is compatible with what's in the file
  std::function<std::string()> createExceptionContext = [&]() {
    std::string exceptionMessageContext = fmt::format(
        "The schema loaded in the reader does not match the schema in the file footer."
        "Input Name: {},\n"
        "File Footer Schema (without partition columns): {},\n"
        "Input Table Schema (with partition columns): {}\n",
        getReader().getBufferedInput().getName(),
        getReader().getSchema()->toString(),
        getType()->toString());
    return exceptionMessageContext;
  };

  dwio::common::typeutils::checkTypeCompatibility(
      *getReader().getSchema(), *columnSelector_, createExceptionContext);

  stripeLoadBatons_.reserve(numberOfStripes);
  for (int i = 0; i < numberOfStripes; i++) {
    stripeLoadBatons_.emplace_back(std::make_unique<folly::Baton<>>());
  }
  stripeLoadStatuses_ = folly::Synchronized(
      std::vector<FetchStatus>(numberOfStripes, FetchStatus::NOT_STARTED));
}

uint64_t DwrfRowReader::seekToRow(uint64_t rowNumber) {
  // Empty file
  if (isEmptyFile()) {
    return 0;
  }

  DWIO_ENSURE(
      !prefetchHasOccurred_,
      "Prefetch already called. Currently, seek after prefetch is disallowed in DwrfRowReader");

  // If we are reading only a portion of the file
  // (bounded by firstStripe and stripeCeiling),
  // seeking before or after the portion of interest should return no data.
  // Implement this by setting previousRow to the number of rows in the file.

  // seeking past stripeCeiling
  auto& footer = getReader().getFooter();
  uint32_t num_stripes = footer.stripesSize();
  if ((stripeCeiling == num_stripes && rowNumber >= footer.numberOfRows()) ||
      (stripeCeiling < num_stripes &&
       rowNumber >= firstRowOfStripe[stripeCeiling])) {
    VLOG(1) << "Trying to seek past stripeCeiling, total rows: "
            << footer.numberOfRows() << " num_stripes: " << num_stripes;

    currentStripe = num_stripes;

    previousRow = footer.numberOfRows();
    return previousRow;
  }

  uint32_t seekToStripe = 0;
  while (seekToStripe + 1 < stripeCeiling &&
         firstRowOfStripe[seekToStripe + 1] <= rowNumber) {
    seekToStripe++;
  }

  // seeking before the first stripe
  if (seekToStripe < firstStripe) {
    currentStripe = num_stripes;
    previousRow = footer.numberOfRows();
    return previousRow;
  }

  currentStripe = seekToStripe;
  currentRowInStripe = rowNumber - firstRowOfStripe[currentStripe];
  previousRow = rowNumber;

  // Reset baton, since seek flow can load a stripe more than once and is
  // synchronous for now.
  VLOG(1) << "Resetting baton at " << currentStripe;
  stripeLoadBatons_[currentStripe] = std::make_unique<folly::Baton<>>();
  stripeLoadStatuses_.wlock()->operator[](currentStripe) =
      FetchStatus::NOT_STARTED;

  VLOG(1) << "rowNumber: " << rowNumber << " currentStripe: " << currentStripe
          << " firstStripe: " << firstStripe
          << " stripeCeiling: " << stripeCeiling;

  // Because prefetch and seek are currently incompatible, there should only
  // ever be 1 stripe fetched at this point.
  VLOG(1) << "Erasing " << currentStripe << " from prefetched_";
  prefetchedStripeStates_.wlock()->erase(currentStripe);
  freeStripeAt(currentStripe);
  newStripeReadyForRead = false;
  startNextStripe();

  if (selectiveColumnReader_) {
    selectiveColumnReader_->skip(currentRowInStripe);
  } else {
    columnReader_->skip(currentRowInStripe);
  }

  return previousRow;
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
  auto& footer = getReader().getFooter();
  if (previousRow == footer.numberOfRows()) {
    VLOG(1) << "previousRow is beyond EOF, nothing to skip";
    return 0;
  }

  if (previousRow == std::numeric_limits<uint64_t>::max()) {
    VLOG(1) << "Start of the file, skipping: " << numberOfRowsToSkip;
    seekToRow(numberOfRowsToSkip);
    if (previousRow == footer.numberOfRows()) {
      VLOG(1) << "Reached end of the file, returning: " << previousRow;
      return previousRow;
    } else {
      VLOG(1) << "previousRow after skipping: " << previousRow;
      return previousRow;
    }
  }

  VLOG(1) << "Previous row: " << previousRow
          << " Skipping: " << numberOfRowsToSkip;

  uint64_t initialRow = previousRow;
  seekToRow(previousRow + numberOfRowsToSkip);

  VLOG(1) << "After skipping: " << previousRow << " InitialRow: " << initialRow;

  if (previousRow == footer.numberOfRows()) {
    VLOG(1) << "When seeking past stripeCeiling";
    return previousRow - initialRow - 1;
  }
  return previousRow - initialRow;
}

void DwrfRowReader::checkSkipStrides(uint64_t strideSize) {
  if (!selectiveColumnReader_ || strideSize == 0 ||
      currentRowInStripe % strideSize != 0) {
    return;
  }

  if (currentRowInStripe == 0 || recomputeStridesToSkip_) {
    StatsContext context(
        getReader().getWriterName(), getReader().getWriterVersion());
    DwrfData::FilterRowGroupsResult res;
    selectiveColumnReader_->filterRowGroups(strideSize, context, res);
    if (auto& metadataFilter = options_.getMetadataFilter()) {
      metadataFilter->eval(res.metadataFilterResults, res.filterResult);
    }
    stridesToSkip_ = res.filterResult.data();
    stridesToSkipSize_ = res.totalCount;
    stripeStridesToSkip_[currentStripe] = std::move(res.filterResult);
    recomputeStridesToSkip_ = false;
  }

  bool foundStridesToSkip = false;
  auto currentStride = currentRowInStripe / strideSize;
  while (currentStride < stridesToSkipSize_ &&
         bits::isBitSet(stridesToSkip_, currentStride)) {
    foundStridesToSkip = true;
    currentRowInStripe =
        std::min(currentRowInStripe + strideSize, rowsInCurrentStripe);
    currentStride++;
    skippedStrides_++;
  }
  if (foundStridesToSkip && currentRowInStripe < rowsInCurrentStripe) {
    selectiveColumnReader_->seekToRowGroup(currentStride);
  }
}

void DwrfRowReader::readNext(
    uint64_t rowsToRead,
    const dwio::common::Mutation* mutation,
    VectorPtr& result) {
  if (!selectiveColumnReader_) {
    const auto startTime = std::chrono::high_resolution_clock::now();
    // TODO: Move row number appending logic here.  Currently this is done in
    // the wrapper reader.
    VELOX_CHECK(
        mutation == nullptr,
        "Mutation pushdown is only supported in selective reader");
    columnReader_->next(rowsToRead, result);
    auto reportDecodingTimeMsMetric = options_.getDecodingTimeMsCallback();
    if (reportDecodingTimeMsMetric) {
      auto decodingTime = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::high_resolution_clock::now() - startTime);
      reportDecodingTimeMsMetric(decodingTime.count());
    }
    return;
  }
  if (!options_.getAppendRowNumberColumn()) {
    selectiveColumnReader_->next(rowsToRead, result, mutation);
    return;
  }
  readWithRowNumber(rowsToRead, mutation, result);
}

void DwrfRowReader::readWithRowNumber(
    uint64_t rowsToRead,
    const dwio::common::Mutation* mutation,
    VectorPtr& result) {
  auto* rowVector = result->asUnchecked<RowVector>();
  column_index_t numChildren = 0;
  for (auto& column : options_.getScanSpec()->children()) {
    if (column->projectOut()) {
      ++numChildren;
    }
  }
  VectorPtr rowNumVector;
  if (rowVector->childrenSize() != numChildren) {
    VELOX_CHECK_EQ(rowVector->childrenSize(), numChildren + 1);
    rowNumVector = rowVector->childAt(numChildren);
    auto& rowType = rowVector->type()->asRow();
    auto names = rowType.names();
    auto types = rowType.children();
    auto children = rowVector->children();
    VELOX_DCHECK(!names.empty() && !types.empty() && !children.empty());
    names.pop_back();
    types.pop_back();
    children.pop_back();
    result = std::make_shared<RowVector>(
        rowVector->pool(),
        ROW(std::move(names), std::move(types)),
        rowVector->nulls(),
        rowVector->size(),
        std::move(children));
  }
  selectiveColumnReader_->next(rowsToRead, result, mutation);
  FlatVector<int64_t>* flatRowNum = nullptr;
  if (rowNumVector && BaseVector::isVectorWritable(rowNumVector)) {
    flatRowNum = rowNumVector->asFlatVector<int64_t>();
  }
  if (flatRowNum) {
    flatRowNum->clearAllNulls();
    flatRowNum->resize(result->size());
  } else {
    rowNumVector = std::make_shared<FlatVector<int64_t>>(
        result->pool(),
        BIGINT(),
        nullptr,
        result->size(),
        AlignedBuffer::allocate<int64_t>(result->size(), result->pool()),
        std::vector<BufferPtr>());
    flatRowNum = rowNumVector->asUnchecked<FlatVector<int64_t>>();
  }
  auto rowOffsets = selectiveColumnReader_->outputRows();
  VELOX_DCHECK_EQ(rowOffsets.size(), result->size());
  auto* rawRowNum = flatRowNum->mutableRawValues();
  for (int i = 0; i < rowOffsets.size(); ++i) {
    rawRowNum[i] = previousRow + rowOffsets[i];
  }
  rowVector = result->asUnchecked<RowVector>();
  auto& rowType = rowVector->type()->asRow();
  auto names = rowType.names();
  auto types = rowType.children();
  auto children = rowVector->children();
  names.emplace_back();
  types.push_back(BIGINT());
  children.push_back(rowNumVector);
  result = std::make_shared<RowVector>(
      rowVector->pool(),
      ROW(std::move(names), std::move(types)),
      rowVector->nulls(),
      rowVector->size(),
      std::move(children));
}

int64_t DwrfRowReader::nextRowNumber() {
  auto strideSize = getReader().getFooter().rowIndexStride();
  while (currentStripe < stripeCeiling) {
    if (currentRowInStripe == 0) {
      startNextStripe();
    }
    checkSkipStrides(strideSize);
    if (currentRowInStripe < rowsInCurrentStripe) {
      return firstRowOfStripe[currentStripe] + currentRowInStripe;
    }
    ++currentStripe;
    currentRowInStripe = 0;
    newStripeReadyForRead = false;
  }
  return kAtEnd;
}

int64_t DwrfRowReader::nextReadSize(uint64_t size) {
  VELOX_DCHECK_GT(size, 0);
  if (nextRowNumber() == kAtEnd) {
    return kAtEnd;
  }
  auto rowsToRead = std::min(size, rowsInCurrentStripe - currentRowInStripe);
  auto strideSize = getReader().getFooter().rowIndexStride();
  if (LIKELY(strideSize > 0)) {
    // Don't allow read to cross stride.
    rowsToRead =
        std::min(rowsToRead, strideSize - currentRowInStripe % strideSize);
  }
  VELOX_DCHECK_GT(rowsToRead, 0);
  return rowsToRead;
}

uint64_t DwrfRowReader::next(
    uint64_t size,
    velox::VectorPtr& result,
    const dwio::common::Mutation* mutation) {
  auto nextRow = nextRowNumber();
  if (nextRow == kAtEnd) {
    if (!isEmptyFile()) {
      previousRow = firstRowOfStripe[stripeCeiling - 1] +
          getReader().getFooter().stripes(stripeCeiling - 1).numberOfRows();
    } else {
      previousRow = 0;
    }
    return 0;
  }
  auto rowsToRead = nextReadSize(size);
  previousRow = nextRow;
  // Record strideIndex for use by the columnReader_ which may delay actual
  // reading of the data.
  auto strideSize = getReader().getFooter().rowIndexStride();
  strideIndex_ = strideSize > 0 ? currentRowInStripe / strideSize : 0;
  readNext(rowsToRead, mutation, result);
  currentRowInStripe += rowsToRead;
  return rowsToRead;
}

void DwrfRowReader::resetFilterCaches() {
  if (selectiveColumnReader_) {
    selectiveColumnReader_->resetFilterCaches();
    recomputeStridesToSkip_ = true;
  }

  // For columnReader_, this is no-op.
}

std::optional<std::vector<velox::dwio::common::RowReader::PrefetchUnit>>
DwrfRowReader::prefetchUnits() {
  auto rowsInStripe = getReader().getRowsPerStripe();
  DWIO_ENSURE(firstStripe <= rowsInStripe.size());
  DWIO_ENSURE(stripeCeiling <= rowsInStripe.size());
  DWIO_ENSURE(firstStripe <= stripeCeiling);

  std::vector<PrefetchUnit> res;
  res.reserve(stripeCeiling - firstStripe);

  for (auto stripe = firstStripe; stripe < stripeCeiling; ++stripe) {
    res.push_back(
        {.rowCount = rowsInStripe[stripe],
         .prefetch = std::bind(&DwrfRowReader::prefetch, this, stripe)});
  }
  return res;
}

DwrfRowReader::FetchResult DwrfRowReader::fetch(uint32_t stripeIndex) {
  FetchStatus prevStatus;
  stripeLoadStatuses_.withWLock([&](auto& loadRequestIssued) {
    if (stripeIndex < 0 || stripeIndex >= loadRequestIssued.size()) {
      prevStatus = FetchStatus::ERROR;
    }

    prevStatus = loadRequestIssued[stripeIndex];
    if (prevStatus == FetchStatus::NOT_STARTED) {
      loadRequestIssued[stripeIndex] = FetchStatus::IN_PROGRESS;
    }
  });

  DWIO_ENSURE(
      prevStatus != FetchStatus::ERROR, "Fetch request was out of bounds");

  if (prevStatus != FetchStatus::NOT_STARTED) {
    bool finishedLoading = prevStatus == FetchStatus::FINISHED;

    VLOG(1) << "Stripe " << stripeIndex << " was not loaded, as it was already "
            << (finishedLoading ? "finished loading" : "in progress");
    return finishedLoading ? FetchResult::kAlreadyFetched
                           : FetchResult::kInProgress;
  }

  DWIO_ENSURE(
      !prefetchedStripeStates_.rlock()->contains(stripeIndex),
      "prefetched stripe state already exists for stripeIndex " +
          std::to_string(stripeIndex) + ", LIKELY RACE CONDITION");

  auto startTime = std::chrono::high_resolution_clock::now();
  bool preload = options_.getPreloadStripe();

  // Currently we only call prefetchStripe through here. If stripeLoadStatuses_
  // says we haven't started a load here yet, then this should always succeed
  DWIO_ENSURE(fetchStripe(stripeIndex, preload));

  VLOG(1) << "fetchStripe success";

  auto prefetchedStripeBase = getStripeBase(stripeIndex);

  auto state = std::make_shared<StripeReadState>(
      readerBaseShared(),
      prefetchedStripeBase->stripeInput.get(),
      prefetchedStripeBase->footer,
      getDecryptionHandler());

  auto stripe = getReader().getFooter().stripes(stripeIndex);
  StripeStreamsImpl stripeStreams(
      state,
      getColumnSelector(),
      options_,
      stripe.offset(),
      stripe.numberOfRows(),
      *this,
      stripeIndex);

  auto scanSpec = options_.getScanSpec().get();
  auto requestedType = getColumnSelector().getSchemaWithId();
  auto fileType = getReader().getSchemaWithId();
  FlatMapContext flatMapContext;
  flatMapContext.keySelectionCallback = options_.getKeySelectionCallback();
  memory::AllocationPool pool(&getReader().getMemoryPool());
  StreamLabels streamLabels(pool);

  PrefetchedStripeState stripeState;
  if (scanSpec) {
    stripeState.selectiveColumnReader = SelectiveDwrfReader::build(
        requestedType,
        fileType,
        stripeStreams,
        streamLabels,
        columnReaderStatistics_,
        scanSpec,
        flatMapContext,
        true); // isRoot
    stripeState.selectiveColumnReader->setIsTopLevel();
  } else {
    stripeState.columnReader = ColumnReader::build( // enqueue streams
        requestedType,
        fileType,
        stripeStreams,
        streamLabels,
        executor_.get(),
        flatMapContext);
  }
  DWIO_ENSURE(
      (stripeState.columnReader != nullptr) !=
          (stripeState.selectiveColumnReader != nullptr),
      "ColumnReader was not created");

  // load data plan according to its updated selector
  // during column reader construction
  // if planReads is off which means stripe data loaded as whole
  if (!preload) {
    VLOG(1) << "[DWRF] Load read plan for stripe " << currentStripe;
    stripeStreams.loadReadPlan();
  }

  stripeState.stripeDictionaryCache = stripeStreams.getStripeDictionaryCache();
  stripeState.preloaded = preload;
  prefetchedStripeStates_.wlock()->operator[](stripeIndex) =
      std::move(stripeState);

  auto endTime = std::chrono::high_resolution_clock::now();
  VLOG(1) << " time to complete prefetch: "
          << std::chrono::duration_cast<std::chrono::microseconds>(
                 endTime - startTime)
                 .count();
  stripeLoadBatons_[stripeIndex]->post();
  VLOG(1) << "done in fetch and baton posted for " << stripeIndex << ", thread "
          << std::this_thread::get_id();

  stripeLoadStatuses_.wlock()->operator[](stripeIndex) = FetchStatus::FINISHED;
  return FetchResult::kFetched;
}

DwrfRowReader::FetchResult DwrfRowReader::prefetch(uint32_t stripeToFetch) {
  DWIO_ENSURE(stripeToFetch < stripeCeiling && stripeToFetch >= 0);
  prefetchHasOccurred_ = true;

  VLOG(1) << "Unlocked lock and calling fetch for " << stripeToFetch
          << ", thread " << std::this_thread::get_id();
  return fetch(stripeToFetch);
}

// Guarantee stripe we are currently on is available and loaded
void DwrfRowReader::safeFetchNextStripe() {
  auto startTime = std::chrono::high_resolution_clock::now();
  auto fetchResult = fetch(currentStripe);
  // If result is fetched by this thread or in progress in another thread,
  // record time spent in this function as time blocked on IO.
  bool shouldRecordTimeBlocked = fetchResult != FetchResult::kAlreadyFetched;

  // Check result of fetch to avoid synchronization if we fetched on this
  // thread.
  if (fetchResult != FetchResult::kFetched) {
    // Now we know the stripe was or is being loaded on another thread,
    // Await the baton for this stripe before we return to ensure load is done.
    VLOG(1) << "Waiting on baton for stripe: " << currentStripe;
    stripeLoadBatons_[currentStripe]->wait();
    VLOG(1) << "Acquired baton for stripe " << currentStripe;
  }
  auto reportBlockedOnIoMetric = options_.getBlockedOnIoCallback();
  if (reportBlockedOnIoMetric) {
    if (shouldRecordTimeBlocked) {
      auto timeBlockedOnIo =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::high_resolution_clock::now() - startTime);
      reportBlockedOnIoMetric(timeBlockedOnIo.count());
    } else {
      // We still want to populate stat if we are not blocking on IO.
      reportBlockedOnIoMetric(0);
    };
  }

  DWIO_ENSURE(prefetchedStripeStates_.rlock()->contains(currentStripe));
}

void DwrfRowReader::startNextStripe() {
  if (newStripeReadyForRead || currentStripe >= stripeCeiling) {
    return;
  }
  columnReader_.reset();
  selectiveColumnReader_.reset();
  safeFetchNextStripe();
  prefetchedStripeStates_.withWLock([&](auto& prefetchedStripeStates) {
    DWIO_ENSURE(prefetchedStripeStates.contains(currentStripe));

    auto stripe = getReader().getFooter().stripes(currentStripe);
    rowsInCurrentStripe = stripe.numberOfRows();

    auto& state = prefetchedStripeStates[currentStripe];
    columnReader_ = std::move(state.columnReader);
    selectiveColumnReader_ = std::move(state.selectiveColumnReader);
    stripeDictionaryCache_ = state.stripeDictionaryCache;

    VLOG(1) << "Erasing " << currentStripe << " from prefetched_";
    // This callsite knows we have already fetched the stripe, and preload arg
    // will not be used.
    bool preloadThrowaway = false;
    loadStripe(currentStripe, preloadThrowaway);

    prefetchedStripeStates.erase(currentStripe);
  });
  auto startTime = std::chrono::high_resolution_clock::now();

  DWIO_ENSURE(freeStripeAt(currentStripe));

  newStripeReadyForRead = true;
  auto endTime = std::chrono::high_resolution_clock::now();
  VLOG(1) << " time to complete startNextStripe: "
          << std::chrono::duration_cast<std::chrono::microseconds>(
                 endTime - startTime)
                 .count();
}

size_t DwrfRowReader::estimatedReaderMemory() const {
  return 2 * DwrfReader::getMemoryUse(getReader(), -1, *columnSelector_);
}

std::optional<size_t> DwrfRowReader::estimatedRowSizeHelper(
    const FooterWrapper& footer,
    const dwio::common::Statistics& stats,
    uint32_t nodeId) const {
  DWIO_ENSURE_LT(nodeId, footer.typesSize(), "Types missing in footer");

  const auto& s = stats.getColumnStatistics(nodeId);
  const auto& t = footer.types(nodeId);
  if (!s.getNumberOfValues()) {
    return std::nullopt;
  }
  auto valueCount = s.getNumberOfValues().value();
  if (valueCount < 1) {
    return 0;
  }
  switch (t.kind()) {
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
    case TypeKind::VARCHAR: {
      auto stringStats =
          dynamic_cast<const dwio::common::StringColumnStatistics*>(&s);
      if (!stringStats) {
        return std::nullopt;
      }
      auto length = stringStats->getTotalLength();
      if (!length) {
        return std::nullopt;
      }
      return length.value();
    }
    case TypeKind::VARBINARY: {
      auto binaryStats =
          dynamic_cast<const dwio::common::BinaryColumnStatistics*>(&s);
      if (!binaryStats) {
        return std::nullopt;
      }
      auto length = binaryStats->getTotalLength();
      if (!length) {
        return std::nullopt;
      }
      return length.value();
    }
    case TypeKind::TIMESTAMP: {
      return valueCount * sizeof(uint64_t) * 2;
    }
    case TypeKind::ARRAY:
    case TypeKind::MAP:
    case TypeKind::ROW: {
      // start the estimate with the offsets and hasNulls vectors sizes
      size_t totalEstimate = valueCount * (sizeof(uint8_t) + sizeof(uint64_t));
      for (int32_t i = 0; i < t.subtypesSize(); ++i) {
        if (!columnSelector_->shouldReadNode(t.subtypes(i))) {
          continue;
        }
        auto subtypeEstimate =
            estimatedRowSizeHelper(footer, stats, t.subtypes(i));
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
  auto& reader = getReader();
  auto& footer = reader.getFooter();

  if (!footer.hasNumberOfRows()) {
    return std::nullopt;
  }

  if (footer.numberOfRows() < 1) {
    return 0;
  }

  // Estimate with projections
  constexpr uint32_t ROOT_NODE_ID = 0;
  auto stats = reader.getStatistics();
  auto projectedSize = estimatedRowSizeHelper(footer, *stats, ROOT_NODE_ID);
  if (projectedSize.has_value()) {
    return projectedSize.value() / footer.numberOfRows();
  }

  return std::nullopt;
}

DwrfReader::DwrfReader(
    const ReaderOptions& options,
    std::unique_ptr<dwio::common::BufferedInput> input)
    : readerBase_(std::make_unique<ReaderBase>(
          options.getMemoryPool(),
          std::move(input),
          options.getDecrypterFactory(),
          options.getDirectorySizeGuess(),
          options.getFilePreloadThreshold(),
          options.getFileFormat() == FileFormat::ORC ? FileFormat::ORC
                                                     : FileFormat::DWRF,
          options.isFileColumnNamesReadAsLowerCase())),
      options_(options) {
  // If we are not using column names to map table columns to file columns, then
  // we use indices. In that case we need to ensure the names completely match,
  // because we are still mapping columns by names further down the code.
  // So we rename column names in the file schema to match table schema.
  // We test the options to have 'fileSchema' (actually table schema) as most of
  // the unit tests fail to provide it.
  if ((not options_.isUseColumnNamesForColumnMapping()) and
      (options_.getFileSchema() != nullptr)) {
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
  auto fileRowType = std::dynamic_pointer_cast<const T>(fileType);
  auto tableRowType = std::dynamic_pointer_cast<const T>(tableType);

  std::vector<std::string> newFileFieldNames{fileRowType->names()};
  std::vector<TypePtr> newFileFieldTypes{fileRowType->children()};

  for (auto childIdx = 0; childIdx < tableRowType->size(); ++childIdx) {
    if (childIdx >= fileRowType->size()) {
      break;
    }

    newFileFieldTypes[childIdx] = updateColumnNames(
        fileRowType->childAt(childIdx),
        tableRowType->childAt(childIdx),
        fileRowType->nameOf(childIdx),
        tableRowType->nameOf(childIdx));

    newFileFieldNames[childIdx] = tableRowType->nameOf(childIdx);
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
  // Check type kind equality. If not equal, no point to continue down the tree.
  if (fileType->kind() != tableType->kind()) {
    logTypeInequality(*fileType, *tableType, fileFieldName, tableFieldName);
    return fileType;
  }

  // For leaf types we return type as is.
  if (fileType->isPrimitiveType()) {
    return fileType;
  }

  std::vector<std::string> fileFieldNames{fileType->size()};
  std::vector<TypePtr> fileFieldTypes{fileType->size()};

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
  const auto& tableSchema = options_.getFileSchema();
  const auto& fileSchema = readerBase_->getSchema();
  auto newSchema = std::dynamic_pointer_cast<const RowType>(
      updateColumnNames(fileSchema, tableSchema, "", ""));
  readerBase_->setSchema(newSchema);
}

std::unique_ptr<StripeInformation> DwrfReader::getStripe(
    uint32_t stripeIndex) const {
  DWIO_ENSURE_LT(
      stripeIndex, getNumberOfStripes(), "stripe index out of range");
  auto stripeInfo = readerBase_->getFooter().stripes(stripeIndex);

  return std::make_unique<StripeInformationImpl>(
      stripeInfo.offset(),
      stripeInfo.indexLength(),
      stripeInfo.dataLength(),
      stripeInfo.footerLength(),
      stripeInfo.numberOfRows());
}

std::vector<std::string> DwrfReader::getMetadataKeys() const {
  std::vector<std::string> result;
  auto& footer = readerBase_->getFooter();
  result.reserve(footer.metadataSize());
  for (int32_t i = 0; i < footer.metadataSize(); ++i) {
    result.push_back(footer.metadata(i).name());
  }
  return result;
}

std::string DwrfReader::getMetadataValue(const std::string& key) const {
  auto& footer = readerBase_->getFooter();
  for (int32_t i = 0; i < footer.metadataSize(); ++i) {
    if (footer.metadata(i).name() == key) {
      return footer.metadata(i).value();
    }
  }
  DWIO_RAISE("key not found");
}

bool DwrfReader::hasMetadataValue(const std::string& key) const {
  auto& footer = readerBase_->getFooter();
  for (int32_t i = 0; i < footer.metadataSize(); ++i) {
    if (footer.metadata(i).name() == key) {
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
  ColumnSelector cs(readerBase_->getSchema());
  return getMemoryUse(*readerBase_, stripeIx, cs);
}

uint64_t DwrfReader::getMemoryUseByFieldId(
    const std::vector<uint64_t>& include,
    int32_t stripeIx) {
  ColumnSelector cs(readerBase_->getSchema(), include);
  return getMemoryUse(*readerBase_, stripeIx, cs);
}

uint64_t DwrfReader::getMemoryUseByName(
    const std::vector<std::string>& names,
    int32_t stripeIx) {
  ColumnSelector cs(readerBase_->getSchema(), names);
  return getMemoryUse(*readerBase_, stripeIx, cs);
}

uint64_t DwrfReader::getMemoryUseByTypeId(
    const std::vector<uint64_t>& include,
    int32_t stripeIx) {
  ColumnSelector cs(readerBase_->getSchema(), include, true);
  return getMemoryUse(*readerBase_, stripeIx, cs);
}

uint64_t DwrfReader::getMemoryUse(
    ReaderBase& readerBase,
    int32_t stripeIx,
    const ColumnSelector& cs) {
  uint64_t maxDataLength = 0;
  auto& footer = readerBase.getFooter();
  if (stripeIx >= 0 && stripeIx < footer.stripesSize()) {
    uint64_t stripe = footer.stripes(stripeIx).dataLength();
    if (maxDataLength < stripe) {
      maxDataLength = stripe;
    }
  } else {
    for (int32_t i = 0; i < footer.stripesSize(); i++) {
      uint64_t stripe = footer.stripes(i).dataLength();
      if (maxDataLength < stripe) {
        maxDataLength = stripe;
      }
    }
  }

  bool hasStringColumn = false;
  uint64_t nSelectedStreams = 0;
  for (int32_t i = 0; !hasStringColumn && i < footer.typesSize(); i++) {
    if (cs.shouldReadNode(i)) {
      const auto type = footer.types(i);
      nSelectedStreams += maxStreamsForType(type);
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

  /* If a string column is read, use stripe datalength as a memory estimate
   * because we don't know the dictionary size. Multiply by 2 because
   * a string column requires two buffers:
   * in the input stream and in the seekable input stream.
   * If no string column is read, estimate from the number of streams.
   */
  uint64_t memory = hasStringColumn ? 2 * maxDataLength
                                    : std::min(
                                          uint64_t(maxDataLength),
                                          nSelectedStreams *
                                              readerBase.getBufferedInput()
                                                  .getReadFile()
                                                  ->getNaturalReadSize());

  // Do we need even more memory to read the footer or the metadata?
  auto footerLength = readerBase.getPostScript().footerLength();
  if (memory < footerLength + readerBase.getDirectorySizeGuess()) {
    memory = footerLength + readerBase.getDirectorySizeGuess();
  }

  // Account for firstRowOfStripe.
  memory += static_cast<uint64_t>(footer.stripesSize()) * sizeof(uint64_t);

  // Decompressors need buffers for each stream
  uint64_t decompressorMemory = 0;
  auto compression = readerBase.getCompressionKind();
  if (compression != common::CompressionKind_NONE) {
    for (int32_t i = 0; i < footer.typesSize(); i++) {
      if (cs.shouldReadNode(i)) {
        const auto type = footer.types(i);
        decompressorMemory +=
            maxStreamsForType(type) * readerBase.getCompressionBlockSize();
      }
    }
    if (compression == common::CompressionKind_SNAPPY) {
      decompressorMemory *= 2; // Snappy decompressor uses a second buffer
    }
  }

  return memory + decompressorMemory;
}

std::unique_ptr<dwio::common::RowReader> DwrfReader::createRowReader(
    const RowReaderOptions& opts) const {
  return createDwrfRowReader(opts);
}

std::unique_ptr<DwrfRowReader> DwrfReader::createDwrfRowReader(
    const RowReaderOptions& opts) const {
  auto rowReader = std::make_unique<DwrfRowReader>(readerBase_, opts);
  if (opts.getEagerFirstStripeLoad()) {
    // Load the first stripe on construction so that readers created in
    // background have a reader tree and can preload the first
    // stripe. Also the reader tree needs to exist in order to receive
    // adaptation from a previous reader.
    rowReader->startNextStripe();
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
