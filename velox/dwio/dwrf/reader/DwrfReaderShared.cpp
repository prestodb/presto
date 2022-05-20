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

#include "velox/dwio/dwrf/reader/DwrfReaderShared.h"
#include <exception>
#include <optional>
#include "velox/dwio/common/TypeUtils.h"
#include "velox/dwio/common/exception/Exceptions.h"
#include "velox/dwio/dwrf/common/Statistics.h"
#include "velox/dwio/dwrf/reader/SelectiveColumnReader.h"
#include "velox/dwio/dwrf/reader/StripeStream.h"

namespace facebook::velox::dwrf {

using dwio::common::ColumnSelector;
using dwio::common::InputStream;
using dwio::common::ReaderOptions;
using dwio::common::RowReaderOptions;
using dwio::common::TypeWithId;
using dwio::common::typeutils::CompatChecker;

DwrfRowReaderShared::DwrfRowReaderShared(
    const std::shared_ptr<ReaderBase>& reader,
    const RowReaderOptions& opts)
    : StripeReaderBase(reader),
      options_(opts),
      columnSelector_{std::make_shared<ColumnSelector>(
          ColumnSelector::apply(opts.getSelector(), reader->getSchema()))} {
  auto& footer = getReader().getFooter();
  uint32_t numberOfStripes = footer.stripes_size();
  currentStripe = numberOfStripes;
  lastStripe = 0;
  currentRowInStripe = 0;
  newStripeLoaded = false;
  rowsInCurrentStripe = 0;
  uint64_t rowTotal = 0;

  firstRowOfStripe.reserve(numberOfStripes);
  for (uint32_t i = 0; i < numberOfStripes; ++i) {
    firstRowOfStripe.push_back(rowTotal);
    proto::StripeInformation stripeInfo = footer.stripes(i);
    rowTotal += stripeInfo.numberofrows();
    if ((stripeInfo.offset() >= opts.getOffset()) &&
        (stripeInfo.offset() < opts.getLimit())) {
      if (i < currentStripe) {
        currentStripe = i;
      }
      if (i >= lastStripe) {
        lastStripe = i + 1;
      }
    }
  }
  firstStripe = currentStripe;

  if (currentStripe == 0) {
    previousRow = std::numeric_limits<uint64_t>::max();
  } else if (currentStripe == numberOfStripes) {
    previousRow = footer.numberofrows();
  } else {
    previousRow = firstRowOfStripe[firstStripe] - 1;
  }

  // Validate the requested type is compatible with what's in the file
  std::function<std::string()> createExceptionContext = [&]() {
    std::string exceptionMessageContext = fmt::format(
        "The schema loaded in the reader does not match the schema in the file footer."
        "Input Stream Name: {},\n"
        "File Footer Schema (without partition columns): {},\n"
        "Input Table Schema (with partition columns): {}\n",
        getReader().getStream().getName(),
        getReader().getSchema()->toString(),
        getType()->toString());
    return exceptionMessageContext;
  };

  if (options_.getScanSpec()) {
    columnReaderFactory_ =
        std::make_unique<SelectiveColumnReaderFactory>(options_.getScanSpec());
  }

  CompatChecker::check(
      *getReader().getSchema(), *getType(), true, createExceptionContext);
}

uint64_t DwrfRowReaderShared::seekToRow(uint64_t rowNumber) {
  // Empty file
  if (isEmptyFile()) {
    return 0;
  }

  LOG(INFO) << "rowNumber: " << rowNumber << " currentStripe: " << currentStripe
            << " firstStripe: " << firstStripe << " lastStripe: " << lastStripe;

  // If we are reading only a portion of the file
  // (bounded by firstStripe and lastStripe),
  // seeking before or after the portion of interest should return no data.
  // Implement this by setting previousRow to the number of rows in the file.

  // seeking past lastStripe
  auto& footer = getReader().getFooter();
  uint32_t num_stripes = footer.stripes_size();
  if ((lastStripe == num_stripes && rowNumber >= footer.numberofrows()) ||
      (lastStripe < num_stripes && rowNumber >= firstRowOfStripe[lastStripe])) {
    LOG(INFO) << "Trying to seek past lastStripe, total rows: "
              << footer.numberofrows() << " num_stripes: " << num_stripes;

    currentStripe = num_stripes;

    previousRow = footer.numberofrows();
    return previousRow;
  }

  uint32_t seekToStripe = 0;
  while (seekToStripe + 1 < lastStripe &&
         firstRowOfStripe[seekToStripe + 1] <= rowNumber) {
    seekToStripe++;
  }

  // seeking before the first stripe
  if (seekToStripe < firstStripe) {
    currentStripe = num_stripes;
    previousRow = footer.numberofrows();
    return previousRow;
  }

  currentStripe = seekToStripe;
  currentRowInStripe = rowNumber - firstRowOfStripe[currentStripe];
  previousRow = rowNumber;
  newStripeLoaded = false;
  startNextStripe();
  seekImpl();

  return previousRow;
}

uint64_t DwrfRowReaderShared::skipRows(uint64_t numberOfRowsToSkip) {
  if (isEmptyFile()) {
    LOG(INFO) << "Empty file, nothing to skip";
    return 0;
  }

  // When no rows to skip - just return 0
  if (numberOfRowsToSkip == 0) {
    LOG(INFO) << "No skipping is needed";
    return 0;
  }

  // when we skipped or exhausted the whole file we can return 0
  auto& footer = getReader().getFooter();
  if (previousRow == footer.numberofrows()) {
    LOG(INFO) << "previousRow is beyond EOF, nothing to skip";
    return 0;
  }

  if (previousRow == std::numeric_limits<uint64_t>::max()) {
    LOG(INFO) << "Start of the file, skipping: " << numberOfRowsToSkip;
    seekToRow(numberOfRowsToSkip);
    if (previousRow == footer.numberofrows()) {
      LOG(INFO) << "Reached end of the file, returning: " << previousRow;
      return previousRow;
    } else {
      LOG(INFO) << "previousRow after skipping: " << previousRow;
      return previousRow;
    }
  }

  LOG(INFO) << "Previous row: " << previousRow
            << " Skipping: " << numberOfRowsToSkip;

  uint64_t initialRow = previousRow;
  seekToRow(previousRow + numberOfRowsToSkip);

  LOG(INFO) << "After skipping: " << previousRow
            << " InitialRow: " << initialRow;

  if (previousRow == footer.numberofrows()) {
    LOG(INFO) << "When seeking past lastStripe";
    return previousRow - initialRow - 1;
  }
  return previousRow - initialRow;
}

DwrfReaderShared::DwrfReaderShared(
    const ReaderOptions& options,
    std::unique_ptr<InputStream> input)
    : readerBase_(std::make_unique<ReaderBase>(
          options.getMemoryPool(),
          std::move(input),
          options.getDecrypterFactory(),
          options.getBufferedInputFactory()
              ? options.getBufferedInputFactory()
              : BufferedInputFactory::baseFactoryShared(),
          options.getFileNum())),
      options_(options) {}

std::unique_ptr<StripeInformation> DwrfReaderShared::getStripe(
    uint32_t stripeIndex) const {
  DWIO_ENSURE_LE(
      stripeIndex, getNumberOfStripes(), "stripe index out of range");
  proto::StripeInformation stripeInfo =
      readerBase_->getFooter().stripes(stripeIndex);

  return std::make_unique<StripeInformationImpl>(
      stripeInfo.offset(),
      stripeInfo.indexlength(),
      stripeInfo.datalength(),
      stripeInfo.footerlength(),
      stripeInfo.numberofrows());
}

std::vector<std::string> DwrfReaderShared::getMetadataKeys() const {
  std::vector<std::string> result;
  auto& footer = readerBase_->getFooter();
  result.reserve(footer.metadata_size());
  for (int32_t i = 0; i < footer.metadata_size(); ++i) {
    result.push_back(footer.metadata(i).name());
  }
  return result;
}

std::string DwrfReaderShared::getMetadataValue(const std::string& key) const {
  auto& footer = readerBase_->getFooter();
  for (int32_t i = 0; i < footer.metadata_size(); ++i) {
    if (footer.metadata(i).name() == key) {
      return footer.metadata(i).value();
    }
  }
  DWIO_RAISE("key not found");
}

bool DwrfReaderShared::hasMetadataValue(const std::string& key) const {
  auto& footer = readerBase_->getFooter();
  for (int32_t i = 0; i < footer.metadata_size(); ++i) {
    if (footer.metadata(i).name() == key) {
      return true;
    }
  }
  return false;
}

uint64_t maxStreamsForType(const proto::Type& type) {
  switch (static_cast<int64_t>(type.kind())) {
    case proto::Type_Kind_STRUCT:
      return 1;
    case proto::Type_Kind_FLOAT:
    case proto::Type_Kind_DOUBLE:
    case proto::Type_Kind_BOOLEAN:
    case proto::Type_Kind_BYTE:
    case proto::Type_Kind_LIST:
    case proto::Type_Kind_MAP:
    case proto::Type_Kind_UNION:
      return 2;
    case proto::Type_Kind_BINARY:
    case proto::Type_Kind_TIMESTAMP:
      return 3;
    case proto::Type_Kind_INT:
    case proto::Type_Kind_LONG:
    case proto::Type_Kind_SHORT:
      return 4;
    case proto::Type_Kind_STRING:
      return 7;
    default:
      return 0;
  }
}

uint64_t DwrfReaderShared::getMemoryUse(int32_t stripeIx) {
  ColumnSelector cs(readerBase_->getSchema());
  return getMemoryUse(*readerBase_, stripeIx, cs);
}

uint64_t DwrfReaderShared::getMemoryUseByFieldId(
    const std::vector<uint64_t>& include,
    int32_t stripeIx) {
  ColumnSelector cs(readerBase_->getSchema(), include);
  return getMemoryUse(*readerBase_, stripeIx, cs);
}

uint64_t DwrfReaderShared::getMemoryUseByName(
    const std::vector<std::string>& names,
    int32_t stripeIx) {
  ColumnSelector cs(readerBase_->getSchema(), names);
  return getMemoryUse(*readerBase_, stripeIx, cs);
}

uint64_t DwrfReaderShared::getMemoryUseByTypeId(
    const std::vector<uint64_t>& include,
    int32_t stripeIx) {
  ColumnSelector cs(readerBase_->getSchema(), include, true);
  return getMemoryUse(*readerBase_, stripeIx, cs);
}

uint64_t DwrfReaderShared::getMemoryUse(
    ReaderBase& readerBase,
    int32_t stripeIx,
    const ColumnSelector& cs) {
  uint64_t maxDataLength = 0;
  auto& footer = readerBase.getFooter();
  if (stripeIx >= 0 && stripeIx < footer.stripes_size()) {
    uint64_t stripe = footer.stripes(stripeIx).datalength();
    if (maxDataLength < stripe) {
      maxDataLength = stripe;
    }
  } else {
    for (int32_t i = 0; i < footer.stripes_size(); i++) {
      uint64_t stripe = footer.stripes(i).datalength();
      if (maxDataLength < stripe) {
        maxDataLength = stripe;
      }
    }
  }

  bool hasStringColumn = false;
  uint64_t nSelectedStreams = 0;
  for (int32_t i = 0; !hasStringColumn && i < footer.types_size(); i++) {
    if (cs.shouldReadNode(i)) {
      const proto::Type& type = footer.types(i);
      nSelectedStreams += maxStreamsForType(type);
      switch (static_cast<int64_t>(type.kind())) {
        case proto::Type_Kind_STRING:
        case proto::Type_Kind_BINARY: {
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
  uint64_t memory = hasStringColumn
      ? 2 * maxDataLength
      : std::min(
            uint64_t(maxDataLength),
            nSelectedStreams * readerBase.getStream().getNaturalReadSize());

  // Do we need even more memory to read the footer or the metadata?
  auto footerLength = readerBase.getPostScript().footerlength();
  if (memory < footerLength + DIRECTORY_SIZE_GUESS) {
    memory = footerLength + DIRECTORY_SIZE_GUESS;
  }

  // Account for firstRowOfStripe.
  memory += static_cast<uint64_t>(footer.stripes_size()) * sizeof(uint64_t);

  // Decompressors need buffers for each stream
  uint64_t decompressorMemory = 0;
  auto compression = readerBase.getCompressionKind();
  if (compression != CompressionKind_NONE) {
    for (int32_t i = 0; i < footer.types_size(); i++) {
      if (cs.shouldReadNode(i)) {
        const proto::Type& type = footer.types(i);
        decompressorMemory +=
            maxStreamsForType(type) * readerBase.getCompressionBlockSize();
      }
    }
    if (compression == CompressionKind_SNAPPY) {
      decompressorMemory *= 2; // Snappy decompressor uses a second buffer
    }
  }

  return memory + decompressorMemory;
}

void DwrfRowReaderShared::startNextStripe() {
  if (newStripeLoaded || currentStripe >= lastStripe) {
    return;
  }

  resetColumnReaderImpl();
  bool preload = options_.getPreloadStripe();
  auto& currentStripeInfo = loadStripe(currentStripe, preload);
  rowsInCurrentStripe = currentStripeInfo.numberofrows();

  StripeStreamsImpl stripeStreams(
      *this,
      getColumnSelector(),
      options_,
      currentStripeInfo.offset(),
      *this,
      currentStripe);
  createColumnReaderImpl(stripeStreams);

  // load data plan according to its updated selector
  // during column reader construction
  // if planReads is off which means stripe data loaded as whole
  if (!preload) {
    VLOG(1) << "[DWRF] Load read plan for stripe " << currentStripe;
    stripeStreams.loadReadPlan();
  }

  stripeDictionaryCache_ = stripeStreams.getStripeDictionaryCache();
  newStripeLoaded = true;
}

size_t DwrfRowReaderShared::estimatedReaderMemory() const {
  return 2 * DwrfReaderShared::getMemoryUse(getReader(), -1, *columnSelector_);
}

std::optional<size_t> DwrfRowReaderShared::estimatedRowSizeHelper(
    const proto::Footer& footer,
    const dwio::common::Statistics& stats,
    uint32_t nodeId) const {
  DWIO_ENSURE_LT(nodeId, footer.types_size(), "Types missing in footer");

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
    case proto::Type_Kind_BOOLEAN: {
      return valueCount * sizeof(uint8_t);
    }
    case proto::Type_Kind_BYTE: {
      return valueCount * sizeof(uint8_t);
    }
    case proto::Type_Kind_SHORT: {
      return valueCount * sizeof(uint16_t);
    }
    case proto::Type_Kind_INT: {
      return valueCount * sizeof(uint32_t);
    }
    case proto::Type_Kind_LONG: {
      return valueCount * sizeof(uint64_t);
    }
    case proto::Type_Kind_FLOAT: {
      return valueCount * sizeof(float);
    }
    case proto::Type_Kind_DOUBLE: {
      return valueCount * sizeof(double);
    }
    case proto::Type_Kind_STRING: {
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
    case proto::Type_Kind_BINARY: {
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
    case proto::Type_Kind_TIMESTAMP: {
      return valueCount * sizeof(uint64_t) * 2;
    }
    case proto::Type_Kind_LIST:
    case proto::Type_Kind_MAP:
    case proto::Type_Kind_STRUCT:
    case proto::Type_Kind_UNION: {
      // start the estimate with the offsets and hasNulls vectors sizes
      size_t totalEstimate = valueCount * (sizeof(uint8_t) + sizeof(uint64_t));
      for (int32_t i = 0; i < t.subtypes_size() &&
           columnSelector_->shouldReadNode(t.subtypes(i));
           ++i) {
        auto subtypeEstimate =
            estimatedRowSizeHelper(footer, stats, t.subtypes(i));
        if (subtypeEstimate.has_value()) {
          totalEstimate = t.kind() == proto::Type_Kind_UNION
              ? std::max(totalEstimate, subtypeEstimate.value())
              : (totalEstimate + subtypeEstimate.value());
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

std::optional<size_t> DwrfRowReaderShared::estimatedRowSize() const {
  auto& reader = getReader();
  auto& footer = reader.getFooter();

  if (!footer.has_numberofrows()) {
    return std::nullopt;
  }

  if (footer.numberofrows() < 1) {
    return 0;
  }

  // Estimate with projections
  constexpr uint32_t ROOT_NODE_ID = 0;
  auto stats = reader.getStatistics();
  auto projectedSize = estimatedRowSizeHelper(footer, *stats, ROOT_NODE_ID);
  if (projectedSize.has_value()) {
    return projectedSize.value() / footer.numberofrows();
  }

  return std::nullopt;
}

} // namespace facebook::velox::dwrf
