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

#include "velox/dwio/dwrf/reader/SelectiveTimestampColumnReader.h"
#include "velox/dwio/common/BufferUtil.h"
#include "velox/dwio/dwrf/common/DecoderUtil.h"

namespace facebook::velox::dwrf {

using namespace dwio::common;

SelectiveTimestampColumnReader::SelectiveTimestampColumnReader(
    const std::shared_ptr<const TypeWithId>& fileType,
    DwrfParams& params,
    common::ScanSpec& scanSpec)
    : SelectiveColumnReader(fileType->type(), fileType, params, scanSpec),
      precision_(
          params.stripeStreams().rowReaderOptions().timestampPrecision()) {
  EncodingKey encodingKey{fileType_->id(), params.flatMapContext().sequence};
  auto& stripe = params.stripeStreams();
  version_ = convertRleVersion(stripe.getEncoding(encodingKey).kind());
  auto data = encodingKey.forKind(proto::Stream_Kind_DATA);
  bool vints = stripe.getUseVInts(data);
  seconds_ = createRleDecoder</*isSigned=*/true>(
      stripe.getStream(data, params.streamLabels().label(), true),
      version_,
      *memoryPool_,
      vints,
      LONG_BYTE_SIZE);
  auto nanoData = encodingKey.forKind(proto::Stream_Kind_NANO_DATA);
  bool nanoVInts = stripe.getUseVInts(nanoData);
  nano_ = createRleDecoder</*isSigned=*/false>(
      stripe.getStream(nanoData, params.streamLabels().label(), true),
      version_,
      *memoryPool_,
      nanoVInts,
      LONG_BYTE_SIZE);
}

uint64_t SelectiveTimestampColumnReader::skip(uint64_t numValues) {
  numValues = SelectiveColumnReader::skip(numValues);
  seconds_->skip(numValues);
  nano_->skip(numValues);
  return numValues;
}

void SelectiveTimestampColumnReader::seekToRowGroup(uint32_t index) {
  SelectiveColumnReader::seekToRowGroup(index);
  auto positionsProvider = formatData_->seekToRowGroup(index);
  seconds_->seekToRowGroup(positionsProvider);
  nano_->seekToRowGroup(positionsProvider);
  // Check that all the provided positions have been consumed.
  VELOX_CHECK(!positionsProvider.hasNext());
}

void SelectiveTimestampColumnReader::read(
    vector_size_t offset,
    const RowSet& rows,
    const uint64_t* incomingNulls) {
  prepareRead<int64_t>(offset, rows, incomingNulls);
  VELOX_CHECK(
      !scanSpec_->valueHook(),
      "Selective reader for TIMESTAMP doesn't support aggregation pushdown yet");
  if (!resultNulls_ || !resultNulls_->unique() ||
      resultNulls_->capacity() * 8 < rows.size()) {
    // Make sure a dedicated resultNulls_ is allocated with enough capacity as
    // RleDecoder always assumes it is available.
    resultNulls_ = AlignedBuffer::allocate<bool>(rows.size(), memoryPool_);
    rawResultNulls_ = resultNulls_->asMutable<uint64_t>();
  }
  bool isDense = rows.back() == rows.size() - 1;
  if (isDense) {
    readHelper<true>(scanSpec_->filter(), rows);
  } else {
    readHelper<false>(scanSpec_->filter(), rows);
  }
  readOffset_ += rows.back() + 1;
}

template <bool isDense>
void SelectiveTimestampColumnReader::readHelper(
    common::Filter* filter,
    const RowSet& rows) {
  ExtractToReader extractValues(this);
  common::AlwaysTrue alwaysTrue;
  DirectRleColumnVisitor<
      int64_t,
      common::AlwaysTrue,
      decltype(extractValues),
      isDense>
      visitor(alwaysTrue, this, rows, extractValues);

  if (version_ == velox::dwrf::RleVersion_1) {
    decodeWithVisitor<velox::dwrf::RleDecoderV1<true>>(seconds_.get(), visitor);
  } else {
    decodeWithVisitor<velox::dwrf::RleDecoderV2<true>>(seconds_.get(), visitor);
  }

  // Save the seconds into their own buffer before reading nanos into
  // 'values_'
  dwio::common::ensureCapacity<uint64_t>(
      secondsValues_, numValues_, memoryPool_);
  secondsValues_->setSize(numValues_ * sizeof(int64_t));
  memcpy(
      secondsValues_->asMutable<char>(),
      rawValues_,
      numValues_ * sizeof(int64_t));

  // We read the nanos into 'values_' starting at index 0.
  numValues_ = 0;
  if (version_ == velox::dwrf::RleVersion_1) {
    decodeWithVisitor<velox::dwrf::RleDecoderV1<false>>(nano_.get(), visitor);
  } else {
    decodeWithVisitor<velox::dwrf::RleDecoderV2<false>>(nano_.get(), visitor);
  }

  // Merge the seconds and nanos into 'values_'
  auto secondsData = secondsValues_->as<int64_t>();
  auto nanosData = values_->as<uint64_t>();
  const auto rawNulls = nullsInReadRange_
      ? (isDense ? nullsInReadRange_->as<uint64_t>() : rawResultNulls_)
      : nullptr;
  auto tsValues = AlignedBuffer::allocate<Timestamp>(numValues_, memoryPool_);
  auto rawTs = tsValues->asMutable<Timestamp>();

  for (vector_size_t i = 0; i < numValues_; i++) {
    if (!rawNulls || !bits::isBitNull(rawNulls, i)) {
      auto nanos = nanosData[i];
      uint64_t zeros = nanos & 0x7;
      nanos >>= 3;
      if (zeros != 0) {
        for (uint64_t j = 0; j <= zeros; ++j) {
          nanos *= 10;
        }
      }
      auto seconds = secondsData[i] + EPOCH_OFFSET;
      if (seconds < 0 && nanos != 0) {
        seconds -= 1;
      }
      switch (precision_) {
        case TimestampPrecision::kMilliseconds:
          nanos = nanos / 1'000'000 * 1'000'000;
          break;
        case TimestampPrecision::kMicroseconds:
          nanos = nanos / 1'000 * 1'000;
          break;
        case TimestampPrecision::kNanoseconds:
          break;
      }
      rawTs[i] = Timestamp(seconds, nanos);
    }
  }
  values_ = tsValues;
  rawValues_ = values_->asMutable<char>();

  // Treat the filter as kAlwaysTrue if any of the following conditions are met:
  // 1) No filter found;
  // 2) Filter is kIsNotNull but rawNulls==NULL (no elements is null).
  switch (
      !filter || (filter->kind() == common::FilterKind::kIsNotNull && !rawNulls)
          ? common::FilterKind::kAlwaysTrue
          : filter->kind()) {
    case common::FilterKind::kAlwaysTrue:
      // Simply add all rows to output.
      for (vector_size_t i = 0; i < numValues_; i++) {
        addOutputRow(rows[i]);
      }
      break;
    case common::FilterKind::kIsNull:
      processNulls(true, rows, rawNulls);
      break;
    case common::FilterKind::kIsNotNull:
      processNulls(false, rows, rawNulls);
      break;
    case common::FilterKind::kTimestampRange:
    case common::FilterKind::kMultiRange:
      processFilter(filter, rows, rawNulls);
      break;
    default:
      VELOX_UNSUPPORTED("Unsupported filter.");
  }
}

void SelectiveTimestampColumnReader::processNulls(
    const bool isNull,
    const RowSet& rows,
    const uint64_t* rawNulls) {
  if (!rawNulls) {
    return;
  }
  auto rawTs = values_->asMutable<Timestamp>();

  returnReaderNulls_ = false;
  anyNulls_ = !isNull;
  allNull_ = isNull;
  vector_size_t idx = 0;
  for (vector_size_t i = 0; i < numValues_; i++) {
    if (isNull) {
      if (bits::isBitNull(rawNulls, i)) {
        bits::setNull(rawResultNulls_, idx);
        addOutputRow(rows[i]);
        idx++;
      }
    } else {
      if (!bits::isBitNull(rawNulls, i)) {
        bits::setNull(rawResultNulls_, idx, false);
        rawTs[idx] = rawTs[i];
        addOutputRow(rows[i]);
        idx++;
      }
    }
  }
}

void SelectiveTimestampColumnReader::processFilter(
    const common::Filter* filter,
    const RowSet& rows,
    const uint64_t* rawNulls) {
  auto rawTs = values_->asMutable<Timestamp>();

  returnReaderNulls_ = false;
  anyNulls_ = false;
  allNull_ = true;
  vector_size_t idx = 0;
  for (vector_size_t i = 0; i < numValues_; i++) {
    if (rawNulls && bits::isBitNull(rawNulls, i)) {
      if (filter->testNull()) {
        bits::setNull(rawResultNulls_, idx);
        addOutputRow(rows[i]);
        anyNulls_ = true;
        idx++;
      }
    } else {
      if (filter->testTimestamp(rawTs[i])) {
        if (rawNulls) {
          bits::setNull(rawResultNulls_, idx, false);
        }
        rawTs[idx] = rawTs[i];
        addOutputRow(rows[i]);
        allNull_ = false;
        idx++;
      }
    }
  }
}

void SelectiveTimestampColumnReader::getValues(
    const RowSet& rows,
    VectorPtr* result) {
  getFlatValues<Timestamp, Timestamp>(rows, result, fileType_->type(), true);
}

} // namespace facebook::velox::dwrf
