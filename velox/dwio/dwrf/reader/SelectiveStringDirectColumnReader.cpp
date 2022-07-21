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

#include "velox/dwio/dwrf/reader/SelectiveStringDirectColumnReader.h"
#include "velox/dwio/common/BufferUtil.h"
#include "velox/dwio/dwrf/common/DecoderUtil.h"

namespace facebook::velox::dwrf {

SelectiveStringDirectColumnReader::SelectiveStringDirectColumnReader(
    const std::shared_ptr<const dwio::common::TypeWithId>& nodeType,
    DwrfParams& params,
    common::ScanSpec& scanSpec)
    : SelectiveColumnReader(nodeType, params, scanSpec, nodeType->type) {
  EncodingKey encodingKey{nodeType->id, params.flatMapContext().sequence};
  auto& stripe = params.stripeStreams();
  RleVersion rleVersion =
      convertRleVersion(stripe.getEncoding(encodingKey).kind());
  auto lenId = encodingKey.forKind(proto::Stream_Kind_LENGTH);
  bool lenVInts = stripe.getUseVInts(lenId);
  lengthDecoder_ = createRleDecoder</*isSigned*/ false>(
      stripe.getStream(lenId, true),
      rleVersion,
      memoryPool_,
      lenVInts,
      dwio::common::INT_BYTE_SIZE);
  blobStream_ =
      stripe.getStream(encodingKey.forKind(proto::Stream_Kind_DATA), true);
}

uint64_t SelectiveStringDirectColumnReader::skip(uint64_t numValues) {
  numValues = SelectiveColumnReader::skip(numValues);
  dwio::common::ensureCapacity<int64_t>(lengths_, numValues, &memoryPool_);
  lengthDecoder_->nextLengths(lengths_->asMutable<int32_t>(), numValues);
  rawLengths_ = lengths_->as<uint32_t>();
  for (auto i = 0; i < numValues; ++i) {
    bytesToSkip_ += rawLengths_[i];
  }
  skipBytes(bytesToSkip_, blobStream_.get(), bufferStart_, bufferEnd_);
  bytesToSkip_ = 0;
  return numValues;
}

void SelectiveStringDirectColumnReader::extractCrossBuffers(
    const int32_t* lengths,
    const int32_t* starts,
    int32_t rowIndex,
    int32_t numValues) {
  int32_t current = 0;
  bool scatter = !outerNonNullRows_.empty();
  for (auto i = 0; i < numValues; ++i) {
    auto gap = starts[i] - current;
    bytesToSkip_ += gap;
    auto size = lengths[i];
    auto value = readValue(size);
    current += size + gap;
    if (!scatter) {
      addValue(value);
    } else {
      auto index = outerNonNullRows_[rowIndex + i];
      if (size <= StringView::kInlineSize) {
        reinterpret_cast<StringView*>(rawValues_)[index] =
            StringView(value.data(), size);
      } else {
        auto copy = copyStringValue(value);
        reinterpret_cast<StringView*>(rawValues_)[index] =
            StringView(copy, size);
      }
    }
  }
  skipBytes(bytesToSkip_, blobStream_.get(), bufferStart_, bufferEnd_);
  bytesToSkip_ = 0;
  if (scatter) {
    numValues_ = outerNonNullRows_[rowIndex + numValues - 1] + 1;
  }
}

inline int32_t
rangeSum(const uint32_t* rows, int32_t start, int32_t begin, int32_t end) {
  for (auto i = begin; i < end; ++i) {
    start += rows[i];
  }
  return start;
}

inline void SelectiveStringDirectColumnReader::makeSparseStarts(
    int32_t startRow,
    const int32_t* rows,
    int32_t numRows,
    int32_t* starts) {
  auto previousRow = lengthIndex_;
  int32_t i = 0;
  int32_t startOffset = 0;
  for (; i < numRows; ++i) {
    int targetRow = rows[startRow + i];
    startOffset = rangeSum(rawLengths_, startOffset, previousRow, targetRow);
    starts[i] = startOffset;
    previousRow = targetRow + 1;
    startOffset += rawLengths_[targetRow];
  }
}

void SelectiveStringDirectColumnReader::extractNSparse(
    const int32_t* rows,
    int32_t row,
    int32_t numValues) {
  int32_t starts[8];
  if (numValues == 8 &&
      (outerNonNullRows_.empty() ? try8Consecutive<false, true>(0, rows, row)
                                 : try8Consecutive<true, true>(0, rows, row))) {
    return;
  }
  int32_t lengths[8];
  for (auto i = 0; i < numValues; ++i) {
    lengths[i] = rawLengths_[rows[row + i]];
  }
  makeSparseStarts(row, rows, numValues, starts);
  extractCrossBuffers(lengths, starts, row, numValues);
  lengthIndex_ = rows[row + numValues - 1] + 1;
}

template <bool scatter, bool sparse>
inline bool SelectiveStringDirectColumnReader::try8Consecutive(
    int32_t start,
    const int32_t* rows,
    int32_t row) {
  // If we haven't read in a buffer yet.
  if (!bufferStart_) {
    return false;
  }
  const char* data = bufferStart_ + start + bytesToSkip_;
  if (!data || bufferEnd_ - data < start + 8 * 12) {
    return false;
  }
  int32_t* result = reinterpret_cast<int32_t*>(rawValues_);
  int32_t resultIndex = numValues_ * 4 - 4;
  auto rawUsed = rawStringUsed_;
  auto previousRow = sparse ? lengthIndex_ : 0;
  auto endRow = row + 8;
  for (auto i = row; i < endRow; ++i) {
    if (scatter) {
      resultIndex = outerNonNullRows_[i] * 4;
    } else {
      resultIndex += 4;
    }
    if (sparse) {
      auto targetRow = rows[i];
      data += rangeSum(rawLengths_, 0, previousRow, rows[i]);
      previousRow = targetRow + 1;
    }
    auto length = rawLengths_[rows[i]];

    if (data + bits::roundUp(length, 16) > bufferEnd_) {
      // Slow path if the string does not fit whole or if there is no
      // space for a 16 byte load.
      return false;
    }
    result[resultIndex] = length;
    auto first16 = xsimd::make_sized_batch_t<int8_t, 16>::load_unaligned(data);
    first16.store_unaligned(reinterpret_cast<char*>(result + resultIndex + 1));
    if (length <= 12) {
      data += length;
      *reinterpret_cast<int64_t*>(
          reinterpret_cast<char*>(result + resultIndex + 1) + length) = 0;
      continue;
    }
    if (!rawStringBuffer_ || rawUsed + length > rawStringSize_) {
      // Slow path if no space in raw strings
      return false;
    }
    *reinterpret_cast<char**>(result + resultIndex + 2) =
        rawStringBuffer_ + rawUsed;
    first16.store_unaligned<char>(rawStringBuffer_ + rawUsed);
    if (length > 16) {
      size_t copySize = bits::roundUp(length - 16, 16);
      VELOX_CHECK_LE(data + copySize, bufferEnd_);
      simd::memcpy(rawStringBuffer_ + rawUsed + 16, data + 16, copySize);
    }
    rawUsed += length;
    data += length;
  }
  // Update the data members only after successful completion.
  bufferStart_ = data;
  bytesToSkip_ = 0;
  rawStringUsed_ = rawUsed;
  numValues_ = scatter ? outerNonNullRows_[row + 7] + 1 : numValues_ + 8;
  lengthIndex_ = sparse ? rows[row + 7] + 1 : lengthIndex_ + 8;
  return true;
}

void SelectiveStringDirectColumnReader::extractSparse(
    const int32_t* rows,
    int32_t numRows) {
  dwio::common::rowLoop(
      rows,
      0,
      numRows,
      8,
      [&](int32_t row) {
        int32_t start = rangeSum(rawLengths_, 0, lengthIndex_, rows[row]);
        lengthIndex_ = rows[row];
        auto lengths =
            reinterpret_cast<const int32_t*>(rawLengths_) + lengthIndex_;

        if (outerNonNullRows_.empty()
                ? try8Consecutive<false, false>(start, rows, row)
                : try8Consecutive<true, false>(start, rows, row)) {
          return;
        }
        int32_t starts[8];
        for (auto i = 0; i < 8; ++i) {
          starts[i] = start;
          start += lengths[i];
        }
        lengthIndex_ += 8;
        extractCrossBuffers(lengths, starts, row, 8);
      },
      [&](int32_t row) { extractNSparse(rows, row, 8); },
      [&](int32_t row, int32_t numRows) {
        extractNSparse(rows, row, numRows);
      });
}

template <bool hasNulls>
void SelectiveStringDirectColumnReader::skipInDecode(
    int32_t numValues,
    int32_t current,
    const uint64_t* nulls) {
  if (hasNulls) {
    numValues = bits::countNonNulls(nulls, current, current + numValues);
  }
  for (size_t i = lengthIndex_; i < lengthIndex_ + numValues; ++i) {
    bytesToSkip_ += rawLengths_[i];
  }
  lengthIndex_ += numValues;
}

folly::StringPiece SelectiveStringDirectColumnReader::readValue(
    int32_t length) {
  skipBytes(bytesToSkip_, blobStream_.get(), bufferStart_, bufferEnd_);
  bytesToSkip_ = 0;
  // bufferStart_ may be null if length is 0 and this is the first string
  // we're reading.
  if (bufferEnd_ - bufferStart_ >= length) {
    bytesToSkip_ = length;
    return folly::StringPiece(bufferStart_, length);
  }
  tempString_.resize(length);
  readBytes(
      length, blobStream_.get(), tempString_.data(), bufferStart_, bufferEnd_);
  return folly::StringPiece(tempString_);
}

template <bool hasNulls, typename Visitor>
void SelectiveStringDirectColumnReader::decode(
    const uint64_t* nulls,
    Visitor visitor) {
  int32_t current = visitor.start();
  bool atEnd = false;
  bool allowNulls = hasNulls && visitor.allowNulls();
  for (;;) {
    int32_t toSkip;
    if (hasNulls && allowNulls && bits::isBitNull(nulls, current)) {
      toSkip = visitor.processNull(atEnd);
    } else {
      if (hasNulls && !allowNulls) {
        toSkip = visitor.checkAndSkipNulls(nulls, current, atEnd);
        if (!Visitor::dense) {
          skipInDecode<false>(toSkip, current, nullptr);
        }
        if (atEnd) {
          return;
        }
      }

      // Check if length passes the filter first. Don't read the value if length
      // doesn't pass.
      auto length = rawLengths_[lengthIndex_++];
      auto toSkipOptional = visitor.processLength(length, atEnd);
      if (toSkipOptional.has_value()) {
        bytesToSkip_ += length;
        toSkip = toSkipOptional.value();
      } else {
        toSkip = visitor.process(readValue(length), atEnd);
      }
    }
    ++current;
    if (toSkip) {
      skipInDecode<hasNulls>(toSkip, current, nulls);
      current += toSkip;
    }
    if (atEnd) {
      return;
    }
  }
}

template <typename TVisitor>
void SelectiveStringDirectColumnReader::readWithVisitor(
    RowSet rows,
    TVisitor visitor) {
  vector_size_t numRows = rows.back() + 1;
  int32_t current = visitor.start();
  constexpr bool isExtract =
      std::is_same<typename TVisitor::FilterType, common::AlwaysTrue>::value &&
      std::is_same<
          typename TVisitor::Extract,
          dwio::common::ExtractToReader<SelectiveStringDirectColumnReader>>::
          value;
  auto nulls = nullsInReadRange_ ? nullsInReadRange_->as<uint64_t>() : nullptr;

  if (process::hasAvx2() && isExtract) {
    if (nullsInReadRange_) {
      if (TVisitor::dense) {
        returnReaderNulls_ = true;
        dwio::common::nonNullRowsFromDense(
            nulls, rows.size(), outerNonNullRows_);
        extractSparse(rows.data(), outerNonNullRows_.size());
      } else {
        int32_t tailSkip = -1;
        anyNulls_ = dwio::common::nonNullRowsFromSparse<false, true>(
            nulls,
            rows,
            innerNonNullRows_,
            outerNonNullRows_,
            rawResultNulls_,
            tailSkip);
        extractSparse(innerNonNullRows_.data(), innerNonNullRows_.size());
        skipInDecode<false>(tailSkip, 0, nullptr);
      }
    } else {
      extractSparse(rows.data(), rows.size());
    }
    numValues_ = rows.size();
    readOffset_ += numRows;
    return;
  }

  if (nulls) {
    skipInDecode<true>(current, 0, nulls);
  } else {
    skipInDecode<false>(current, 0, nulls);
  }
  if (nulls) {
    decode<true, TVisitor>(nullsInReadRange_->as<uint64_t>(), visitor);
  } else {
    decode<false, TVisitor>(nullptr, visitor);
  }
  readOffset_ += numRows;
}

template <typename TFilter, bool isDense, typename ExtractValues>
void SelectiveStringDirectColumnReader::readHelper(
    common::Filter* filter,
    RowSet rows,
    ExtractValues extractValues) {
  readWithVisitor(
      rows,
      dwio::common::
          ColumnVisitor<folly::StringPiece, TFilter, ExtractValues, isDense>(
              *reinterpret_cast<TFilter*>(filter), this, rows, extractValues));
}

template <bool isDense, typename ExtractValues>
void SelectiveStringDirectColumnReader::processFilter(
    common::Filter* filter,
    RowSet rows,
    ExtractValues extractValues) {
  switch (filter ? filter->kind() : common::FilterKind::kAlwaysTrue) {
    case common::FilterKind::kAlwaysTrue:
      readHelper<common::AlwaysTrue, isDense>(filter, rows, extractValues);
      break;
    case common::FilterKind::kIsNull:
      filterNulls<StringView>(
          rows,
          true,
          !std::is_same<decltype(extractValues), DropValues>::value);
      break;
    case common::FilterKind::kIsNotNull:
      if (std::is_same<decltype(extractValues), DropValues>::value) {
        filterNulls<StringView>(rows, false, false);
      } else {
        readHelper<common::IsNotNull, isDense>(filter, rows, extractValues);
      }
      break;
    case common::FilterKind::kBytesRange:
      readHelper<common::BytesRange, isDense>(filter, rows, extractValues);
      break;
    case common::FilterKind::kNegatedBytesRange:
      readHelper<common::NegatedBytesRange, isDense>(
          filter, rows, extractValues);
      break;
    case common::FilterKind::kBytesValues:
      readHelper<common::BytesValues, isDense>(filter, rows, extractValues);
      break;
    case common::FilterKind::kNegatedBytesValues:
      readHelper<common::NegatedBytesValues, isDense>(
          filter, rows, extractValues);
      break;
    default:
      readHelper<common::Filter, isDense>(filter, rows, extractValues);
      break;
  }
}

void SelectiveStringDirectColumnReader::read(
    vector_size_t offset,
    RowSet rows,
    const uint64_t* incomingNulls) {
  prepareRead<folly::StringPiece>(offset, rows, incomingNulls);
  bool isDense = rows.back() == rows.size() - 1;

  auto end = rows.back() + 1;
  auto numNulls =
      nullsInReadRange_ ? BaseVector::countNulls(nullsInReadRange_, 0, end) : 0;
  dwio::common::ensureCapacity<int32_t>(lengths_, end - numNulls, &memoryPool_);
  lengthDecoder_->nextLengths(lengths_->asMutable<int32_t>(), end - numNulls);
  rawLengths_ = lengths_->as<uint32_t>();
  lengthIndex_ = 0;
  if (scanSpec_->keepValues()) {
    if (scanSpec_->valueHook()) {
      if (isDense) {
        readHelper<common::AlwaysTrue, true>(
            &dwio::common::alwaysTrue(),
            rows,
            dwio::common::ExtractToGenericHook(scanSpec_->valueHook()));
      } else {
        readHelper<common::AlwaysTrue, false>(
            &dwio::common::alwaysTrue(),
            rows,
            dwio::common::ExtractToGenericHook(scanSpec_->valueHook()));
      }
      return;
    }
    if (isDense) {
      processFilter<true>(
          scanSpec_->filter(), rows, dwio::common::ExtractToReader(this));
    } else {
      processFilter<false>(
          scanSpec_->filter(), rows, dwio::common::ExtractToReader(this));
    }
  } else {
    if (isDense) {
      processFilter<true>(
          scanSpec_->filter(), rows, dwio::common::DropValues());
    } else {
      processFilter<false>(
          scanSpec_->filter(), rows, dwio::common::DropValues());
    }
  }
}

} // namespace facebook::velox::dwrf
