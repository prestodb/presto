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

#include "velox/common/testutil/TestValue.h"
#include "velox/dwio/common/BufferUtil.h"
#include "velox/dwio/dwrf/common/DecoderUtil.h"

namespace facebook::velox::dwrf {

SelectiveStringDirectColumnReader::SelectiveStringDirectColumnReader(
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    DwrfParams& params,
    common::ScanSpec& scanSpec)
    : SelectiveColumnReader(fileType->type(), fileType, params, scanSpec) {
  EncodingKey encodingKey{fileType->id(), params.flatMapContext().sequence};
  auto& stripe = params.stripeStreams();
  RleVersion rleVersion =
      convertRleVersion(stripe.getEncoding(encodingKey).kind());
  auto lenId = encodingKey.forKind(proto::Stream_Kind_LENGTH);
  bool lenVInts = stripe.getUseVInts(lenId);
  lengthDecoder_ = createRleDecoder</*isSigned*/ false>(
      stripe.getStream(lenId, params.streamLabels().label(), true),
      rleVersion,
      memoryPool_,
      lenVInts,
      dwio::common::INT_BYTE_SIZE);
  blobStream_ = stripe.getStream(
      encodingKey.forKind(proto::Stream_Kind_DATA),
      params.streamLabels().label(),
      true);
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

namespace {

#if XSIMD_WITH_AVX2
xsimd::make_sized_batch_t<uint16_t, 8> toUint16x8(xsimd::batch<uint32_t> x) {
  auto y = _mm256_castsi128_si256(_mm256_extracti128_si256(x, 1));
  return _mm256_castsi256_si128(_mm256_packus_epi32(x, y));
}
#endif

bool allSmallEnough(const uint32_t* lengths, uint16_t* offsets, bool& gt4) {
#if XSIMD_WITH_AVX2
  auto vlength = xsimd::load_unaligned(lengths);
  static_assert(vlength.size == 8);
  if (simd::toBitMask(vlength > xsimd::broadcast<uint32_t>(12))) {
    return false;
  }
  gt4 = simd::toBitMask(vlength > xsimd::broadcast<uint32_t>(4));
  // Convert to 128 bit vector to calculate prefix sums, because
  // _mm256_slli_si256 is not shifting all 8 lanes together.
  auto vlength16 = toUint16x8(vlength);
  vlength16 += _mm_slli_si128(vlength16, 2);
  vlength16 += _mm_slli_si128(vlength16, 4);
  vlength16 += _mm_slli_si128(vlength16, 8);
  offsets[0] = 0;
  vlength16.store_unaligned(offsets + 1);
#else
  for (int i = 0; i < 8; ++i) {
    if (lengths[i] > 12) {
      return false;
    }
  }
  gt4 = false;
  for (int i = 0; i < 8; ++i) {
    gt4 = gt4 || lengths[i] > 4;
  }
  offsets[0] = 0;
  for (int i = 0; i < 8; ++i) {
    offsets[i + 1] = offsets[i] + lengths[i];
  }
#endif
  return true;
}

} // namespace

template <bool kScatter, bool kGreaterThan4>
bool SelectiveStringDirectColumnReader::try8ConsecutiveSmall(
    const char* data,
    const uint16_t* offsets,
    int startRow) {
#ifndef NDEBUG
  bool testCoverage[] = {kScatter, kGreaterThan4};
  common::testutil::TestValue::adjust(
      "facebook::velox::dwrf::SelectiveStringDirectColumnReader::try8ConsecutiveSmall",
      testCoverage);
#endif
  auto* result = reinterpret_cast<uint64_t*>(rawValues_);
  // Make sure the iterations are independent with each other.
  for (int i = 0; i < 8; ++i) {
    unsigned j = kScatter ? outerNonNullRows_[startRow + i] : numValues_ + i;
    uint64_t word;
    memcpy(&word, data + offsets[i], 4);
    uint64_t length = offsets[i + 1] - offsets[i];
    if (kGreaterThan4 && length > 4) {
      uint64_t word2;
      memcpy(&word2, data + offsets[i] + 4, 8);
      uint64_t mask = length == 12 ? -1ull : (1ull << (8 * (length - 4))) - 1;
      result[2 * j] = length | (word << 32);
      result[2 * j + 1] = word2 & mask;
    } else {
      uint64_t mask = (1ull << (8 * length)) - 1;
      result[2 * j] = length | ((word & mask) << 32);
      result[2 * j + 1] = 0;
    }
  }
  bufferStart_ = data + offsets[8];
  bytesToSkip_ = 0;
  if constexpr (!kScatter) {
    numValues_ += 8;
  } else {
    numValues_ = outerNonNullRows_[startRow + 7] + 1;
  }
  lengthIndex_ += 8;
  return true;
}

template <bool scatter, bool sparse>
inline bool SelectiveStringDirectColumnReader::try8Consecutive(
    int32_t start,
    const int32_t* rows,
    int32_t row) {
  // If we haven't read in a buffer yet, or there is not enough data left.  This
  // check is important to make sure the subsequent fast path will have enough
  // data to read.
  if (!bufferStart_ ||
      bufferEnd_ - bufferStart_ - bytesToSkip_ < start + 8 * 12) {
    return false;
  }
  const char* data = bufferStart_ + start + bytesToSkip_;
  if constexpr (!sparse) {
    auto* lengths = rawLengths_ + rows[row];
    uint16_t offsets[9];
    bool gt4;
    if (allSmallEnough(lengths, offsets, gt4)) {
      if (gt4) {
        VELOX_DCHECK_LE(data + offsets[7] + 12, bufferEnd_);
        return try8ConsecutiveSmall<scatter, true>(data, offsets, row);
      } else {
        return try8ConsecutiveSmall<scatter, false>(data, offsets, row);
      }
    }
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
    xsimd::make_sized_batch_t<int8_t, 16> first16;
    if (length > 0) {
      first16 = decltype(first16)::load_unaligned(data);
      first16.store_unaligned(
          reinterpret_cast<char*>(result + resultIndex + 1));
    }
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
      VELOX_CHECK_LE(copySize, bufferEnd_ - data - 16);
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
  int32_t current = visitor.start();
  constexpr bool isExtract =
      std::is_same_v<typename TVisitor::FilterType, common::AlwaysTrue> &&
      std::is_same_v<
          typename TVisitor::Extract,
          dwio::common::ExtractToReader<SelectiveStringDirectColumnReader>>;
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
  if (filter == nullptr) {
    readHelper<common::AlwaysTrue, isDense>(
        &dwio::common::alwaysTrue(), rows, extractValues);
    return;
  }

  switch (filter->kind()) {
    case common::FilterKind::kAlwaysTrue:
      readHelper<common::AlwaysTrue, isDense>(filter, rows, extractValues);
      break;
    case common::FilterKind::kIsNull:
      filterNulls<StringView>(
          rows,
          true,
          !std::is_same_v<decltype(extractValues), dwio::common::DropValues>);
      break;
    case common::FilterKind::kIsNotNull:
      if (std::is_same_v<decltype(extractValues), dwio::common::DropValues>) {
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

  auto numRows = rows.back() + 1;
  auto numNulls = nullsInReadRange_
      ? BaseVector::countNulls(nullsInReadRange_, 0, numRows)
      : 0;
  dwio::common::ensureCapacity<int32_t>(
      lengths_, numRows - numNulls, &memoryPool_);
  lengthDecoder_->nextLengths(
      lengths_->asMutable<int32_t>(), numRows - numNulls);
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
    } else {
      if (isDense) {
        processFilter<true>(
            scanSpec_->filter(), rows, dwio::common::ExtractToReader(this));
      } else {
        processFilter<false>(
            scanSpec_->filter(), rows, dwio::common::ExtractToReader(this));
      }
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

  readOffset_ += numRows;
}

} // namespace facebook::velox::dwrf
