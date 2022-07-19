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

#include "velox/dwio/dwrf/reader/SelectiveByteRleColumnReader.h"
#include "velox/dwio/dwrf/reader/SelectiveColumnReaderInternal.h"

#include "velox/dwio/dwrf/reader/SelectiveFloatingPointColumnReader.h"
#include "velox/dwio/dwrf/reader/SelectiveIntegerDictionaryColumnReader.h"
#include "velox/dwio/dwrf/reader/SelectiveIntegerDirectColumnReader.h"
#include "velox/dwio/dwrf/reader/SelectiveStringDirectColumnReader.h"

#include "velox/dwio/dwrf/reader/SelectiveStringDictionaryColumnReader.h"
#include "velox/dwio/dwrf/reader/SelectiveTimestampColumnReader.h"

#include "velox/dwio/dwrf/reader/SelectiveRepeatedColumnReader.h"
#include "velox/dwio/dwrf/reader/SelectiveStructColumnReader.h"

namespace facebook::velox::dwrf {

using dwio::common::TypeWithId;
using dwio::common::typeutils::CompatChecker;

// Buffer size for reading length stream
constexpr uint64_t BUFFER_SIZE = 1024;

common::AlwaysTrue& alwaysTrue() {
  static common::AlwaysTrue alwaysTrue;
  return alwaysTrue;
}

dwio::common::NoHook& noHook() {
  static dwio::common::NoHook hook;
  return hook;
}

void ScanState::updateRawState() {
  rawState.dictionary.values =
      dictionary.values ? dictionary.values->as<void>() : nullptr;
  rawState.dictionary.numValues = dictionary.numValues;
  rawState.dictionary2.values =
      dictionary2.values ? dictionary2.values->as<void*>() : nullptr;
  rawState.dictionary2.numValues = dictionary2.numValues;
  rawState.inDictionary = inDictionary ? inDictionary->as<uint64_t>() : nullptr;
  rawState.filterCache = filterCache.data();
}

SelectiveColumnReader::SelectiveColumnReader(
    std::shared_ptr<const dwio::common::TypeWithId> requestedType,
    StripeStreams& stripe,
    common::ScanSpec* scanSpec,
    // TODO: why is data type instead of requested type passed in?
    const TypePtr& type,
    FlatMapContext flatMapContext)
    : notNullDecoder_{},
      nodeType_{requestedType},
      memoryPool_{stripe.getMemoryPool()},
      flatMapContext_{std::move(flatMapContext)},
      scanSpec_(scanSpec),
      type_{type},
      rowsPerRowGroup_{stripe.rowsPerRowGroup()} {
  EncodingKey encodingKey{nodeType_->id, flatMapContext_.sequence};

  std::unique_ptr<dwio::common::SeekableInputStream> stream =
      stripe.getStream(encodingKey.forKind(proto::Stream_Kind_PRESENT), false);
  if (stream) {
    notNullDecoder_ = createBooleanRleDecoder(std::move(stream), encodingKey);
  }

  // We always initialize indexStream_ because indices are needed as
  // soon as there is a single filter that can trigger row group skips
  // anywhere in the reader tree. This is not known at construct time
  // because the first filter can come from a hash join or other run
  // time pushdown.
  indexStream_ = stripe.getStream(
      encodingKey.forKind(proto::Stream_Kind_ROW_INDEX), false);
}

std::vector<uint32_t> SelectiveColumnReader::filterRowGroups(
    uint64_t rowGroupSize,
    const StatsContext& context) const {
  if ((!index_ && !indexStream_) || !scanSpec_->filter()) {
    static const std::vector<uint32_t> kEmpty;
    return kEmpty;
  }

  ensureRowGroupIndex();
  auto filter = scanSpec_->filter();

  std::vector<uint32_t> stridesToSkip;
  for (auto i = 0; i < index_->entry_size(); i++) {
    const auto& entry = index_->entry(i);
    auto columnStats =
        buildColumnStatisticsFromProto(entry.statistics(), context);
    if (!testFilter(filter, columnStats.get(), rowGroupSize, type_)) {
      VLOG(1) << "Drop stride " << i << " on " << scanSpec_->toString();
      stridesToSkip.push_back(i); // Skipping stride based on column stats.
    }
  }
  return stridesToSkip;
}

uint64_t SelectiveColumnReader::skip(uint64_t numValues) {
  if (notNullDecoder_) {
    // page through the values that we want to skip
    // and count how many are non-null
    std::array<char, BUFFER_SIZE> buffer;
    constexpr auto bitCount = BUFFER_SIZE * 8;
    uint64_t remaining = numValues;
    while (remaining > 0) {
      uint64_t chunkSize = std::min(remaining, bitCount);
      notNullDecoder_->next(buffer.data(), chunkSize, nullptr);
      remaining -= chunkSize;
      numValues -= bits::countNulls(
          reinterpret_cast<uint64_t*>(buffer.data()), 0, chunkSize);
    }
  }
  return numValues;
}

void SelectiveColumnReader::seekTo(vector_size_t offset, bool readsNullsOnly) {
  if (offset == readOffset_) {
    return;
  }
  if (readOffset_ < offset) {
    if (readsNullsOnly) {
      SelectiveColumnReader::skip(offset - readOffset_);
    } else {
      skip(offset - readOffset_);
    }
    readOffset_ = offset;
  } else {
    VELOX_FAIL("Seeking backward on a ColumnReader");
  }
}

void SelectiveColumnReader::prepareNulls(RowSet rows, bool hasNulls) {
  if (!hasNulls) {
    anyNulls_ = false;
    return;
  }
  auto numRows = rows.size();
  if (useBulkPath()) {
    bool isDense = rows.back() == rows.size() - 1;
    if (!scanSpec_->filter()) {
      anyNulls_ = nullsInReadRange_ != nullptr;
      returnReaderNulls_ = anyNulls_ && isDense;
      // No need for null flags if fast path
      if (returnReaderNulls_) {
        return;
      }
    }
  }
  if (resultNulls_ && resultNulls_->unique() &&
      resultNulls_->capacity() >= bits::nbytes(numRows) + simd::kPadding) {
    // Clear whole capacity because future uses could hit
    // uncleared data between capacity() and 'numBytes'.
    simd::memset(rawResultNulls_, bits::kNotNullByte, resultNulls_->capacity());
    anyNulls_ = false;
    return;
  }

  anyNulls_ = false;
  resultNulls_ = AlignedBuffer::allocate<bool>(
      numRows + (simd::kPadding * 8), &memoryPool_);
  rawResultNulls_ = resultNulls_->asMutable<uint64_t>();
  simd::memset(rawResultNulls_, bits::kNotNullByte, resultNulls_->capacity());
}

bool SelectiveColumnReader::shouldMoveNulls(RowSet rows) {
  if (rows.size() == numValues_) {
    // Nulls will only be moved if there is a selection on values. A cast
    // alone does not move nulls.
    return false;
  }
  VELOX_CHECK(
      !returnReaderNulls_,
      "Do not return reader nulls if retrieving a subset of values");
  if (anyNulls_) {
    VELOX_CHECK(
        resultNulls_ && resultNulls_->as<uint64_t>() == rawResultNulls_);
    VELOX_CHECK_GT(resultNulls_->capacity() * 8, rows.size());
    return true;
  }
  return false;
}

void SelectiveColumnReader::readNulls(
    vector_size_t numValues,
    const uint64_t* incomingNulls,
    VectorPtr* result,
    BufferPtr& nulls) {
  if (!notNullDecoder_ && !incomingNulls) {
    nulls = nullptr;
    if (result && *result) {
      (*result)->resetNulls();
    }
    return;
  }
  auto numBytes = bits::nbytes(numValues);
  if (result && *result) {
    nulls = (*result)->mutableNulls(numValues + (simd::kPadding * 8));
    detail::resetIfNotWritable(*result, nulls);
  }
  if (!nulls || nulls->capacity() < numBytes + simd::kPadding) {
    nulls =
        AlignedBuffer::allocate<char>(numBytes + simd::kPadding, &memoryPool_);
  }
  nulls->setSize(numBytes);
  auto* nullsPtr = nulls->asMutable<uint64_t>();
  if (!notNullDecoder_) {
    memcpy(nullsPtr, incomingNulls, numBytes);
    return;
  }
  memset(nullsPtr, bits::kNotNullByte, numBytes);
  notNullDecoder_->next(
      reinterpret_cast<char*>(nullsPtr), numValues, incomingNulls);
}

void SelectiveColumnReader::getIntValues(
    RowSet rows,
    const Type* requestedType,
    VectorPtr* result) {
  switch (requestedType->kind()) {
    case TypeKind::SMALLINT: {
      switch (valueSize_) {
        case 8:
          getFlatValues<int64_t, int16_t>(rows, result);
          break;
        case 4:
          getFlatValues<int32_t, int16_t>(rows, result);
          break;
        case 2:
          getFlatValues<int16_t, int16_t>(rows, result);
          break;
        default:
          VELOX_FAIL("Unsupported value size");
      }
      break;
      case TypeKind::INTEGER:
        switch (valueSize_) {
          case 8:
            getFlatValues<int64_t, int32_t>(rows, result);
            break;
          case 4:
            getFlatValues<int32_t, int32_t>(rows, result);
            break;
          case 2:
            getFlatValues<int16_t, int32_t>(rows, result);
            break;
          default:
            VELOX_FAIL("Unsupported value size");
        }
        break;
      case TypeKind::BIGINT:
        switch (valueSize_) {
          case 8:
            getFlatValues<int64_t, int64_t>(rows, result);
            break;
          case 4:
            getFlatValues<int32_t, int64_t>(rows, result);
            break;
          case 2:
            getFlatValues<int16_t, int64_t>(rows, result);
            break;
          default:
            VELOX_FAIL("Unsupported value size");
        }
        break;
      default:
        VELOX_FAIL(
            "Not a valid type for integer reader: {}",
            requestedType->toString());
    }
  }
}

template <>
void SelectiveColumnReader::getFlatValues<int8_t, bool>(
    RowSet rows,
    VectorPtr* result,
    const TypePtr& type,
    bool isFinal) {
  constexpr int32_t kWidth = xsimd::batch<int8_t>::size;
  VELOX_CHECK_EQ(valueSize_, sizeof(int8_t));
  compactScalarValues<int8_t, int8_t>(rows, isFinal);
  auto boolValues =
      AlignedBuffer::allocate<bool>(numValues_, &memoryPool_, false);
  auto rawBytes = values_->as<int8_t>();
  auto zero = xsimd::broadcast<int8_t>(0);
  if constexpr (kWidth == 32) {
    auto rawBits = boolValues->asMutable<uint32_t>();
    for (auto i = 0; i < numValues_; i += kWidth) {
      rawBits[i / kWidth] =
          ~simd::toBitMask(zero == xsimd::load_unaligned(rawBytes + i));
    }
  } else {
    VELOX_DCHECK_EQ(kWidth, 16);
    auto rawBits = boolValues->asMutable<uint16_t>();
    for (auto i = 0; i < numValues_; i += kWidth) {
      rawBits[i / kWidth] =
          ~simd::toBitMask(zero == xsimd::load_unaligned(rawBytes + i));
    }
  }
  BufferPtr nulls = anyNulls_
      ? (returnReaderNulls_ ? nullsInReadRange_ : resultNulls_)
      : nullptr;
  *result = std::make_shared<FlatVector<bool>>(
      &memoryPool_,
      type,
      nulls,
      numValues_,
      std::move(boolValues),
      std::move(stringBuffers_));
}

template <>
void SelectiveColumnReader::compactScalarValues<bool, bool>(
    RowSet rows,
    bool isFinal) {
  if (!values_ || rows.size() == numValues_) {
    if (values_) {
      values_->setSize(bits::nbytes(numValues_));
    }
    return;
  }
  auto rawBits = reinterpret_cast<uint64_t*>(rawValues_);
  vector_size_t rowIndex = 0;
  auto nextRow = rows[rowIndex];
  bool moveNulls = shouldMoveNulls(rows);
  for (size_t i = 0; i < numValues_; i++) {
    if (outputRows_[i] < nextRow) {
      continue;
    }

    VELOX_DCHECK(outputRows_[i] == nextRow);

    bits::setBit(rawBits, rowIndex, bits::isBitSet(rawBits, i));
    if (moveNulls && rowIndex != i) {
      bits::setBit(
          rawResultNulls_, rowIndex, bits::isBitSet(rawResultNulls_, i));
    }
    if (!isFinal) {
      outputRows_[rowIndex] = nextRow;
    }
    rowIndex++;
    if (rowIndex >= rows.size()) {
      break;
    }
    nextRow = rows[rowIndex];
  }
  numValues_ = rows.size();
  outputRows_.resize(numValues_);
  values_->setSize(bits::nbytes(numValues_));
}

char* SelectiveColumnReader::copyStringValue(folly::StringPiece value) {
  uint64_t size = value.size();
  if (stringBuffers_.empty() || rawStringUsed_ + size > rawStringSize_) {
    if (!stringBuffers_.empty()) {
      stringBuffers_.back()->setSize(rawStringUsed_);
    }
    auto bytes = std::max(size, kStringBufferSize);
    BufferPtr buffer = AlignedBuffer::allocate<char>(bytes, &memoryPool_);
    stringBuffers_.push_back(buffer);
    rawStringBuffer_ = buffer->asMutable<char>();
    rawStringUsed_ = 0;
    // Adjust the size downward so that the last store can take place
    // at full width.
    rawStringSize_ = buffer->capacity() - simd::kPadding;
  }
  memcpy(rawStringBuffer_ + rawStringUsed_, value.data(), size);
  auto start = rawStringUsed_;
  rawStringUsed_ += size;
  return rawStringBuffer_ + start;
}

void SelectiveColumnReader::addStringValue(folly::StringPiece value) {
  auto copy = copyStringValue(value);
  reinterpret_cast<StringView*>(rawValues_)[numValues_++] =
      StringView(copy, value.size());
}

void SelectiveColumnReader::resetFilterCaches() {
  if (!scanState_.filterCache.empty()) {
    simd::memset(
        scanState_.filterCache.data(),
        FilterResult::kUnknown,
        scanState_.filterCache.size());
  }
}

std::vector<uint64_t> toPositions(const proto::RowIndexEntry& entry) {
  return std::vector<uint64_t>(
      entry.positions().begin(), entry.positions().end());
}

std::unique_ptr<SelectiveColumnReader> buildIntegerReader(
    const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
    FlatMapContext flatMapContext,
    const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
    StripeStreams& stripe,
    uint32_t numBytes,
    common::ScanSpec* scanSpec) {
  EncodingKey ek{requestedType->id, flatMapContext.sequence};
  switch (static_cast<int64_t>(stripe.getEncoding(ek).kind())) {
    case proto::ColumnEncoding_Kind_DICTIONARY:
      return std::make_unique<SelectiveIntegerDictionaryColumnReader>(
          requestedType, dataType, stripe, scanSpec, numBytes);
    case proto::ColumnEncoding_Kind_DIRECT:
      return std::make_unique<SelectiveIntegerDirectColumnReader>(
          requestedType, dataType, stripe, numBytes, scanSpec);
    default:
      DWIO_RAISE("buildReader unhandled integer encoding");
  }
}

std::unique_ptr<SelectiveColumnReader> SelectiveColumnReader::build(
    const std::shared_ptr<const TypeWithId>& requestedType,
    const std::shared_ptr<const TypeWithId>& dataType,
    StripeStreams& stripe,
    common::ScanSpec* scanSpec,
    FlatMapContext flatMapContext) {
  CompatChecker::check(*dataType->type, *requestedType->type);
  EncodingKey ek{dataType->id, flatMapContext.sequence};

  switch (dataType->type->kind()) {
    case TypeKind::INTEGER:
      return buildIntegerReader(
          requestedType,
          std::move(flatMapContext),
          dataType,
          stripe,
          dwio::common::INT_BYTE_SIZE,
          scanSpec);
    case TypeKind::BIGINT:
      return buildIntegerReader(
          requestedType,
          std::move(flatMapContext),
          dataType,
          stripe,
          dwio::common::LONG_BYTE_SIZE,
          scanSpec);
    case TypeKind::SMALLINT:
      return buildIntegerReader(
          requestedType,
          std::move(flatMapContext),
          dataType,
          stripe,
          dwio::common::SHORT_BYTE_SIZE,
          scanSpec);
    case TypeKind::ARRAY:
      return std::make_unique<SelectiveListColumnReader>(
          requestedType, dataType, stripe, scanSpec, flatMapContext);
    case TypeKind::MAP:
      if (stripe.getEncoding(ek).kind() ==
          proto::ColumnEncoding_Kind_MAP_FLAT) {
        VELOX_UNSUPPORTED("SelectiveColumnReader does not support flat maps");
      }
      return std::make_unique<SelectiveMapColumnReader>(
          requestedType, dataType, stripe, scanSpec, std::move(flatMapContext));
    case TypeKind::REAL:
      if (requestedType->type->kind() == TypeKind::REAL) {
        return std::make_unique<
            SelectiveFloatingPointColumnReader<float, float>>(
            requestedType, stripe, scanSpec, std::move(flatMapContext));
      } else {
        return std::make_unique<
            SelectiveFloatingPointColumnReader<float, double>>(
            requestedType, stripe, scanSpec, std::move(flatMapContext));
      }
    case TypeKind::DOUBLE:
      return std::make_unique<
          SelectiveFloatingPointColumnReader<double, double>>(
          requestedType, stripe, scanSpec, std::move(flatMapContext));
    case TypeKind::ROW:
      return std::make_unique<SelectiveStructColumnReader>(
          requestedType, dataType, stripe, scanSpec, std::move(flatMapContext));
    case TypeKind::BOOLEAN:
      return std::make_unique<SelectiveByteRleColumnReader>(
          requestedType,
          dataType,
          stripe,
          scanSpec,
          true,
          std::move(flatMapContext));
    case TypeKind::TINYINT:
      return std::make_unique<SelectiveByteRleColumnReader>(
          requestedType,
          dataType,
          stripe,
          scanSpec,
          false,
          std::move(flatMapContext));
    case TypeKind::VARBINARY:
    case TypeKind::VARCHAR:
      switch (static_cast<int64_t>(stripe.getEncoding(ek).kind())) {
        case proto::ColumnEncoding_Kind_DIRECT:
          return std::make_unique<SelectiveStringDirectColumnReader>(
              requestedType, stripe, scanSpec, std::move(flatMapContext));
        case proto::ColumnEncoding_Kind_DICTIONARY:
          return std::make_unique<SelectiveStringDictionaryColumnReader>(
              requestedType, stripe, scanSpec, std::move(flatMapContext));
        default:
          DWIO_RAISE("buildReader string unknown encoding");
      }
    case TypeKind::TIMESTAMP:
      return std::make_unique<SelectiveTimestampColumnReader>(
          requestedType, stripe, scanSpec, std::move(flatMapContext));
    default:
      DWIO_RAISE(
          "buildReader unhandled type: " +
          mapTypeKindToName(dataType->type->kind()));
  }
}

} // namespace facebook::velox::dwrf
