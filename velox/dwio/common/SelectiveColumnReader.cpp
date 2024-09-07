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

#include "velox/dwio/common/SelectiveColumnReaderInternal.h"

namespace facebook::velox::dwio::common {

using dwio::common::TypeWithId;

velox::common::AlwaysTrue& alwaysTrue() {
  static velox::common::AlwaysTrue alwaysTrue;
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
    const TypePtr& requestedType,
    std::shared_ptr<const dwio::common::TypeWithId> fileType,
    dwio::common::FormatParams& params,
    velox::common::ScanSpec& scanSpec)
    : memoryPool_(&params.pool()),
      requestedType_(requestedType),
      fileType_(fileType),
      formatData_(params.toFormatData(fileType, scanSpec)),
      scanSpec_(&scanSpec) {}

void SelectiveColumnReader::filterRowGroups(
    uint64_t rowGroupSize,
    const dwio::common::StatsContext& context,
    FormatData::FilterRowGroupsResult& result) const {
  formatData_->filterRowGroups(*scanSpec_, rowGroupSize, context, result);
}

const std::vector<SelectiveColumnReader*>& SelectiveColumnReader::children()
    const {
  static std::vector<SelectiveColumnReader*> empty;
  return empty;
}

void SelectiveColumnReader::seekTo(vector_size_t offset, bool readsNullsOnly) {
  if (offset == readOffset_) {
    return;
  }
  if (readOffset_ < offset) {
    if (numParentNulls_ > 0) {
      VELOX_CHECK_LE(
          parentNullsRecordedTo_,
          offset,
          "Must not seek to before parentNullsRecordedTo_");
    }
    const auto distance = offset - readOffset_ - numParentNulls_;
    numParentNulls_ = 0;
    parentNullsRecordedTo_ = 0;
    if (readsNullsOnly) {
      formatData_->skipNulls(distance, true);
    } else {
      skip(distance);
    }
    readOffset_ = offset;
  } else {
    VELOX_FAIL(
        "Seeking backward on a ColumnReader from {} to {}",
        readOffset_,
        offset);
  }
}

void SelectiveColumnReader::initReturnReaderNulls(const RowSet& rows) {
  if (useBulkPath() && !scanSpec_->hasFilter()) {
    anyNulls_ = nullsInReadRange_ != nullptr;
    const bool isDense = rows.back() == rows.size() - 1;
    returnReaderNulls_ = anyNulls_ && isDense;
  } else {
    returnReaderNulls_ = false;
  }
}

void SelectiveColumnReader::prepareNulls(
    const RowSet& rows,
    bool hasNulls,
    int32_t extraRows) {
  if (!hasNulls) {
    anyNulls_ = false;
    return;
  }

  initReturnReaderNulls(rows);
  if (returnReaderNulls_) {
    // No need for null flags if fast path.
    return;
  }

  const auto numRows = rows.size() + extraRows;
  if (resultNulls_ && resultNulls_->unique() &&
      resultNulls_->capacity() >= bits::nbytes(numRows) + simd::kPadding) {
    resultNulls_->setSize(bits::nbytes(numRows));
  } else {
    resultNulls_ = AlignedBuffer::allocate<bool>(
        numRows + (simd::kPadding * 8), memoryPool_);
    rawResultNulls_ = resultNulls_->asMutable<uint64_t>();
  }
  anyNulls_ = false;
  // Clear whole capacity because future uses could hit uncleared data between
  // capacity() and 'numBytes'.
  simd::memset(rawResultNulls_, bits::kNotNullByte, resultNulls_->capacity());
}

const uint64_t* SelectiveColumnReader::shouldMoveNulls(const RowSet& rows) {
  if (rows.size() == numValues_ || !anyNulls_) {
    // Nulls will only be moved if there is a selection on values. A cast
    // alone does not move nulls.
    return nullptr;
  }
  const uint64_t* moveFrom = rawResultNulls_;
  if (returnReaderNulls_) {
    if (!(resultNulls_ && resultNulls_->unique() &&
          resultNulls_->capacity() >= rows.size() + simd::kPadding)) {
      resultNulls_ = AlignedBuffer::allocate<bool>(
          rows.size() + (simd::kPadding * 8), memoryPool_);
      rawResultNulls_ = resultNulls_->asMutable<uint64_t>();
    }
    moveFrom = nullsInReadRange_->as<uint64_t>();
    bits::copyBits(moveFrom, 0, rawResultNulls_, 0, rows.size());
    returnReaderNulls_ = false;
  }
  VELOX_CHECK(resultNulls_ && resultNulls_->as<uint64_t>() == rawResultNulls_);
  VELOX_CHECK_GT(resultNulls_->capacity() * 8, rows.size());
  return moveFrom;
}

void SelectiveColumnReader::setComplexNulls(
    const RowSet& rows,
    VectorPtr& result) const {
  if (!nullsInReadRange_) {
    if (result->isNullsWritable()) {
      result->clearNulls(0, rows.size());
    } else {
      result->resetNulls();
    }
    return;
  }

  const bool dense = 1 + rows.back() == rows.size();
  auto& nulls = result->nulls();
  if (dense &&
      !(nulls && nulls->isMutable() &&
        nulls->capacity() >= bits::nbytes(rows.size()))) {
    result->setNulls(nullsInReadRange_);
    return;
  }

  auto* readerNulls = nullsInReadRange_->as<uint64_t>();
  auto* resultNulls = result->mutableNulls(rows.size())->asMutable<uint64_t>();
  if (dense) {
    bits::copyBits(readerNulls, 0, resultNulls, 0, rows.size());
    return;
  }
  for (vector_size_t i = 0; i < rows.size(); ++i) {
    bits::setBit(resultNulls, i, bits::isBitSet(readerNulls, rows[i]));
  }
}

void SelectiveColumnReader::getIntValues(
    const RowSet& rows,
    const TypePtr& requestedType,
    VectorPtr* result) {
  switch (requestedType->kind()) {
    case TypeKind::SMALLINT:
      switch (valueSize_) {
        case 8:
          getFlatValues<int64_t, int16_t>(rows, result, requestedType);
          break;
        case 4:
          getFlatValues<int32_t, int16_t>(rows, result, requestedType);
          break;
        case 2:
          getFlatValues<int16_t, int16_t>(rows, result, requestedType);
          break;
        default:
          VELOX_FAIL("Unsupported value size: {}", valueSize_);
      }
      break;
    case TypeKind::TINYINT:
      switch (valueSize_) {
        case 4:
          getFlatValues<int32_t, int8_t>(rows, result, requestedType);
          break;
        case 2:
          getFlatValues<int16_t, int8_t>(rows, result, requestedType);
          break;
        default:
          VELOX_FAIL("Unsupported value size: {}", valueSize_);
      }
      break;
    case TypeKind::INTEGER:
      switch (valueSize_) {
        case 8:
          getFlatValues<int64_t, int32_t>(rows, result, requestedType);
          break;
        case 4:
          getFlatValues<int32_t, int32_t>(rows, result, requestedType);
          break;
        case 2:
          getFlatValues<int16_t, int32_t>(rows, result, requestedType);
          break;
        default:
          VELOX_FAIL("Unsupported value size: {}", valueSize_);
      }
      break;
    case TypeKind::HUGEINT:
      getFlatValues<int128_t, int128_t>(rows, result, requestedType);
      break;
    case TypeKind::BIGINT:
      switch (valueSize_) {
        case 8:
          getFlatValues<int64_t, int64_t>(rows, result, requestedType);
          break;
        case 4:
          getFlatValues<int32_t, int64_t>(rows, result, requestedType);
          break;
        case 2:
          getFlatValues<int16_t, int64_t>(rows, result, requestedType);
          break;
        default:
          VELOX_FAIL("Unsupported value size: {}", valueSize_);
      }
      break;
    default:
      VELOX_FAIL(
          "Not a valid type for integer reader: {}", requestedType->toString());
  }
}

void SelectiveColumnReader::getUnsignedIntValues(
    const RowSet& rows,
    const TypePtr& requestedType,
    VectorPtr* result) {
  switch (requestedType->kind()) {
    case TypeKind::TINYINT:
      switch (valueSize_) {
        case 1:
          getFlatValues<uint8_t, uint8_t>(rows, result, requestedType);
          break;
        case 4:
          getFlatValues<uint32_t, uint8_t>(rows, result, requestedType);
          break;
        default:
          VELOX_FAIL("Unsupported value size: {}", valueSize_);
      }
      break;
    case TypeKind::SMALLINT:
      switch (valueSize_) {
        case 2:
          getFlatValues<uint16_t, uint16_t>(rows, result, requestedType);
          break;
        case 4:
          getFlatValues<uint32_t, uint16_t>(rows, result, requestedType);
          break;
        default:
          VELOX_FAIL("Unsupported value size: {}", valueSize_);
      }
      break;
    case TypeKind::INTEGER:
      switch (valueSize_) {
        case 4:
          getFlatValues<uint32_t, uint32_t>(rows, result, requestedType);
          break;
        default:
          VELOX_FAIL("Unsupported value size: {}", valueSize_);
      }
      break;
    case TypeKind::BIGINT:
      switch (valueSize_) {
        case 4:
          getFlatValues<uint32_t, uint64_t>(rows, result, requestedType);
          break;
        case 8:
          getFlatValues<uint64_t, uint64_t>(rows, result, requestedType);
          break;
        default:
          VELOX_FAIL("Unsupported value size: {}", valueSize_);
      }
      break;
    case TypeKind::HUGEINT:
      switch (valueSize_) {
        case 8:
          getFlatValues<uint64_t, uint128_t>(rows, result, requestedType);
          break;
        case 16:
          getFlatValues<uint128_t, uint128_t>(rows, result, requestedType);
          break;
        default:
          VELOX_FAIL("Unsupported value size: {}", valueSize_);
      }
      break;
    default:
      VELOX_FAIL(
          "Not a valid type for unsigned integer reader: {}",
          requestedType->toString());
  }
}

template <>
void SelectiveColumnReader::getFlatValues<int8_t, bool>(
    const RowSet& rows,
    VectorPtr* result,
    const TypePtr& type,
    bool isFinal) {
  constexpr int32_t kWidth = xsimd::batch<int8_t>::size;
  VELOX_CHECK_EQ(valueSize_, sizeof(int8_t));
  compactScalarValues<int8_t, int8_t>(rows, isFinal);
  auto boolValues =
      AlignedBuffer::allocate<bool>(numValues_, memoryPool_, false);
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
  *result = std::make_shared<FlatVector<bool>>(
      memoryPool_,
      type,
      resultNulls(),
      numValues_,
      std::move(boolValues),
      std::move(stringBuffers_));
}

template <>
void SelectiveColumnReader::compactScalarValues<bool, bool>(
    const RowSet& rows,
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
  auto* moveNullsFrom = shouldMoveNulls(rows);
  for (size_t i = 0; i < numValues_; i++) {
    if (outputRows_[i] < nextRow) {
      continue;
    }

    VELOX_DCHECK_EQ(outputRows_[i], nextRow);

    bits::setBit(rawBits, rowIndex, bits::isBitSet(rawBits, i));
    if (moveNullsFrom && rowIndex != i) {
      bits::setBit(rawResultNulls_, rowIndex, bits::isBitSet(moveNullsFrom, i));
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
    auto bytes = std::max(size, kStringBufferSize);
    BufferPtr buffer = AlignedBuffer::allocate<char>(bytes, memoryPool_);
    // Use the preferred size instead of the requested one to improve memory
    // efficiency.
    buffer->setSize(buffer->capacity());
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

void SelectiveColumnReader::setNulls(BufferPtr resultNulls) {
  resultNulls_ = resultNulls;
  rawResultNulls_ = resultNulls ? resultNulls->asMutable<uint64_t>() : nullptr;
  anyNulls_ = rawResultNulls_ &&
      !bits::isAllSet(rawResultNulls_, 0, numValues_, bits::kNotNull);
  allNull_ =
      anyNulls_ && bits::isAllSet(rawResultNulls_, 0, numValues_, bits::kNull);
  returnReaderNulls_ = false;
}

void SelectiveColumnReader::resetFilterCaches() {
  if (scanState_.filterCache.empty() && scanSpec_->hasFilter()) {
    scanState_.filterCache.resize(std::max<int32_t>(
        1, scanState_.dictionary.numValues + scanState_.dictionary2.numValues));
    scanState_.updateRawState();
  }
  if (!scanState_.filterCache.empty()) {
    simd::memset(
        scanState_.filterCache.data(),
        FilterResult::kUnknown,
        scanState_.filterCache.size());
  }
}

void SelectiveColumnReader::addParentNulls(
    int32_t firstRowInNulls,
    const uint64_t* nulls,
    const RowSet& rows) {
  const int32_t firstNullIndex =
      readOffset_ < firstRowInNulls ? 0 : readOffset_ - firstRowInNulls;
  numParentNulls_ +=
      nulls ? bits::countNulls(nulls, firstNullIndex, rows.back() + 1) : 0;
  parentNullsRecordedTo_ = firstRowInNulls + rows.back() + 1;
}

void SelectiveColumnReader::addSkippedParentNulls(
    vector_size_t from,
    vector_size_t to,
    int32_t numNulls) {
  auto rowsPerRowGroup = formatData_->rowsPerRowGroup();
  if (rowsPerRowGroup.has_value() &&
      from / rowsPerRowGroup.value() >
          parentNullsRecordedTo_ / rowsPerRowGroup.value()) {
    // the new nulls are in a different row group than the last.
    parentNullsRecordedTo_ = from;
    numParentNulls_ = 0;
  }
  if (parentNullsRecordedTo_) {
    VELOX_CHECK_EQ(parentNullsRecordedTo_, from);
  }
  numParentNulls_ += numNulls;
  parentNullsRecordedTo_ = to;
}

} // namespace facebook::velox::dwio::common
