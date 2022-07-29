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
using dwio::common::typeutils::CompatChecker;

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
    std::shared_ptr<const dwio::common::TypeWithId> requestedType,
    dwio::common::FormatParams& params,
    velox::common::ScanSpec& scanSpec,
    const TypePtr& type)
    : memoryPool_(params.pool()),
      nodeType_(requestedType),
      formatData_(params.toFormatData(requestedType, scanSpec)),
      scanSpec_(&scanSpec),
      type_{type} {}

std::vector<uint32_t> SelectiveColumnReader::filterRowGroups(
    uint64_t rowGroupSize,
    const dwio::common::StatsContext& context) const {
  return formatData_->filterRowGroups(*scanSpec_, rowGroupSize, context);
}

void SelectiveColumnReader::seekTo(vector_size_t offset, bool readsNullsOnly) {
  if (offset == readOffset_) {
    return;
  }
  if (readOffset_ < offset) {
    if (readsNullsOnly) {
      formatData_->skipNulls(offset - readOffset_);
    } else {
      skip(offset - readOffset_);
    }
    readOffset_ = offset;
  } else {
    VELOX_FAIL("Seeking backward on a ColumnReader");
  }
}

void SelectiveColumnReader::prepareNulls(
    RowSet rows,
    bool hasNulls,
    int32_t extraRows) {
  if (!hasNulls) {
    anyNulls_ = false;
    return;
  }
  auto numRows = rows.size() + extraRows;
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

bool SelectiveColumnReader::readsNullsOnly() const {
  auto filter = scanSpec_->filter();
  if (filter) {
    auto kind = filter->kind();
    return kind == velox::common::FilterKind::kIsNull ||
        (!scanSpec_->keepValues() &&
         kind == velox::common::FilterKind::kIsNotNull);
  }
  return false;
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
  if (!scanState_.filterCache.empty()) {
    simd::memset(
        scanState_.filterCache.data(),
        FilterResult::kUnknown,
        scanState_.filterCache.size());
  }
}

} // namespace facebook::velox::dwio::common
