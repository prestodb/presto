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

#include "velox/dwio/common/SelectiveRepeatedColumnReader.h"

#include "velox/dwio/common/BufferUtil.h"
#include "velox/dwio/common/SelectiveColumnReaderInternal.h"

namespace facebook::velox::dwio::common {

namespace {

int sumLengths(
    const int32_t* lengths,
    const uint64_t* nulls,
    int first,
    int last) {
  int sum = 0;
  if (!nulls) {
    for (auto i = first; i < last; ++i) {
      sum += lengths[i];
    }
  } else if (last - first < 64) {
    bits::forEachSetBit(nulls, first, last, [&](int i) { sum += lengths[i]; });
  } else {
    xsimd::batch<int32_t> sums{};
    static_assert(sums.size <= 64);
    auto submask = bits::lowMask(sums.size);
    bits::forEachWord(first, last, [&](int i, uint64_t mask) {
      mask &= nulls[i];
      for (int j = 0; j < 64 && mask; j += sums.size) {
        if (auto m = (mask >> j) & submask) {
          auto selected = simd::fromBitMask<int32_t>(m);
          sums += simd::maskLoad(&lengths[i * 64 + j], selected);
        }
      }
    });
    sum = xsimd::reduce_add(sums);
  }
  return sum;
}

void prepareResult(
    VectorPtr& result,
    const TypePtr& type,
    vector_size_t size,
    memory::MemoryPool* pool) {
  if (!(result &&
        ((type->kind() == TypeKind::ARRAY &&
          result->encoding() == VectorEncoding::Simple::ARRAY) ||
         (type->kind() == TypeKind::MAP &&
          result->encoding() == VectorEncoding::Simple::MAP)) &&
        result.unique())) {
    VLOG(1) << "Reallocating result " << type->kind() << " vector of size "
            << size;
    result = BaseVector::create(type, size, pool);
    return;
  }
  result->resetDataDependentFlags(nullptr);
  result->resize(size);
  // Nulls are handled in getValues calls.  Offsets and sizes are handled in
  // makeOffsetsAndSizes.  Child vectors are handled in child column readers.
}

vector_size_t
advanceNestedRows(const RowSet& rows, vector_size_t i, vector_size_t last) {
  while (i + 16 < rows.size() && rows[i + 16] < last) {
    i += 16;
  }
  while (i < rows.size() && rows[i] < last) {
    ++i;
  }
  return i;
}

} // namespace

void SelectiveRepeatedColumnReader::makeNestedRowSet(
    RowSet rows,
    int32_t maxRow) {
  if (!allLengthsHolder_ ||
      allLengthsHolder_->capacity() < (maxRow + 1) * sizeof(vector_size_t)) {
    allLengthsHolder_ = allocateIndices(maxRow + 1, &memoryPool_);
    allLengths_ = allLengthsHolder_->asMutable<vector_size_t>();
  }
  auto nulls = nullsInReadRange_ ? nullsInReadRange_->as<uint64_t>() : nullptr;
  // Reads the lengths, leaves an uninitialized gap for a null
  // map/list. Reading these checks the null mask.
  readLengths(allLengths_, maxRow + 1, nulls);
  vector_size_t nestedLength = 0;
  for (auto row : rows) {
    if (!nulls || !bits::isBitNull(nulls, row)) {
      nestedLength +=
          std::min(scanSpec_->maxArrayElementsCount(), allLengths_[row]);
    }
  }
  nestedRowsHolder_.resize(nestedLength);
  vector_size_t currentRow = 0;
  vector_size_t nestedRow = 0;
  vector_size_t nestedOffset = 0;
  for (auto rowIndex = 0; rowIndex < rows.size(); ++rowIndex) {
    auto row = rows[rowIndex];
    // Add up the lengths of non-null rows skipped since the last
    // non-null.
    nestedOffset += sumLengths(allLengths_, nulls, currentRow, row);
    currentRow = row + 1;
    if (nulls && bits::isBitNull(nulls, row)) {
      continue;
    }
    auto lengthAtRow =
        std::min(scanSpec_->maxArrayElementsCount(), allLengths_[row]);
    std::iota(
        nestedRowsHolder_.data() + nestedRow,
        nestedRowsHolder_.data() + nestedRow + lengthAtRow,
        nestedOffset);
    nestedRow += lengthAtRow;
    nestedOffset += allLengths_[row];
  }
  nestedOffset += sumLengths(allLengths_, nulls, currentRow, maxRow + 1);
  childTargetReadOffset_ += nestedOffset;
  nestedRows_ = nestedRowsHolder_;
}

void SelectiveRepeatedColumnReader::makeOffsetsAndSizes(
    RowSet rows,
    ArrayVectorBase& result) {
  auto* rawOffsets =
      result.mutableOffsets(rows.size())->asMutable<vector_size_t>();
  auto* rawSizes = result.mutableSizes(rows.size())->asMutable<vector_size_t>();
  auto* nulls = nullsInReadRange_ ? nullsInReadRange_->as<uint64_t>() : nullptr;
  vector_size_t currentRow = 0;
  vector_size_t currentOffset = 0;
  vector_size_t nestedRowIndex = 0;
  for (int i = 0; i < rows.size(); ++i) {
    auto row = rows[i];
    currentOffset += sumLengths(allLengths_, nulls, currentRow, row);
    currentRow = row + 1;
    nestedRowIndex =
        advanceNestedRows(nestedRows_, nestedRowIndex, currentOffset);
    rawOffsets[i] = nestedRowIndex;
    if (nulls && bits::isBitNull(nulls, row)) {
      rawSizes[i] = 0;
      if (!returnReaderNulls_) {
        bits::setNull(rawResultNulls_, i);
      }
      anyNulls_ = true;
    } else {
      currentOffset += allLengths_[row];
      auto newNestedRowIndex =
          advanceNestedRows(nestedRows_, nestedRowIndex, currentOffset);
      rawSizes[i] = newNestedRowIndex - nestedRowIndex;
      nestedRowIndex = newNestedRowIndex;
    }
  }
  numValues_ = rows.size();
}

RowSet SelectiveRepeatedColumnReader::applyFilter(RowSet rows) {
  if (!scanSpec_->filter()) {
    return rows;
  }
  switch (scanSpec_->filter()->kind()) {
    case velox::common::FilterKind::kIsNull:
      filterNulls<int32_t>(rows, true, false);
      break;
    case velox::common::FilterKind::kIsNotNull:
      filterNulls<int32_t>(rows, false, false);
      break;
    default:
      VELOX_UNSUPPORTED(
          "Unsupported filter for column {}, only IS NULL and IS NOT NULL are supported: {}",
          scanSpec_->fieldName(),
          scanSpec_->filter()->toString());
  }
  return outputRows_;
}

SelectiveListColumnReader::SelectiveListColumnReader(
    const TypePtr& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    FormatParams& params,
    velox::common::ScanSpec& scanSpec)
    : SelectiveRepeatedColumnReader(requestedType, params, scanSpec, fileType) {
}

uint64_t SelectiveListColumnReader::skip(uint64_t numValues) {
  numValues = formatData_->skipNulls(numValues);
  if (child_) {
    std::array<int32_t, kBufferSize> buffer;
    uint64_t childElements = 0;
    uint64_t lengthsRead = 0;
    while (lengthsRead < numValues) {
      uint64_t chunk =
          std::min(numValues - lengthsRead, static_cast<uint64_t>(kBufferSize));
      readLengths(buffer.data(), chunk, nullptr);
      for (size_t i = 0; i < chunk; ++i) {
        childElements += static_cast<size_t>(buffer[i]);
      }
      lengthsRead += chunk;
    }
    child_->seekTo(child_->readOffset() + childElements, false);
    childTargetReadOffset_ += childElements;
  } else {
    VELOX_FAIL("Repeated reader with no children");
  }
  return numValues;
}

void SelectiveListColumnReader::read(
    vector_size_t offset,
    RowSet rows,
    const uint64_t* incomingNulls) {
  // Catch up if the child is behind the length stream.
  child_->seekTo(childTargetReadOffset_, false);
  prepareRead<char>(offset, rows, incomingNulls);
  auto activeRows = applyFilter(rows);
  makeNestedRowSet(activeRows, rows.back());
  if (child_ && !nestedRows_.empty()) {
    child_->read(child_->readOffset(), nestedRows_, nullptr);
  }
  numValues_ = activeRows.size();
  readOffset_ = offset + rows.back() + 1;
}

void SelectiveListColumnReader::getValues(RowSet rows, VectorPtr* result) {
  VELOX_DCHECK_NOT_NULL(result);
  prepareResult(*result, requestedType_, rows.size(), &memoryPool_);
  auto* resultArray = result->get()->asUnchecked<ArrayVector>();
  makeOffsetsAndSizes(rows, *resultArray);
  result->get()->setNulls(resultNulls());
  if (child_ && !nestedRows_.empty()) {
    auto& elements = resultArray->elements();
    prepareStructResult(requestedType_->childAt(0), &elements);
    child_->getValues(nestedRows_, &elements);
  }
}

SelectiveMapColumnReader::SelectiveMapColumnReader(
    const TypePtr& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    FormatParams& params,
    velox::common::ScanSpec& scanSpec)
    : SelectiveRepeatedColumnReader(requestedType, params, scanSpec, fileType) {
}

uint64_t SelectiveMapColumnReader::skip(uint64_t numValues) {
  numValues = formatData_->skipNulls(numValues);
  if (keyReader_ || elementReader_) {
    std::array<int32_t, kBufferSize> buffer;
    uint64_t childElements = 0;
    uint64_t lengthsRead = 0;
    while (lengthsRead < numValues) {
      uint64_t chunk =
          std::min(numValues - lengthsRead, static_cast<uint64_t>(kBufferSize));
      readLengths(buffer.data(), chunk, nullptr);
      for (size_t i = 0; i < chunk; ++i) {
        childElements += buffer[i];
      }
      lengthsRead += chunk;
    }
    if (keyReader_) {
      keyReader_->seekTo(keyReader_->readOffset() + childElements, false);
    }
    if (elementReader_) {
      elementReader_->seekTo(
          elementReader_->readOffset() + childElements, false);
    }
    childTargetReadOffset_ += childElements;

  } else {
    VELOX_FAIL("repeated reader with no children");
  }
  return numValues;
}

void SelectiveMapColumnReader::read(
    vector_size_t offset,
    RowSet rows,
    const uint64_t* incomingNulls) {
  // Catch up if child readers are behind the length stream.
  if (keyReader_) {
    keyReader_->seekTo(childTargetReadOffset_, false);
  }
  if (elementReader_) {
    elementReader_->seekTo(childTargetReadOffset_, false);
  }

  prepareRead<char>(offset, rows, incomingNulls);
  auto activeRows = applyFilter(rows);
  makeNestedRowSet(activeRows, rows.back());
  if (keyReader_ && elementReader_ && !nestedRows_.empty()) {
    keyReader_->read(keyReader_->readOffset(), nestedRows_, nullptr);
    nestedRows_ = keyReader_->outputRows();
    if (!nestedRows_.empty()) {
      elementReader_->read(elementReader_->readOffset(), nestedRows_, nullptr);
    }
  }
  numValues_ = activeRows.size();
  readOffset_ = offset + rows.back() + 1;
}

void SelectiveMapColumnReader::getValues(RowSet rows, VectorPtr* result) {
  VELOX_DCHECK_NOT_NULL(result);
  VELOX_CHECK(
      !result->get() || result->get()->type()->isMap(),
      "Expect MAP result vector, got {}",
      result->get()->type()->toString());
  prepareResult(*result, requestedType_, rows.size(), &memoryPool_);
  auto* resultMap = result->get()->asUnchecked<MapVector>();
  makeOffsetsAndSizes(rows, *resultMap);
  result->get()->setNulls(resultNulls());
  VELOX_CHECK(
      keyReader_ && elementReader_,
      "keyReader_ and elementReaer_ must exist in "
      "SelectiveMapColumnReader::getValues");
  if (!nestedRows_.empty()) {
    keyReader_->getValues(nestedRows_, &resultMap->mapKeys());
    auto& values = resultMap->mapValues();
    prepareStructResult(requestedType_->childAt(1), &values);
    elementReader_->getValues(nestedRows_, &values);
  }
}

} // namespace facebook::velox::dwio::common
