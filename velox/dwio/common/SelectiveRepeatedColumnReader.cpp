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

void SelectiveRepeatedColumnReader::makeNestedRowSet(
    RowSet rows,
    int32_t maxRow) {
  allLengths_.resize(maxRow + 1);
  assert(!allLengths_.empty()); // for lint only.
  auto nulls = nullsInReadRange_ ? nullsInReadRange_->as<uint64_t>() : nullptr;
  // Reads the lengths, leaves an uninitialized gap for a null
  // map/list. Reading these checks the null mask.
  readLengths(allLengths_.data(), maxRow + 1, nulls);
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
    for (auto i = currentRow; i < row; ++i) {
      if (!nulls || !bits::isBitNull(nulls, i)) {
        nestedOffset += allLengths_[i];
      }
    }
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
  for (auto i = currentRow; i <= maxRow; ++i) {
    if (!nulls || !bits::isBitNull(nulls, i)) {
      nestedOffset += allLengths_[i];
    }
  }
  childTargetReadOffset_ += nestedOffset;
  nestedRows_ = nestedRowsHolder_;
}

void SelectiveRepeatedColumnReader::makeOffsetsAndSizes(RowSet rows) {
  dwio::common::ensureCapacity<vector_size_t>(
      offsets_, rows.size(), &memoryPool_);
  dwio::common::ensureCapacity<vector_size_t>(
      sizes_, rows.size(), &memoryPool_);
  auto* rawOffsets = offsets_->asMutable<vector_size_t>();
  auto* rawSizes = sizes_->asMutable<vector_size_t>();
  auto* nulls = nullsInReadRange_ ? nullsInReadRange_->as<uint64_t>() : nullptr;
  vector_size_t currentRow = 0;
  vector_size_t currentOffset = 0;
  vector_size_t nestedRowIndex = 0;
  for (int i = 0; i < rows.size(); ++i) {
    auto row = rows[i];
    for (auto j = currentRow; j < row; ++j) {
      if (!nulls || !bits::isBitNull(nulls, j)) {
        currentOffset += allLengths_[j];
      }
    }
    currentRow = row + 1;
    while (nestedRowIndex < nestedRows_.size() &&
           nestedRows_[nestedRowIndex] < currentOffset) {
      ++nestedRowIndex;
    }
    rawOffsets[i] = nestedRowIndex;
    if (nulls && bits::isBitNull(nulls, row)) {
      rawSizes[i] = 0;
      bits::setNull(rawResultNulls_, i);
      anyNulls_ = true;
    } else {
      vector_size_t length = 0;
      currentOffset += allLengths_[row];
      while (nestedRowIndex < nestedRows_.size() &&
             nestedRows_[nestedRowIndex] < currentOffset) {
        ++length;
        ++nestedRowIndex;
      }
      rawSizes[i] = length;
    }
  }
  numValues_ = rows.size();
  offsets_->setSize(numValues_ * sizeof(vector_size_t));
  sizes_->setSize(numValues_ * sizeof(vector_size_t));
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
    const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
    FormatParams& params,
    velox::common::ScanSpec& scanSpec)
    : SelectiveRepeatedColumnReader(dataType, params, scanSpec, dataType->type),
      requestedType_{requestedType} {}

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
    child_->skip(childElements);
    childTargetReadOffset_ += childElements;
    child_->setReadOffset(child_->readOffset() + childElements);
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
  makeOffsetsAndSizes(rows);
  VectorPtr elements;
  if (child_ && !nestedRows_.empty()) {
    prepareStructResult(type_->childAt(0), &elements);
    child_->getValues(nestedRows_, &elements);
  }
  *result = std::make_shared<ArrayVector>(
      &memoryPool_,
      requestedType_->type,
      anyNulls_ ? resultNulls_ : nullptr,
      rows.size(),
      offsets_,
      sizes_,
      elements);
}

SelectiveMapColumnReader::SelectiveMapColumnReader(
    const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
    FormatParams& params,
    velox::common::ScanSpec& scanSpec)
    : SelectiveRepeatedColumnReader(dataType, params, scanSpec, dataType->type),
      requestedType_{requestedType} {}

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
      keyReader_->skip(childElements);
      keyReader_->setReadOffset(keyReader_->readOffset() + childElements);
    }
    if (elementReader_) {
      elementReader_->skip(childElements);
      elementReader_->setReadOffset(
          elementReader_->readOffset() + childElements);
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
    elementReader_->read(elementReader_->readOffset(), nestedRows_, nullptr);
  }
  numValues_ = activeRows.size();
  readOffset_ = offset + rows.back() + 1;
}

void SelectiveMapColumnReader::getValues(RowSet rows, VectorPtr* result) {
  makeOffsetsAndSizes(rows);
  VectorPtr keys;
  VectorPtr values;
  VELOX_CHECK(
      keyReader_ && elementReader_,
      "keyReader_ and elementReaer_ must exist in "
      "SelectiveMapColumnReader::getValues");
  if (!nestedRows_.empty()) {
    keyReader_->getValues(nestedRows_, &keys);
    prepareStructResult(type_->childAt(1), &values);
    elementReader_->getValues(nestedRows_, &values);
  }
  *result = std::make_shared<MapVector>(
      &memoryPool_,
      requestedType_->type,
      anyNulls_ ? resultNulls_ : nullptr,
      rows.size(),
      offsets_,
      sizes_,
      keys,
      values);
}

} // namespace facebook::velox::dwio::common
