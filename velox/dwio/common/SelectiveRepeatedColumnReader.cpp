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

namespace facebook::velox::dwio::common {

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
  makeNestedRowSet(rows);
  if (child_ && !nestedRows_.empty()) {
    child_->read(child_->readOffset(), nestedRows_, nullptr);
  }
  numValues_ = rows.size();
  readOffset_ = offset + rows.back() + 1;
}

void SelectiveListColumnReader::getValues(RowSet rows, VectorPtr* result) {
  compactOffsets(rows);
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
  makeNestedRowSet(rows);
  if (keyReader_ && elementReader_ && !nestedRows_.empty()) {
    keyReader_->read(keyReader_->readOffset(), nestedRows_, nullptr);
    elementReader_->read(elementReader_->readOffset(), nestedRows_, nullptr);
  }
  numValues_ = rows.size();
  readOffset_ = offset + rows.back() + 1;
}

void SelectiveMapColumnReader::getValues(RowSet rows, VectorPtr* result) {
  compactOffsets(rows);
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
