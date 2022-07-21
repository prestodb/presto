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

#pragma once

#include "velox/dwio/common/BufferUtil.h"
#include "velox/dwio/common/SelectiveColumnReaderInternal.h"
#include "velox/dwio/dwrf/common/DecoderUtil.h"
#include "velox/dwio/dwrf/reader/DwrfData.h"

namespace facebook::velox::dwrf {

// Abstract superclass for list and map readers. Encapsulates common
// logic for dealing with mapping between enclosing and nested rows.
class SelectiveRepeatedColumnReader
    : public dwio::common::SelectiveColumnReader {
 public:
  bool useBulkPath() const override {
    return false;
  }

 protected:
  // Buffer size for reading length stream
  static constexpr size_t kBufferSize = 1024;

  SelectiveRepeatedColumnReader(
      std::shared_ptr<const dwio::common::TypeWithId> nodeType,
      DwrfParams& params,
      common::ScanSpec& scanSpec,
      const TypePtr& type)
      : SelectiveColumnReader(std::move(nodeType), params, scanSpec, type) {
    EncodingKey encodingKey{nodeType_->id, params.flatMapContext().sequence};
    auto& stripe = params.stripeStreams();
    auto rleVersion = convertRleVersion(stripe.getEncoding(encodingKey).kind());
    auto lenId = encodingKey.forKind(proto::Stream_Kind_LENGTH);
    bool lenVints = stripe.getUseVInts(lenId);
    length_ = createRleDecoder</*isSigned*/ false>(
        stripe.getStream(lenId, true),
        rleVersion,
        memoryPool_,
        lenVints,
        dwio::common::INT_BYTE_SIZE);
  }

  void makeNestedRowSet(RowSet rows) {
    allLengths_.resize(rows.back() + 1);
    assert(!allLengths_.empty()); // for lint only.
    auto nulls =
        nullsInReadRange_ ? nullsInReadRange_->as<uint64_t>() : nullptr;
    // Reads the lengths, leaves an uninitialized gap for a null
    // map/list. Reading these checks the null nask.
    length_->next(allLengths_.data(), rows.back() + 1, nulls);
    dwio::common::ensureCapacity<vector_size_t>(
        offsets_, rows.size(), &memoryPool_);
    dwio::common::ensureCapacity<vector_size_t>(
        sizes_, rows.size(), &memoryPool_);
    auto rawOffsets = offsets_->asMutable<vector_size_t>();
    auto rawSizes = sizes_->asMutable<vector_size_t>();
    vector_size_t nestedLength = 0;
    for (auto row : rows) {
      if (!nulls || !bits::isBitNull(nulls, row)) {
        nestedLength += allLengths_[row];
      }
    }
    nestedRows_.resize(nestedLength);
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
      // Check if parent is null after adding up the lengths leading
      // up to this. If null, add a null to the result and keep
      // looping. If the null is last, the lengths will all have been
      // added up.
      if (nulls && bits::isBitNull(nulls, row)) {
        rawOffsets[rowIndex] = 0;
        rawSizes[rowIndex] = 0;
        bits::setNull(rawResultNulls_, rowIndex);
        anyNulls_ = true;
        continue;
      }

      auto lengthAtRow = allLengths_[row];
      std::iota(
          &nestedRows_[nestedRow],
          &nestedRows_[nestedRow + lengthAtRow],
          nestedOffset);
      rawOffsets[rowIndex] = nestedRow;
      rawSizes[rowIndex] = lengthAtRow;
      nestedRow += lengthAtRow;
      nestedOffset += lengthAtRow;
    }
    childTargetReadOffset_ += nestedOffset;
  }

  void compactOffsets(RowSet rows) {
    auto rawOffsets = offsets_->asMutable<vector_size_t>();
    auto rawSizes = sizes_->asMutable<vector_size_t>();
    VELOX_CHECK(
        outputRows_.empty(), "Repeated reader does not support filters");
    RowSet rowsToCompact;
    if (valueRows_.empty()) {
      valueRows_.resize(rows.size());
      rowsToCompact = inputRows_;
    } else {
      rowsToCompact = valueRows_;
    }
    if (rows.size() == rowsToCompact.size()) {
      return;
    }

    int32_t current = 0;
    bool moveNulls = shouldMoveNulls(rows);
    for (int i = 0; i < rows.size(); ++i) {
      auto row = rows[i];
      while (rowsToCompact[current] < row) {
        ++current;
      }
      VELOX_CHECK(rowsToCompact[current] == row);
      valueRows_[i] = row;
      rawOffsets[i] = rawOffsets[current];
      rawSizes[i] = rawSizes[current];
      if (moveNulls && i != current) {
        bits::setBit(
            rawResultNulls_, i, bits::isBitSet(rawResultNulls_, current));
      }
    }
    numValues_ = rows.size();
    valueRows_.resize(numValues_);
    offsets_->setSize(numValues_ * sizeof(vector_size_t));
    sizes_->setSize(numValues_ * sizeof(vector_size_t));
  }

  // Creates a struct if '*result' is empty and 'type' is a row.
  void prepareStructResult(const TypePtr& type, VectorPtr* result) {
    if (!*result && type->kind() == TypeKind::ROW) {
      *result = BaseVector::create(type, 0, &memoryPool_);
    }
  }

  std::vector<int64_t> allLengths_;
  raw_vector<vector_size_t> nestedRows_;
  BufferPtr offsets_;
  BufferPtr sizes_;
  // The position in the child readers that corresponds to the
  // position in the length stream. The child readers can be behind if
  // the last parents were null, so that the child stream was only
  // read up to the last position corresponding to
  // the last non-null parent.
  vector_size_t childTargetReadOffset_ = 0;
  std::unique_ptr<dwio::common::IntDecoder</*isSigned*/ false>> length_;
};

class SelectiveListColumnReader : public SelectiveRepeatedColumnReader {
 public:
  SelectiveListColumnReader(
      const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
      DwrfParams& params,
      common::ScanSpec& scanSpec);

  void resetFilterCaches() override {
    child_->resetFilterCaches();
  }

  void seekToRowGroup(uint32_t index) override {
    auto positionsProvider = formatData_->seekToRowGroup(index);
    length_->seekToRowGroup(positionsProvider);

    VELOX_CHECK(!positionsProvider.hasNext());

    child_->seekToRowGroup(index);
    child_->setReadOffsetRecursive(0);
    childTargetReadOffset_ = 0;
  }

  uint64_t skip(uint64_t numValues) override;

  void read(vector_size_t offset, RowSet rows, const uint64_t* incomingNulls)
      override;

  void getValues(RowSet rows, VectorPtr* result) override;

 private:
  std::unique_ptr<SelectiveColumnReader> child_;
  const std::shared_ptr<const dwio::common::TypeWithId> requestedType_;
};

class SelectiveMapColumnReader : public SelectiveRepeatedColumnReader {
 public:
  SelectiveMapColumnReader(
      const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
      DwrfParams& params,
      common::ScanSpec& scanSpec);

  void resetFilterCaches() override {
    keyReader_->resetFilterCaches();
    elementReader_->resetFilterCaches();
  }

  void seekToRowGroup(uint32_t index) override {
    auto positionsProvider = formatData_->seekToRowGroup(index);

    length_->seekToRowGroup(positionsProvider);

    VELOX_CHECK(!positionsProvider.hasNext());

    keyReader_->seekToRowGroup(index);
    keyReader_->setReadOffsetRecursive(0);
    elementReader_->seekToRowGroup(index);
    elementReader_->setReadOffsetRecursive(0);
    childTargetReadOffset_ = 0;
  }

  uint64_t skip(uint64_t numValues) override;

  void read(vector_size_t offset, RowSet rows, const uint64_t* incomingNulls)
      override;

  void getValues(RowSet rows, VectorPtr* result) override;

 private:
  std::unique_ptr<SelectiveColumnReader> keyReader_;
  std::unique_ptr<SelectiveColumnReader> elementReader_;
  const std::shared_ptr<const dwio::common::TypeWithId> requestedType_;
};

} // namespace facebook::velox::dwrf
