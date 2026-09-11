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

#include "velox/dwio/common/SelectiveMapColumnReader.h"

#include "velox/dwio/common/BufferUtil.h"

namespace facebook::velox::dwio::common {

SelectiveMapColumnReader::SelectiveMapColumnReader(
    const TypePtr& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    FormatParams& params,
    common::ScanSpec& scanSpec)
    : SelectiveColumnReader(fileType->type(), fileType, params, scanSpec),
      requestedType_(requestedType) {}

void SelectiveMapColumnReader::makeNestedRowSet(
    RowSet rows,
    vector_size_t maxRow) {
  auto* nulls =
      nullsInReadRange_ ? nullsInReadRange_->as<uint64_t>() : nullptr;
  const auto numLengths = maxRow + 1;
  ensureCapacity<vector_size_t>(allLengths_, numLengths, memoryPool_);
  auto* lengths = allLengths_->asMutable<vector_size_t>();

  // Must pass combined nulls (parent + present). Length stream stores one
  // value per non-null map only; nullptr here over-reads and hits EOF on
  // files with null/empty map entries (Hive ORC).
  readLengths(reinterpret_cast<int32_t*>(lengths), numLengths, nulls);

  if (nulls) {
    for (vector_size_t i = 0; i < numLengths; ++i) {
      if (bits::isBitNull(nulls, i)) {
        lengths[i] = 0;
      }
    }
  }

  vector_size_t nestedOffset = 0;
  std::vector<vector_size_t> offsets(numLengths + 1);
  for (vector_size_t i = 0; i < numLengths; ++i) {
    offsets[i] = nestedOffset;
    nestedOffset += lengths[i];
  }
  offsets[numLengths] = nestedOffset;

  nestedRows_.clear();
  vector_size_t nestedSize = 0;
  for (auto row : rows) {
    nestedSize += lengths[row];
  }
  nestedRows_.reserve(nestedSize);
  for (auto row : rows) {
    for (vector_size_t j = offsets[row]; j < offsets[row] + lengths[row];
         ++j) {
      nestedRows_.push_back(j);
    }
  }
}

uint64_t SelectiveMapColumnReader::skip(uint64_t numValues) {
  // Advance present stream; only non-null maps have length entries.
  numValues = formatData_->skipNulls(numValues);
  if (numValues == 0) {
    return 0;
  }
  ensureCapacity<int32_t>(allLengths_, numValues, memoryPool_);
  auto* lengths = allLengths_->asMutable<int32_t>();
  // After skipNulls, every remaining value is non-null: nulls=nullptr is correct.
  readLengths(lengths, static_cast<int32_t>(numValues), nullptr);
  uint64_t numElements = 0;
  for (uint64_t i = 0; i < numValues; ++i) {
    numElements += static_cast<uint64_t>(lengths[i]);
  }
  if (keyReader_) {
    keyReader_->skip(numElements);
  }
  if (elementReader_) {
    elementReader_->skip(numElements);
  }
  return numValues;
}

void SelectiveMapColumnReader::read(
    vector_size_t offset,
    RowSet rows,
    const uint64_t* incomingNulls) {
  prepareRead<false>(offset, rows, incomingNulls);
  if (rows.empty()) {
    readOffset_ = offset;
    return;
  }
  makeNestedRowSet(rows, rows.back());
  if (keyReader_ && elementReader_ && !nestedRows_.empty()) {
    keyReader_->read(0, nestedRows_, nullptr);
    elementReader_->read(0, nestedRows_, nullptr);
  } else if (keyReader_ && elementReader_) {
    // Nested rows empty (all selected maps null or empty): still sync children.
    keyReader_->read(0, nestedRows_, nullptr);
    elementReader_->read(0, nestedRows_, nullptr);
  }
  readOffset_ = offset + rows.back() + 1;
}

void SelectiveMapColumnReader::getValues(RowSet rows, VectorPtr* result) {
  compactScalarValues<int32_t, int32_t>(rows, false);
  prepareResult(*result, requestedType_, rows.size());
  auto* mapResult = (*result)->asUnchecked<MapVector>();
  mapResult->setNulls(resultNulls_);

  auto* lengths = allLengths_->as<vector_size_t>();
  auto* offsetsOut = mapResult->mutableOffsets(rows.size())
                         ->asMutable<vector_size_t>();
  auto* sizesOut =
      mapResult->mutableSizes(rows.size())->asMutable<vector_size_t>();

  vector_size_t childOffset = 0;
  auto* nulls =
      nullsInReadRange_ ? nullsInReadRange_->as<uint64_t>() : nullptr;
  for (vector_size_t i = 0; i < rows.size(); ++i) {
    const auto row = rows[i];
    if (nulls && bits::isBitNull(nulls, row)) {
      offsetsOut[i] = childOffset;
      sizesOut[i] = 0;
    } else {
      offsetsOut[i] = childOffset;
      sizesOut[i] = lengths[row];
      childOffset += lengths[row];
    }
  }

  if (keyReader_) {
    keyReader_->getValues(nestedRows_, &mapResult->mapKeys());
  }
  if (elementReader_) {
    elementReader_->getValues(nestedRows_, &mapResult->mapValues());
  }
}

} // namespace facebook::velox::dwio::common
