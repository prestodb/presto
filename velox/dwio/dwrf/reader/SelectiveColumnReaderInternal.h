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

#include "velox/common/base/Portability.h"
#include "velox/dwio/common/DirectDecoder.h"
#include "velox/dwio/common/TypeUtils.h"
#include "velox/dwio/dwrf/common/FloatingPointDecoder.h"
#include "velox/dwio/dwrf/common/RLEv1.h"
#include "velox/dwio/dwrf/reader/ColumnVisitors.h"
#include "velox/dwio/dwrf/reader/SelectiveColumnReader.h"
#include "velox/dwio/dwrf/utils/ProtoUtils.h"
#include "velox/exec/AggregationHook.h"
#include "velox/type/Timestamp.h"
#include "velox/vector/ConstantVector.h"
#include "velox/vector/DictionaryVector.h"
#include "velox/vector/FlatVector.h"

#include <numeric>

namespace facebook::velox::dwrf {

common::AlwaysTrue& alwaysTrue();

class Timer {
 public:
  Timer() : startClocks_{folly::hardware_timestamp()} {}

  uint64_t elapsedClocks() const {
    return folly::hardware_timestamp() - startClocks_;
  }

 private:
  const uint64_t startClocks_;
};

inline RleVersion convertRleVersion(proto::ColumnEncoding_Kind kind) {
  switch (static_cast<int64_t>(kind)) {
    case proto::ColumnEncoding_Kind_DIRECT:
    case proto::ColumnEncoding_Kind_DICTIONARY:
      return RleVersion_1;
    default:
      DWIO_RAISE("Unknown encoding in convertRleVersion");
  }
}

template <typename T>
void SelectiveColumnReader::ensureValuesCapacity(vector_size_t numRows) {
  if (values_ && values_->unique() &&
      values_->capacity() >=
          BaseVector::byteSize<T>(numRows) + simd::kPadding) {
    return;
  }
  values_ = AlignedBuffer::allocate<T>(
      numRows + (simd::kPadding / sizeof(T)), &memoryPool_);
  rawValues_ = values_->asMutable<char>();
}

template <typename T>
void SelectiveColumnReader::prepareRead(
    vector_size_t offset,
    RowSet rows,
    const uint64_t* incomingNulls) {
  seekTo(offset, scanSpec_->readsNullsOnly());
  vector_size_t numRows = rows.back() + 1;

  // Do not re-use unless singly-referenced.
  if (nullsInReadRange_ && !nullsInReadRange_->unique()) {
    nullsInReadRange_.reset();
  }
  formatData_->readNulls(numRows, incomingNulls, nullsInReadRange_);
  // We check for all nulls and no nulls. We expect both calls to
  // bits::isAllSet to fail early in the common case. We could do a
  // single traversal of null bits counting the bits and then compare
  // this to 0 and the total number of rows but this would end up
  // reading more in the mixed case and would not be better in the all
  // (non)-null case.
  allNull_ = nullsInReadRange_ &&
      bits::isAllSet(
                 nullsInReadRange_->as<uint64_t>(), 0, numRows, bits::kNull);
  if (nullsInReadRange_ &&
      bits::isAllSet(
          nullsInReadRange_->as<uint64_t>(), 0, numRows, bits::kNotNull)) {
    nullsInReadRange_ = nullptr;
  }
  innerNonNullRows_.clear();
  outerNonNullRows_.clear();
  outputRows_.clear();
  // is part of read() and after read returns getValues may be called.
  mayGetValues_ = true;
  numOutConfirmed_ = 0;
  numValues_ = 0;
  valueSize_ = sizeof(T);
  inputRows_ = rows;
  if (scanSpec_->filter()) {
    outputRows_.reserve(rows.size());
  }
  ensureValuesCapacity<T>(rows.size());
  if (scanSpec_->keepValues() && !scanSpec_->valueHook()) {
    valueRows_.clear();
    prepareNulls(rows, nullsInReadRange_ != nullptr);
  }
}

template <typename T, typename TVector>
void SelectiveColumnReader::getFlatValues(
    RowSet rows,
    VectorPtr* result,
    const TypePtr& type,
    bool isFinal) {
  VELOX_CHECK_NE(valueSize_, kNoValueSize);
  VELOX_CHECK(mayGetValues_);
  if (isFinal) {
    mayGetValues_ = false;
  }
  if (allNull_) {
    *result = std::make_shared<ConstantVector<TVector>>(
        &memoryPool_,
        rows.size(),
        true,
        type,
        T(),
        SimpleVectorStats<TVector>{},
        sizeof(TVector) * rows.size());
    return;
  }
  if (valueSize_ == sizeof(TVector)) {
    compactScalarValues<TVector, TVector>(rows, isFinal);
  } else if (sizeof(T) >= sizeof(TVector)) {
    compactScalarValues<T, TVector>(rows, isFinal);
  } else {
    upcastScalarValues<T, TVector>(rows);
  }
  valueSize_ = sizeof(TVector);
  BufferPtr nulls = anyNulls_
      ? (returnReaderNulls_ ? nullsInReadRange_ : resultNulls_)
      : nullptr;
  *result = std::make_shared<FlatVector<TVector>>(
      &memoryPool_,
      type,
      nulls,
      numValues_,
      values_,
      std::move(stringBuffers_));
}

template <>
void SelectiveColumnReader::getFlatValues<int8_t, bool>(
    RowSet rows,
    VectorPtr* result,
    const TypePtr& type,
    bool isFinal);

template <typename T, typename TVector>
void SelectiveColumnReader::upcastScalarValues(RowSet rows) {
  VELOX_CHECK_LE(rows.size(), numValues_);
  VELOX_CHECK(!rows.empty());
  if (!values_) {
    return;
  }
  VELOX_CHECK_GT(sizeof(TVector), sizeof(T));
  // Since upcast is not going to be a common path, allocate buffer to copy
  // upcasted values to and then copy back to the values buffer.
  std::vector<TVector> buf;
  buf.resize(rows.size());
  T* typedSourceValues = reinterpret_cast<T*>(rawValues_);
  RowSet sourceRows;
  // The row numbers corresponding to elements in 'values_' are in
  // 'valueRows_' if values have been accessed before. Otherwise
  // they are in 'outputRows_' if these are non-empty (there is a
  // filter) and in 'inputRows_' otherwise.
  if (!valueRows_.empty()) {
    sourceRows = valueRows_;
  } else if (!outputRows_.empty()) {
    sourceRows = outputRows_;
  } else {
    sourceRows = inputRows_;
  }
  if (valueRows_.empty()) {
    valueRows_.resize(rows.size());
  }
  vector_size_t rowIndex = 0;
  auto nextRow = rows[rowIndex];
  bool moveNulls = shouldMoveNulls(rows);
  for (size_t i = 0; i < numValues_; i++) {
    if (sourceRows[i] < nextRow) {
      continue;
    }

    VELOX_DCHECK(sourceRows[i] == nextRow);
    buf[rowIndex] = typedSourceValues[i];
    if (moveNulls && rowIndex != i) {
      bits::setBit(
          rawResultNulls_, rowIndex, bits::isBitSet(rawResultNulls_, i));
    }
    valueRows_[rowIndex] = nextRow;
    rowIndex++;
    if (rowIndex >= rows.size()) {
      break;
    }
    nextRow = rows[rowIndex];
  }
  ensureValuesCapacity<TVector>(rows.size());
  std::memcpy(rawValues_, buf.data(), rows.size() * sizeof(TVector));
  numValues_ = rows.size();
  valueRows_.resize(numValues_);
  values_->setSize(numValues_ * sizeof(TVector));
}

template <typename T, typename TVector>
void SelectiveColumnReader::compactScalarValues(RowSet rows, bool isFinal) {
  VELOX_CHECK_LE(rows.size(), numValues_);
  VELOX_CHECK(!rows.empty());
  if (!values_ || (rows.size() == numValues_ && sizeof(T) == sizeof(TVector))) {
    if (values_) {
      values_->setSize(numValues_ * sizeof(T));
    }
    return;
  }
  VELOX_CHECK_LE(sizeof(TVector), sizeof(T));
  T* typedSourceValues = reinterpret_cast<T*>(rawValues_);
  TVector* typedDestValues = reinterpret_cast<TVector*>(rawValues_);
  RowSet sourceRows;
  // The row numbers corresponding to elements in 'values_' are in
  // 'valueRows_' if values have been accessed before. Otherwise
  // they are in 'outputRows_' if these are non-empty (there is a
  // filter) and in 'inputRows_' otherwise.
  if (!valueRows_.empty()) {
    sourceRows = valueRows_;
  } else if (!outputRows_.empty()) {
    sourceRows = outputRows_;
  } else {
    sourceRows = inputRows_;
  }
  if (valueRows_.empty()) {
    valueRows_.resize(rows.size());
  }
  vector_size_t rowIndex = 0;
  auto nextRow = rows[rowIndex];
  bool moveNulls = shouldMoveNulls(rows);
  for (size_t i = 0; i < numValues_; i++) {
    if (sourceRows[i] < nextRow) {
      continue;
    }

    VELOX_DCHECK(sourceRows[i] == nextRow);
    typedDestValues[rowIndex] = typedSourceValues[i];
    if (moveNulls && rowIndex != i) {
      bits::setBit(
          rawResultNulls_, rowIndex, bits::isBitSet(rawResultNulls_, i));
    }
    if (!isFinal) {
      valueRows_[rowIndex] = nextRow;
    }
    rowIndex++;
    if (rowIndex >= rows.size()) {
      break;
    }
    nextRow = rows[rowIndex];
  }
  numValues_ = rows.size();
  valueRows_.resize(numValues_);
  values_->setSize(numValues_ * sizeof(TVector));
}

template <>
void SelectiveColumnReader::compactScalarValues<bool, bool>(
    RowSet rows,
    bool isFinal);

inline int32_t sizeOfIntKind(TypeKind kind) {
  switch (kind) {
    case TypeKind::SMALLINT:
      return 2;
    case TypeKind::INTEGER:
      return 4;
    case TypeKind::BIGINT:
      return 8;
    default:
      VELOX_FAIL("Not an integer TypeKind");
  }
}

template <typename T>
void SelectiveColumnReader::filterNulls(
    RowSet rows,
    bool isNull,
    bool extractValues) {
  if (!formatData_->hasNulls()) {
    if (isNull) {
      // The whole stripe will be empty. We do not update
      // 'readOffset' since nothing is read from either nulls or data.
      return;
    }
    // All pass. We do not update 'readOffset' because neither nulls
    // or data will be read for the whole stripe.
    setOutputRows(rows);
    return;
  }
  bool isDense = rows.back() == rows.size() - 1;
  auto rawNulls =
      nullsInReadRange_ ? nullsInReadRange_->as<uint64_t>() : nullptr;
  if (isNull) {
    if (!rawNulls) {
      // The stripe has nulls but the current range does not. Nothing matches.
    } else if (isDense) {
      bits::forEachUnsetBit(
          rawNulls, 0, rows.back() + 1, [&](vector_size_t row) {
            addOutputRow(row);
            if (extractValues) {
              addNull<T>();
            }
          });
    } else {
      for (auto row : rows) {
        if (bits::isBitNull(rawNulls, row)) {
          addOutputRow(row);
          if (extractValues) {
            addNull<T>();
          }
        }
      }
    }
    readOffset_ += rows.back() + 1;
    return;
  }

  VELOX_CHECK(
      !extractValues,
      "filterNulls for not null only applies to filter-only case");
  if (!rawNulls) {
    // All pass.
    for (auto row : rows) {
      addOutputRow(row);
    }
  } else if (isDense) {
    bits::forEachSetBit(rawNulls, 0, rows.back() + 1, [&](vector_size_t row) {
      addOutputRow(row);
    });
  } else {
    for (auto row : rows) {
      if (!bits::isBitNull(rawNulls, row)) {
        addOutputRow(row);
      }
    }
  }
  readOffset_ += rows.back() + 1;
}

std::vector<uint64_t> toPositions(const proto::RowIndexEntry& entry);

} // namespace facebook::velox::dwrf
