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

#include <folly/Conv.h>

#include "velox/vector/BuilderTypeUtils.h"

namespace facebook {
namespace velox {

template <typename T>
SequenceVector<T>::SequenceVector(
    velox::memory::MemoryPool* pool,
    size_t length,
    std::shared_ptr<BaseVector> sequenceValues,
    BufferPtr sequenceLengths,
    const SimpleVectorStats<T>& stats,
    std::optional<int32_t> distinctCount,
    std::optional<vector_size_t> nullCount,
    std::optional<bool> isSorted,
    std::optional<ByteCount> representedBytes,
    std::optional<ByteCount> storageByteCount)
    : SimpleVector<T>(
          pool,
          sequenceValues->type(),
          BufferPtr(nullptr),
          length,
          stats,
          distinctCount,
          nullCount,
          isSorted,
          representedBytes,
          storageByteCount),
      sequenceValues_(std::move(sequenceValues)),
      sequenceLengths_(std::move(sequenceLengths)) {
  VELOX_CHECK_GE(length, 0, "Must provide length >= 0");
  VELOX_CHECK(sequenceLengths_ != nullptr, "Sequence Lengths must be not null");
  VELOX_CHECK_EQ(
      sequenceLengths_->size(),
      BaseVector::byteSize<SequenceLength>(sequenceValues_->size()),
      "Sequence Lengths must be sized to hold lengths for each sequence value");
  setInternalState();
}

template <typename T>
void SequenceVector<T>::setInternalState() {
  if (sequenceValues_->isScalar()) {
    scalarSequenceValues_ =
        reinterpret_cast<SimpleVector<T>*>(sequenceValues_.get());
  }
  lengths_ = sequenceLengths_->as<vector_size_t>();
  lastIndexRangeEnd_ = lengths_[0];
  BaseVector::inMemoryBytes_ += sequenceValues_->inMemoryBytes();
  BaseVector::inMemoryBytes_ += sequenceLengths_->capacity();
}

template <typename T>
bool SequenceVector<T>::isNullAtFast(vector_size_t idx) const {
  size_t offset = offsetOfIndex(idx);
  DCHECK(offset >= 0) << "Invalid index";
  return sequenceValues_->isNullAt(offset);
}

template <typename T>
const T SequenceVector<T>::valueAtFast(vector_size_t idx) const {
  size_t offset = offsetOfIndex(idx);
  VELOX_DCHECK_GE(offset, 0, "Invalid index");
  return scalarSequenceValues_->valueAt(offset);
}

template <typename T>
std::unique_ptr<SimpleVector<uint64_t>> SequenceVector<T>::hashAll() const {
  VELOX_CHECK(isScalar(), "Complex types not yet supported");
  // TODO T70734527 dealing with zero length vector
  if (BaseVector::length_ == 0) {
    return nullptr;
  }
  // If there is at least one value, then indices_ is set and has a pool.
  auto sequenceCount = numSequences();
  BufferPtr hashes =
      AlignedBuffer::allocate<uint64_t>(sequenceCount, BaseVector::pool_);
  uint64_t* rawHashes = hashes->asMutable<uint64_t>();
  for (size_t i = 0; i < sequenceCount; ++i) {
    rawHashes[i] = sequenceValues_->hashValueAt(i);
  }
  auto hashValues = std::make_shared<FlatVector<uint64_t>>(
      BaseVector::pool_,
      BufferPtr(nullptr),
      sequenceCount,
      hashes,
      std::vector<BufferPtr>(0) /*stringBuffers*/,
      SimpleVectorStats<uint64_t>{}, /*stats*/
      std::nullopt /*distinctValueCount*/,
      0 /* nullCount */,
      false /* sorted */,
      sizeof(uint64_t) * sequenceCount /* representedBytes */);

  return std::make_unique<SequenceVector<uint64_t>>(
      BaseVector::pool_,
      BaseVector::length_,
      std::move(hashValues),
      sequenceLengths_,
      SimpleVectorStats<uint64_t>{}, /*stats*/
      std::nullopt /*distinctCount*/,
      0 /* nullCount */,
      false /* sorted */,
      sizeof(uint64_t) * BaseVector::length_ /* representedBytes */,
      0 /* nullSequenceCount */);
}

template <typename T>
__m256i SequenceVector<T>::loadSIMDValueBufferAt(size_t byteOffset) const {
  auto startIndex = byteOffset / sizeof(T);
  if (checkLoadRange(startIndex, simd::Vectors<T>::VSize)) {
    return simd::setAll256i(valueAtFast(startIndex));
  }

  if constexpr (
      std::is_same<T, int64_t>::value || std::is_same<T, uint64_t>::value) {
    // Note it's important to retrieve these in order for performance
    // reasons which is why we enregister them here and reorder them below
    auto b0 = valueAtFast(startIndex);
    auto b1 = valueAtFast(startIndex + 1);
    auto b2 = valueAtFast(startIndex + 2);
    auto b3 = valueAtFast(startIndex + 3);
    return _mm256_set_epi64x(b3, b2, b1, b0);
  } else if constexpr (
      std::is_same<T, int32_t>::value || std::is_same<T, uint32_t>::value) {
    // Note it's important to retrieve these in order for performance
    // reasons which is why we enregister them here and reorder them below
    auto b0 = valueAtFast(startIndex);
    auto b1 = valueAtFast(startIndex + 1);
    auto b2 = valueAtFast(startIndex + 2);
    auto b3 = valueAtFast(startIndex + 3);
    auto b4 = valueAtFast(startIndex + 4);
    auto b5 = valueAtFast(startIndex + 5);
    auto b6 = valueAtFast(startIndex + 6);
    auto b7 = valueAtFast(startIndex + 7);
    return _mm256_set_epi32(b7, b6, b5, b4, b3, b2, b1, b0);

  } else if constexpr (
      std::is_same<T, int16_t>::value || std::is_same<T, uint16_t>::value) {
    auto b0 = valueAtFast(startIndex);
    auto b1 = valueAtFast(startIndex + 1);
    auto b2 = valueAtFast(startIndex + 2);
    auto b3 = valueAtFast(startIndex + 3);
    auto b4 = valueAtFast(startIndex + 4);
    auto b5 = valueAtFast(startIndex + 5);
    auto b6 = valueAtFast(startIndex + 6);
    auto b7 = valueAtFast(startIndex + 7);
    auto b8 = valueAtFast(startIndex + 8);
    auto b9 = valueAtFast(startIndex + 9);
    auto b10 = valueAtFast(startIndex + 10);
    auto b11 = valueAtFast(startIndex + 11);
    auto b12 = valueAtFast(startIndex + 12);
    auto b13 = valueAtFast(startIndex + 13);
    auto b14 = valueAtFast(startIndex + 14);
    auto b15 = valueAtFast(startIndex + 15);
    return _mm256_set_epi16(
        b15, b14, b13, b12, b11, b10, b9, b8, b7, b6, b5, b4, b3, b2, b1, b0);
  } else if constexpr (
      std::is_same<T, int8_t>::value || std::is_same<T, uint8_t>::value) {
    auto b0 = valueAtFast(startIndex);
    auto b1 = valueAtFast(startIndex + 1);
    auto b2 = valueAtFast(startIndex + 2);
    auto b3 = valueAtFast(startIndex + 3);
    auto b4 = valueAtFast(startIndex + 4);
    auto b5 = valueAtFast(startIndex + 5);
    auto b6 = valueAtFast(startIndex + 6);
    auto b7 = valueAtFast(startIndex + 7);
    auto b8 = valueAtFast(startIndex + 8);
    auto b9 = valueAtFast(startIndex + 9);
    auto b10 = valueAtFast(startIndex + 10);
    auto b11 = valueAtFast(startIndex + 11);
    auto b12 = valueAtFast(startIndex + 12);
    auto b13 = valueAtFast(startIndex + 13);
    auto b14 = valueAtFast(startIndex + 14);
    auto b15 = valueAtFast(startIndex + 15);
    auto b16 = valueAtFast(startIndex + 16);
    auto b17 = valueAtFast(startIndex + 17);
    auto b18 = valueAtFast(startIndex + 18);
    auto b19 = valueAtFast(startIndex + 19);
    auto b20 = valueAtFast(startIndex + 20);
    auto b21 = valueAtFast(startIndex + 21);
    auto b22 = valueAtFast(startIndex + 22);
    auto b23 = valueAtFast(startIndex + 23);
    auto b24 = valueAtFast(startIndex + 24);
    auto b25 = valueAtFast(startIndex + 25);
    auto b26 = valueAtFast(startIndex + 26);
    auto b27 = valueAtFast(startIndex + 27);
    auto b28 = valueAtFast(startIndex + 28);
    auto b29 = valueAtFast(startIndex + 29);
    auto b30 = valueAtFast(startIndex + 30);
    auto b31 = valueAtFast(startIndex + 31);

    return _mm256_set_epi8(
        b31,
        b30,
        b29,
        b28,
        b27,
        b26,
        b25,
        b24,
        b23,
        b22,
        b21,
        b20,
        b19,
        b18,
        b17,
        b16,
        b15,
        b14,
        b13,
        b12,
        b11,
        b10,
        b9,
        b8,
        b7,
        b6,
        b5,
        b4,
        b3,
        b2,
        b1,
        b0);
  }
  throw std::runtime_error(
      "Sequence encoding only supports SIMD operations on integers");
}

template <typename T>
bool SequenceVector<T>::checkLoadRange(size_t index, size_t count) const {
  // If the entire range is below the sequence threshold then we can load
  // everything at once
  offsetOfIndex(index); // set the internal index variables
  return (index + count) <= lastIndexRangeEnd_;
}

template <typename T>
vector_size_t SequenceVector<T>::offsetOfIndex(vector_size_t index) const {
  VELOX_DCHECK_LE(0, index);
  VELOX_DCHECK_LT(index, BaseVector::length_);

  if (index < lastIndexRangeStart_) {
    // walk down the range indices until we are in the range requested
    do {
      --lastRangeIndex_;
      lastIndexRangeEnd_ = lastIndexRangeStart_;
      lastIndexRangeStart_ -= lengths_[lastRangeIndex_];
    } while (index < lastIndexRangeStart_);
  } else if (index >= lastIndexRangeEnd_) {
    // walk up the range indicies until we are in the range requested
    do {
      ++lastRangeIndex_;
      lastIndexRangeStart_ = lastIndexRangeEnd_;
      lastIndexRangeEnd_ += lengths_[lastRangeIndex_];
    } while (index >= lastIndexRangeEnd_);
  }

  return lastRangeIndex_;
}

static inline vector_size_t offsetOfIndex(
    const vector_size_t* lengths,
    vector_size_t index,
    vector_size_t* lastRangeBegin,
    vector_size_t* lastRangeEnd,
    vector_size_t* lastIndex) {
  if (index < *lastRangeBegin) {
    // walk down the range indicies until we are in the range requested
    do {
      --*lastIndex;
      *lastRangeEnd = *lastRangeBegin;
      *lastRangeBegin -= lengths[*lastIndex];
    } while (index < *lastRangeBegin);
  } else if (index >= *lastRangeEnd) {
    // walk up the range indicies until we are in the range requested
    do {
      ++*lastIndex;
      *lastRangeBegin = *lastRangeEnd;
      *lastRangeEnd += lengths[*lastIndex];
    } while (index >= *lastRangeEnd);
  }
  return *lastIndex;
}

template <typename T>
const uint64_t* SequenceVector<T>::computeFlatNulls(
    const SelectivityVector& rows) {
  int32_t bytes = BaseVector::byteSize<bool>(BaseVector::length_);
  flatNullsBuffer_ = AlignedBuffer::allocate<char>(bytes, BaseVector::pool_);
  auto flatNulls = flatNullsBuffer_->asMutable<uint64_t>();
  int32_t current = 0;
  auto numLengths = numSequences();
  for (int32_t i = 0; i < numLengths; ++i) {
    VELOX_CHECK(current < bytes * 8);
    bits::fillBits(
        flatNulls,
        current,
        current + lengths_[i],
        !sequenceValues_->isNullAt(i));
    current += lengths_[i];
  }
  return flatNulls;
}

} // namespace velox
} // namespace facebook
