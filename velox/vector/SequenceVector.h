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

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/SimdUtil.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/SimpleVector.h"
#include "velox/vector/TypeAliases.h"

namespace facebook::velox {

/**
 * Provides a vector type that enacts run-length encoding for vectors that have
 * repeated runs of the same value.  For example, this would be usefor for
 * {foo, foo, foo, foo, bar, bar, bar, foo, foo, foo, foo, foo} as it can be
 * reduced to {foo, bar, foo} along with run length information of {4, 3, 5}.
 *
 * NOTE - current implementation is not thread safe when accessing values.
 */
template <typename T>
class SequenceVector : public SimpleVector<T> {
 public:
  static constexpr bool can_simd =
      (std::is_same<T, int64_t>::value || std::is_same<T, int32_t>::value ||
       std::is_same<T, int16_t>::value || std::is_same<T, int8_t>::value ||
       std::is_same<T, size_t>::value);

  SequenceVector(
      velox::memory::MemoryPool* pool,
      size_t length,
      std::shared_ptr<BaseVector> sequenceValues,
      BufferPtr sequenceLengths,
      const SimpleVectorStats<T>& stats = {},
      std::optional<int32_t> distinctCount = std::nullopt,
      std::optional<vector_size_t> nullCount = std::nullopt,
      std::optional<bool> sorted = std::nullopt,
      std::optional<ByteCount> representedBytes = std::nullopt,
      std::optional<ByteCount> storageByteCount = std::nullopt);

 public:
  ~SequenceVector() override = default;

  bool mayHaveNulls() const override {
    return sequenceValues_->mayHaveNulls();
  }

  bool mayHaveNullsRecursive() const override {
    return sequenceValues_->mayHaveNullsRecursive();
  }

  bool isNullAtFast(vector_size_t idx) const;

  bool isNullAt(vector_size_t idx) const override {
    return isNullAtFast(idx);
  }

  const T valueAtFast(vector_size_t idx) const;

  const T valueAt(vector_size_t idx) const override {
    return valueAtFast(idx);
  }

  std::unique_ptr<SimpleVector<uint64_t>> hashAll() const override;

  /**
   * Loads a 256bit vector of data at the virtual byteOffset given
   * Note this method is implemented on each vector type, but is intentionally
   * not virtual for performance reasons
   *
   * @param byteOffset - the byte offset to laod from
   */
  xsimd::batch<T> loadSIMDValueBufferAt(size_t index) const;

  /**
   * Returns a shared_ptr to the underlying byte buffer holding the values for
   * this vector as SequenceLength types. This is used during execution to
   * process over the subset of values when possible.
   */
  inline std::shared_ptr<BaseVector> getSequenceValues() const {
    return sequenceValues_;
  }

  BaseVector* loadedVector() override {
    auto loaded = BaseVector::loadedVectorShared(sequenceValues_);
    if (loaded == sequenceValues_) {
      return this;
    }
    sequenceValues_ = loaded;
    setInternalState();
    return this;
  }

  const BaseVector* loadedVector() const override {
    return const_cast<SequenceVector<T>*>(this)->loadedVector();
  }

  VectorPtr valueVector() const override {
    return sequenceValues_;
  }

  BufferPtr getSequenceLengths() const {
    return sequenceLengths_;
  }

  /**
   * Returns a shared_ptr to the underlying values container.
   */
  inline std::shared_ptr<BaseVector> getValuesVectorShared() const {
    return sequenceValues_;
  }

  vector_size_t numSequences() const {
    return sequenceValues_->size();
  }

  BufferPtr wrapInfo() const override {
    return sequenceLengths_;
  }

  uint64_t retainedSize() const override {
    return sequenceValues_->retainedSize() + sequenceLengths_->capacity();
  }

  bool isConstant(const SelectivityVector& rows) const override {
    return offsetOfIndex(rows.begin()) == offsetOfIndex(rows.end() - 1);
  }

  bool isScalar() const override {
    return sequenceValues_->isScalar();
  }

  /**
   * @returns the physical index for the location of the sequence value that
   * is logically resident at the given index, or, -1 for an index that is < 0
   * or an index that is >= the logical length of this vector.
   */
  vector_size_t offsetOfIndex(vector_size_t idx) const;

  const BaseVector* wrappedVector() const override {
    return sequenceValues_->wrappedVector();
  }

  vector_size_t wrappedIndex(vector_size_t index) const override {
    return sequenceValues_->wrappedIndex(offsetOfIndex(index));
  }

  void addNulls(const uint64_t* bits, const SelectivityVector& rows) override {
    throw std::runtime_error("addNulls not supported");
  }

  std::string toString(vector_size_t index) const override {
    if (BaseVector::isNullAt(index)) {
      return "null";
    }
    auto inner = offsetOfIndex(index);
    std::stringstream out;
    out << "[" << index << "->" << inner << "] "
        << sequenceValues_->toString(inner);
    return out.str();
  }

  VectorPtr slice(vector_size_t, vector_size_t) const override {
    VELOX_NYI();
  }

 private:
  // Prepares for use after construction.
  void setInternalState();

  bool checkLoadRange(size_t idx, size_t count) const;

  std::shared_ptr<BaseVector> sequenceValues_;
  SimpleVector<T>* scalarSequenceValues_ = nullptr;
  BufferPtr sequenceLengths_;
  // Caches 'sequenceLengths_->as<vector_size_t>()'
  const vector_size_t* lengths_ = nullptr;

  // manage the state of the offset index operations to allow for the common
  // path of iterating in order to not require repeated trips through the
  // sequenceLengths_ array
  mutable vector_size_t lastIndexRangeStart_ = 0;
  mutable vector_size_t lastIndexRangeEnd_ = 0;
  mutable vector_size_t lastRangeIndex_ = 0;
};

template <typename T>
using SequenceVectorPtr = std::shared_ptr<SequenceVector<T>>;

} // namespace facebook::velox

#include "velox/vector/SequenceVector-inl.h"
