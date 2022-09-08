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

#include <memory>
#include <type_traits>

#include <folly/container/F14Map.h>

#include "velox/common/base/SimdUtil.h"
#include "velox/vector/LazyVector.h"
#include "velox/vector/SimpleVector.h"
#include "velox/vector/TypeAliases.h"

namespace facebook {
namespace velox {

template <typename T>
class DictionaryVector : public SimpleVector<T> {
 public:
  static constexpr bool can_simd = std::is_same_v<T, int64_t>;

  // Creates dictionary vector using base vector (dictionaryValues) and a set
  // of indices (dictionaryIndexArray).
  //
  // Base vector itself can be encoded as dictionary or constant. Base vector
  // can also be a lazy vector. If base vector is lazy and has not been
  // loaded yet, loading will be delayed until loadedVector() is called.
  //
  // Base vector can contain duplicate values. This happens when dictionary
  // encoding is used to represent a result of a cardinality increasing
  // operator, for example, probe-side columns after cardinality increasing join
  // or result of an unnest.
  //
  // The number of indices can be less than the size of the base array, e.g. not
  // all elements of the base vector may be referenced. This happens when
  // dictionary encoding is used to represent a result of a filter or another
  // cardinality reducing operator, e.g. a selective join.
  DictionaryVector(
      velox::memory::MemoryPool* pool,
      BufferPtr nulls,
      size_t length,
      VectorPtr dictionaryValues,
      BufferPtr dictionaryIndexArray,
      const SimpleVectorStats<T>& stats = {},
      std::optional<vector_size_t> distinctValueCount = std::nullopt,
      std::optional<vector_size_t> nullCount = std::nullopt,
      std::optional<bool> isSorted = std::nullopt,
      std::optional<ByteCount> representedBytes = std::nullopt,
      std::optional<ByteCount> storageByteCount = std::nullopt);

  virtual ~DictionaryVector() override = default;

  bool mayHaveNulls() const override {
    VELOX_DCHECK(initialized_);
    return BaseVector::nulls() || dictionaryValues_->mayHaveNulls();
  }

  bool mayHaveNullsRecursive() const override {
    VELOX_DCHECK(initialized_);
    return BaseVector::mayHaveNullsRecursive() ||
        dictionaryValues_->mayHaveNullsRecursive();
  }

  bool isNullAt(vector_size_t idx) const override;

  const T valueAtFast(vector_size_t idx) const;

  /**
   * @return the value at the given index value for a dictionary entry, i.e.
   * gets the dictionary value by its indexed value.
   */
  const T valueAt(vector_size_t idx) const override {
    VELOX_DCHECK(initialized_);
    VELOX_DCHECK(!isNullAt(idx), "found null value at: {}", idx);
    auto innerIndex = getDictionaryIndex(idx);
    return scalarDictionaryValues_->valueAt(innerIndex);
  }

  std::unique_ptr<SimpleVector<uint64_t>> hashAll() const override;

  /**
   * Loads a SIMD vector of data at the virtual byteOffset given
   * Note this method is implemented on each vector type, but is intentionally
   * not virtual for performance reasons
   *
   * @param index at which to start the vector load
   * @return the vector of values starting at the given index
   */
  xsimd::batch<T> loadSIMDValueBufferAt(size_t index) const;

  inline const BufferPtr& indices() const {
    return indices_;
  }

  VectorPtr valueVector() const override {
    return dictionaryValues_;
  }

  BufferPtr wrapInfo() const override {
    return indices_;
  }

  BufferPtr mutableIndices(vector_size_t size) {
    if (indices_ && indices_->isMutable() &&
        indices_->capacity() >= size * sizeof(vector_size_t)) {
      return indices_;
    }

    indices_ = AlignedBuffer::allocate<vector_size_t>(size, BaseVector::pool_);
    rawIndices_ = indices_->as<vector_size_t>();
    return indices_;
  }

  uint64_t retainedSize() const override {
    return BaseVector::retainedSize() + dictionaryValues_->retainedSize() +
        indices_->capacity();
  }

  bool isConstant(const SelectivityVector& rows) const override {
    VELOX_CHECK(rows.hasSelections(), "No selected rows in isConstant()");
    auto firstIdx = getDictionaryIndex(rows.begin());
    auto firstNull = BaseVector::isNullAt(rows.begin());
    return rows.testSelected([&](auto row) {
      bool isNull = BaseVector::isNullAt(row);
      return (firstNull && isNull) ||
          (firstIdx == getDictionaryIndex(row) && !firstNull && !isNull);
    });
  }

  bool isScalar() const override {
    return dictionaryValues_->isScalar();
  }

  BaseVector* loadedVector() override {
    if (initialized_) {
      return this;
    }

    SelectivityVector rows(dictionaryValues_->size(), false);
    for (vector_size_t i = 0; i < this->size(); i++) {
      if (!BaseVector::isNullAt(i)) {
        auto ind = getDictionaryIndex(i);
        rows.setValid(ind, true);
      }
    }
    rows.updateBounds();

    LazyVector::ensureLoadedRows(dictionaryValues_, rows);
    setInternalState();
    return this;
  }

  const BaseVector* loadedVector() const override {
    return const_cast<DictionaryVector<T>*>(this)->loadedVector();
  }

  const BaseVector* wrappedVector() const override {
    return dictionaryValues_->wrappedVector();
  }

  vector_size_t wrappedIndex(vector_size_t index) const override {
    return dictionaryValues_->wrappedIndex(rawIndices_[index]);
  }

  std::string toString(vector_size_t index) const override {
    if (BaseVector::isNullAt(index)) {
      return "null";
    }
    auto inner = rawIndices_[index];
    std::stringstream out;
    out << "[" << index << "->" << inner << "] "
        << dictionaryValues_->toString(inner);
    return out.str();
  }

  void setDictionaryValues(VectorPtr dictionaryValues) {
    dictionaryValues_ = dictionaryValues;
    initialized_ = false;
    setInternalState();
  }

  /// Resizes the vector to be of size 'size'. If setNotNull is true
  /// the newly added elements point to the value at the 0th index.
  /// If setNotNull is false then the values and isNull is undefined.
  void resize(vector_size_t size, bool setNotNull = true) override {
    if (size > BaseVector::length_) {
      BaseVector::resizeIndices(size, 0, &indices_, &rawIndices_);
    }

    BaseVector::resize(size, setNotNull);
  }

  VectorPtr slice(vector_size_t offset, vector_size_t length) const override;

 private:
  // return the dictionary index for the specified vector index.
  inline vector_size_t getDictionaryIndex(vector_size_t idx) const {
    return rawIndices_[idx];
  }

  void setInternalState();

  BufferPtr indices_;
  const vector_size_t* rawIndices_ = nullptr;

  VectorPtr dictionaryValues_;
  // Caches 'dictionaryValues_.get()' if scalar type.
  SimpleVector<T>* scalarDictionaryValues_ = nullptr;
  // Caches 'scalarDictionaryValues_->getRawValues()' if 'dictionaryValues_'
  // is a FlatVector<T>.
  const T* rawDictionaryValues_ = nullptr;
  bool initialized_{false};
};

template <typename T>
using DictionaryVectorPtr = std::shared_ptr<DictionaryVector<T>>;

} // namespace velox
} // namespace facebook

#include "velox/vector/DictionaryVector-inl.h"
