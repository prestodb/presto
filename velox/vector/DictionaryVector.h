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

#include <folly/container/F14Map.h>
#include <memory>
#include <type_traits>

#include "velox/common/base/SimdUtil.h"
#include "velox/vector/LazyVector.h"
#include "velox/vector/SimpleVector.h"
#include "velox/vector/TypeAliases.h"

namespace facebook {
namespace velox {

template <typename T>
class DictionaryVector : public SimpleVector<T> {
 public:
  DictionaryVector(const DictionaryVector&) = delete;
  DictionaryVector& operator=(const DictionaryVector&) = delete;

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

  virtual ~DictionaryVector() override {
    dictionaryValues_->clearContainingLazyAndWrapped();
  }

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

  bool containsNullAt(vector_size_t idx) const override {
    if constexpr (std::is_same_v<T, ComplexType>) {
      if (isNullAt(idx)) {
        return true;
      }

      auto innerIndex = getDictionaryIndex(idx);
      return dictionaryValues_->containsNullAt(innerIndex);
    } else {
      return isNullAt(idx);
    }
  }

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

  inline BufferPtr& indices() {
    return indices_;
  }

  const VectorPtr& valueVector() const override {
    return dictionaryValues_;
  }

  VectorPtr& valueVector() override {
    return dictionaryValues_;
  }

  BufferPtr wrapInfo() const override {
    return indices_;
  }

  BufferPtr mutableIndices(vector_size_t size) {
    BaseVector::resizeIndices(
        BaseVector::length_, size, BaseVector::pool_, indices_, &rawIndices_);
    return indices_;
  }

  uint64_t retainedSize() const override {
    return BaseVector::retainedSize() + dictionaryValues_->retainedSize() +
        indices_->capacity();
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
    dictionaryValues_ = BaseVector::loadedVectorShared(dictionaryValues_);
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
    dictionaryValues_->clearContainingLazyAndWrapped();
    dictionaryValues_ = dictionaryValues;
    initialized_ = false;
    setInternalState();
  }

  /// Resizes the vector to be of size 'size'. If setNotNull is true
  /// the newly added elements point to the value at the 0th index.
  /// If setNotNull is false then the values and isNull is undefined.
  void resize(vector_size_t size, bool setNotNull = true) override {
    if (size > BaseVector::length_) {
      BaseVector::resizeIndices(
          BaseVector::length_,
          size,
          BaseVector::pool(),
          indices_,
          &rawIndices_);
    }

    // TODO Fix the case when base vector is empty.
    // https://github.com/facebookincubator/velox/issues/7828

    BaseVector::resize(size, setNotNull);
  }

  VectorPtr slice(vector_size_t offset, vector_size_t length) const override;

  void validate(const VectorValidateOptions& options) const override;

  VectorPtr copyPreserveEncodings(
      velox::memory::MemoryPool* pool = nullptr) const override {
    auto selfPool = pool ? pool : BaseVector::pool_;
    return std::make_shared<DictionaryVector<T>>(
        selfPool,
        AlignedBuffer::copy(selfPool, BaseVector::nulls_),
        BaseVector::length_,
        dictionaryValues_->copyPreserveEncodings(),
        AlignedBuffer::copy(selfPool, indices_),
        SimpleVector<T>::stats_,
        BaseVector::distinctValueCount_,
        BaseVector::nullCount_,
        SimpleVector<T>::isSorted_,
        BaseVector::representedByteCount_,
        BaseVector::storageByteCount_);
  }

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

  // Indicates whether internal state has been set. Can also be false if there
  // is an unloaded lazy vector under the encoding layers.
  bool initialized_{false};
};

template <typename T>
using DictionaryVectorPtr = std::shared_ptr<DictionaryVector<T>>;

} // namespace velox
} // namespace facebook

#include "velox/vector/DictionaryVector-inl.h"
