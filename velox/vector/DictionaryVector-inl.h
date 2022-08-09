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

#include "velox/common/base/Exceptions.h"
#include "velox/vector/BuilderTypeUtils.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/TypeAliases.h"

namespace facebook {
namespace velox {

template <typename T>
void DictionaryVector<T>::setInternalState() {
  rawIndices_ = indices_->as<vector_size_t>();

  // Sanity check indices for non-null positions. Enabled in debug mode only to
  // avoid performance hit in production.
#ifndef NDEBUG
  for (auto i = 0; i < BaseVector::length_; ++i) {
    const bool isNull =
        BaseVector::rawNulls_ && bits::isBitNull(BaseVector::rawNulls_, i);
    if (isNull) {
      continue;
    }

    // Verify index for a non-null position. It must be >= 0 and < size of the
    // base vector.
    VELOX_DCHECK_GE(
        rawIndices_[i],
        0,
        "Dictionary index must be greater than zero. Index: {}.",
        i);
    VELOX_DCHECK_LT(
        rawIndices_[i],
        dictionaryValues_->size(),
        "Dictionary index must be less than base vector's size. Index: {}.",
        i);
  }
#endif

  if (isLazyNotLoaded(*dictionaryValues_)) {
    // Do not load Lazy vector
    return;
  }

  if (dictionaryValues_->isScalar()) {
    scalarDictionaryValues_ =
        reinterpret_cast<SimpleVector<T>*>(dictionaryValues_->loadedVector());
    if (scalarDictionaryValues_->isFlatEncoding() &&
        !std::is_same<T, bool>::value) {
      rawDictionaryValues_ =
          reinterpret_cast<FlatVector<T>*>(scalarDictionaryValues_)
              ->rawValues();
    }
  }
  initialized_ = true;

  BaseVector::inMemoryBytes_ =
      BaseVector::nulls_ ? BaseVector::nulls_->capacity() : 0;
  BaseVector::inMemoryBytes_ += indices_->capacity();
  BaseVector::inMemoryBytes_ += dictionaryValues_->inMemoryBytes();
}

template <typename T>
DictionaryVector<T>::DictionaryVector(
    velox::memory::MemoryPool* pool,
    BufferPtr nulls,
    size_t length,
    std::shared_ptr<BaseVector> dictionaryValues,
    BufferPtr dictionaryIndices,
    const SimpleVectorStats<T>& stats,
    std::optional<vector_size_t> distinctValueCount,
    std::optional<vector_size_t> nullCount,
    std::optional<bool> isSorted,
    std::optional<ByteCount> representedBytes,
    std::optional<ByteCount> storageByteCount)
    : SimpleVector<T>(
          pool,
          dictionaryValues->type(),
          VectorEncoding::Simple::DICTIONARY,
          nulls,
          length,
          stats,
          distinctValueCount,
          nullCount,
          isSorted,
          representedBytes,
          storageByteCount) {
  VELOX_CHECK(dictionaryValues != nullptr, "dictionaryValues must not be null");
  VELOX_CHECK(
      dictionaryIndices != nullptr, "dictionaryIndices must not be null");
  VELOX_CHECK_GE(
      dictionaryIndices->size(),
      length * sizeof(vector_size_t),
      "Malformed dictionary, index array is shorter than DictionaryVector");
  dictionaryValues_ = dictionaryValues;
  indices_ = dictionaryIndices;
  setInternalState();
}

template <typename T>
bool DictionaryVector<T>::isNullAt(vector_size_t idx) const {
  VELOX_DCHECK(initialized_);
  if (BaseVector::isNullAt(idx)) {
    return true;
  }
  auto innerIndex = getDictionaryIndex(idx);
  return dictionaryValues_->isNullAt(innerIndex);
}

template <typename T>
const T DictionaryVector<T>::valueAtFast(vector_size_t idx) const {
  VELOX_DCHECK(initialized_);
  if (rawDictionaryValues_) {
    return rawDictionaryValues_[getDictionaryIndex(idx)];
  }
  return scalarDictionaryValues_->valueAt(getDictionaryIndex(idx));
}

template <>
inline const bool DictionaryVector<bool>::valueAtFast(vector_size_t idx) const {
  VELOX_DCHECK(initialized_);
  auto innerIndex = getDictionaryIndex(idx);
  return scalarDictionaryValues_->valueAt(innerIndex);
}

template <typename T>
std::unique_ptr<SimpleVector<uint64_t>> DictionaryVector<T>::hashAll() const {
  // TODO (T58177479) - optimize to reuse the index vector allowing us to only
  // worry about hashing the dictionary values themselves.  Challenge is that
  // the null information is a part of the index array and so does not have a
  // "null" representation in the dictionary.
  // TODO T70734527 dealing with zero length vector
  if (BaseVector::length_ == 0) {
    return nullptr;
  }
  // If there is at least one value, then indices_ is set and has a pool.
  BufferPtr hashes =
      AlignedBuffer::allocate<uint64_t>(BaseVector::length_, indices_->pool());
  uint64_t* rawHashes = hashes->asMutable<uint64_t>();
  for (vector_size_t i = 0; i < BaseVector::length_; ++i) {
    if (BaseVector::isNullAt(i)) {
      rawHashes[i] = BaseVector::kNullHash;
    } else {
      rawHashes[i] = dictionaryValues_->hashValueAt(rawIndices_[i]);
    }
  }
  return std::make_unique<FlatVector<uint64_t>>(
      BaseVector::pool_,
      BufferPtr(nullptr),
      BaseVector::length_,
      hashes,
      std::vector<BufferPtr>(0) /* stringBuffers */,
      SimpleVectorStats<uint64_t>{},
      BaseVector::distinctValueCount_.value() +
          (BaseVector::nullCount_.value_or(0) > 0 ? 1 : 0),
      0 /* nullCount */,
      false /* sorted */,
      sizeof(uint64_t) * BaseVector::length_ /* representedBytes */);
}

template <typename T>
xsimd::batch<T> DictionaryVector<T>::loadSIMDValueBufferAt(
    size_t byteOffset) const {
  if constexpr (can_simd) {
    constexpr int N = xsimd::batch<T>::size;
    alignas(xsimd::default_arch::alignment()) T tmp[N];
    auto startIndex = byteOffset / sizeof(T);
    for (int i = 0; i < N; ++i) {
      tmp[i] = valueAtFast(startIndex + i);
    }
    return xsimd::load_aligned(tmp);
  } else {
    VELOX_UNREACHABLE();
  }
}

} // namespace velox
} // namespace facebook
