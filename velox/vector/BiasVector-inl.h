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

#include <type_traits>

#include "velox/common/base/Exceptions.h"
#include "velox/vector/BuilderTypeUtils.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/TypeAliases.h"

namespace facebook {
namespace velox {

/*
 * NOTE - biased vector is stored solely as a standard numeric flat array in
 * 2 buffers.
 *
 * Buffer order: [nullData, valueData]
 */
template <typename T>
BiasVector<T>::BiasVector(
    velox::memory::MemoryPool* pool,
    BufferPtr nulls,
    size_t length,
    TypeKind valueType,
    BufferPtr values,
    T bias,
    const SimpleVectorStats<T>& stats,
    std::optional<vector_size_t> distinctCount,
    std::optional<vector_size_t> nullCount,
    std::optional<bool> sorted,
    std::optional<ByteCount> representedBytes,
    std::optional<ByteCount> storageByteCount)
    : SimpleVector<T>(
          pool,
          VectorEncoding::Simple::BIASED,
          nulls,
          length,
          stats,
          distinctCount,
          nullCount,
          sorted,
          representedBytes,
          storageByteCount),
      valueType_(valueType),
      values_(std::move(values)),
      bias_(bias) {
  VELOX_CHECK(
      valueType_ == TypeKind::INTEGER || valueType_ == TypeKind::SMALLINT ||
          valueType_ == TypeKind::TINYINT,
      "Invalid array type for biased values");

  biasBuffer_ = simd::setAll(bias_);
  rawValues_ = values_->as<uint8_t>();
  BaseVector::inMemoryBytes_ += values_->size();
}

template <typename T>

std::unique_ptr<SimpleVector<uint64_t>> BiasVector<T>::hashAll() const {
  // TODO T70734527 dealing with zero length vector
  if (BaseVector::length_ == 0) {
    return nullptr;
  }
  // If there is at least one value, then indices_ is set and has a pool.
  BufferPtr hashes =
      AlignedBuffer::allocate<uint64_t>(BaseVector::length_, values_->pool());
  uint64_t* rawHashes = hashes->asMutable<uint64_t>();
  for (size_t i = 0; i < BaseVector::length_; ++i) {
    rawHashes[i] = this->hashValueAt(i);
  }
  return std::make_unique<FlatVector<uint64_t>>(
      BaseVector::pool_,
      BufferPtr(nullptr),
      BaseVector::length_,
      hashes,
      std::vector<BufferPtr>(0) /*stringBuffers*/,
      SimpleVectorStats<uint64_t>{},
      std::nullopt /*distinctValueCount*/,
      0 /* nullCount */,
      false /*isSorted*/,
      sizeof(uint64_t) * BaseVector::length_ /*representedBytes*/);
}

template <typename T>
const T BiasVector<T>::valueAtFast(vector_size_t idx) const {
  switch (valueType_) {
    case TypeKind::INTEGER:
      return bias_ + reinterpret_cast<const int32_t*>(rawValues_)[idx];
    case TypeKind::SMALLINT:
      return bias_ + reinterpret_cast<const int16_t*>(rawValues_)[idx];
    case TypeKind::TINYINT:
      return bias_ + reinterpret_cast<const int8_t*>(rawValues_)[idx];
    default:
      VELOX_UNSUPPORTED("Invalid type");
  }
}

template <typename T>
xsimd::batch<T> BiasVector<T>::loadSIMDValueBufferAt(size_t index) const {
  if constexpr (std::is_same<T, int64_t>::value) {
    switch (valueType_) {
      case TypeKind::INTEGER:
        return biasBuffer_ + loadSIMDInternal<int32_t>(index);
      case TypeKind::SMALLINT:
        return biasBuffer_ + loadSIMDInternal<int16_t>(index);
      case TypeKind::TINYINT:
        return biasBuffer_ + loadSIMDInternal<int8_t>(index);
      default:
        VELOX_UNSUPPORTED("Invalid type");
    }
  } else if constexpr (std::is_same<T, int32_t>::value) {
    switch (valueType_) {
      case TypeKind::SMALLINT:
        return biasBuffer_ + loadSIMDInternal<int16_t>(index);
      case TypeKind::TINYINT:
        return biasBuffer_ + loadSIMDInternal<int8_t>(index);
      default:
        VELOX_UNSUPPORTED("Invalid type");
    }
  } else if constexpr (std::is_same<T, int16_t>::value) {
    switch (valueType_) {
      case TypeKind::TINYINT:
        return biasBuffer_ + loadSIMDInternal<int8_t>(index);
      default:
        VELOX_UNSUPPORTED("Invalid type");
    }
  } else {
    VELOX_UNSUPPORTED("Unsupported type for biased vector");
  }
}

} // namespace velox
} // namespace facebook
