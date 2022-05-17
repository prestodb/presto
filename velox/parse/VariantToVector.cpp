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

#include "velox/parse/VariantToVector.h"
#include "velox/buffer/StringViewBufferHolder.h"
#include "velox/type/Variant.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::core {
namespace {

template <TypeKind KIND>
ArrayVectorPtr variantArrayToVectorImpl(
    const std::vector<variant>& variantArray,
    velox::memory::MemoryPool* pool) {
  using T = typename TypeTraits<KIND>::NativeType;

  // First generate internal arrayVector elements.
  const size_t variantArraySize = variantArray.size();

  // Allocate buffer and set all values to null by default.
  BufferPtr arrayElementsBuffer =
      AlignedBuffer::allocate<T>(variantArraySize, pool);
  BufferPtr nulls =
      AlignedBuffer::allocate<bool>(variantArraySize, pool, bits::kNullByte);

  // Create array elements internal flat vector.
  auto arrayElements = std::make_shared<FlatVector<T>>(
      pool,
      Type::create<KIND>(),
      nulls,
      variantArraySize,
      std::move(arrayElementsBuffer),
      std::vector<BufferPtr>());

  // Populate internal array elements (flat vector).
  for (vector_size_t i = 0; i < variantArraySize; i++) {
    if (!variantArray[i].isNull()) {
      // `getOwnedValue` copies the content to its internal buffers (in case of
      // string/StringView); no-op for other primitive types.
      arrayElements->set(i, T(variantArray[i].value<KIND>()));
    }
  }

  // Create ArrayVector around the FlatVector containing array elements.
  BufferPtr offsets = AlignedBuffer::allocate<vector_size_t>(1, pool, 0);
  BufferPtr sizes = AlignedBuffer::allocate<vector_size_t>(1, pool, 0);

  auto rawSizes = sizes->asMutable<vector_size_t>();
  rawSizes[0] = variantArraySize;

  return std::make_shared<ArrayVector>(
      pool,
      ARRAY(Type::create<KIND>()),
      BufferPtr(nullptr),
      1,
      offsets,
      sizes,
      arrayElements,
      0);
}
} // namespace

ArrayVectorPtr variantArrayToVector(
    const std::vector<variant>& variantArray,
    velox::memory::MemoryPool* pool) {
  if (variantArray.empty()) {
    return variantArrayToVectorImpl<TypeKind::UNKNOWN>(variantArray, pool);
  }

  const auto elementType = variantArray.front().inferType();
  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      variantArrayToVectorImpl, elementType->kind(), variantArray, pool);
}

} // namespace facebook::velox::core
