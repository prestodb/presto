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
#include "velox/functions/lib/LambdaFunctionUtil.h"

namespace facebook::velox::functions {
namespace {
BufferPtr flattenNulls(
    const SelectivityVector& rows,
    const DecodedVector& decodedVector) {
  if (!decodedVector.mayHaveNulls()) {
    return BufferPtr(nullptr);
  }

  BufferPtr nulls =
      AlignedBuffer::allocate<bool>(rows.end(), decodedVector.base()->pool());
  auto rawNulls = nulls->asMutable<uint64_t>();
  rows.applyToSelected([&](vector_size_t row) {
    bits::setNull(rawNulls, row, decodedVector.isNullAt(row));
  });
  return nulls;
}

template <typename T>
void flattenBuffers(
    const SelectivityVector& rows,
    vector_size_t newNumElements,
    DecodedVector& decodedVector,
    BufferPtr& newNulls,
    BufferPtr& elementIndices,
    BufferPtr& newSizes,
    BufferPtr& newOffsets) {
  auto pool = decodedVector.base()->pool();

  newNulls = flattenNulls(rows, decodedVector);
  uint64_t* rawNewNulls = newNulls ? newNulls->asMutable<uint64_t>() : nullptr;

  elementIndices = allocateIndices(newNumElements, pool);
  auto rawElementIndices = elementIndices->asMutable<vector_size_t>();
  newSizes = allocateSizes(rows.end(), pool);
  auto rawNewSizes = newSizes->asMutable<vector_size_t>();
  newOffsets = allocateOffsets(rows.end(), pool);
  auto rawNewOffsets = newOffsets->asMutable<vector_size_t>();

  auto indices = decodedVector.indices();
  auto vector = decodedVector.base()->as<T>();
  auto rawSizes = vector->rawSizes();
  auto rawOffsets = vector->rawOffsets();

  vector_size_t elementIndex = 0;
  rows.applyToSelected([&](vector_size_t row) {
    if (rawNewNulls && bits::isBitNull(rawNewNulls, row)) {
      return;
    }
    auto size = rawSizes[indices[row]];
    auto offset = rawOffsets[indices[row]];
    rawNewSizes[row] = size;
    rawNewOffsets[row] = elementIndex;

    for (auto i = 0; i < size; i++) {
      rawElementIndices[elementIndex++] = offset + i;
    }
  });
}
} // namespace

ArrayVectorPtr flattenArray(
    const SelectivityVector& rows,
    const VectorPtr& vector,
    DecodedVector& decodedVector) {
  if (decodedVector.isIdentityMapping()) {
    return std::dynamic_pointer_cast<ArrayVector>(vector);
  }

  auto newNumElements = countElements<ArrayVector>(rows, decodedVector);

  BufferPtr newNulls;
  BufferPtr elementIndices;
  BufferPtr newSizes;
  BufferPtr newOffsets;
  flattenBuffers<ArrayVector>(
      rows,
      newNumElements,
      decodedVector,
      newNulls,
      elementIndices,
      newSizes,
      newOffsets);

  auto array = decodedVector.base()->as<ArrayVector>();
  return std::make_shared<ArrayVector>(
      array->pool(),
      array->type(),
      newNulls,
      rows.end(),
      newOffsets,
      newSizes,
      BaseVector::wrapInDictionary(
          BufferPtr(nullptr),
          elementIndices,
          newNumElements,
          array->elements()));
}

MapVectorPtr flattenMap(
    const SelectivityVector& rows,
    const VectorPtr& vector,
    DecodedVector& decodedVector) {
  if (decodedVector.isIdentityMapping()) {
    return std::dynamic_pointer_cast<MapVector>(vector);
  }

  auto newNumElements = countElements<MapVector>(rows, decodedVector);

  BufferPtr newNulls;
  BufferPtr elementIndices;
  BufferPtr newSizes;
  BufferPtr newOffsets;
  flattenBuffers<MapVector>(
      rows,
      newNumElements,
      decodedVector,
      newNulls,
      elementIndices,
      newSizes,
      newOffsets);

  auto map = decodedVector.base()->as<MapVector>();
  return std::make_shared<MapVector>(
      map->pool(),
      map->type(),
      newNulls,
      rows.end(),
      newOffsets,
      newSizes,
      BaseVector::wrapInDictionary(
          BufferPtr(nullptr), elementIndices, newNumElements, map->mapKeys()),
      BaseVector::wrapInDictionary(
          BufferPtr(nullptr),
          elementIndices,
          newNumElements,
          map->mapValues()));
}

BufferPtr addNullsForUnselectedRows(
    const VectorPtr& vector,
    const SelectivityVector& rows) {
  // Set nulls for rows not present in 'rows'.
  BufferPtr nulls = allocateNulls(rows.size(), vector->pool(), bits::kNull);

  // bits::kNull is 0. Hence, bits::orBits() simply copies the bits from the
  // selectivity vector into the nulls buffer. We cannot use memcpy because it
  // will copy extra bits at the tail if rows.end() is not a multiple of 8.
  bits::orBits(
      nulls->asMutable<uint64_t>(),
      rows.asRange().bits(),
      rows.begin(),
      rows.end());

  if (vector->nulls() != nullptr) {
    // Transfer original nulls
    bits::andBits(
        nulls->asMutable<uint64_t>(),
        vector->rawNulls(),
        rows.begin(),
        rows.end());
  }
  return nulls;
}
} // namespace facebook::velox::functions
