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
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FunctionVector.h"

namespace facebook::velox::functions {

// Returns the total number of nested elements for the specified top-levels rows
// in array or map vector. T is either ArrayVector or MapVector.
template <typename T>
vector_size_t countElements(
    const SelectivityVector& rows,
    DecodedVector& decodedVector) {
  auto indices = decodedVector.indices();
  auto rawSizes = decodedVector.base()->as<T>()->rawSizes();

  vector_size_t count = 0;
  rows.applyToSelected([&](vector_size_t row) {
    if (decodedVector.isNullAt(row)) {
      return;
    }
    count += rawSizes[indices[row]];
  });
  return count;
}

// Returns an array of indices that allows aligning captures with the nested
// elements of an array or vector. For each top-level row, the index equal to
// the row number is repeated for each of the nested rows.
template <typename T>
BufferPtr toWrapCapture(
    vector_size_t size,
    const Callable* callable,
    const SelectivityVector& topLevelRows,
    const std::shared_ptr<T>& topLevelVector) {
  if (!callable->hasCapture()) {
    return nullptr;
  }

  auto rawNulls = topLevelVector->rawNulls();
  auto rawSizes = topLevelVector->rawSizes();
  auto rawOffsets = topLevelVector->rawOffsets();

  BufferPtr wrapCapture = allocateIndices(size, topLevelVector->pool());
  auto rawWrapCapture = wrapCapture->asMutable<vector_size_t>();
  topLevelRows.applyToSelected([&](vector_size_t row) {
    if (rawNulls && bits::isBitNull(rawNulls, row)) {
      return;
    }
    auto size = rawSizes[row];
    auto offset = rawOffsets[row];
    for (auto i = 0; i < size; ++i) {
      rawWrapCapture[offset + i] = row;
    }
  });
  return wrapCapture;
}

// Given possibly wrapped array vector, flattens the wrappings and returns a
// flat array vector. Returns the original vector unmodified if the vector is
// not wrapped. Flattening is shallow, e.g. elements vector may still be
// wrapped.
ArrayVectorPtr flattenArray(
    const SelectivityVector& rows,
    const VectorPtr& vector,
    DecodedVector& decodedVector);

// Given possibly wrapped map vector, flattens the wrappings and returns a flat
// map vector. Returns the original vector unmodified if the vector is not
// wrapped. Flattening is shallow, e.g. keys and values vectors may still be
// wrapped.
MapVectorPtr flattenMap(
    const SelectivityVector& rows,
    const VectorPtr& vector,
    DecodedVector& decodedVector);
} // namespace facebook::velox::functions
