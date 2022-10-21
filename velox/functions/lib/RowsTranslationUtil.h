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
#include "velox/common/base/Nulls.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox::functions {

/// Returns SelectivityVector for the nested vector with all rows corresponding
/// to specified top-level rows selected. The optional topLevelRowMapping is
/// used to pass the dictionary indices if the topLevelVector is dictionary
/// encoded.
template <typename T>
SelectivityVector toElementRows(
    vector_size_t size,
    const SelectivityVector& topLevelRows,
    const T* topLevelVector,
    const vector_size_t* topLevelRowMapping = nullptr) {
  auto rawNulls = topLevelVector->rawNulls();
  auto rawSizes = topLevelVector->rawSizes();
  auto rawOffsets = topLevelVector->rawOffsets();

  SelectivityVector elementRows(size, false);
  topLevelRows.applyToSelected([&](vector_size_t row) {
    auto index = topLevelRowMapping ? topLevelRowMapping[row] : row;
    if (rawNulls && bits::isBitNull(rawNulls, index)) {
      return;
    }
    auto size = rawSizes[index];
    auto offset = rawOffsets[index];
    elementRows.setValidRange(offset, offset + size, true);
  });
  elementRows.updateBounds();
  return elementRows;
}

/// Returns a buffer of vector_size_t that represents the mapping from
/// topLevelRows's element rows to its top-level rows. For example, suppose
/// `result` is the returned buffer, result[i] == j means the value at index i
/// in the element vector belongs to row j of the top-level vector. topLevelRows
/// must be non-null rows.
BufferPtr getElementToTopLevelRows(
    vector_size_t numElements,
    const SelectivityVector& topLevelRows,
    const vector_size_t* rawOffsets,
    const vector_size_t* rawSizes,
    const uint64_t* rawNulls,
    memory::MemoryPool* pool);

template <typename T>
BufferPtr getElementToTopLevelRows(
    vector_size_t numElements,
    const SelectivityVector& topLevelRows,
    const T* topLevelVector,
    memory::MemoryPool* pool) {
  auto rawNulls = topLevelVector->rawNulls();
  auto rawSizes = topLevelVector->rawSizes();
  auto rawOffsets = topLevelVector->rawOffsets();

  return getElementToTopLevelRows(
      numElements, topLevelRows, rawOffsets, rawSizes, rawNulls, pool);
}

} // namespace facebook::velox::functions
