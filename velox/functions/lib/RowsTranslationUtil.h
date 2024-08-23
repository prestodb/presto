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

#include "velox/common/base/Nulls.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox::functions {

/// This function returns a SelectivityVector for ARRAY/MAP vectors that selects
/// all rows corresponding to the specified rows. If the base vector was
/// dictionary encoded, an optional rowMapping parameter can be used to pass
/// the dictionary indices. In this case, it is important to ensure that the
/// topLevelRows parameter has already filtered out any null rows added by the
/// dictionary encoding.
template <typename T>
SelectivityVector toElementRows(
    vector_size_t size,
    const SelectivityVector& topLevelNonNullRows,
    const T* arrayBaseVector,
    const vector_size_t* rowMapping = nullptr) {
  VELOX_CHECK(
      arrayBaseVector->encoding() == VectorEncoding::Simple::MAP ||
      arrayBaseVector->encoding() == VectorEncoding::Simple::ARRAY);

  auto rawNulls = arrayBaseVector->rawNulls();
  auto rawSizes = arrayBaseVector->rawSizes();
  auto rawOffsets = arrayBaseVector->rawOffsets();
  const auto sizeRange =
      arrayBaseVector->sizes()->template asRange<vector_size_t>().end();
  const auto offsetRange =
      arrayBaseVector->offsets()->template asRange<vector_size_t>().end();

  SelectivityVector elementRows(size, false);
  topLevelNonNullRows.applyToSelected([&](vector_size_t row) {
    auto index = rowMapping ? rowMapping[row] : row;
    if (rawNulls && bits::isBitNull(rawNulls, index)) {
      return;
    }

    VELOX_DCHECK_LE(index, sizeRange);
    VELOX_DCHECK_LE(index, offsetRange);

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
